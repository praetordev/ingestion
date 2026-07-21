package core

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

// Proves that a failure after host insertion but during group reconciliation
// rolls the entire sync back, including its partial host mutations. CI supplies
// a migrated TEST_DATABASE_URL; local unit-only runs skip this test.
func TestInventorySyncPartialFailureRollsBack(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL required")
	}
	db, err := sqlx.Connect("postgres", dbURL)
	if err != nil {
		t.Skipf("cannot reach TEST_DATABASE_URL: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	suffix := uuid.NewString()
	var orgID, inventoryID, sourceID, jobID int64
	if err := db.QueryRowContext(ctx, `INSERT INTO organizations(name) VALUES($1) RETURNING id`, "sync-history-"+suffix).Scan(&orgID); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, `DELETE FROM organizations WHERE id=$1`, orgID)
	if err := db.QueryRowContext(ctx, `INSERT INTO inventories(organization_id,name,kind) VALUES($1,$2,'standard') RETURNING id`, orgID, "inventory-"+suffix).Scan(&inventoryID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `INSERT INTO inventory_sources(inventory_id,name,source_kind,source) VALUES($1,$2,'inventory','plugin: test') RETURNING id`, inventoryID, "source-"+suffix).Scan(&sourceID); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `INSERT INTO unified_jobs(name,status,job_args) VALUES($1,'running',jsonb_build_object('inventory_source_id',$2::bigint)) RETURNING id`, "sync-"+suffix, sourceID).Scan(&jobID); err != nil {
		t.Fatal(err)
	}

	functionName := "fail_sync_group_" + strings.ReplaceAll(suffix, "-", "")
	triggerName := functionName + "_trigger"
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced group failure'; END $$`, functionName)); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(ctx, fmt.Sprintf(`DROP FUNCTION IF EXISTS %s() CASCADE`, functionName))
	if _, err := db.ExecContext(ctx, fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON groups FOR EACH ROW EXECUTE FUNCTION %s()`, triggerName, functionName)); err != nil {
		t.Fatal(err)
	}

	runID := uuid.New()
	payload := []byte(`{"_meta":{"hostvars":{"host-a":{"region":"eu"}}},"web":{"hosts":["host-a"]}}`)
	service := NewIngestionService(db, nil, nil)
	if err := service.UpsertInventory(ctx, InventorySyncContext{InventoryID: inventoryID, UnifiedJobID: jobID, RunID: runID}, payload); err == nil {
		t.Fatal("sync unexpectedly succeeded")
	}
	var count int
	if err := db.GetContext(ctx, &count, `SELECT count(*) FROM hosts WHERE inventory_id=$1`, inventoryID); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("partial host mutations survived rollback: %d", count)
	}
	var status, phase string
	if err := db.QueryRowContext(ctx, `SELECT status,phase FROM inventory_sync_history WHERE unified_job_id=$1`, jobID).Scan(&status, &phase); err != nil {
		t.Fatal(err)
	}
	if status != "failed" || phase != "reconciliation" {
		t.Fatalf("history = %s/%s, want failed/reconciliation", status, phase)
	}
}
