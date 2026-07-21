package core

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/praetordev/plog"
	"io"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/praetordev/credentials"
	"github.com/praetordev/events"
	"github.com/praetordev/ingestion/inventoryrender"
	"github.com/praetordev/objectstore"
)

// logger is the ingestion package component logger (handler installed by pkg/plog).
var logger = plog.New("ingestion")

type EventPublisher interface {
	PublishJobEvent(event *events.JobEvent) error
	PublishLogChunk(chunk *events.LogChunk) error
}

type IngestionService struct {
	DB        *sqlx.DB
	Publisher EventPublisher
	Store     objectstore.LogStore
}

type InventorySyncContext struct {
	InventoryID  int64
	UnifiedJobID int64
	RunID        uuid.UUID
}

type InventorySyncDelta struct {
	HostsAdded      int `json:"hosts_added"`
	HostsUpdated    int `json:"hosts_updated"`
	HostsDisabled   int `json:"hosts_disabled"`
	HostsUnchanged  int `json:"hosts_unchanged"`
	GroupsAdded     int `json:"groups_added"`
	GroupsUpdated   int `json:"groups_updated"`
	GroupsUnchanged int `json:"groups_unchanged"`
}

func NewIngestionService(db *sqlx.DB, pub EventPublisher, store objectstore.LogStore) *IngestionService {
	return &IngestionService{
		DB:        db,
		Publisher: pub,
		Store:     store,
	}
}

// RecordHeartbeat stamps a run's liveness. The reconciler reads
// last_heartbeat_at to distinguish a live long-running job from a lost one. A
// truly terminal run is left untouched (a late heartbeat can't revive it), but
// a provisionally-failed run whose host is demonstrably alive again should be
// revived to 'running' so the control plane reflects reality until the eventual
// terminal event finalizes it. Both provisional states qualify: 'lost' (host
// rebooted and resumed) and 'reconciling' (a transient blip moved it there, but
// the host is still heartbeating) — otherwise a reconciling run would sit stale
// until the reconciler next SSHes in, even though it's plainly alive.
// ResolveRunCredentials decrypts and returns the AWX-style injectors for the
// Machine credential the scheduler snapshotted onto this run (execution_runs.
// credential_id). Resolution is strictly run-scoped: a caller can only obtain the
// credential that run was dispatched with, never an arbitrary one, and only while
// the run is still live (not terminal). The plaintext is returned for the
// executor's in-memory use — it is never persisted here or logged.
func (s *IngestionService) ResolveRunCredentials(ctx context.Context, runID uuid.UUID) (env, files map[string]string, err error) {
	var row struct {
		CredentialID *int64 `db:"credential_id"`
		State        string `db:"state"`
	}
	if e := s.DB.GetContext(ctx, &row,
		`SELECT credential_id, state FROM execution_runs WHERE id = $1`, runID); e != nil {
		if errors.Is(e, sql.ErrNoRows) {
			return nil, nil, fmt.Errorf("run not found")
		}
		return nil, nil, e
	}
	switch row.State {
	case "successful", "failed", "canceled", "lost":
		return nil, nil, fmt.Errorf("run is not live (%s)", row.State)
	}
	if row.CredentialID == nil {
		return nil, nil, fmt.Errorf("run has no credential")
	}
	return credentials.ResolveInjectors(ctx, s.DB, *row.CredentialID)
}

// IsRunnable reports whether a run may still be bootstrapped/executed — it exists
// and has not reached a terminal or reconciler-owned state. The executor calls
// this before bootstrapping so a launch that was reaped (queued-timeout) or
// canceled while sitting in the work queue is not run as a ghost after the fact.
// A missing run is treated as not runnable.
func (s *IngestionService) IsRunnable(ctx context.Context, runID uuid.UUID) (bool, error) {
	var state string
	if err := s.DB.GetContext(ctx, &state,
		`SELECT state FROM execution_runs WHERE id = $1`, runID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	switch state {
	case "successful", "failed", "canceled", "lost":
		return false, nil
	}
	return true, nil
}

func (s *IngestionService) RecordHeartbeat(ctx context.Context, runID uuid.UUID) (bool, error) {
	_, err := s.DB.ExecContext(ctx, `
		UPDATE execution_runs
		SET last_heartbeat_at = now(),
		    state = CASE WHEN state IN ('lost', 'reconciling') THEN 'running' ELSE state END,
		    finished_at = CASE WHEN state IN ('lost', 'reconciling') THEN NULL ELSE finished_at END
		WHERE id = $1 AND NOT run_is_terminal(state)`, runID)
	if err != nil {
		return false, fmt.Errorf("record heartbeat: %w", err)
	}
	// Report back whether the operator asked to cancel this run's job, so the
	// host-runner can stop the play cooperatively (it has no other channel).
	var cancel bool
	if qerr := s.DB.GetContext(ctx, &cancel, `
		SELECT uj.cancel_requested FROM unified_jobs uj
		JOIN execution_runs er ON er.unified_job_id = uj.id WHERE er.id = $1`, runID); qerr != nil {
		return false, nil // best-effort: a lookup failure must not fail the heartbeat
	}
	return cancel, nil
}

// StoreFacts upserts the facts a run gathered, keyed by host. Each entry's host
// name is resolved to a host_id within the run's inventory; names that don't map
// to a host in that inventory are ignored.
func (s *IngestionService) StoreFacts(ctx context.Context, runID uuid.UUID, facts map[string]json.RawMessage) error {
	if len(facts) == 0 {
		return nil
	}
	var inventoryID *int64
	err := s.DB.GetContext(ctx, &inventoryID, `
		SELECT jt.inventory_id
		FROM execution_runs er
		JOIN unified_jobs uj ON uj.id = er.unified_job_id
		JOIN job_templates jt ON jt.unified_job_template_id = uj.unified_job_template_id
		WHERE er.id = $1`, runID)
	if err != nil || inventoryID == nil {
		return nil // no inventory => nowhere to attach facts
	}

	for host, f := range facts {
		if _, err := s.DB.ExecContext(ctx, `
			INSERT INTO host_facts (host_id, facts, modified_at)
			SELECT h.id, $3::jsonb, now() FROM hosts h
			WHERE h.inventory_id = $1 AND h.name = $2
			ON CONFLICT (host_id) DO UPDATE SET facts = EXCLUDED.facts, modified_at = now()`,
			*inventoryID, host, []byte(f)); err != nil {
			logger.Error("facts upsert for host failed", "host", host, "err", err)
		}
	}
	return nil
}

// UpsertInventory parses `ansible-inventory --list` JSON and upserts its hosts,
// groups, and memberships into the given inventory (idempotent, so re-syncing
// updates in place). Host names that already exist keep their id; new ones are
// inserted. Variables come from _meta.hostvars.
func (s *IngestionService) UpsertInventory(ctx context.Context, syncCtx InventorySyncContext, data []byte) error {
	hostvars, allHosts, groups, err := decodeInventorySync(data)
	if err != nil {
		s.failInventorySync(ctx, syncCtx, "parsing", "invalid_inventory_payload", err)
		return err
	}
	if err := validateInventorySync(allHosts, groups); err != nil {
		s.failInventorySync(ctx, syncCtx, "validation", "invalid_inventory_model", err)
		return err
	}
	if _, err := s.DB.ExecContext(ctx, `UPDATE inventory_sync_history SET phase='reconciliation', status='running', execution_run_id=$2, started_at=COALESCE(started_at,now()), modified_at=now() WHERE unified_job_id=$1 AND inventory_id=$3`, syncCtx.UnifiedJobID, syncCtx.RunID, syncCtx.InventoryID); err != nil {
		return fmt.Errorf("start sync history: %w", err)
	}

	tx, err := s.DB.BeginTxx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin inventory reconciliation: %w", err)
	}
	defer tx.Rollback()
	delta, err := reconcileInventory(ctx, tx, syncCtx.InventoryID, syncCtx.UnifiedJobID, hostvars, allHosts, groups)
	if err != nil {
		// Release the history row lock before recording the durable failure in a
		// separate transaction; otherwise this connection waits on its own tx.
		_ = tx.Rollback()
		s.failInventorySync(ctx, syncCtx, "reconciliation", "reconciliation_failed", err)
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE inventory_sync_history SET phase='completed', status='successful', execution_run_id=$2,
		 hosts_added=$3, hosts_updated=$4, hosts_disabled=$5, hosts_unchanged=$6,
		 groups_added=$7, groups_updated=$8, groups_unchanged=$9,
		 diagnostic_code=NULL, diagnostic_message=NULL, diagnostic_details='{}'::jsonb,
		 finished_at=now(), modified_at=now()
		 WHERE unified_job_id=$1`, syncCtx.UnifiedJobID, syncCtx.RunID,
		delta.HostsAdded, delta.HostsUpdated, delta.HostsDisabled, delta.HostsUnchanged,
		delta.GroupsAdded, delta.GroupsUpdated, delta.GroupsUnchanged); err != nil {
		return fmt.Errorf("complete sync history: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE inventory_sources SET last_synced_at=now() WHERE id=(SELECT inventory_source_id FROM inventory_sync_history WHERE unified_job_id=$1)`, syncCtx.UnifiedJobID); err != nil {
		return fmt.Errorf("mark inventory source synced: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit inventory reconciliation: %w", err)
	}
	logger.Info("inventory synced", "inventory_id", syncCtx.InventoryID, "delta", delta)
	return nil
}

func validateInventorySync(hosts map[string]bool, groups map[string][]string) error {
	for host := range hosts {
		if strings.TrimSpace(host) == "" || strings.ContainsAny(host, "\x00\r\n") {
			return fmt.Errorf("inventory contains an invalid host name")
		}
	}
	for group, members := range groups {
		if strings.TrimSpace(group) == "" || strings.ContainsAny(group, "\x00\r\n") {
			return fmt.Errorf("inventory contains an invalid group name")
		}
		for _, host := range members {
			if !hosts[host] {
				return fmt.Errorf("group %q references unknown host %q", group, host)
			}
		}
	}
	return nil
}

type existingHost struct {
	ID        int64           `db:"id"`
	Name      string          `db:"name"`
	Variables json.RawMessage `db:"variables"`
	Enabled   bool            `db:"enabled"`
}

func canonicalJSON(raw json.RawMessage) string {
	if len(raw) == 0 {
		return "{}"
	}
	var value any
	if json.Unmarshal(raw, &value) != nil {
		return string(raw)
	}
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func classifyPresentHost(old existingHost, found bool, vars json.RawMessage) string {
	if !found {
		return "added"
	}
	if canonicalJSON(old.Variables) == canonicalJSON(vars) && old.Enabled {
		return "unchanged"
	}
	return "updated"
}

func shouldDisableMissingHost(policy string, old existingHost, present bool) bool {
	return policy == "disable_missing" && !present && old.Enabled
}

func reconcileInventory(ctx context.Context, tx *sqlx.Tx, inventoryID, unifiedJobID int64, hostvars map[string]json.RawMessage, allHosts map[string]bool, groups map[string][]string) (InventorySyncDelta, error) {
	var delta InventorySyncDelta
	var source struct {
		ID     int64  `db:"id"`
		Policy string `db:"reconciliation_policy"`
	}
	if err := tx.GetContext(ctx, &source, `SELECT inventory_source_id AS id, reconciliation_policy FROM inventory_sync_history WHERE unified_job_id=$1 AND inventory_id=$2 FOR UPDATE`, unifiedJobID, inventoryID); err != nil {
		return delta, fmt.Errorf("resolve sync source: %w", err)
	}
	existing := []existingHost{}
	if err := tx.SelectContext(ctx, &existing, `SELECT id,name,variables,enabled FROM hosts WHERE inventory_id=$1 AND inventory_source_id=$2 FOR UPDATE`, inventoryID, source.ID); err != nil {
		return delta, fmt.Errorf("lock existing hosts: %w", err)
	}
	byName := make(map[string]existingHost, len(existing))
	for _, host := range existing {
		byName[host.Name] = host
	}
	hostID := map[string]int64{}
	for name := range allHosts {
		vars := hostvars[name]
		if len(vars) == 0 {
			vars = json.RawMessage("{}")
		}
		old, found := byName[name]
		mutation := classifyPresentHost(old, found, vars)
		if mutation == "added" {
			var id int64
			if err := tx.QueryRowContext(ctx, `INSERT INTO hosts (inventory_id,inventory_source_id,name,variables,enabled) VALUES ($1,$2,$3,$4::jsonb,true) RETURNING id`, inventoryID, source.ID, name, []byte(vars)).Scan(&id); err != nil {
				return delta, fmt.Errorf("insert host %q: %w", name, err)
			}
			hostID[name] = id
			delta.HostsAdded++
			continue
		}
		hostID[name] = old.ID
		if mutation == "unchanged" {
			delta.HostsUnchanged++
			continue
		}
		if _, err := tx.ExecContext(ctx, `UPDATE hosts SET variables=$2::jsonb,enabled=true,modified_at=now() WHERE id=$1`, old.ID, []byte(vars)); err != nil {
			return delta, fmt.Errorf("update host %q: %w", name, err)
		}
		delta.HostsUpdated++
	}
	if source.Policy == "disable_missing" {
		for _, old := range existing {
			if !shouldDisableMissingHost(source.Policy, old, allHosts[old.Name]) {
				continue
			}
			if _, err := tx.ExecContext(ctx, `UPDATE hosts SET enabled=false,modified_at=now() WHERE id=$1`, old.ID); err != nil {
				return delta, fmt.Errorf("disable missing host %q: %w", old.Name, err)
			}
			delta.HostsDisabled++
		}
	}

	for name, members := range groups {
		var gid int64
		var existed bool
		err := tx.QueryRowContext(ctx, `SELECT id,true FROM groups WHERE inventory_id=$1 AND inventory_source_id=$2 AND name=$3 FOR UPDATE`, inventoryID, source.ID, name).Scan(&gid, &existed)
		if errors.Is(err, sql.ErrNoRows) {
			if err := tx.QueryRowContext(ctx, `INSERT INTO groups (inventory_id,inventory_source_id,name) VALUES ($1,$2,$3) RETURNING id`, inventoryID, source.ID, name).Scan(&gid); err != nil {
				return delta, fmt.Errorf("insert group %q: %w", name, err)
			}
			delta.GroupsAdded++
		} else if err != nil {
			return delta, fmt.Errorf("find group %q: %w", name, err)
		}
		var prior []int64
		if err := tx.SelectContext(ctx, &prior, `SELECT host_id FROM host_groups WHERE group_id=$1 ORDER BY host_id`, gid); err != nil {
			return delta, fmt.Errorf("read group %q membership: %w", name, err)
		}
		wanted := make([]int64, 0, len(members))
		for _, host := range members {
			wanted = append(wanted, hostID[host])
		}
		sort.Slice(wanted, func(i, j int) bool { return wanted[i] < wanted[j] })
		if existed && slices.Equal(prior, wanted) {
			delta.GroupsUnchanged++
			continue
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM host_groups WHERE group_id=$1`, gid); err != nil {
			return delta, fmt.Errorf("reset group %q membership: %w", name, err)
		}
		for _, id := range wanted {
			if _, err := tx.ExecContext(ctx, `INSERT INTO host_groups (host_id,group_id) VALUES ($1,$2)`, id, gid); err != nil {
				return delta, fmt.Errorf("link group %q: %w", name, err)
			}
		}
		if existed {
			delta.GroupsUpdated++
		}
	}
	return delta, nil
}

func (s *IngestionService) failInventorySync(ctx context.Context, syncCtx InventorySyncContext, phase, code string, cause error) {
	message := cause.Error()
	if len(message) > 2000 {
		message = message[:2000]
	}
	if _, err := s.DB.ExecContext(ctx, `UPDATE inventory_sync_history SET phase=$2,status='failed',execution_run_id=$3,diagnostic_code=$4,diagnostic_message=$5,diagnostic_details='{}'::jsonb,started_at=COALESCE(started_at,now()),finished_at=now(),modified_at=now() WHERE unified_job_id=$1 AND inventory_id=$6`, syncCtx.UnifiedJobID, phase, syncCtx.RunID, code, message, syncCtx.InventoryID); err != nil {
		logger.Error("record inventory sync failure", "job_id", syncCtx.UnifiedJobID, "err", err)
	}
}

// RecordInventorySyncFailure persists a bounded executor-side acquisition
// failure. Callers provide a safe summary only; raw provider output and resolved
// credential material must never cross this boundary.
func (s *IngestionService) RecordInventorySyncFailure(ctx context.Context, syncCtx InventorySyncContext, phase, code, message string) {
	if phase != "acquisition" && phase != "validation" {
		phase = "acquisition"
	}
	if strings.TrimSpace(code) == "" {
		code = "inventory_sync_failed"
	}
	s.failInventorySync(ctx, syncCtx, phase, code, errors.New(message))
}

func decodeInventorySync(data []byte) (map[string]json.RawMessage, map[string]bool, map[string][]string, error) {
	var inv map[string]json.RawMessage
	if err := json.Unmarshal(data, &inv); err != nil {
		return nil, nil, nil, fmt.Errorf("parse inventory json: %w", err)
	}

	hostvars := map[string]json.RawMessage{}
	if meta, ok := inv["_meta"]; ok {
		var m struct {
			HostVars map[string]json.RawMessage `json:"hostvars"`
		}
		if err := json.Unmarshal(meta, &m); err != nil {
			return nil, nil, nil, fmt.Errorf("parse inventory _meta: %w", err)
		}
		if m.HostVars != nil {
			hostvars = m.HostVars
		}
	}

	allHosts := map[string]bool{}
	groups := map[string][]string{}
	for key, raw := range inv {
		if key == "_meta" {
			continue
		}
		var group struct {
			Hosts []string `json:"hosts"`
		}
		if err := json.Unmarshal(raw, &group); err != nil {
			return nil, nil, nil, fmt.Errorf("parse inventory group %q: %w", key, err)
		}
		for _, host := range group.Hosts {
			allHosts[host] = true
		}
		if key != "all" && key != "ungrouped" && len(group.Hosts) > 0 {
			groups[key] = group.Hosts
		}
	}
	for host := range hostvars {
		allHosts[host] = true
	}
	return hostvars, allHosts, groups, nil
}

// LatestLogSeq returns the highest stored chunk seq for a run, or -1 if none.
// It lets a reader advance its tail cursor without parsing the streamed bytes.
func (s *IngestionService) LatestLogSeq(ctx context.Context, runID uuid.UUID) (int64, error) {
	var seq int64
	err := s.DB.GetContext(ctx, &seq,
		`SELECT COALESCE(MAX(seq), -1) FROM job_output_chunks WHERE execution_run_id = $1`, runID)
	return seq, err
}

// RenderInventory returns the Ansible INI for an inventory, generated on demand
// from stored hosts/groups. The executor fetches this at dispatch (the manifest
// ships only the inventory id) so a large inventory never bloats the outbox row /
// NATS message (#13).
func (s *IngestionService) RenderInventory(ctx context.Context, inventoryID int64) (string, error) {
	return inventoryrender.Render(ctx, s.DB, inventoryID)
}

// InventoryFacts returns the stored host facts for an inventory, keyed by host
// name. The executor fetches these at dispatch (by reference) for fact-cache jobs
// so the facts don't bloat the outbox/NATS message (#48).
func (s *IngestionService) InventoryFacts(ctx context.Context, inventoryID int64) (map[string]json.RawMessage, error) {
	return inventoryrender.Facts(ctx, s.DB, inventoryID)
}

// LogCursor returns the authoritative resume point for a run's stdout: the total
// bytes already stored and the highest chunk seq (or -1 if none). The host-runner
// calls this after losing its local sync cursor so it can continue appending new
// chunks from the exact stored position, instead of re-reading stdout from offset
// 0 (which, with timing-dependent chunk boundaries, would overwrite some chunks
// and leave stale higher-seq chunks — corrupting the reassembled log, issue #9).
// It mirrors the offset/seq accounting the reconciler already uses to resume.
func (s *IngestionService) LogCursor(ctx context.Context, runID uuid.UUID) (bytes int64, maxSeq int64, err error) {
	err = s.DB.QueryRowxContext(ctx,
		`SELECT COALESCE(SUM(byte_length),0), COALESCE(MAX(seq),-1)
		 FROM job_output_chunks WHERE execution_run_id = $1`, runID).Scan(&bytes, &maxSeq)
	return bytes, maxSeq, err
}

// StreamLogs writes the run's stored output, in chunk order, to w. sinceSeq
// supports incremental tailing: a caller polling for live updates passes the
// highest seq it has already seen, and only later chunks are written.
func (s *IngestionService) StreamLogs(ctx context.Context, runID uuid.UUID, sinceSeq int64, w io.Writer) error {
	if s.Store == nil {
		return fmt.Errorf("log store not configured")
	}

	rows, err := s.DB.QueryxContext(ctx, `
		SELECT storage_key FROM job_output_chunks
		WHERE execution_run_id = $1 AND seq > $2
		ORDER BY seq`, runID, sinceSeq)
	if err != nil {
		return fmt.Errorf("list log chunks: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return err
		}
		data, err := s.Store.Get(key)
		if err != nil {
			return fmt.Errorf("fetch chunk %s: %w", key, err)
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
	}
	return rows.Err()
}

// IngestLogChunk persists a raw stdout chunk to the object store and publishes a
// LogChunk index notification. The bytes are written to durable storage first so
// that, if the index publish fails and the host-runner retries the chunk, the
// re-upload is an idempotent overwrite of the same key and the consumer dedups
// the index row on (execution_run_id, seq).
func (s *IngestionService) IngestLogChunk(ctx context.Context, runID uuid.UUID, seq int64, data []byte) error {
	if s.Store == nil {
		return fmt.Errorf("log store not configured")
	}

	key := objectstore.ChunkKey(runID.String(), seq)
	if err := s.Store.Put(key, data); err != nil {
		return fmt.Errorf("store log chunk: %w", err)
	}

	if err := s.Publisher.PublishLogChunk(&events.LogChunk{
		ExecutionRunID: runID,
		Seq:            seq,
		StorageKey:     key,
		ByteLength:     len(data),
		Timestamp:      time.Now(),
	}); err != nil {
		return fmt.Errorf("publish log chunk: %w", err)
	}
	return nil
}

// IngestEvents publishes a batch of events for a run to the durable event stream.
//
// No Postgres access: the runID is already authenticated (per-run token) and is
// the source of truth for the run. unified_job_id is resolved authoritatively by
// the consumer (the Postgres boundary) at projection time, so ingestion needs no
// DB lookup and keeps accepting events — buffering them in JetStream — even while
// Postgres is down (#16). The on-target WAL plus JetStream, not a DB round-trip,
// are the real durability buffer.
func (s *IngestionService) IngestEvents(ctx context.Context, runID uuid.UUID, eventsList []events.JobEvent) error {
	if len(eventsList) == 0 {
		return nil
	}
	EventsIngested.Add(float64(len(eventsList)))

	for _, event := range eventsList {
		// The run id comes from the authenticated URL, not the event body.
		event.ExecutionRunID = runID
		if event.Timestamp.IsZero() {
			event.Timestamp = time.Now()
		}
		// Ensure EventData is valid JSON for the downstream JSONB column.
		if len(event.EventData) == 0 {
			event.EventData = json.RawMessage("{}")
		}

		if err := s.Publisher.PublishJobEvent(&event); err != nil {
			logger.Error("publish event to NATS failed", "err", err)
			return fmt.Errorf("failed to publish event to NATS: %w", err)
		}
	}

	return nil
}
