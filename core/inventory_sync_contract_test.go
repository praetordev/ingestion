package core

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestDecodeInventorySyncV1(t *testing.T) {
	payload, err := os.ReadFile(filepath.Join("testdata", "inventory_sync.v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	hostvars, hosts, groups, err := decodeInventorySync(payload)
	if err != nil {
		t.Fatalf("decode canonical inventory sync: %v", err)
	}
	if len(hostvars) != 2 || len(hosts) != 2 {
		t.Fatalf("decoded hostvars=%d hosts=%d, want 2 each", len(hostvars), len(hosts))
	}
	if len(groups) != 1 || len(groups["web"]) != 1 || groups["web"][0] != "web-1" {
		t.Fatalf("decoded groups = %#v", groups)
	}
}

func TestInventorySyncHostDeltaClassification(t *testing.T) {
	old := existingHost{Enabled: true, Variables: json.RawMessage(`{"region":"eu"}`)}
	tests := []struct {
		name  string
		old   existingHost
		found bool
		vars  json.RawMessage
		want  string
	}{
		{name: "added", found: false, vars: json.RawMessage(`{}`), want: "added"},
		{name: "unchanged canonical json", old: old, found: true, vars: json.RawMessage(`{"region":"eu"}`), want: "unchanged"},
		{name: "updated variables", old: old, found: true, vars: json.RawMessage(`{"region":"us"}`), want: "updated"},
		{name: "reenabled", old: existingHost{Enabled: false, Variables: json.RawMessage(`{}`)}, found: true, vars: json.RawMessage(`{}`), want: "updated"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyPresentHost(tt.old, tt.found, tt.vars); got != tt.want {
				t.Fatalf("classification = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestInventorySyncReconciliationPolicies(t *testing.T) {
	enabled := existingHost{Enabled: true}
	disabled := existingHost{Enabled: false}
	for _, tt := range []struct {
		name, policy string
		host         existingHost
		present      bool
		want         bool
	}{
		{name: "disable missing", policy: "disable_missing", host: enabled, want: true},
		{name: "retain missing", policy: "retain_missing", host: enabled, want: false},
		{name: "present is retained", policy: "disable_missing", host: enabled, present: true, want: false},
		{name: "already disabled is unchanged", policy: "disable_missing", host: disabled, want: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldDisableMissingHost(tt.policy, tt.host, tt.present); got != tt.want {
				t.Fatalf("disable = %v, want %v", got, tt.want)
			}
		})
	}
}
