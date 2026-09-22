package migrations

import (
	"strings"
	"testing"
)

func TestAllMigrationsEmbedded(t *testing.T) {
	ms, err := All()
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) == 0 || ms[0].Version != "0001_init" {
		t.Fatalf("migrations=%+v", ms)
	}
	for _, m := range ms {
		for _, table := range []string{"legs", "matches", "breaks", "recon_events"} {
			if m.Version == "0001_init" && !strings.Contains(m.SQL, "CREATE TABLE IF NOT EXISTS "+table) {
				t.Fatalf("0001_init missing table %s", table)
			}
		}
	}
}
