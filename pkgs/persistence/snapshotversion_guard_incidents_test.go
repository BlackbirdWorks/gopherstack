package persistence_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSnapshotVersionGuard_HistoricalIncidents reconstructs the two real
// data-loss incidents behind gopherstack-5i6p (apigateway, reverted in
// cb188a8a7; cloudfront, reverted before commit in the gopherstack-4ara
// session) plus the legitimate rds-style bump (gopherstack-i101) through the
// real AST scanner, not synthetic snapshotInfo values, so a regression in the
// scanner itself -- not just in diffSnapshots -- would be caught here too.
func TestSnapshotVersionGuard_HistoricalIncidents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		before  string
		after   string
		wantErr string
	}{
		{
			// apigateway: Tags landed on the nested stageSnapshot, not on the
			// version-carrying backendSnapshot. A scanner limited to the
			// latter sees an unchanged field list and only soft-warns.
			name:    "nested struct field addition with version bump",
			before:  fixtureBefore,
			after:   fixtureAfterNestedAddition,
			wantErr: "PURELY ADDITIVE",
		},
		{
			// cloudfront: KeyValueStoreData/KeyValueDataETags landed directly
			// on backendSnapshot, so even the pre-fix scanner saw them --
			// this is a non-regression check, not a fix demonstration.
			name:    "top-level struct field addition with version bump",
			before:  fixtureBefore,
			after:   fixtureAfterTopLevelAddition,
			wantErr: "PURELY ADDITIVE",
		},
		{
			// rds: InstanceRoles went []string -> map[string]string, a
			// genuine retype a stale snapshot cannot decode. Must NOT be
			// flagged as a purely-additive violation.
			name:    "incompatible retype with version bump",
			before:  fixtureBefore,
			after:   fixtureAfterRetype,
			wantErr: "incompatible struct change",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			before := scanFixture(t, tt.before)
			after := scanFixture(t, tt.after)

			violations := diffSnapshots(
				map[string]snapshotInfo{"fixture": after},
				map[string]snapshotInfo{"fixture": before},
			)

			require.Len(t, violations, 1)
			// "PURELY ADDITIVE" is the exact substring TestSnapshotVersionGuard's
			// -update codepath checks for to refuse writing the golden -- so
			// asserting it here also proves this violation survives -update unconfirmed.
			assert.Contains(t, violations[0], tt.wantErr)
		})
	}
}

func scanFixture(t *testing.T, src string) snapshotInfo {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "persistence.go")
	require.NoError(t, os.WriteFile(path, []byte(src), 0o600))

	info, ok, err := scanPersistenceFile(path)
	require.NoError(t, err)
	require.True(t, ok)

	return info
}

const fixtureBefore = `package fixture

const svcSnapshotVersion = 1

type stageSnapshot struct {
	CanarySettings *CanarySettings ` + "`json:\"canarySettings,omitempty\"`" + `
}

type backendSnapshot struct {
	Account       *Account          ` + "`json:\"account,omitempty\"`" + `
	InstanceRoles []string          ` + "`json:\"instanceRoles\"`" + `
	Tables        map[string]string ` + "`json:\"tables\"`" + `
	Version       int               ` + "`json:\"version\"`" + `
}
`

const fixtureAfterNestedAddition = `package fixture

const svcSnapshotVersion = 2

type stageSnapshot struct {
	Tags           *tags.Tags       ` + "`json:\"tags,omitempty\"`" + `
	CanarySettings *CanarySettings  ` + "`json:\"canarySettings,omitempty\"`" + `
}

type backendSnapshot struct {
	Account       *Account          ` + "`json:\"account,omitempty\"`" + `
	InstanceRoles []string          ` + "`json:\"instanceRoles\"`" + `
	Tables        map[string]string ` + "`json:\"tables\"`" + `
	Version       int               ` + "`json:\"version\"`" + `
}
`

const fixtureAfterTopLevelAddition = `package fixture

const svcSnapshotVersion = 2

type stageSnapshot struct {
	CanarySettings *CanarySettings ` + "`json:\"canarySettings,omitempty\"`" + `
}

type backendSnapshot struct {
	Account           *Account          ` + "`json:\"account,omitempty\"`" + `
	InstanceRoles     []string          ` + "`json:\"instanceRoles\"`" + `
	KeyValueDataETags map[string]string ` + "`json:\"keyValueDataETags,omitempty\"`" + `
	Tables            map[string]string ` + "`json:\"tables\"`" + `
	Version           int               ` + "`json:\"version\"`" + `
}
`

const fixtureAfterRetype = `package fixture

const svcSnapshotVersion = 2

type stageSnapshot struct {
	CanarySettings *CanarySettings ` + "`json:\"canarySettings,omitempty\"`" + `
}

type backendSnapshot struct {
	Account       *Account                     ` + "`json:\"account,omitempty\"`" + `
	InstanceRoles map[string]map[string]string ` + "`json:\"instanceRoles\"`" + `
	Tables        map[string]string            ` + "`json:\"tables\"`" + `
	Version       int                          ` + "`json:\"version\"`" + `
}
`
