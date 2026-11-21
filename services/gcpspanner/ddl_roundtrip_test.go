package gcpspanner_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

func TestDDL_PrintedSchemaRoundTrips(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		batch    []string
		postgres bool
	}{
		{name: "column options", batch: []string{
			"ALTER TABLE t ALTER COLUMN ts SET OPTIONS (allow_commit_timestamp = null)",
			"ALTER TABLE t ALTER COLUMN a SET OPTIONS (locality_group = 'x')",
		}},
		{name: "defaults", batch: []string{
			"ALTER TABLE t ALTER COLUMN a SET DEFAULT ('x')",
			"ALTER TABLE t ALTER COLUMN d SET ON UPDATE (2)",
		}},
		{name: "row deletion policy", batch: []string{
			"ALTER TABLE t ADD ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL 30 DAY))",
			"ALTER TABLE t SET OPTIONS (locality_group = 'g')",
			"ALTER TABLE t REPLACE ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL 5 DAY))",
		}},
		{name: "synonym and rename", batch: []string{"ALTER TABLE t RENAME TO t2, ADD SYNONYM old"}},
		{name: "rename with quoted and similar names", batch: []string{
			"CREATE TABLE tt (id INT64) PRIMARY KEY (id)",
			"CREATE TABLE `x` (id INT64, s STRING(MAX) DEFAULT ('t'), CONSTRAINT fk FOREIGN KEY (id) REFERENCES t (id)) " +
				"PRIMARY KEY (id)",
			"ALTER TABLE t RENAME TO t3",
		}},
		{name: "interleave", batch: []string{"ALTER TABLE c SET INTERLEAVE IN PARENT t ON DELETE CASCADE"}},
		{name: "index", batch: []string{"ALTER INDEX i ADD STORED COLUMN ts", "ALTER INDEX i DROP STORED COLUMN d"}},
		{name: "sequence", batch: []string{"ALTER SEQUENCE sq SET OPTIONS (skip_range_min = 1, skip_range_max = 9)"}},
		{name: "change stream", batch: []string{
			"ALTER CHANGE STREAM cs SET OPTIONS (retention_period = '3d', exclude_insert = true)",
			"ALTER CHANGE STREAM cs SET FOR ALL",
		}},
		{name: "database options", batch: []string{
			"ALTER DATABASE db SET OPTIONS (version_retention_period = '3d', optimizer_version = 2)",
		}},
		{name: "proto bundle", batch: []string{"ALTER PROTO BUNDLE INSERT (c.D)"}},
		{name: "grants", batch: []string{
			"GRANT SELECT, INSERT ON TABLE t TO ROLE r", "GRANT UPDATE(a) ON TABLE t TO ROLE r",
			"GRANT ROLE r TO ROLE r2", "REVOKE INSERT ON TABLE t FROM ROLE r",
			"GRANT SELECT ON TABLE t TO ROLE r, r2", "ALTER TABLE t RENAME TO tt",
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			first, err := gcpspanner.FoldDDL(afBase(), tt.batch, tt.postgres)
			require.NoError(t, err)

			second, err := gcpspanner.FoldDDL(nil, first, tt.postgres)
			require.NoError(t, err, "printed schema must be accepted by CreateDatabase")
			assert.Equal(t, first, second)
		})
	}
}
