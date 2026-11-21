package gcpspanner_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

type rtCase = struct {
	name     string
	batch    []string
	postgres bool
}

func pgBase() []string {
	return []string{
		"CREATE TABLE t (id bigint NOT NULL, a varchar(10), ts timestamptz, d bigint DEFAULT 1, PRIMARY KEY (id))",
		"CREATE TABLE c (id bigint NOT NULL, k bigint NOT NULL, PRIMARY KEY (id, k)) INTERLEAVE IN PARENT t",
		"CREATE INDEX i ON t (a) INCLUDE (d)",
		"CREATE SEQUENCE sq BIT_REVERSED_POSITIVE",
		"CREATE ROLE r",
		"CREATE ROLE r2",
	}
}

func pgRoundTripCases() []rtCase {
	return []rtCase{
		{name: "pg ttl", postgres: true, batch: []string{
			"ALTER TABLE t ADD TTL INTERVAL '30 days' ON ts",
			"ALTER TABLE t ALTER TTL INTERVAL '5 days' ON ts",
		}},
		{name: "pg rename", postgres: true, batch: []string{"ALTER TABLE t RENAME TO t2"}},
		{
			name: "pg rename with synonym", postgres: true,
			batch: []string{"ALTER TABLE t RENAME TO t2, ADD SYNONYM old"},
		},
		{name: "pg synonym", postgres: true, batch: []string{"ALTER TABLE t ADD SYNONYM s"}},
		{name: "pg grants", postgres: true, batch: []string{
			"GRANT SELECT, INSERT ON TABLE t TO r", "GRANT UPDATE(a) ON TABLE t TO r", "GRANT r TO r2",
			"REVOKE INSERT ON TABLE t FROM r", "GRANT SELECT ON t TO r2",
		}},
		{name: "pg grants then rename", postgres: true, batch: []string{
			"GRANT SELECT ON TABLE t TO r", "ALTER TABLE t RENAME TO tt",
		}},
		{name: "pg schema grants", postgres: true, batch: []string{
			"GRANT USAGE ON SCHEMA public TO r", "GRANT SELECT ON ALL TABLES IN SCHEMA public TO r2",
		}},
		{name: "pg database options", postgres: true, batch: []string{
			"ALTER DATABASE db SET spanner.version_retention_period = '3d'",
		}},
	}
}

func TestDDL_PrintedSchemaRoundTrips(t *testing.T) {
	t.Parallel()

	tests := append(pgRoundTripCases(), []rtCase{
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
	}...)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := afBase()
			if tt.postgres {
				base = pgBase()
			}

			first, err := gcpspanner.FoldDDL(base, tt.batch, tt.postgres)
			require.NoError(t, err)

			second, err := gcpspanner.FoldDDL(nil, first, tt.postgres)
			require.NoError(t, err, "printed schema must be accepted by CreateDatabase")
			assert.Equal(t, first, second)
		})
	}
}

func TestDDL_GrantReferenceGrammarAccepted(t *testing.T) {
	t.Parallel()

	goog := []string{
		"CREATE ROLE r", "CREATE TABLE t (id INT64, a INT64) PRIMARY KEY (id)",
		"CREATE VIEW v SQL SECURITY INVOKER AS SELECT id FROM t", "CREATE CHANGE STREAM cs FOR ALL",
		"CREATE SEQUENCE sq OPTIONS (sequence_kind = 'bit_reversed_positive')", "CREATE SCHEMA sch",
	}
	pg := []string{
		"CREATE ROLE r", "CREATE TABLE t (id bigint PRIMARY KEY, a bigint)", "CREATE SEQUENCE sq BIT_REVERSED_POSITIVE",
		"CREATE SCHEMA sch",
	}

	tests := []struct {
		name     string
		stmt     string
		base     []string
		postgres bool
	}{
		{"usage on default schema", "GRANT USAGE ON SCHEMA DEFAULT TO ROLE r", goog, false},
		{"usage on named schemas", "GRANT USAGE ON SCHEMA sch, DEFAULT TO ROLE r", goog, false},
		{"all tables in default schema", "GRANT SELECT ON ALL TABLES IN SCHEMA default TO ROLE r", goog, false},
		{"all views", "GRANT SELECT ON ALL VIEWS IN SCHEMA sch TO ROLE r", goog, false},
		{"all change streams", "GRANT SELECT ON ALL CHANGE STREAMS IN SCHEMA sch TO ROLE r", goog, false},
		{"all sequences", "GRANT SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA default TO ROLE r", goog, false},
		{"quoted table and role", "GRANT SELECT ON TABLE `t` TO ROLE `r`", goog, false},
		{"quoted view", "GRANT SELECT ON VIEW `v` TO ROLE r", goog, false},
		{"pg usage on public", "GRANT USAGE ON SCHEMA public TO r", pg, true},
		{"pg all sequences", "GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO r", pg, true},
		{"pg quoted table", `GRANT SELECT ON TABLE "t" TO "r"`, pg, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			first, err := gcpspanner.FoldDDL(tt.base, []string{tt.stmt}, tt.postgres)
			require.NoError(t, err)

			second, err := gcpspanner.FoldDDL(nil, first, tt.postgres)
			require.NoError(t, err)
			assert.Equal(t, first, second)
		})
	}
}

func TestDDL_RenameRewritesOnlyReferences(t *testing.T) {
	t.Parallel()

	base := []string{
		"CREATE TABLE Singers (id INT64) PRIMARY KEY (id)",
		"CREATE TABLE SingersArchive (id INT64, n STRING(99) DEFAULT ('REFERENCES Singers'), " +
			"CONSTRAINT fk FOREIGN KEY (id) REFERENCES Singers (id), -- REFERENCES Singers\n x INT64) PRIMARY KEY (id)",
		"CREATE TABLE Albums (id INT64, a INT64) PRIMARY KEY (id, a), INTERLEAVE IN PARENT `Singers`",
		"CREATE INDEX i ON Singers (id)",
		"CREATE CHANGE STREAM cs FOR Singers, SingersArchive",
	}

	got, err := gcpspanner.FoldDDL(base, []string{"ALTER TABLE Singers RENAME TO Artists"}, false)
	require.NoError(t, err)

	joined := strings.Join(got, "\n")

	assert.Contains(t, joined, "CREATE TABLE Artists (")
	assert.Contains(t, joined, "DEFAULT ('REFERENCES Singers')", "string literals are not rewritten")
	assert.Contains(t, joined, "-- REFERENCES Singers", "comments are not rewritten")
	assert.Contains(t, joined, "FOREIGN KEY (id) REFERENCES Artists (id)")
	assert.Contains(t, joined, "INTERLEAVE IN PARENT Artists")
	assert.Contains(t, joined, "CREATE INDEX i ON Artists (id)")
	assert.Contains(t, joined, "FOR Artists, SingersArchive", "longer identifiers sharing the prefix stay")
	assert.Contains(t, joined, "CREATE TABLE SingersArchive (")
}
