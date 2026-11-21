package gcpspanner_test

import (
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

const (
	afTable = "CREATE TABLE t (id INT64 NOT NULL, a STRING(10), ts TIMESTAMP OPTIONS (allow_commit_timestamp = true), " +
		"d INT64 DEFAULT (1)) PRIMARY KEY (id)"
	afChild  = "CREATE TABLE c (id INT64, k INT64) PRIMARY KEY (id, k), INTERLEAVE IN PARENT t"
	afIndex  = "CREATE INDEX i ON t (a) STORING (d)"
	afSeq    = "CREATE SEQUENCE sq OPTIONS (sequence_kind = 'bit_reversed_positive')"
	afStream = "CREATE CHANGE STREAM cs FOR t(a) OPTIONS (retention_period = '7d')"
)

func afBase() []string {
	return []string{
		afTable,
		afChild,
		afIndex,
		afSeq,
		afStream,
		"CREATE ROLE r",
		"CREATE ROLE r2",
		"CREATE PROTO BUNDLE (a.B)",
	}
}

func afWith(overrides map[int]string, extra ...string) []string {
	out := afBase()
	for i, s := range overrides {
		out[i] = s
	}

	return append(out, extra...)
}

func TestDDL_AlterFoldsIntoPrintedSchema(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want  []string
		name  string
		batch []string
	}{
		{
			name:  "column options null removes the clause",
			batch: []string{"ALTER TABLE t ALTER COLUMN ts SET OPTIONS (allow_commit_timestamp = null)"},
			want: afWith(
				map[int]string{0: "CREATE TABLE t (\n  id INT64 NOT NULL,\n  a STRING(10),\n  ts TIMESTAMP,\n" +
					"  d INT64 DEFAULT (1)\n) PRIMARY KEY (id)"},
			),
		},
		{
			name:  "column options added",
			batch: []string{"ALTER TABLE t ALTER COLUMN a SET OPTIONS (locality_group = 'x')"},
			want: afWith(
				map[int]string{
					0: "CREATE TABLE t (\n  id INT64 NOT NULL,\n  a STRING(10) OPTIONS (locality_group = 'x'),\n" +
						"  ts TIMESTAMP OPTIONS (allow_commit_timestamp = true),\n  d INT64 DEFAULT (1)\n) PRIMARY KEY (id)",
				},
			),
		},
		{
			name:  "set default",
			batch: []string{"ALTER TABLE t ALTER COLUMN a SET DEFAULT ('x')"},
			want: afWith(map[int]string{0: "CREATE TABLE t (\n  id INT64 NOT NULL,\n  a STRING(10) DEFAULT ('x'),\n" +
				"  ts TIMESTAMP OPTIONS (allow_commit_timestamp = true),\n  d INT64 DEFAULT (1)\n) PRIMARY KEY (id)"}),
		},
		{
			name: "set then drop default round trips",
			batch: []string{
				"ALTER TABLE t ALTER COLUMN a SET DEFAULT ('x')",
				"ALTER TABLE t ALTER COLUMN a DROP DEFAULT",
			},
			want: afWith(map[int]string{0: "CREATE TABLE t (\n  id INT64 NOT NULL,\n  a STRING(10),\n" +
				"  ts TIMESTAMP OPTIONS (allow_commit_timestamp = true),\n  d INT64 DEFAULT (1)\n) PRIMARY KEY (id)"}),
		},
		{
			name:  "drop default",
			batch: []string{"ALTER TABLE t ALTER COLUMN d DROP DEFAULT"},
			want: afWith(map[int]string{0: "CREATE TABLE t (\n  id INT64 NOT NULL,\n  a STRING(10),\n" +
				"  ts TIMESTAMP OPTIONS (allow_commit_timestamp = true),\n  d INT64\n) PRIMARY KEY (id)"}),
		},
		{
			name:  "on update follows the default",
			batch: []string{"ALTER TABLE t ALTER COLUMN d SET ON UPDATE (2)"},
			want: afWith(map[int]string{0: "CREATE TABLE t (\n  id INT64 NOT NULL,\n  a STRING(10),\n" +
				"  ts TIMESTAMP OPTIONS (allow_commit_timestamp = true),\n" +
				"  d INT64 DEFAULT (1) ON UPDATE (2)\n) PRIMARY KEY (id)"}),
		},
		{
			name: "row deletion policy added replaced with options after it",
			batch: []string{
				"ALTER TABLE t ADD ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL 30 DAY))",
				"ALTER TABLE t SET OPTIONS (locality_group = 'g')",
				"ALTER TABLE t REPLACE ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL 5 DAY))",
			},
			want: afWith(map[int]string{0: "CREATE TABLE t (\n  id INT64 NOT NULL,\n  a STRING(10),\n" +
				"  ts TIMESTAMP OPTIONS (allow_commit_timestamp = true),\n  d INT64 DEFAULT (1)\n) PRIMARY KEY (id), " +
				"ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL 5 DAY)), OPTIONS (locality_group = 'g')"}),
		},
		{
			name: "row deletion policy dropped",
			batch: []string{
				"ALTER TABLE t ADD ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL 30 DAY))",
				"ALTER TABLE t DROP ROW DELETION POLICY",
			},
			want: afWith(map[int]string{0: "CREATE TABLE t (\n  id INT64 NOT NULL,\n  a STRING(10),\n" +
				"  ts TIMESTAMP OPTIONS (allow_commit_timestamp = true),\n  d INT64 DEFAULT (1)\n) PRIMARY KEY (id)"}),
		},
		{
			name:  "synonym",
			batch: []string{"ALTER TABLE t ADD SYNONYM s"},
			want: afWith(map[int]string{0: "CREATE TABLE t (\n  id INT64 NOT NULL,\n  a STRING(10),\n" +
				"  ts TIMESTAMP OPTIONS (allow_commit_timestamp = true),\n" +
				"  d INT64 DEFAULT (1),\n  SYNONYM (s)\n) PRIMARY KEY (id)"}),
		},
		{
			name:  "rename follows every reference",
			batch: []string{"ALTER TABLE t RENAME TO t2, ADD SYNONYM old"},
			want: []string{
				"CREATE TABLE t2 (\n  id INT64 NOT NULL,\n  a STRING(10),\n" +
					"  ts TIMESTAMP OPTIONS (allow_commit_timestamp = true),\n" +
					"  d INT64 DEFAULT (1),\n  SYNONYM (old)\n) PRIMARY KEY (id)",
				"CREATE TABLE c (id INT64, k INT64) PRIMARY KEY (id, k), INTERLEAVE IN PARENT t2",
				"CREATE INDEX i ON t2 (a) STORING (d)",
				afSeq,
				"CREATE CHANGE STREAM cs FOR t2(a) OPTIONS (retention_period = '7d')",
				"CREATE ROLE r",
				"CREATE ROLE r2",
				"CREATE PROTO BUNDLE (a.B)",
			},
		},
		{
			name:  "set interleave",
			batch: []string{"ALTER TABLE c SET INTERLEAVE IN PARENT t ON DELETE CASCADE"},
			want: afWith(
				map[int]string{
					1: "CREATE TABLE c (\n  id INT64,\n  k INT64\n) PRIMARY KEY (id, k), INTERLEAVE IN PARENT t ON DELETE CASCADE",
				},
			),
		},
		{
			name:  "stored column added",
			batch: []string{"ALTER INDEX i ADD STORED COLUMN ts"},
			want:  afWith(map[int]string{2: "CREATE INDEX i ON t (a) STORING (d, ts)"}),
		},
		{
			name:  "stored column dropped",
			batch: []string{"ALTER INDEX i DROP STORED COLUMN d"},
			want:  afWith(map[int]string{2: "CREATE INDEX i ON t (a)"}),
		},
		{
			name:  "sequence options",
			batch: []string{"ALTER SEQUENCE sq SET OPTIONS (skip_range_min = 1, skip_range_max = 9)"},
			want: afWith(map[int]string{3: "CREATE SEQUENCE sq OPTIONS (sequence_kind = 'bit_reversed_positive', " +
				"skip_range_min = 1, skip_range_max = 9)"}),
		},
		{
			name:  "change stream for",
			batch: []string{"ALTER CHANGE STREAM cs SET FOR ALL"},
			want:  afWith(map[int]string{4: "CREATE CHANGE STREAM cs FOR ALL OPTIONS (retention_period = '7d')"}),
		},
		{
			name:  "change stream options",
			batch: []string{"ALTER CHANGE STREAM cs SET OPTIONS (retention_period = '3d', exclude_insert = true)"},
			want: afWith(
				map[int]string{
					4: "CREATE CHANGE STREAM cs FOR t(a) OPTIONS (retention_period = '3d', exclude_insert = true)",
				},
			),
		},
		{
			name: "database options merge and null removes",
			batch: []string{
				"ALTER DATABASE db SET OPTIONS (version_retention_period = '3d', optimizer_version = 2)",
				"ALTER DATABASE db SET OPTIONS (optimizer_version = null)",
			},
			want: afWith(nil, "ALTER DATABASE db SET OPTIONS (version_retention_period = '3d')"),
		},
		{
			name:  "proto bundle insert",
			batch: []string{"ALTER PROTO BUNDLE INSERT (c.D)"},
			want:  afWith(map[int]string{7: "CREATE PROTO BUNDLE (a.B, c.D)"}),
		},
		{
			name: "grants merge into one statement per role and object and revoke removes",
			batch: []string{
				"GRANT SELECT, INSERT ON TABLE t TO ROLE r", "GRANT UPDATE(a) ON TABLE t TO ROLE r",
				"GRANT ROLE r TO ROLE r2", "REVOKE INSERT ON TABLE t FROM ROLE r",
			},
			want: afWith(nil, "GRANT SELECT, UPDATE(a) ON TABLE t TO ROLE r", "GRANT ROLE r TO ROLE r2"),
		},
		{
			name:  "revoking everything leaves no grant",
			batch: []string{"GRANT SELECT ON TABLE t TO ROLE r", "REVOKE SELECT ON TABLE t FROM ROLE r"},
			want:  afBase(),
		},
		{
			name: "grants follow a table rename and a dropped role",
			batch: []string{
				"GRANT SELECT ON TABLE t TO ROLE r, r2",
				"REVOKE SELECT ON TABLE t FROM ROLE r2",
				"ALTER TABLE t RENAME TO tt",
			},
			want: []string{
				"CREATE TABLE tt (\n  id INT64 NOT NULL,\n  a STRING(10),\n" +
					"  ts TIMESTAMP OPTIONS (allow_commit_timestamp = true),\n" +
					"  d INT64 DEFAULT (1)\n) PRIMARY KEY (id)",
				"CREATE TABLE c (id INT64, k INT64) PRIMARY KEY (id, k), INTERLEAVE IN PARENT tt",
				"CREATE INDEX i ON tt (a) STORING (d)",
				afSeq,
				"CREATE CHANGE STREAM cs FOR tt(a) OPTIONS (retention_period = '7d')",
				"CREATE ROLE r",
				"CREATE ROLE r2",
				"CREATE PROTO BUNDLE (a.B)",
				"GRANT SELECT ON TABLE tt TO ROLE r",
			},
		},
		{
			name:  "dropping a role drops its grants",
			batch: []string{"GRANT SELECT ON TABLE t TO ROLE r", "DROP ROLE r"},
			want:  []string{afTable, afChild, afIndex, afSeq, afStream, "CREATE ROLE r2", "CREATE PROTO BUNDLE (a.B)"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := gcpspanner.FoldDDL(afBase(), tt.batch, false)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDDL_AlterRejectsInvalidForms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		batch []string
		code  codes.Code
	}{
		{
			"unknown column option",
			[]string{"ALTER TABLE t ALTER COLUMN a SET OPTIONS (bogus = 1)"},
			codes.InvalidArgument,
		},
		{
			"commit timestamp on a non timestamp",
			[]string{"ALTER TABLE t ALTER COLUMN a SET OPTIONS (allow_commit_timestamp = true)"},
			codes.InvalidArgument,
		},
		{"default on missing column", []string{"ALTER TABLE t ALTER COLUMN zz SET DEFAULT (1)"}, codes.NotFound},
		{
			"on update without default",
			[]string{"ALTER TABLE t ALTER COLUMN a SET ON UPDATE (1)"},
			codes.FailedPrecondition,
		},
		{
			"drop default under on update",
			[]string{"ALTER TABLE t ALTER COLUMN d SET ON UPDATE (2)", "ALTER TABLE t ALTER COLUMN d DROP DEFAULT"},
			codes.FailedPrecondition,
		},
		{
			"policy on a non timestamp",
			[]string{"ALTER TABLE t ADD ROW DELETION POLICY (OLDER_THAN(a, INTERVAL 1 DAY))"},
			codes.InvalidArgument,
		},
		{
			"second policy",
			[]string{
				"ALTER TABLE t ADD ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL 1 DAY))",
				"ALTER TABLE t ADD ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL 2 DAY))",
			},
			codes.AlreadyExists,
		},
		{
			"replace without policy",
			[]string{"ALTER TABLE t REPLACE ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL 1 DAY))"},
			codes.NotFound,
		},
		{
			"fulltext dictionary table",
			[]string{"ALTER TABLE t SET OPTIONS (fulltext_dictionary_table = true)"},
			codes.InvalidArgument,
		},
		{"rename onto an existing name", []string{"ALTER TABLE t RENAME TO c"}, codes.AlreadyExists},
		{
			"second synonym",
			[]string{"ALTER TABLE t ADD SYNONYM a1", "ALTER TABLE t ADD SYNONYM a2"},
			codes.AlreadyExists,
		},
		{
			"interleave in to parent cascade",
			[]string{"ALTER TABLE c SET INTERLEAVE IN t", "ALTER TABLE c SET INTERLEAVE IN PARENT t ON DELETE CASCADE"},
			codes.InvalidArgument,
		},
		{"stored column already stored", []string{"ALTER INDEX i ADD STORED COLUMN d"}, codes.AlreadyExists},
		{"stored column of a key", []string{"ALTER INDEX i ADD STORED COLUMN a"}, codes.AlreadyExists},
		{"stored column missing", []string{"ALTER INDEX i DROP STORED COLUMN ts"}, codes.NotFound},
		{"index missing", []string{"ALTER INDEX nope ADD STORED COLUMN ts"}, codes.NotFound},
		{
			"retention too short",
			[]string{"ALTER DATABASE db SET OPTIONS (version_retention_period = '30m')"},
			codes.InvalidArgument,
		},
		{
			"retention too long",
			[]string{"ALTER DATABASE db SET OPTIONS (version_retention_period = '8d')"},
			codes.InvalidArgument,
		},
		{
			"optimizer version",
			[]string{"ALTER DATABASE db SET OPTIONS (optimizer_version = 10)"},
			codes.InvalidArgument,
		},
		{"unknown database option", []string{"ALTER DATABASE db SET OPTIONS (nope = 1)"}, codes.InvalidArgument},
		{
			"time zone on a database with tables",
			[]string{"ALTER DATABASE db SET OPTIONS (default_time_zone = 'UTC')"},
			codes.FailedPrecondition,
		},
		{
			"sequence range",
			[]string{"ALTER SEQUENCE sq SET OPTIONS (skip_range_min = 9, skip_range_max = 1)"},
			codes.InvalidArgument,
		},
		{"change stream missing table", []string{"ALTER CHANGE STREAM cs SET FOR nope"}, codes.NotFound},
		{"drop for all on a table stream", []string{"ALTER CHANGE STREAM cs DROP FOR ALL"}, codes.FailedPrecondition},
		{"bundle duplicate", []string{"ALTER PROTO BUNDLE INSERT (a.B)"}, codes.AlreadyExists},
		{"bundle missing delete", []string{"ALTER PROTO BUNDLE DELETE (z.Z)"}, codes.NotFound},
		{"grant to a missing role", []string{"GRANT SELECT ON TABLE t TO ROLE nope"}, codes.NotFound},
		{"grant on a missing table", []string{"GRANT SELECT ON TABLE nope TO ROLE r"}, codes.NotFound},
		{"grant a missing column", []string{"GRANT SELECT(zz) ON TABLE t TO ROLE r"}, codes.NotFound},
		{"column level delete", []string{"GRANT DELETE(a) ON TABLE t TO ROLE r"}, codes.InvalidArgument},
		{"privilege the object lacks", []string{"GRANT INSERT ON CHANGE STREAM cs TO ROLE r"}, codes.InvalidArgument},
		{"grant syntax", []string{"GRANT SELECT ON TABLE t"}, codes.InvalidArgument},
		{"role grant of a missing role", []string{"GRANT ROLE nope TO ROLE r"}, codes.NotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, applied, err := gcpspanner.FoldDDLApplied(afBase(), tt.batch, false)
			require.Error(t, err)
			assert.Equal(t, tt.code, status.Code(err), err.Error())
			assert.Less(t, applied, len(tt.batch))
			assert.NotNil(t, got)
		})
	}
}

func TestDDL_AlterPostgres(t *testing.T) {
	t.Parallel()

	const create = "CREATE TABLE singers (id bigint NOT NULL, nick varchar(10) DEFAULT 'x', ts timestamptz, " +
		"PRIMARY KEY (id))"

	tests := []struct {
		name  string
		batch []string
		want  []string
	}{
		{
			name:  "type",
			batch: []string{"ALTER TABLE singers ALTER COLUMN nick TYPE varchar(20)"},
			want: []string{
				"CREATE TABLE singers (\n  id bigint NOT NULL,\n  nick varchar(20) DEFAULT 'x',\n  ts timestamptz,\n" +
					"  PRIMARY KEY (id)\n)",
			},
		},
		{
			name:  "not null",
			batch: []string{"ALTER TABLE singers ALTER COLUMN nick SET NOT NULL"},
			want: []string{
				"CREATE TABLE singers (\n  id bigint NOT NULL,\n  nick varchar(10) NOT NULL DEFAULT 'x',\n" +
					"  ts timestamptz,\n  PRIMARY KEY (id)\n)",
			},
		},
		{
			name:  "drop default",
			batch: []string{"ALTER TABLE singers ALTER COLUMN nick DROP DEFAULT"},
			want: []string{
				"CREATE TABLE singers (\n  id bigint NOT NULL,\n  nick varchar(10),\n  ts timestamptz,\n  PRIMARY KEY (id)\n)",
			},
		},
		{
			name: "ttl add and alter",
			batch: []string{
				"ALTER TABLE singers ADD TTL INTERVAL '3 days' ON ts",
				"ALTER TABLE singers ALTER TTL INTERVAL '5 days' ON ts",
			},
			want: []string{
				"CREATE TABLE singers (\n  id bigint NOT NULL,\n  nick varchar(10) DEFAULT 'x',\n  ts timestamptz,\n" +
					"  PRIMARY KEY (id)\n) TTL INTERVAL '5 days' ON ts",
			},
		},
		{
			name: "grants",
			batch: []string{
				"CREATE ROLE hr", "CREATE ROLE boss", "GRANT SELECT, INSERT ON TABLE singers TO hr",
				"GRANT hr TO boss", "REVOKE INSERT ON TABLE singers FROM hr",
			},
			want: []string{
				create, "CREATE ROLE hr", "CREATE ROLE boss", "GRANT SELECT ON TABLE singers TO hr", "GRANT hr TO boss",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := gcpspanner.FoldDDL([]string{create}, tt.batch, true)
			require.NoError(t, err)

			if tt.want[0] == create {
				assert.Equal(t, tt.want, got)

				return
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDDL_AlterPostgresIndexAndSequence(t *testing.T) {
	t.Parallel()

	base := []string{
		"CREATE TABLE singers (id bigint NOT NULL, nick varchar(10), ts timestamptz, PRIMARY KEY (id))",
		"CREATE INDEX i ON singers (nick) INCLUDE (ts)",
		"CREATE SEQUENCE s BIT_REVERSED_POSITIVE",
	}

	tests := []struct {
		name  string
		batch []string
		want  []string
	}{
		{
			name: "include column added", batch: []string{"ALTER INDEX i ADD INCLUDE COLUMN id"},
			want: []string{base[0], "CREATE INDEX i ON singers (nick) INCLUDE (ts, id)", base[2]},
		},
		{
			name: "include column dropped", batch: []string{"ALTER INDEX i DROP INCLUDE COLUMN ts"},
			want: []string{base[0], "CREATE INDEX i ON singers (nick)", base[2]},
		},
		{
			name: "skip range replaced", batch: []string{
				"ALTER SEQUENCE s SKIP RANGE 1 10", "ALTER SEQUENCE s SKIP RANGE 2 20 NO CYCLE",
			},
			want: []string{base[0], base[1], "CREATE SEQUENCE s BIT_REVERSED_POSITIVE SKIP RANGE 2 20"},
		},
		{
			name: "rename", batch: []string{"ALTER TABLE singers RENAME TO s2"},
			want: []string{
				"CREATE TABLE s2 (\n  id bigint NOT NULL,\n  nick varchar(10),\n  ts timestamptz,\n  PRIMARY KEY (id)\n)",
				"CREATE INDEX i ON s2 (nick) INCLUDE (ts)",
				base[2],
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := gcpspanner.FoldDDL(base, tt.batch, true)
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func alterFuzzSeeds() []string {
	return []string{
		"ALTER TABLE t ALTER COLUMN a SET OPTIONS (locality_group = 'x', allow_commit_timestamp = null)",
		"ALTER TABLE t ALTER COLUMN a SET DEFAULT ('x')",
		"ALTER TABLE t ALTER COLUMN d SET ON UPDATE (2)",
		"ALTER TABLE t ALTER COLUMN d DROP ON UPDATE",
		"ALTER TABLE t ADD ROW DELETION POLICY (OLDER_THAN(ts, INTERVAL 3 DAY))",
		"ALTER TABLE t RENAME TO t2, ADD SYNONYM old",
		"ALTER TABLE c SET INTERLEAVE IN PARENT t ON DELETE CASCADE",
		"ALTER TABLE t SET OPTIONS (locality_group = 'g')",
		"ALTER INDEX i ADD STORED COLUMN ts",
		"ALTER INDEX i DROP STORED COLUMN d",
		"ALTER SEQUENCE sq SET OPTIONS (skip_range_min = 1, skip_range_max = 9)",
		"ALTER CHANGE STREAM cs SET FOR t(a), c",
		"ALTER CHANGE STREAM cs DROP FOR ALL",
		"ALTER DATABASE db SET OPTIONS (version_retention_period = '3d', default_leader = 'x')",
		"ALTER PROTO BUNDLE INSERT (c.D) UPDATE (a.B) DELETE (c.D)",
		"ALTER STATISTICS p SET OPTIONS (allow_gc = false)",
		"GRANT SELECT, UPDATE(a, ts) ON TABLE t TO ROLE r, r2",
		"GRANT ROLE r TO ROLE r2",
		"REVOKE SELECT(a) ON TABLE t FROM ROLE r",
		"GRANT EXECUTE ON TABLE FUNCTION READ_cs TO ROLE r",
		"GRANT SELECT ON ALL TABLES IN SCHEMA public TO r",
		"ALTER TABLE t ADD TTL INTERVAL '3 days' ON ts",
		"ALTER TABLE t ALTER COLUMN a TYPE varchar(20)",
		"ALTER TABLE t ALTER COLUMN a SET NOT NULL",
		"ALTER INDEX i ADD INCLUDE COLUMN id",
		"ALTER SEQUENCE sq SKIP RANGE 1 10 NO CYCLE",
	}
}

func TestDDL_AlterFuzzNeverPanics(t *testing.T) {
	t.Parallel()

	rng := rand.New(rand.NewPCG(3, 5))
	punct := []string{"(", ")", "'", "\"", "`", ",", ".", "--", "/*", "\x00", "''"}

	var cases []string

	for _, st := range alterFuzzSeeds() {
		for i := 0; i <= len(st); i++ {
			cases = append(cases, st[:i])
		}

		for range 12 {
			b := []byte(st)
			at := rng.IntN(len(b) + 1)
			cases = append(cases, string(append(b[:at], append([]byte(punct[rng.IntN(len(punct))]), b[at:]...)...)))
			cases = append(cases, st[:rng.IntN(len(st))]+st[rng.IntN(len(st)):])
		}
	}

	for _, postgres := range []bool{false, true} {
		for _, c := range cases {
			assert.NotPanics(t, func() {
				_, _, _ = gcpspanner.FoldDDLApplied(afBase(), []string{c}, postgres)
				_, _, _ = gcpspanner.FoldDDLApplied(nil, []string{c}, postgres)
			}, "%q", c)
		}
	}
}
