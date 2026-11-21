package gcpspanner_test

import (
	"context"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type dockerSnapshotCase struct {
	mutate   func(t *testing.T, c *spanner.Client)
	checks   map[string][]string
	wantRows map[string]int
	name     string
	create   string
	ddl      []string
	dialect  databasepb.DatabaseDialect
}

func dumpTable(t *testing.T, c *spanner.Client, table string, cols []string) []string {
	t.Helper()

	it := c.Single().Read(t.Context(), table, spanner.AllKeys(), cols)
	defer it.Stop()

	var out []string

	for {
		row, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}

		require.NoError(t, err)

		var line strings.Builder

		for i := range cols {
			var g spanner.GenericColumnValue

			require.NoError(t, row.Column(i, &g))

			line.WriteString(cols[i] + "=" + protojson.Format(g.Value) + ";")
		}

		out = append(out, line.String())
	}
}

func dockerSnapshotCases() []dockerSnapshotCase {
	return []dockerSnapshotCase{
		{
			name:    "googlesql",
			dialect: databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL,
			create:  "CREATE DATABASE `app`",
			ddl: []string{
				"CREATE TABLE Singers (id INT64 NOT NULL, name STRING(MAX), b BOOL, f64 FLOAT64, f32 FLOAT32, " +
					"num NUMERIC, bs BYTES(MAX), d DATE, ts TIMESTAMP, js JSON, ai ARRAY<INT64>, " +
					"astr ARRAY<STRING(MAX)>, ab ARRAY<BYTES(MAX)>, af ARRAY<FLOAT64>, an ARRAY<NUMERIC>, " +
					"ad ARRAY<DATE>, ats ARRAY<TIMESTAMP>, ajs ARRAY<JSON>, abool ARRAY<BOOL>, " +
					"upd TIMESTAMP OPTIONS (allow_commit_timestamp = true), " +
					"gen STRING(MAX) AS (UPPER(name)) STORED) PRIMARY KEY (id)",
				"CREATE TABLE Albums (id INT64 NOT NULL, aid INT64 NOT NULL, title STRING(MAX) NOT NULL) " +
					"PRIMARY KEY (id, aid), INTERLEAVE IN PARENT Singers ON DELETE CASCADE",
				"CREATE TABLE Tracks (id INT64 NOT NULL, aid INT64 NOT NULL, tid INT64 NOT NULL, " +
					"len INT64) PRIMARY KEY (id, aid, tid), INTERLEAVE IN PARENT Albums ON DELETE NO ACTION",
				"CREATE TABLE Keys (k STRING(MAX) NOT NULL, v STRING(MAX)) PRIMARY KEY (k)",
				"CREATE TABLE Refs (rid INT64 NOT NULL, singer INT64 NOT NULL, " +
					"CONSTRAINT fk_singer FOREIGN KEY (singer) REFERENCES Singers (id)) PRIMARY KEY (rid)",
				"CREATE INDEX SingersByName ON Singers (name)",
			},
			mutate: mutateGoogleSQL,
			checks: map[string][]string{
				"Singers": {
					"id", "name", "b", "f64", "f32", "num", "bs", "d", "ts", "js", "ai", "astr", "ab", "af", "an",
					"ad", "ats", "ajs", "abool", "upd", "gen",
				},
				"Albums": {"id", "aid", "title"},
				"Tracks": {"id", "aid", "tid", "len"},
				"Keys":   {"k", "v"},
				"Refs":   {"rid", "singer"},
			},
			wantRows: map[string]int{"Singers": 3, "Albums": 2, "Tracks": 1, "Keys": 3, "Refs": 1},
		},
		{
			name:    "postgresql",
			dialect: databasepb.DatabaseDialect_POSTGRESQL,
			create:  `CREATE DATABASE "app"`,
			ddl: []string{
				"CREATE TABLE singers (id bigint PRIMARY KEY, name varchar(100), b boolean, " +
					"f double precision, num numeric, bs bytea, ts timestamptz, d date, js jsonb, ai bigint[], " +
					"astr varchar[])",
				"CREATE TABLE albums (id bigint NOT NULL, aid bigint NOT NULL, title varchar NOT NULL, " +
					"PRIMARY KEY (id, aid)) INTERLEAVE IN PARENT singers ON DELETE CASCADE",
			},
			mutate: mutatePostgres,
			checks: map[string][]string{
				"singers": {"id", "name", "b", "f", "num", "bs", "ts", "d", "js", "ai", "astr"},
				"albums":  {"id", "aid", "title"},
			},
			wantRows: map[string]int{"singers": 2, "albums": 1},
		},
	}
}

func mutateGoogleSQL(t *testing.T, c *spanner.Client) {
	t.Helper()

	_, err := c.Apply(t.Context(), []*spanner.Mutation{
		spanner.Insert("Singers", []string{"id", "name"}, []any{int64(3), spanner.NullString{}}),
		spanner.InsertMap("Singers", map[string]any{
			"id": int64(1), "name": "Ann é世", "b": true, "f64": 3.141592653589793,
			"f32": float32(1.5), "num": mustNumeric("12345678901234567890.123456789"),
			"bs": []byte{0, 1, 2, 255}, "d": civilDate(), "ts": ts("2024-01-02T03:04:05.123456789Z"),
			"js": spanner.NullJSON{Value: map[string]any{"a": []any{1.5, "x"}}, Valid: true},
			"ai": []int64{1, -9223372036854775808, 9223372036854775807}, "astr": []string{"a", "", "b"},
			"ab": [][]byte{{1}, {}}, "af": []float64{1.5, math.Copysign(0, -1)}, "abool": []bool{true, false},
			"upd": spanner.CommitTimestamp,
		}),
		spanner.InsertMap("Singers", map[string]any{
			"id": int64(2), "name": "", "b": false, "f64": 0.0, "bs": []byte{},
			"ai": []int64{},
		}),
		spanner.Insert("Albums", []string{"id", "aid", "title"}, []any{int64(1), int64(1), "Blue"}),
		spanner.Insert("Albums", []string{"id", "aid", "title"}, []any{int64(1), int64(2), "Green"}),
		spanner.Insert("Tracks", []string{"id", "aid", "tid", "len"}, []any{int64(1), int64(1), int64(1), int64(10)}),
		spanner.Insert("Keys", []string{"k", "v"}, []any{"a/b\"c'd`e\\f\n\x00z", "v1"}),
		spanner.Insert("Keys", []string{"k", "v"}, []any{"é\U0001F600", spanner.NullString{}}),
		spanner.Insert("Keys", []string{"k", "v"}, []any{"", "empty key"}),
		spanner.Insert("Refs", []string{"rid", "singer"}, []any{int64(1), int64(1)}),
	})
	require.NoError(t, err)
}

func mutatePostgres(t *testing.T, c *spanner.Client) {
	t.Helper()

	_, err := c.Apply(t.Context(), []*spanner.Mutation{
		spanner.InsertMap("singers", map[string]any{
			"id": int64(1), "name": "Ann", "b": true, "f": 2.5, "bs": []byte{1, 2}, "ai": []int64{1, 2},
			"astr": []string{"x", "y"},
		}),
		spanner.Insert("singers", []string{"id"}, []any{int64(2)}),
		spanner.Insert("albums", []string{"id", "aid", "title"}, []any{int64(1), int64(1), "Blue"}),
	})
	require.NoError(t, err)
}

func TestDocker_SnapshotRoundTrip(t *testing.T) {
	t.Parallel()

	for _, tt := range dockerSnapshotCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := dockerEnv(t)
			src.createInstance(t, "snap")

			dbPath := parent + "/instances/snap/databases/app"
			op, err := src.databases.CreateDatabase(t.Context(), &databasepb.CreateDatabaseRequest{
				Parent: parent + "/instances/snap", CreateStatement: tt.create, ExtraStatements: tt.ddl,
				DatabaseDialect: tt.dialect,
			})
			require.NoError(t, err)

			_, err = await(t, op)
			require.NoError(t, err)

			sc := newSpannerClient(t, src, dbPath)
			tt.mutate(t, sc)

			want := map[string][]string{}
			for table, cols := range tt.checks {
				want[table] = dumpTable(t, sc, table, cols)
				assert.Len(t, want[table], tt.wantRows[table], table)
			}

			snap := src.handler.Snapshot(t.Context())

			dst := dockerEnv(t)
			require.NoError(t, dst.handler.Restore(t.Context(), snap))

			dc := newSpannerClient(t, dst, dbPath)
			for table, cols := range tt.checks {
				assert.Equal(t, want[table], dumpTable(t, dc, table, cols), table)
			}
		})
	}
}

func newSpannerClient(t *testing.T, e *env, db string) *spanner.Client {
	t.Helper()

	c, err := spanner.NewClientWithConfig(context.WithoutCancel(t.Context()), db,
		spanner.ClientConfig{DisableNativeMetrics: true}, e.opts...)
	require.NoError(t, err)
	t.Cleanup(c.Close)

	return c
}

func mustNumeric(s string) *big.Rat {
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		panic("bad numeric " + s)
	}

	return r
}

func civilDate() civil.Date { return civil.Date{Year: 2024, Month: 2, Day: 29} }

func ts(s string) time.Time {
	v, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}

	return v
}

func TestDocker_BackupsRoundTripRows(t *testing.T) {
	t.Parallel()

	e := dockerEnv(t)
	e.createInstance(t, "bk")

	inst := parent + "/instances/bk"
	db := e.createDB(t, "bk",
		"CREATE TABLE t (id INT64 NOT NULL, v STRING(MAX), n NUMERIC) PRIMARY KEY (id)")

	sc := newSpannerClient(t, e, db.GetName())
	first, err := sc.Apply(t.Context(), []*spanner.Mutation{
		spanner.InsertOrUpdateMap("t", map[string]any{"id": int64(1), "v": "one", "n": mustNumeric("1.5")}),
	})
	require.NoError(t, err)

	_, err = sc.Apply(t.Context(), []*spanner.Mutation{
		spanner.InsertOrUpdateMap("t", map[string]any{"id": int64(2), "v": "two", "n": mustNumeric("2")}),
	})
	require.NoError(t, err)

	want := dumpTable(t, sc, "t", []string{"id", "v", "n"})
	require.Len(t, want, 2)

	cb, err := e.databases.CreateBackup(t.Context(), &databasepb.CreateBackupRequest{
		Parent: inst, BackupId: "b1",
		Backup: &databasepb.Backup{Database: db.GetName(), ExpireTime: timestamppb.New(time.Now().Add(48 * time.Hour))},
	})
	require.NoError(t, err)

	bk, err := cb.Wait(t.Context())
	require.NoError(t, err)
	assert.Equal(t, databasepb.Backup_READY, bk.GetState())

	_, err = sc.Apply(t.Context(), []*spanner.Mutation{spanner.Delete("t", spanner.AllKeys())})
	require.NoError(t, err)
	assert.Empty(t, dumpTable(t, sc, "t", []string{"id", "v", "n"}))

	old, err := e.databases.CreateBackup(t.Context(), &databasepb.CreateBackupRequest{
		Parent: inst, BackupId: "stale",
		Backup: &databasepb.Backup{
			Database: db.GetName(), ExpireTime: timestamppb.New(time.Now().Add(48 * time.Hour)),
			VersionTime: timestamppb.New(first),
		},
	})
	require.NoError(t, err)

	staleBackup, err := old.Wait(t.Context())
	require.NoError(t, err)
	assert.True(t, first.Equal(staleBackup.GetVersionTime().AsTime()))

	sop, err := e.databases.RestoreDatabase(t.Context(), &databasepb.RestoreDatabaseRequest{
		Parent:     inst,
		DatabaseId: "stale",
		Source:     &databasepb.RestoreDatabaseRequest_Backup{Backup: staleBackup.GetName()},
	})
	require.NoError(t, err)

	staleDB, err := sop.Wait(t.Context())
	require.NoError(t, err)

	stc := newSpannerClient(t, e, staleDB.GetName())
	assert.Equal(
		t,
		want[:1],
		dumpTable(t, stc, "t", []string{"id", "v", "n"}),
		"a backup holds the rows as of its version_time",
	)

	rop, err := e.databases.RestoreDatabase(t.Context(), &databasepb.RestoreDatabaseRequest{
		Parent: inst, DatabaseId: "restored", Source: &databasepb.RestoreDatabaseRequest_Backup{Backup: bk.GetName()},
	})
	require.NoError(t, err)

	restored, err := rop.Wait(t.Context())
	require.NoError(t, err)

	rc := newSpannerClient(t, e, restored.GetName())
	assert.Equal(
		t,
		want,
		dumpTable(t, rc, "t", []string{"id", "v", "n"}),
		"the restored database holds the backed up rows",
	)

	snap := e.handler.Snapshot(t.Context())

	dst := dockerEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.databases.GetBackup(t.Context(), &databasepb.GetBackupRequest{Name: bk.GetName()})
	require.NoError(t, err)
	assert.Equal(t, bk.GetSizeBytes(), got.GetSizeBytes())

	rop2, err := dst.databases.RestoreDatabase(t.Context(), &databasepb.RestoreDatabaseRequest{
		Parent: inst, DatabaseId: "again", Source: &databasepb.RestoreDatabaseRequest_Backup{Backup: bk.GetName()},
	})
	require.NoError(t, err)

	again, err := rop2.Wait(t.Context())
	require.NoError(t, err)

	ac := newSpannerClient(t, dst, again.GetName())
	assert.Equal(t, want, dumpTable(t, ac, "t", []string{"id", "v", "n"}), "backups survive a snapshot with their rows")
}
