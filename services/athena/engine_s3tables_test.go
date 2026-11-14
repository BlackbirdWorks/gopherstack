package athena_test

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"encoding/json/jsontext"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

var errNoSuchTable = errors.New("no such table")

type fakeS3Tables struct {
	tables map[string]*athena.S3TableInfo
	mu     sync.Mutex
}

func (f *fakeS3Tables) GetTableInfo(_, bucket, ns, table string) (*athena.S3TableInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	info, ok := f.tables[bucket+"/"+ns+"/"+table]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errNoSuchTable, table)
	}

	cp := *info

	return &cp, nil
}

func (f *fakeS3Tables) CommitMetadataLocation(_, bucket, ns, table, expected, next string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	info := f.tables[bucket+"/"+ns+"/"+table]
	if info == nil || info.MetadataLocation != expected {
		return athena.ErrCatalogCommitConflict
	}

	info.MetadataLocation = next

	return nil
}

func (f *fakeS3Tables) CreateTable(_, bucket, ns, table string) (*athena.S3TableInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if ns != "ns" && ns != "other" {
		return nil, fmt.Errorf("%w: namespace %s", athena.ErrCatalogEntityNotFound, ns)
	}

	key := bucket + "/" + ns + "/" + table
	if _, ok := f.tables[key]; ok {
		return nil, fmt.Errorf("%w: %s", athena.ErrCatalogEntityExists, table)
	}

	info := &athena.S3TableInfo{Location: "s3://abc123--table-s3/" + ns + "/" + table}
	f.tables[key] = info

	return &athena.S3TableInfo{Location: info.Location}, nil
}

func (f *fakeS3Tables) DeleteTable(_, bucket, ns, table string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	key := bucket + "/" + ns + "/" + table
	if _, ok := f.tables[key]; !ok {
		return fmt.Errorf("%w: %s", errNoSuchTable, table)
	}

	delete(f.tables, key)

	return nil
}

func (f *fakeS3Tables) ListNamespaces(_, _ string) ([]string, error) {
	return []string{"ns", "other"}, nil
}

func (f *fakeS3Tables) ListTables(_, bucket, ns string) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []string

	for k := range f.tables {
		var b, n, t string
		if parts := strings.SplitN(k, "/", 3); len(parts) == 3 {
			b, n, t = parts[0], parts[1], parts[2]
		}

		if b == bucket && n == ns {
			out = append(out, t)
		}
	}

	sort.Strings(out)

	return out, nil
}

func seedS3Table(t *testing.T, e *iceEnv, src *fakeS3Tables) {
	t.Helper()

	schema, err := iceberg.SchemaFromColumns(
		[]iceberg.ColumnSpec{{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"}},
	)
	require.NoError(t, err)

	const wh = "s3://abc123--table-s3/ns/tbl"

	meta := iceberg.NewTableMetadata(iceberg.NewParams{
		Location: wh, Schema: schema, Spec: iceberg.PartitionSpec{Fields: []iceberg.PartitionField{}}, Now: time.Now(),
	})

	body, err := iceberg.EncodeMetadata(meta)
	require.NoError(t, err)

	loc := iceberg.InitialMetadataLocation(wh)
	putObj(t, e.store, loc, body)
	src.tables["mybucket/ns/tbl"] = &athena.S3TableInfo{Location: wh, MetadataLocation: loc}
}

func TestS3TablesCatalog_QueryAndAppend(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  []string
		want [][]string
	}{
		{
			"insert then select qualified",
			[]string{`INSERT INTO "s3tablescatalog/mybucket"."ns"."tbl" VALUES (1, 'a'), (2, 'b')`},
			[][]string{{"1", "a"}, {"2", "b"}},
		},
		{
			"two appends",
			[]string{
				`INSERT INTO "s3tablescatalog/mybucket"."ns"."tbl" VALUES (1, 'a')`,
				`INSERT INTO "s3tablescatalog/mybucket"."ns"."tbl" VALUES (2, 'b')`,
			},
			[][]string{{"1", "a"}, {"2", "b"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newIceEnv(t)
			src := &fakeS3Tables{tables: map[string]*athena.S3TableInfo{}}
			e.b.SetS3TablesSource(src)
			seedS3Table(t, e, src)

			for _, q := range tc.sql {
				e.mustRun(t, q)
			}

			assert.Equal(t, tc.want,
				e.mustRun(t, `SELECT id, name FROM "s3tablescatalog/mybucket"."ns"."tbl" ORDER BY id`))

			meta := parseMetadataAt(t, e.store, src.tables["mybucket/ns/tbl"].MetadataLocation)
			assert.Len(t, meta.Snapshots, len(tc.sql))
		})
	}
}

func TestS3TablesCatalog_ContextAndErrors(t *testing.T) {
	t.Parallel()

	e := newIceEnv(t)
	src := &fakeS3Tables{tables: map[string]*athena.S3TableInfo{}}
	e.b.SetS3TablesSource(src)
	seedS3Table(t, e, src)
	e.mustRun(t, `INSERT INTO "s3tablescatalog/mybucket".ns.tbl VALUES (7, 'z')`)

	id, err := e.b.StartQueryExecution("SELECT id FROM tbl", "primary",
		athena.QueryExecutionContext{Catalog: "s3tablescatalog/mybucket", Database: "ns"},
		athena.ResultConfiguration{OutputLocation: "s3://out/res/"}, nil, nil)
	require.NoError(t, err)

	qe := waitDone(t, e.b, id)
	require.Equal(t, "SUCCEEDED", qe.Status.State, qe.Status.StateChangeReason)
	assert.Equal(t, [][]string{{"7"}}, resultRows(t, e.b, id))

	fails := []struct{ name, sql, want string }{
		{"missing table", `SELECT * FROM "s3tablescatalog/mybucket".ns.ghost`, "TABLE_NOT_FOUND"},
		{
			"hive create",
			`CREATE TABLE "s3tablescatalog/mybucket".ns.h (id int) TBLPROPERTIES ('table_type'='HIVE')`,
			"ICEBERG",
		},
		{"missing namespace", `CREATE TABLE "s3tablescatalog/mybucket".nope.n (id int)`, "Database does not exist"},
		{"duplicate", `CREATE TABLE "s3tablescatalog/mybucket".ns.tbl (id int)`, "already exists"},
	}

	for _, tc := range fails {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Contains(t, e.failure(t, tc.sql), tc.want)
		})
	}
}

func TestS3TablesCatalog_DDLAndMetadata(t *testing.T) {
	t.Parallel()

	e := newIceEnv(t)
	src := &fakeS3Tables{tables: map[string]*athena.S3TableInfo{}}
	e.b.SetS3TablesSource(src)

	e.mustRun(t, `CREATE TABLE "s3tablescatalog/mybucket".ns.events (id bigint, name string, ts timestamp) `+
		`PARTITIONED BY (day(ts)) TBLPROPERTIES ('table_type'='ICEBERG')`)
	e.mustRun(t, `CREATE TABLE "s3tablescatalog/mybucket".ns.second (id int)`)
	e.mustRun(t, `INSERT INTO "s3tablescatalog/mybucket".ns.events VALUES (1, 'a', TIMESTAMP '2024-01-02 03:04:05')`)

	assert.Equal(t, [][]string{{"1", "a"}},
		e.mustRun(t, `SELECT id, name FROM "s3tablescatalog/mybucket".ns.events`))

	meta := parseMetadataAt(t, e.store, src.tables["mybucket/ns/events"].MetadataLocation)
	assert.Len(t, meta.DefaultSpec().Fields, 1)

	e.mustRun(t, `CREATE TABLE "s3tablescatalog/mybucket".ns.copy WITH (format = 'PARQUET') AS `+
		`SELECT id, name FROM "s3tablescatalog/mybucket".ns.events`)
	assert.Equal(t, [][]string{{"1", "a"}},
		e.mustRun(t, `SELECT id, name FROM "s3tablescatalog/mybucket".ns.copy`))

	dbs, err := e.b.ListDatabases("s3tablescatalog/mybucket")
	require.NoError(t, err)
	require.Len(t, dbs, 2)
	assert.Equal(t, "ns", dbs[0].Name)

	db, err := e.b.GetDatabase("s3tablescatalog/mybucket", "other")
	require.NoError(t, err)
	assert.Equal(t, "other", db.Name)

	_, err = e.b.GetDatabase("s3tablescatalog/mybucket", "ghost")
	require.Error(t, err)

	tables, err := e.b.ListTableMetadata("s3tablescatalog/mybucket", "ns", "")
	require.NoError(t, err)
	require.Len(t, tables, 3)
	assert.Equal(t, "copy", tables[0].Name)

	filtered, err := e.b.ListTableMetadata("s3tablescatalog/mybucket", "ns", "^ev")
	require.NoError(t, err)
	require.Len(t, filtered, 1)

	tm, err := e.b.GetTableMetadata("s3tablescatalog/mybucket", "ns", "events")
	require.NoError(t, err)
	require.Len(t, tm.Columns, 3)
	assert.Equal(t, "bigint", tm.Columns[0].Type)
	assert.Equal(t, "ICEBERG", tm.Parameters["table_type"])

	_, err = e.b.GetTableMetadata("s3tablescatalog/mybucket", "ns", "ghost")
	require.Error(t, err)

	e.mustRun(t, `DROP TABLE "s3tablescatalog/mybucket".ns.second`)
	assert.NotContains(t, src.tables, "mybucket/ns/second")
	assert.Contains(t, e.failure(t, `SELECT * FROM "s3tablescatalog/mybucket".ns.second`), "TABLE_NOT_FOUND")
}

func TestS3TablesCatalog_FailedCTASReleasesTable(t *testing.T) {
	t.Parallel()

	e := newIceEnv(t)
	src := &fakeS3Tables{tables: map[string]*athena.S3TableInfo{}}
	e.b.SetS3TablesSource(src)
	seedS3Table(t, e, src)

	assert.Contains(t, e.failure(t,
		`CREATE TABLE "s3tablescatalog/mybucket".ns.bad AS SELECT id, id FROM "s3tablescatalog/mybucket".ns.tbl`),
		"Duplicate column")
	assert.NotContains(t, src.tables, "mybucket/ns/bad")
}

func TestS3TablesCatalog_RowColumnDotAccess(t *testing.T) {
	t.Parallel()

	e := newIceEnv(t)
	src := &fakeS3Tables{tables: map[string]*athena.S3TableInfo{}}
	e.b.SetS3TablesSource(src)

	schema, err := iceberg.SchemaFromColumns([]iceberg.ColumnSpec{{Name: "id", Type: "int"}})
	require.NoError(t, err)

	schema.Fields = append(schema.Fields, iceberg.Field{
		Name: "r", ID: 10,
		Type: jsontext.Value(`{"type":"struct","fields":[{"id":11,"name":"a","required":false,"type":"int"}]}`),
	})

	const wh = "s3://abc123--table-s3/ns/nested"

	meta := iceberg.NewTableMetadata(iceberg.NewParams{
		Location: wh, Schema: schema, Spec: iceberg.PartitionSpec{Fields: []iceberg.PartitionField{}}, Now: time.Now(),
	})
	body, err := iceberg.EncodeMetadata(meta)
	require.NoError(t, err)

	loc := iceberg.InitialMetadataLocation(wh)
	putObj(t, e.store, loc, body)
	src.tables["mybucket/ns/nested"] = &athena.S3TableInfo{Location: wh, MetadataLocation: loc}

	assert.Empty(t, e.mustRun(t, `SELECT id, r.a FROM "s3tablescatalog/mybucket".ns.nested WHERE r.a = 1`))
}
