package athena_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

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
			"create unsupported", `CREATE TABLE "s3tablescatalog/mybucket".ns.n (id int) LOCATION 's3://x/y/' ` +
				`TBLPROPERTIES ('table_type'='ICEBERG')`,
			"S3 Tables API",
		},
	}

	for _, tc := range fails {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Contains(t, e.failure(t, tc.sql), tc.want)
		})
	}
}
