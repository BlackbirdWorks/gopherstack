package athena_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

const deltaSchema = `{"type":"struct","fields":[` +
	`{"name":"id","type":"long","nullable":true,"metadata":{}},` +
	`{"name":"name","type":"string","nullable":true,"metadata":{}},` +
	`{"name":"dt","type":"string","nullable":true,"metadata":{}}]}`

func deltaCommit(lines ...string) []byte { return []byte(strings.Join(lines, "\n") + "\n") }

func seedDelta(t *testing.T, e *iceEnv) {
	t.Helper()

	cols := []parquetio.Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"}}
	putParquet(t, e.store, "dl/part-0.parquet", cols, [][]any{{int64(1), "old"}})
	putParquet(t, e.store, "dl/part-1.parquet", cols, [][]any{{int64(2), "b"}})
	putParquet(t, e.store, "dl/dt=2024-01-02/part-2.parquet", cols, [][]any{{int64(3), "c"}, {int64(4), "d"}})

	schema := strings.ReplaceAll(deltaSchema, `"`, `\"`)
	e.store.objects["lake/dl/_delta_log/00000000000000000000.json"] = deltaCommit(
		`{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}`,
		`{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":"`+schema+
			`","partitionColumns":["dt"],"configuration":{}}}`,
		`{"add":{"path":"part-0.parquet","partitionValues":{"dt":"2024-01-01"},"size":10,"dataChange":true}}`,
		`{"add":{"path":"part-1.parquet","partitionValues":{"dt":"2024-01-01"},"size":10,"dataChange":true}}`,
	)
	e.store.objects["lake/dl/_delta_log/00000000000000000001.json"] = deltaCommit(
		`{"remove":{"path":"part-0.parquet","deletionTimestamp":1,"dataChange":true}}`,
		`{"add":{"path":"dt%3D2024-01-02/part-2.parquet","partitionValues":{"dt":"2024-01-02"},"size":10,"dataChange":true}}`,
	)
}

func TestDelta_CreateAndSelect(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want [][]string
	}{
		{"active files only", "SELECT id, name, dt FROM dl ORDER BY id",
			[][]string{{"2", "b", "2024-01-01"}, {"3", "c", "2024-01-02"}, {"4", "d", "2024-01-02"}}},
		{"partition filter", "SELECT count(*) FROM dl WHERE dt = '2024-01-02'", [][]string{{"2"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newIceEnv(t)
			seedDelta(t, e)
			e.mustRun(t, "CREATE EXTERNAL TABLE dl LOCATION 's3://lake/dl/' TBLPROPERTIES ('table_type'='DELTA')")

			def := e.cat.defs["shop/dl"]
			require.NotNil(t, def)
			assert.Equal(t, "DELTA", def.Parameters["table_type"])
			assert.Equal(t, []athena.Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"}}, def.Columns)
			assert.Equal(t, []athena.Column{{Name: "dt", Type: "string"}}, def.PartitionKeys)

			assert.Equal(t, tc.want, e.mustRun(t, tc.sql))
		})
	}
}

func TestDelta_Errors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		setup      func(e *iceEnv)
		sql        string
		want       string
		registered bool
	}{
		{
			name: "no transaction log",
			sql:  "CREATE EXTERNAL TABLE d LOCATION 's3://lake/none/' TBLPROPERTIES ('table_type'='DELTA')",
			want: "is not a Delta Lake table",
		},
		{
			name: "columns not allowed",
			sql:  "CREATE EXTERNAL TABLE d (id int) LOCATION 's3://lake/dl/' TBLPROPERTIES ('table_type'='DELTA')",
			want: "accepts only LOCATION",
		},
		{
			name:  "insert is read only",
			setup: func(e *iceEnv) { seedDelta(t, e) }, registered: true,
			sql:  "INSERT INTO dl VALUES (9, 'x', 'y')",
			want: "Delta Lake tables are read-only",
		},
		{
			name:  "time travel unsupported",
			setup: func(e *iceEnv) { seedDelta(t, e) }, registered: true,
			sql:  "SELECT * FROM dl FOR VERSION AS OF 0",
			want: "time travel is not supported for Delta Lake",
		},
		{
			name: "reader version too new",
			setup: func(e *iceEnv) {
				e.store.objects["lake/new/_delta_log/00000000000000000000.json"] = deltaCommit(
					`{"protocol":{"minReaderVersion":9,"minWriterVersion":9}}`)
			},
			sql:  "CREATE EXTERNAL TABLE d LOCATION 's3://lake/new/' TBLPROPERTIES ('table_type'='DELTA')",
			want: "NOT_SUPPORTED",
		},
		{
			name: "missing commit",
			setup: func(e *iceEnv) {
				e.store.objects["lake/gap/_delta_log/00000000000000000000.json"] = deltaCommit(
					`{"metaData":{"schemaString":"{\"type\":\"struct\",\"fields\":[]}","partitionColumns":[]}}`)
				e.store.objects["lake/gap/_delta_log/00000000000000000002.json"] = deltaCommit(`{}`)
			},
			sql:  "CREATE EXTERNAL TABLE d LOCATION 's3://lake/gap/' TBLPROPERTIES ('table_type'='DELTA')",
			want: "DELTA_LAKE_BAD_DATA",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newIceEnv(t)
			if tc.setup != nil {
				tc.setup(e)
			}

			if tc.registered {
				e.mustRun(t, "CREATE EXTERNAL TABLE dl LOCATION 's3://lake/dl/' TBLPROPERTIES ('table_type'='DELTA')")
			}

			assert.Contains(t, e.failure(t, tc.sql), tc.want)
		})
	}
}
