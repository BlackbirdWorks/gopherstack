package athena_test

import (
	"encoding/binary"
	"hash/crc32"
	"maps"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

const z85Alphabet = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.-:+=^!/*?&<>()[]{}@%$#"

func z85Encode(b []byte) string {
	var out strings.Builder

	for i := 0; i < len(b); i += 4 {
		v := binary.BigEndian.Uint32(b[i:])
		g := make([]byte, 5)

		for j := 4; j >= 0; j-- {
			g[j] = z85Alphabet[v%85]
			v /= 85
		}

		out.Write(g)
	}

	return out.String()
}

func portableDV(vals ...uint16) []byte {
	b := binary.LittleEndian.AppendUint32(nil, 1681511377)
	b = binary.LittleEndian.AppendUint64(b, 1)
	b = binary.LittleEndian.AppendUint32(b, 0)
	b = binary.LittleEndian.AppendUint32(b, 12346)
	b = binary.LittleEndian.AppendUint32(b, 1)
	b = binary.LittleEndian.AppendUint16(b, 0)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(vals)-1))
	b = binary.LittleEndian.AppendUint32(b, 16)

	for _, v := range vals {
		b = binary.LittleEndian.AppendUint16(b, v)
	}

	return b
}

func seedDeltaDV(t *testing.T, e *iceEnv, protocol, dv string) {
	t.Helper()

	cols := []parquetio.Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"}}
	rows := [][]any{{int64(0), "a"}, {int64(1), "b"}, {int64(2), "c"}, {int64(3), "d"}}
	putParquet(t, e.store, "dv/part-0.parquet", cols, rows)

	schema := `{"type":"struct","fields":[` +
		`{"name":"id","type":"long","nullable":true,"metadata":{}},` +
		`{"name":"name","type":"string","nullable":true,"metadata":{}}]}`
	e.store.objects["lake/dv/_delta_log/00000000000000000000.json"] = deltaCommit(
		protocol,
		`{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":"`+strings.ReplaceAll(schema, `"`, `\"`)+
			`","partitionColumns":[],"configuration":{}}}`,
		`{"add":{"path":"part-0.parquet","partitionValues":{},"size":10,"dataChange":true,"deletionVector":`+dv+`}}`,
	)
}

func TestDelta_DeletionVectors(t *testing.T) {
	t.Parallel()

	const proto = `{"protocol":{"minReaderVersion":3,"minWriterVersion":7,` +
		`"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}`

	inline := portableDV(1, 3)
	file := []byte{1}
	file = binary.BigEndian.AppendUint32(file, uint32(len(inline)))
	file = append(file, inline...)
	file = binary.BigEndian.AppendUint32(file, crc32.ChecksumIEEE(inline))

	uuid := []byte{0xd2, 0xc6, 0x39, 0xaa, 0x88, 0x16, 0x43, 0x1a, 0xaa, 0xf6, 0xd3, 0xfe, 0x25, 0x12, 0xff, 0x61}

	cases := []struct {
		name string
		dv   string
		file map[string][]byte
		want [][]string
	}{
		{
			"inline",
			`{"storageType":"i","pathOrInlineDv":"` + z85Encode(inline) + `","sizeInBytes":` +
				itoa(len(inline)) + `,"cardinality":2}`,
			nil, [][]string{{"0"}, {"2"}},
		},
		{
			"relative file",
			`{"storageType":"u","pathOrInlineDv":"ab` + z85Encode(uuid) + `","offset":1,"sizeInBytes":` +
				itoa(len(inline)) + `,"cardinality":2}`,
			map[string][]byte{"lake/dv/ab/deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin": file},
			[][]string{{"0"}, {"2"}},
		},
		{
			"absolute file",
			`{"storageType":"p","pathOrInlineDv":"s3://lake/elsewhere/dv.bin","offset":1,"sizeInBytes":` +
				itoa(len(inline)) + `,"cardinality":2}`,
			map[string][]byte{"lake/elsewhere/dv.bin": file},
			[][]string{{"0"}, {"2"}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newIceEnv(t)
			seedDeltaDV(t, e, proto, tc.dv)

			maps.Copy(e.store.objects, tc.file)

			e.mustRun(t, "CREATE EXTERNAL TABLE dv LOCATION 's3://lake/dv/' TBLPROPERTIES ('table_type'='DELTA')")
			assert.Equal(t, tc.want, e.mustRun(t, "SELECT id FROM dv ORDER BY id"))
			assert.Equal(t, [][]string{{"2"}}, e.mustRun(t, "SELECT count(*) FROM dv"))
		})
	}
}

func TestDelta_ReaderFeatures(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, protocol, want string
	}{
		{
			"timestampNtz",
			`{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["timestampNtz"]}}`,
			"",
		},
		{
			"v2Checkpoint",
			`{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["v2Checkpoint"]}}`,
			"v2Checkpoint",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newIceEnv(t)
			seedDeltaDV(t, e, tc.protocol, "null")

			sql := "CREATE EXTERNAL TABLE dv LOCATION 's3://lake/dv/' TBLPROPERTIES ('table_type'='DELTA')"
			if tc.want == "" {
				e.mustRun(t, sql)
				assert.Equal(t, [][]string{{"4"}}, e.mustRun(t, "SELECT count(*) FROM dv"))

				return
			}

			assert.Contains(t, e.failure(t, sql), tc.want)
		})
	}
}

func TestDelta_ColumnMappingAndNtz(t *testing.T) {
	t.Parallel()

	const proto = `{"protocol":{"minReaderVersion":3,"minWriterVersion":7,` +
		`"readerFeatures":["columnMapping","timestampNtz"],"writerFeatures":["columnMapping","timestampNtz"]}}`

	cases := []struct {
		name, mode string
		fileCols   []parquetio.Column
	}{
		{"name mode", "name", []parquetio.Column{{Name: "col-aaa", Type: "bigint"}, {Name: "col-bbb", Type: "string"}}},
		{
			"id mode reads by field id",
			"id",
			[]parquetio.Column{
				{Name: "renamed1", Type: "bigint", FieldID: 1},
				{Name: "renamed2", Type: "string", FieldID: 2},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newIceEnv(t)
			putParquet(t, e.store, "cm/part-0.parquet", tc.fileCols, [][]any{{int64(7), "x"}})

			field := func(name, typ string, id int, phys string) string {
				return `{"name":"` + name + `","type":"` + typ + `","nullable":true,"metadata":` +
					`{"delta.columnMapping.id":` + itoa(id) + `,"delta.columnMapping.physicalName":"` + phys + `"}}`
			}
			schema := `{"type":"struct","fields":[` + field("id", "long", 1, "col-aaa") + `,` +
				field("name", "string", 2, "col-bbb") + `,` + field("at", "timestamp_ntz", 3, "col-ccc") + `]}`
			e.store.objects["lake/cm/_delta_log/00000000000000000000.json"] = deltaCommit(
				proto,
				`{"metaData":{"id":"x","format":{"provider":"parquet"},"schemaString":"`+strings.ReplaceAll(
					schema,
					`"`,
					`\"`,
				)+
					`","partitionColumns":[],"configuration":{"delta.columnMapping.mode":"`+tc.mode+`"}}}`,
				`{"add":{"path":"part-0.parquet","partitionValues":{},"size":10,"dataChange":true}}`,
			)

			e.mustRun(t, "CREATE EXTERNAL TABLE cm LOCATION 's3://lake/cm/' TBLPROPERTIES ('table_type'='DELTA')")
			assert.Equal(t, "timestamp", e.cat.defs["shop/cm"].Columns[2].Type)
			assert.Equal(t, [][]string{{"7", "x"}}, e.mustRun(t, "SELECT id, name FROM cm"))
		})
	}
}
