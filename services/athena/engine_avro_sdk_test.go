package athena_test

import (
	"bytes"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/iskorotkov/avro/v2/ocf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/athena"
)

const avroSerde = "org.apache.hadoop.hive.serde2.avro.AvroSerDe"

const eventsAvroSchema = `{"type":"record","name":"events","fields":[
 {"name":"id","type":"long"},
 {"name":"Name","type":["null","string"],"default":null},
 {"name":"score","type":["null","double"],"default":null},
 {"name":"ok","type":["null","boolean"],"default":null},
 {"name":"day","type":{"type":"int","logicalType":"date"}},
 {"name":"at","type":{"type":"long","logicalType":"timestamp-millis"}},
 {"name":"price","type":{"type":"bytes","logicalType":"decimal","precision":10,"scale":2}},
 {"name":"tags","type":{"type":"array","items":"string"}},
 {"name":"attrs","type":{"type":"map","values":"long"}},
 {"name":"owner","type":{"type":"record","name":"owner_t","fields":[{"name":"first","type":"string"},{"name":"age","type":"int"}]}}
]}`

func eventsAvroFile(t *testing.T, codec ocf.CodecName) []byte {
	t.Helper()

	var buf bytes.Buffer

	enc, err := ocf.NewEncoder(eventsAvroSchema, &buf, ocf.WithCodec(codec))
	require.NoError(t, err)

	day := time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)
	at := time.Date(2024, 3, 5, 12, 30, 45, 123000000, time.UTC)

	rows := []map[string]any{
		{
			"id": int64(1), "Name": "ann", "score": 1.5, "ok": true, "day": day, "at": at,
			"price": big.NewRat(1234, 100), "tags": []string{"a", "b"}, "attrs": map[string]int64{"x": 7},
			"owner": map[string]any{"first": "Ann", "age": int32(30)},
		},
		{
			"id": int64(2), "Name": nil, "score": nil, "ok": false, "day": day.AddDate(0, 0, 1), "at": at,
			"price": big.NewRat(5, 1), "tags": []string{}, "attrs": map[string]int64{},
			"owner": map[string]any{"first": "Bob", "age": int32(41)},
		},
	}

	for _, r := range rows {
		require.NoError(t, enc.Encode(r))
	}

	require.NoError(t, enc.Close())

	return buf.Bytes()
}

func eventsAvroDef() *athena.GlueTableDef {
	return &athena.GlueTableDef{
		Name: "events", Location: "s3://lake/events/", SerdeLibrary: avroSerde,
		InputFormat: "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
		Columns: []athena.Column{
			{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"}, {Name: "score", Type: "double"},
			{Name: "ok", Type: "boolean"}, {Name: "day", Type: "date"}, {Name: "at", Type: "timestamp"},
			{Name: "price", Type: "decimal(10,2)"}, {Name: "tags", Type: "array<string>"},
			{Name: "attrs", Type: "map<string,bigint>"}, {Name: "owner", Type: "struct<first:string,age:int>"},
		},
	}
}

func TestSDK_AvroRead(t *testing.T) {
	t.Parallel()

	for _, codec := range []ocf.CodecName{ocf.Null, ocf.Deflate, ocf.Snappy} {
		t.Run(string(codec), func(t *testing.T) {
			t.Parallel()

			client := sdkClientFor(t, map[string][]byte{"lake/events/a.avro": eventsAvroFile(t, codec)},
				map[string]*athena.GlueTableDef{"shop/events": eventsAvroDef()})

			cases := []struct {
				name string
				sql  string
				want [][]string
			}{
				{
					"scalars",
					"SELECT id, name, score, ok, day, at, price FROM events ORDER BY id",
					[][]string{
						{"1", "ann", "1.5", "true", "2024-03-05", "2024-03-05 12:30:45.123", "12.34"},
						{"2", "", "", "false", "2024-03-06", "2024-03-05 12:30:45.123", "5.00"},
					},
				},
				{
					"complex",
					"SELECT tags[1], attrs['x'], owner.first, owner.age FROM events WHERE id = 1",
					[][]string{{"a", "7", "Ann", "30"}},
				},
				{"filter", "SELECT count(*) FROM events WHERE name IS NULL AND ok = false", [][]string{{"1"}}},
			}

			for _, tc := range cases {
				got := sdkRun(t, client, tc.sql)
				assert.Equal(t, tc.want, got.rows, tc.name)
			}
		})
	}
}

func TestSDK_AvroWriteRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		compress string
	}{
		{"none", ""},
		{"snappy", ", write_compression = 'SNAPPY'"},
		{"deflate", ", write_compression = 'DEFLATE'"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, store := newEngineBackend(t,
				map[string][]byte{"lake/events/a.avro": eventsAvroFile(t, ocf.Null)},
				map[string]*athena.GlueTableDef{"shop/events": eventsAvroDef()})
			client := newTestAthenaClient(t, athena.NewHandler(b))

			sdkRun(t, client, "CREATE DATABASE shop")
			sdkRun(t, client, "CREATE TABLE shop.copy WITH (format = 'AVRO', external_location = 's3://lake/copy/'"+
				tc.compress+") AS SELECT id, name, score, ok, day, at, price FROM events")

			store.mu.Lock()

			var file []byte

			for k, v := range store.objects {
				if strings.HasPrefix(k, "lake/copy/") {
					file = v
				}
			}
			store.mu.Unlock()

			require.NotEmpty(t, file)

			dec, err := ocf.NewDecoder(bytes.NewReader(file))
			require.NoError(t, err)

			var rec map[string]any

			require.True(t, dec.HasNext())
			require.NoError(t, dec.Decode(&rec))
			assert.EqualValues(t, 1, rec["id"])
			assert.Equal(t, "ann", rec["name"])
			assert.Equal(t, true, rec["ok"])
			assert.Contains(t, string(dec.Metadata()["avro.schema"]), `"logicalType":"timestamp-millis"`)

			got := sdkRun(t, client, "SELECT id, name, ok, day, at, price FROM shop.copy ORDER BY id")
			assert.Equal(t, [][]string{
				{"1", "ann", "true", "2024-03-05", "2024-03-05 12:30:45.123", "12.34"},
				{"2", "", "false", "2024-03-06", "2024-03-05 12:30:45.123", "5.00"},
			}, got.rows)

			sdkRun(t, client, "INSERT INTO shop.copy SELECT id + 10, name, score, ok, day, at, price "+
				"FROM events WHERE id = 1")

			got = sdkRun(t, client, "SELECT count(*), max(id) FROM shop.copy")
			assert.Equal(t, [][]string{{"3", "11"}}, got.rows)
		})
	}
}

func TestSDK_AvroInsertNestedColumns(t *testing.T) {
	t.Parallel()

	b, store := newEngineBackend(t,
		map[string][]byte{"lake/events/a.avro": eventsAvroFile(t, ocf.Null)},
		map[string]*athena.GlueTableDef{"shop/events": eventsAvroDef()})
	client := newTestAthenaClient(t, athena.NewHandler(b))

	sdkRun(t, client, "INSERT INTO events SELECT id + 10, name, score, ok, day, at, price, tags, attrs, owner "+
		"FROM events WHERE id = 1")

	got := sdkRun(t, client, "SELECT id, tags[2], attrs['x'], owner.first, owner.age FROM events WHERE id > 5")
	assert.Equal(t, [][]string{{"11", "b", "7", "Ann", "30"}}, got.rows)

	store.mu.Lock()
	defer store.mu.Unlock()

	var written []byte

	for k, v := range store.objects {
		if strings.HasPrefix(k, "lake/events/") && k != "lake/events/a.avro" {
			written = v
		}
	}

	dec, err := ocf.NewDecoder(bytes.NewReader(written))
	require.NoError(t, err)

	var rec map[string]any

	require.True(t, dec.HasNext())
	require.NoError(t, dec.Decode(&rec))
	assert.Equal(t, map[string]any{"array": []any{"a", "b"}}, rec["tags"])
	assert.Equal(t, map[string]any{"map": map[string]any{"x": int64(7)}}, rec["attrs"])
	assert.Equal(t, map[string]any{"struct_1": map[string]any{"first": "Ann", "age": 30}}, rec["owner"])
}

func TestSDK_AvroWriteErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, sql, reason string
	}{
		{
			"gzip", "CREATE TABLE shop.o WITH (format = 'AVRO', write_compression = 'GZIP', external_location = 's3://lake/o/') " +
				"AS SELECT id FROM events",
			"compression GZIP is not supported for AVRO",
		},
		{
			"orc", "CREATE TABLE shop.o WITH (format = 'ORC', external_location = 's3://lake/o/') AS SELECT id FROM events",
			"format ORC is not supported",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := sdkClientFor(t, map[string][]byte{"lake/events/a.avro": eventsAvroFile(t, ocf.Null)},
				map[string]*athena.GlueTableDef{"shop/events": eventsAvroDef()})

			sdkRun(t, client, "CREATE DATABASE shop")
			assert.Contains(t, sdkFailure(t, client, tc.sql), tc.reason)
		})
	}
}
