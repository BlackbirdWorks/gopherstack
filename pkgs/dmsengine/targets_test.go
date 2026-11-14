package dmsengine_test

import (
	"context"
	"encoding/json/v2"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

type memObjects struct {
	objs map[string]string
	mu   sync.Mutex
}

func (m *memObjects) PutObject(_ context.Context, bucket, key string, body []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.objs == nil {
		m.objs = map[string]string{}
	}

	m.objs[bucket+"/"+key] = string(body)

	return nil
}

func (m *memObjects) ListObjects(_ context.Context, bucket, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var keys []string

	for k := range m.objs {
		if rest, ok := strings.CutPrefix(k, bucket+"/"); ok && strings.HasPrefix(rest, prefix) {
			keys = append(keys, rest)
		}
	}

	sort.Strings(keys)

	return keys, nil
}

func (m *memObjects) GetObject(_ context.Context, bucket, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	body, ok := m.objs[bucket+"/"+key]
	if !ok {
		return nil, errBoom
	}

	return []byte(body), nil
}

func (m *memObjects) CheckBucket(_ context.Context, bucket string) error {
	if bucket == "missing" {
		return errBoom
	}

	return nil
}

type memStream struct {
	recs []string
	keys []string
}

func (m *memStream) PutRecord(_ context.Context, _, key string, data []byte) error {
	m.recs, m.keys = append(m.recs, string(data)), append(m.keys, key)

	return nil
}

func (m *memStream) CheckStream(context.Context, string) error { return nil }

type memDynamo struct {
	keys    map[string][]dmsengine.DynamoKey
	items   map[string]map[string]dmsengine.DynamoValue
	dropped []string
}

func newMemDynamo() *memDynamo {
	return &memDynamo{keys: map[string][]dmsengine.DynamoKey{}, items: map[string]map[string]dmsengine.DynamoValue{}}
}

func (m *memDynamo) CreateTable(_ context.Context, name string, keys []dmsengine.DynamoKey) error {
	m.keys[name] = keys

	return nil
}

func (m *memDynamo) DropTable(_ context.Context, name string) error {
	m.dropped = append(m.dropped, name)

	return nil
}

func (m *memDynamo) PutItem(_ context.Context, table string, item map[string]dmsengine.DynamoValue) error {
	m.items[table+"|"+item["id"].S] = item

	return nil
}

func (m *memDynamo) DeleteItem(_ context.Context, table string, key map[string]dmsengine.DynamoValue) error {
	delete(m.items, table+"|"+key["id"].S)

	return nil
}

func itemsTarget() dmsengine.TargetTable {
	return dmsengine.TargetTable{
		SourceFamily: dmsengine.FamilyMySQL, Schema: "app", Name: "items", PK: []string{"id"},
		Columns: []dmsengine.TargetColumn{
			{Name: "id", Native: "int", Kind: dmsengine.KindInt, Source: 0},
			{Name: "name", Native: "varchar(10)", Nullable: true, Source: 1},
		},
	}
}

func TestS3TargetCSV(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want     map[string]string
		name     string
		settings string
	}{
		{
			name:     "defaults",
			settings: `{"BucketName":"b","BucketFolder":"out"}`,
			want: map[string]string{
				"b/out/app/items/LOAD00000001.csv": "1,alpha\n2,NULL\n3,\"x,y\"\n",
				"b/out/app/items/cdc":              "I,4,new\nU,1,ALPHA\nD,2,NULL\n",
			},
		},
		{
			name: "custom delimiters header op and null",
			settings: `{"BucketName":"b","CsvDelimiter":"|","CsvRowDelimiter":"\r\n","CsvNullValue":"\\N",` +
				`"AddColumnName":true,"IncludeOpForFullLoad":true}`,
			want: map[string]string{
				"b/app/items/LOAD00000001.csv": "Op|id|name\r\nI|1|alpha\r\nI|2|\\N\r\nI|3|x,y\r\n",
				"b/app/items/cdc":              "Op|id|name\r\nI|4|new\r\nU|1|ALPHA\r\nD|2|\\N\r\n",
			},
		},
		{
			name:     "inserts only drops the op column and other ops",
			settings: `{"BucketName":"b","CdcInsertsOnly":true}`,
			want: map[string]string{
				"b/app/items/LOAD00000001.csv": "1,alpha\n2,NULL\n3,\"x,y\"\n",
				"b/app/items/cdc":              "4,new\n",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := &memObjects{}
			tgt, err := dmsengine.NewOpener(dmsengine.Stores{Objects: store}).OpenTarget(
				t.Context(), dmsengine.EndpointConfig{Engine: "s3", S3Settings: tt.settings}, dmsengine.FamilyMySQL)
			require.NoError(t, err)

			tab := itemsTarget()
			require.NoError(t, tgt.Prepare(t.Context(), tab, dmsengine.PrepDropAndCreate))
			require.NoError(t, tgt.Load(t.Context(), tab, []dmsengine.Row{{"1", "alpha"}, {"2", nil}, {"3", "x,y"}}))
			require.NoError(t, tgt.EndLoad(t.Context(), tab))
			require.NoError(
				t,
				tgt.Apply(t.Context(), tab, dmsengine.Change{Op: dmsengine.OpInsert, After: dmsengine.Row{"4", "new"}}),
			)
			require.NoError(t, tgt.Apply(t.Context(), tab, dmsengine.Change{
				Op: dmsengine.OpUpdate, Before: dmsengine.Row{"1", "alpha"}, After: dmsengine.Row{"1", "ALPHA"},
			}))
			require.NoError(
				t,
				tgt.Apply(t.Context(), tab, dmsengine.Change{Op: dmsengine.OpDelete, Before: dmsengine.Row{"2", nil}}),
			)
			require.NoError(t, tgt.Commit(t.Context()))

			got := map[string]string{}

			for k, v := range store.objs {
				if strings.Contains(k, "LOAD") {
					got[k] = v
				} else {
					got[k[:strings.LastIndex(k, "/")]+"/cdc"] = v
					assert.Regexp(t, `/\d{8}-\d{6}\d{4}\.csv$`, k)
				}
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestS3TargetRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stores   dmsengine.Stores
		name     string
		settings string
	}{
		{name: "no bucket", settings: `{}`, stores: dmsengine.Stores{Objects: &memObjects{}}},
		{name: "bad json", settings: `{`, stores: dmsengine.Stores{Objects: &memObjects{}}},
		{name: "no store", settings: `{"BucketName":"b"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := dmsengine.NewOpener(tt.stores).OpenTarget(
				t.Context(), dmsengine.EndpointConfig{Engine: "s3", S3Settings: tt.settings}, dmsengine.FamilyMySQL)
			require.Error(t, err)
		})
	}
}

func TestEndpointPing(t *testing.T) {
	t.Parallel()

	o := dmsengine.NewOpener(dmsengine.Stores{Objects: &memObjects{}})

	require.NoError(t, dmsengine.TestEndpoint(t.Context(), o, dmsengine.EndpointConfig{
		Engine: "s3", S3Settings: `{"BucketName":"b"}`,
	}, false))
	require.Error(t, dmsengine.TestEndpoint(t.Context(), o, dmsengine.EndpointConfig{
		Engine: "s3", S3Settings: `{"BucketName":"missing"}`,
	}, false))
	require.ErrorIs(t, dmsengine.TestEndpoint(t.Context(), o, dmsengine.EndpointConfig{Engine: "oracle"}, true),
		dmsengine.ErrUnsupportedEngine)
}

func TestKinesisTargetRecords(t *testing.T) {
	t.Parallel()

	store := &memStream{}
	tgt, err := dmsengine.NewOpener(dmsengine.Stores{Streams: store}).OpenTarget(t.Context(), dmsengine.EndpointConfig{
		Engine:          "kinesis",
		KinesisSettings: `{"StreamArn":"arn:aws:kinesis:us-east-1:000000000000:stream/s","MessageFormat":"json-unformatted"}`,
	}, dmsengine.FamilyMySQL)
	require.NoError(t, err)

	tab := itemsTarget()
	require.NoError(t, tgt.Load(t.Context(), tab, []dmsengine.Row{{"1", "alpha"}, {"2", nil}}))
	require.NoError(t, tgt.Apply(t.Context(), tab, dmsengine.Change{
		Op:     dmsengine.OpDelete,
		Before: dmsengine.Row{"1", "alpha"},
		Time:   time.Date(2024, 1, 2, 3, 4, 5, 123456000, time.UTC),
	}))

	require.Len(t, store.recs, 3)
	assert.Equal(t, []string{"app.items", "app.items", "app.items"}, store.keys)
	assert.NotContains(t, store.recs[0], "\n")

	var first, second, del map[string]map[string]any

	require.NoError(t, json.Unmarshal([]byte(store.recs[0]), &first, jsonx.V1()))
	require.NoError(t, json.Unmarshal([]byte(store.recs[1]), &second, jsonx.V1()))
	require.NoError(t, json.Unmarshal([]byte(store.recs[2]), &del, jsonx.V1()))

	assert.Equal(t, map[string]any{"id": float64(1), "name": "alpha"}, first["data"])
	assert.Equal(t, map[string]any{"id": float64(2)}, second["data"], "nulls are omitted by default")
	assert.Equal(t, "load", first["metadata"]["operation"])
	assert.Equal(t, "delete", del["metadata"]["operation"])
	assert.Equal(t, "2024-01-02T03:04:05.123456Z", del["metadata"]["timestamp"])
	assert.Equal(t, "schema-table", del["metadata"]["partition-key-type"])
}

func TestKinesisTargetControlRecordAndNulls(t *testing.T) {
	t.Parallel()

	store := &memStream{}
	tgt, err := dmsengine.NewOpener(dmsengine.Stores{Streams: store}).OpenTarget(t.Context(), dmsengine.EndpointConfig{
		Engine: "kinesis",
		KinesisSettings: `{"StreamArn":"arn:aws:kinesis:us-east-1:000000000000:stream/s",` +
			`"IncludeControlDetails":true,"IncludeNullAndEmpty":true}`,
	}, dmsengine.FamilyMySQL)
	require.NoError(t, err)

	tab := itemsTarget()
	require.NoError(t, tgt.Prepare(t.Context(), tab, dmsengine.PrepDropAndCreate))
	require.NoError(t, tgt.Load(t.Context(), tab, []dmsengine.Row{{"2", nil}}))

	require.Len(t, store.recs, 2)
	assert.Contains(t, store.recs[0], `"record-type": "control"`)
	assert.Contains(t, store.recs[0], `"operation": "create-table"`)
	assert.Contains(t, store.recs[1], `"name": null`)
}

func TestDynamoTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		mode        dmsengine.PrepMode
		pk          []string
		wantErr     bool
		wantDropped bool
	}{
		{name: "creates table keyed by pk", pk: []string{"id"}, mode: dmsengine.PrepDropAndCreate, wantDropped: true},
		{name: "do nothing keeps table", pk: []string{"id"}, mode: dmsengine.PrepDoNothing},
		{name: "no key fails", pk: nil, mode: dmsengine.PrepDropAndCreate, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := newMemDynamo()
			tgt, err := dmsengine.NewOpener(dmsengine.Stores{Dynamo: store}).OpenTarget(
				t.Context(), dmsengine.EndpointConfig{Engine: "dynamodb"}, dmsengine.FamilyMySQL)
			require.NoError(t, err)

			tab := itemsTarget()
			tab.PK = tt.pk

			err = tgt.Prepare(t.Context(), tab, tt.mode)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, []dmsengine.DynamoKey{{Name: "id", Number: true}}, store.keys["items"])
			assert.Equal(t, tt.wantDropped, len(store.dropped) == 1)

			require.NoError(t, tgt.Load(t.Context(), tab, []dmsengine.Row{{"1", "alpha"}, {"2", nil}}))
			assert.Equal(t, dmsengine.DynamoValue{Type: "N", S: "1"}, store.items["items|1"]["id"])
			assert.Equal(t, dmsengine.DynamoValue{Type: "S", S: "alpha"}, store.items["items|1"]["name"])
			assert.NotContains(t, store.items["items|2"], "name")

			require.NoError(t, tgt.Apply(t.Context(), tab, dmsengine.Change{
				Op: dmsengine.OpUpdate, Before: dmsengine.Row{"1", "alpha"}, After: dmsengine.Row{"1", "beta"},
			}))
			assert.Equal(t, "beta", store.items["items|1"]["name"].S)

			require.NoError(
				t,
				tgt.Apply(t.Context(), tab, dmsengine.Change{Op: dmsengine.OpDelete, Before: dmsengine.Row{"2", nil}}),
			)

			keys := make([]string, 0, len(store.items))
			for k := range store.items {
				keys = append(keys, k)
			}

			sort.Strings(keys)
			assert.Equal(t, []string{"items|1"}, keys)
		})
	}
}

func TestS3TargetParquet(t *testing.T) {
	t.Parallel()

	store := &memObjects{}
	tgt, err := dmsengine.NewOpener(dmsengine.Stores{Objects: store}).OpenTarget(
		t.Context(), dmsengine.EndpointConfig{Engine: "s3", S3Settings: `{"BucketName":"b","DataFormat":"parquet"}`},
		dmsengine.FamilyMySQL)
	require.NoError(t, err)

	tab := itemsTarget()
	require.NoError(t, tgt.Prepare(t.Context(), tab, dmsengine.PrepDropAndCreate))
	require.NoError(t, tgt.Load(t.Context(), tab, []dmsengine.Row{{"1", "alpha"}, {"2", nil}}))
	require.NoError(t, tgt.EndLoad(t.Context(), tab))
	require.NoError(
		t,
		tgt.Apply(t.Context(), tab, dmsengine.Change{Op: dmsengine.OpInsert, After: dmsengine.Row{"3", "new"}}),
	)
	require.NoError(t, tgt.Commit(t.Context()))

	var load, cdc string

	for k, v := range store.objs {
		if strings.Contains(k, "LOAD00000001.parquet") {
			load = v
		} else if strings.HasSuffix(k, ".parquet") {
			cdc = v
		}
	}

	read := func(data string) [][]any {
		f, oerr := parquetio.Open([]byte(data))
		require.NoError(t, oerr)

		var rows [][]any

		require.NoError(t, f.ForEach(f.Columns(), func(vals []any) error {
			rows = append(rows, append([]any(nil), vals...))

			return nil
		}))

		return rows
	}

	assert.Equal(t, [][]any{{int64(1), "alpha"}, {int64(2), nil}}, read(load))
	assert.Equal(t, [][]any{{"I", int64(3), "new"}}, read(cdc))
}
