package iceberg_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
)

func TestTransformSpecVectors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want      any
		name      string
		transform string
		source    string
		in        string
	}{
		{
			name:      "bucket long 34",
			transform: "bucket[100]",
			source:    "long",
			in:        "34",
			want:      int32(2017239379 & 0x7fffffff % 100),
		},
		{
			name:      "bucket string",
			transform: "bucket[100]",
			source:    "string",
			in:        "iceberg",
			want:      int32(1210000089 % 100),
		},
		{name: "truncate int", transform: "truncate[10]", source: "int", in: "-1", want: int32(-10)},
		{name: "truncate string", transform: "truncate[3]", source: "string", in: "iceberg", want: "ice"},
		{name: "year", transform: "year", source: "date", in: "2017-11-16", want: int32(47)},
		{name: "month", transform: "month", source: "date", in: "2017-11-16", want: int32(574)},
		{name: "day", transform: "day", source: "date", in: "2017-11-16", want: int32(17486)},
		{name: "hour", transform: "hour", source: "timestamp", in: "2017-11-16 22:31:08", want: int32(419686)},
		{name: "identity date", transform: "identity", source: "date", in: "1970-01-02", want: int32(1)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tr, err := iceberg.ParseTransform(tc.transform)
			require.NoError(t, err)

			got, err := tr.Apply(tc.source, new(tc.in))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func testSchema(t *testing.T) iceberg.Schema {
	t.Helper()

	s, err := iceberg.SchemaFromColumns([]iceberg.ColumnSpec{
		{Name: "id", Type: "bigint"}, {Name: "category", Type: "string"}, {Name: "ts", Type: "timestamp"},
	})
	require.NoError(t, err)

	return s
}

func TestAppendRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		part map[string]any
		name string
		spec iceberg.PartitionSpec
	}{
		{
			name: "unpartitioned",
			spec: iceberg.PartitionSpec{Fields: []iceberg.PartitionField{}},
			part: map[string]any{},
		},
		{
			name: "partitioned",
			spec: iceberg.PartitionSpec{Fields: []iceberg.PartitionField{
				{Name: "category", Transform: "identity", SourceID: 2, FieldID: 1000},
				{Name: "id_bucket", Transform: "bucket[16]", SourceID: 1, FieldID: 1001},
				{Name: "ts_day", Transform: "day", SourceID: 3, FieldID: 1002},
			}},
			part: map[string]any{"category": "a", "id_bucket": int32(3), "ts_day": int32(19000)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			now := time.UnixMilli(1_700_000_000_000)
			meta := iceberg.NewTableMetadata(iceberg.NewParams{
				Location: "s3://b/tbl", Schema: testSchema(t), Spec: tc.spec, Now: now,
			})

			body, err := iceberg.EncodeMetadata(meta)
			require.NoError(t, err)

			parsed, err := iceberg.ParseMetadata(body)
			require.NoError(t, err)
			assert.Equal(t, iceberg.NoSnapshot, parsed.CurrentSnapshotID)

			files := []iceberg.DataFile{
				{
					Path:        "s3://b/tbl/data/f1.parquet",
					Format:      "PARQUET",
					Partition:   tc.part,
					RecordCount: 5,
					FileSize:    100,
					ValueCounts: map[int]int64{1: 5},
					NullCounts:  map[int]int64{1: 0},
				},
			}

			plan, err := parsed.PlanAppend("s3://b/tbl/metadata/00000.json", nil, files, now.Add(time.Second))
			require.NoError(t, err)
			require.Len(t, plan.Objects, 3)

			again, err := iceberg.ParseMetadata(plan.Objects[2].Body)
			require.NoError(t, err)

			snap, ok := again.CurrentSnapshot()
			require.True(t, ok)
			assert.Equal(t, "5", snap.Summary["total-records"])
			assert.Equal(t, int64(1), snap.SequenceNumber)

			list, err := iceberg.ReadManifestList(plan.Objects[1].Body)
			require.NoError(t, err)
			require.Len(t, list, 1)
			assert.Equal(t, plan.Objects[0].Location, list[0].Path)

			entries, err := iceberg.ReadManifest(plan.Objects[0].Body)
			require.NoError(t, err)
			require.Len(t, entries, 1)
			assert.Equal(t, files[0].Path, entries[0].DataFile.Path)
			assert.Equal(t, int64(5), entries[0].DataFile.RecordCount)
			assert.Equal(t, int64(5), entries[0].DataFile.ValueCounts[1])

			for k, v := range tc.part {
				assert.EqualValues(t, v, entries[0].DataFile.Partition[k])
			}

			p2, err := again.PlanAppend(plan.MetadataLocation, list, files, now.Add(2*time.Second))
			require.NoError(t, err)

			final, err := iceberg.ParseMetadata(p2.Objects[2].Body)
			require.NoError(t, err)
			assert.Len(t, final.Snapshots, 2)
			assert.Len(t, final.MetadataLog, 2)
			assert.Equal(t, "10", final.Snapshots[1].Summary["total-records"])

			old, err := final.SnapshotAsOf(now.Add(1500 * time.Millisecond))
			require.NoError(t, err)
			assert.Equal(t, final.Snapshots[0].SnapshotID, old.SnapshotID)

			_, err = final.SnapshotAsOf(now.Add(-time.Hour))
			require.ErrorIs(t, err, iceberg.ErrNoSnapshot)
		})
	}
}

func TestMetadataPreservesUnknownFields(t *testing.T) {
	t.Parallel()

	src := `{"format-version":2,"table-uuid":"u","location":"s3://b/t","last-sequence-number":0,
	"last-updated-ms":1,"last-column-id":1,"current-schema-id":0,
	"schemas":[{"type":"struct","schema-id":0,"fields":[{"id":1,"name":"a","required":false,"type":"int"}]}],
	"default-spec-id":0,"partition-specs":[{"spec-id":0,"fields":[]}],"last-partition-id":999,
	"default-sort-order-id":0,"sort-orders":[{"order-id":0,"fields":[]}],"properties":{},
	"current-snapshot-id":-1,"statistics":[{"snapshot-id":1}]}`

	m, err := iceberg.ParseMetadata([]byte(src))
	require.NoError(t, err)

	out, err := iceberg.EncodeMetadata(m)
	require.NoError(t, err)

	var generic map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &generic))
	assert.Contains(t, generic, "statistics")

	_, err = iceberg.ParseMetadata([]byte(`{"format-version":9}`))
	require.ErrorIs(t, err, iceberg.ErrInvalidMetadata)
}

func TestHiveType(t *testing.T) {
	t.Parallel()

	tests := []struct{ in, want string }{
		{`"long"`, "bigint"},
		{`"decimal(10, 2)"`, "decimal(10,2)"},
		{`{"type":"list","element-id":3,"element":"string","element-required":false}`, "array<string>"},
		{`{"type":"struct","fields":[{"id":1,"name":"a","required":false,"type":"int"}]}`, "struct<a:int>"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()

			got, err := iceberg.HiveType(json.RawMessage(tc.in))
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestParseTransformWidthBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{name: "max int32", in: "bucket[2147483647]"},
		{name: "over int32", in: "bucket[2147483648]", wantErr: true},
		{name: "huge", in: "truncate[99999999999999999999]", wantErr: true},
		{name: "zero", in: "bucket[0]", wantErr: true},
		{name: "negative", in: "bucket[-1]", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := iceberg.ParseTransform(tc.in)
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
