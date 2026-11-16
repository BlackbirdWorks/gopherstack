package athena_test

import (
	"bytes"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
	"github.com/blackbirdworks/gopherstack/pkgs/orcio"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

// seedFile is one file of a hand-built Iceberg snapshot.
type seedFile struct {
	part    map[string]any
	bounds  map[int][2]int64
	format  string
	raw     []byte
	cols    []parquetio.Column
	rows    [][]any
	eqIDs   []int
	content int
	seq     int64
}

// seedIceberg registers table shop.<name> (id bigint #10, label string #20, region string #30) holding files.
func seedIceberg(t *testing.T, e *iceEnv, name string, partitioned bool, files []seedFile, props ...string) {
	t.Helper()

	str, _ := iceberg.FromHive("string")
	lng, _ := iceberg.FromHive("bigint")
	schema := iceberg.Schema{Fields: []iceberg.Field{
		{ID: 10, Name: "id", Type: []byte(`"` + lng + `"`)},
		{ID: 20, Name: "label", Type: []byte(`"` + str + `"`)},
		{ID: 30, Name: "region", Type: []byte(`"` + str + `"`)},
	}}
	spec := iceberg.PartitionSpec{Fields: []iceberg.PartitionField{}}

	if partitioned {
		spec.Fields = []iceberg.PartitionField{{Name: "region", Transform: "identity", SourceID: 30, FieldID: 1000}}
	}

	now := time.Now().UTC()
	loc := "s3://lake/" + name
	properties := map[string]string{}
	for i := 0; i+1 < len(props); i += 2 {
		properties[props[i]] = props[i+1]
	}

	meta := iceberg.NewTableMetadata(iceberg.NewParams{
		Location: loc, Schema: schema, Spec: spec, Now: now, Properties: properties,
	})
	snapID := int64(4242)

	list := make([]iceberg.ManifestFile, 0, len(files))

	var maxSeq int64

	totals := map[string]int64{}

	for i, f := range files {
		key := fmt.Sprintf("%s/data/f%d.parquet", name, i)
		format := "PARQUET"

		var size int

		if f.format != "" {
			format = f.format
			key = fmt.Sprintf("%s/data/f%d.%s", name, i, strings.ToLower(f.format))
			putObj(t, e.store, "s3://lake/"+key, f.raw)
			size = len(f.raw)
		} else {
			size = putParquet(t, e.store, key, f.cols, f.rows)
		}
		seq := f.seq

		df := iceberg.DataFile{
			Path: "s3://lake/" + key, Format: format, Content: f.content, Partition: map[string]any{},
			RecordCount: int64(len(f.rows)), FileSize: int64(size), EqualityIDs: f.eqIDs,
			LowerBounds: map[int][]byte{}, UpperBounds: map[int][]byte{},
		}

		maps.Copy(df.Partition, f.part)

		for id, b := range f.bounds {
			df.LowerBounds[id], _ = iceberg.EncodeBound("long", b[0])
			df.UpperBounds[id], _ = iceberg.EncodeBound("long", b[1])
		}

		entry := iceberg.ManifestEntry{
			Status:         iceberg.StatusAdded,
			SnapshotID:     &snapID,
			SequenceNumber: &seq,
			DataFile:       df,
		}
		body, err := iceberg.EncodeManifest(meta, spec, f.content, []iceberg.ManifestEntry{entry})
		require.NoError(t, err)

		path := fmt.Sprintf("%s/metadata/m%d.avro", loc, i)
		putObj(t, e.store, path, body)

		content := 0
		if f.content != iceberg.ContentData {
			content = 1
		}

		list = append(list, iceberg.ManifestFile{
			Path: path, Length: int64(len(body)), Content: content, SequenceNumber: seq, MinSequenceNumber: seq,
			AddedSnapshotID: snapID, AddedFiles: 1, AddedRows: int64(len(f.rows)),
		})
		maxSeq = max(maxSeq, seq)

		switch f.content {
		case iceberg.ContentData:
			totals["total-data-files"]++
			totals["total-records"] += int64(len(f.rows))
			totals["total-file-size"] += int64(size)
		case iceberg.ContentEqualityDeletes:
			totals["total-delete-files"]++
			totals["total-equality-deletes"] += int64(len(f.rows))
		default:
			totals["total-delete-files"]++
			totals["total-position-deletes"] += int64(len(f.rows))
		}
	}

	listBody, err := iceberg.EncodeManifestList(snapID, nil, maxSeq, list)
	require.NoError(t, err)

	listPath := loc + "/metadata/snap-4242.avro"
	putObj(t, e.store, listPath, listBody)

	summary := map[string]string{"operation": "overwrite"}
	for k, v := range totals {
		summary[k] = strconv.FormatInt(v, 10)
	}

	next := meta.AppendSnapshot(iceberg.Snapshot{
		SnapshotID: snapID, SequenceNumber: maxSeq, TimestampMS: now.UnixMilli(),
		ManifestList: listPath, Summary: summary,
	}, "", now)

	body, err := iceberg.EncodeMetadata(next)
	require.NoError(t, err)

	putObj(t, e.store, loc+"/metadata/00001.metadata.json", body)

	e.cat.defs["shop/"+name] = &athena.GlueTableDef{
		Name:     name,
		Location: loc,
		Parameters: map[string]string{
			"table_type":        "ICEBERG",
			"metadata_location": loc + "/metadata/00001.metadata.json",
		},
	}
}

func dataCols() []parquetio.Column {
	return []parquetio.Column{
		{Name: "id", Type: "bigint", FieldID: 10},
		{Name: "label", Type: "string", FieldID: 20},
		{Name: "region", Type: "string", FieldID: 30},
	}
}

func TestIceberg_EqualityDeletes(t *testing.T) {
	t.Parallel()

	rows := [][]any{{int64(1), "a", "eu"}, {int64(2), "b", "eu"}, {int64(3), nil, "us"}}
	idDel := []parquetio.Column{{Name: "id", Type: "bigint", FieldID: 10}}
	labelDel := []parquetio.Column{{Name: "label", Type: "string", FieldID: 20}}
	pairDel := []parquetio.Column{
		{Name: "id", Type: "bigint", FieldID: 10},
		{Name: "label", Type: "string", FieldID: 20},
	}

	tests := []struct {
		name  string
		files []seedFile
		want  [][]string
		part  bool
	}{
		{
			name: "single column",
			files: []seedFile{
				{cols: dataCols(), rows: rows, seq: 1},
				{
					cols:    idDel,
					rows:    [][]any{{int64(2)}},
					content: iceberg.ContentEqualityDeletes,
					eqIDs:   []int{10},
					seq:     2,
				},
			},
			want: [][]string{{"1"}, {"3"}},
		},
		{
			name: "newer data survives",
			files: []seedFile{
				{cols: dataCols(), rows: rows, seq: 1},
				{
					cols:    idDel,
					rows:    [][]any{{int64(2)}},
					content: iceberg.ContentEqualityDeletes,
					eqIDs:   []int{10},
					seq:     2,
				},
				{cols: dataCols(), rows: [][]any{{int64(2), "again", "eu"}}, seq: 3},
			},
			want: [][]string{{"1"}, {"2"}, {"3"}},
		},
		{
			name: "same sequence does not apply",
			files: []seedFile{
				{cols: dataCols(), rows: rows, seq: 2},
				{
					cols:    idDel,
					rows:    [][]any{{int64(2)}},
					content: iceberg.ContentEqualityDeletes,
					eqIDs:   []int{10},
					seq:     2,
				},
			},
			want: [][]string{{"1"}, {"2"}, {"3"}},
		},
		{
			name: "null matches null",
			files: []seedFile{
				{cols: dataCols(), rows: rows, seq: 1},
				{
					cols:    labelDel,
					rows:    [][]any{{nil}},
					content: iceberg.ContentEqualityDeletes,
					eqIDs:   []int{20},
					seq:     2,
				},
			},
			want: [][]string{{"1"}, {"2"}},
		},
		{
			name: "composite key needs every column",
			files: []seedFile{
				{cols: dataCols(), rows: rows, seq: 1},
				{
					cols: pairDel, rows: [][]any{{int64(1), "b"}, {int64(2), "b"}},
					content: iceberg.ContentEqualityDeletes, eqIDs: []int{10, 20}, seq: 2,
				},
			},
			want: [][]string{{"1"}, {"3"}},
		},
		{
			name: "partition scoped",
			part: true,
			files: []seedFile{
				{cols: dataCols(), rows: rows[:2], seq: 1, part: map[string]any{"region": "eu"}},
				{cols: dataCols(), rows: rows[2:], seq: 1, part: map[string]any{"region": "us"}},
				{
					cols: idDel, rows: [][]any{{int64(2)}, {int64(3)}}, content: iceberg.ContentEqualityDeletes,
					eqIDs: []int{10}, seq: 2, part: map[string]any{"region": "us"},
				},
			},
			want: [][]string{{"1"}, {"2"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newIceEnv(t)
			seedIceberg(t, e, "eq", tc.part, tc.files)

			assert.Equal(t, tc.want, e.mustRun(t, "SELECT id FROM eq ORDER BY id"))
			assert.Equal(t, [][]string{{strconv.Itoa(len(tc.want))}}, e.mustRun(t, "SELECT count(*) FROM eq"))
		})
	}
}

func TestIceberg_OptimizeRetiresEqualityDeletes(t *testing.T) {
	t.Parallel()

	idDel := []parquetio.Column{{Name: "id", Type: "bigint", FieldID: 10}}
	e := newIceEnv(t)
	seedIceberg(t, e, "eq", false, []seedFile{
		{cols: dataCols(), rows: [][]any{{int64(1), "a", "eu"}, {int64(2), "b", "eu"}}, seq: 1},
		{cols: dataCols(), rows: [][]any{{int64(3), "c", "eu"}}, seq: 1},
		{cols: idDel, rows: [][]any{{int64(2)}}, content: iceberg.ContentEqualityDeletes, eqIDs: []int{10}, seq: 2},
	}, "optimize_rewrite_data_file_threshold", "2")

	e.mustRun(t, "OPTIMIZE eq REWRITE DATA USING BIN_PACK")

	assert.Equal(t, [][]string{{"1"}, {"3"}}, e.mustRun(t, "SELECT id FROM eq ORDER BY id"))

	meta, _ := e.metadata(t, "eq")
	cur, _ := meta.CurrentSnapshot()
	assert.Equal(t, "1", cur.Summary["total-data-files"])
	assert.Equal(t, "0", cur.Summary["total-delete-files"])
	assert.Equal(t, "0", cur.Summary["total-equality-deletes"])
}

func TestIceberg_OrcDataFiles(t *testing.T) {
	t.Parallel()

	t.Run("write and read", func(t *testing.T) {
		t.Parallel()

		e := newIceEnv(t)
		e.mustRun(t, "CREATE TABLE o (id bigint, label string, ts timestamp) PARTITIONED BY (label) "+
			"LOCATION 's3://lake/o/' TBLPROPERTIES ('table_type'='ICEBERG', 'format'='orc', 'write_compression'='zstd')")
		e.mustRun(
			t,
			"INSERT INTO o VALUES (1, 'a', TIMESTAMP '2024-01-02 03:04:05.678'), (2, 'b', NULL), (3, 'a', NULL)",
		)
		e.mustRun(t, "DELETE FROM o WHERE id = 3")

		assert.Equal(t, [][]string{{"1", "a", "2024-01-02 03:04:05.678"}, {"2", "b", ""}},
			e.mustRun(t, "SELECT id, label, ts FROM o ORDER BY id"))

		meta, _ := e.metadata(t, "o")
		assert.Equal(t, "orc", meta.Properties["write.format.default"])

		cur, _ := meta.CurrentSnapshot()
		list, err := iceberg.ReadManifestList(e.objectAt(t, cur.ManifestList))
		require.NoError(t, err)

		formats := map[string]bool{}

		for _, mf := range list {
			entries, merr := iceberg.ReadManifest(e.objectAt(t, mf.Path))
			require.NoError(t, merr)

			for _, en := range entries {
				formats[en.DataFile.Format] = true

				if en.DataFile.Content == iceberg.ContentData {
					f, oerr := orcio.Open(e.objectAt(t, en.DataFile.Path))
					require.NoError(t, oerr)
					assert.Equal(t, map[int]string{1: "id", 2: "label", 3: "ts"}, f.ColumnIDs())
				}
			}
		}

		assert.True(t, formats["ORC"])
	})

	t.Run("external file matched by field id", func(t *testing.T) {
		t.Parallel()

		e := newIceEnv(t)

		var buf bytes.Buffer
		require.NoError(t, orcio.Write(&buf, []orcio.Column{
			{Name: "renamed", Type: "bigint", FieldID: 10}, {Name: "lbl", Type: "string", FieldID: 20},
		}, [][]any{{int64(7), "seven"}, {int64(8), nil}}, "ZLIB"))

		seedIceberg(t, e, "ext", false, []seedFile{{format: "ORC", raw: buf.Bytes(), rows: [][]any{{}, {}}, seq: 1}})

		assert.Equal(t, [][]string{{"7", "seven"}, {"8", ""}}, e.mustRun(t, "SELECT id, label FROM ext ORDER BY id"))
	})
}
