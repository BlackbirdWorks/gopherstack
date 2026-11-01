package athena_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

// iceCatalog adds the optimistic metadata_location swap Glue offers to the engine fake catalog.
type iceCatalog struct {
	*engineCatalog
	mu sync.Mutex
}

func (c *iceCatalog) CommitTableMetadata(db, name, expected, next string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	d, ok := c.defs[db+"/"+name]
	if !ok {
		return athena.ErrCatalogEntityNotFound
	}

	if d.Parameters["metadata_location"] != expected {
		return athena.ErrCatalogCommitConflict
	}

	d.Parameters["metadata_location"] = next

	return nil
}

type stepClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *stepClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(time.Second)

	return c.t
}

type iceEnv struct {
	b     *athena.InMemoryBackend
	store *engineS3
	cat   *iceCatalog
}

func newIceEnv(t *testing.T) *iceEnv {
	t.Helper()

	cat := &iceCatalog{engineCatalog: &engineCatalog{
		fakeGlueSource: newFakeGlueSource(),
		defs:           map[string]*athena.GlueTableDef{},
		parts:          map[string][]athena.GluePartitionDef{},
	}}
	store := &engineS3{objects: map[string][]byte{}}
	clock := &stepClock{t: time.Now().UTC().Truncate(time.Second)}

	b := athena.NewInMemoryBackend("us-east-1", "123456789012")
	b.SetGlueMetadataSource(cat)
	b.SetS3Backend(store)
	b.SetClock(clock.now)

	return &iceEnv{b: b, store: store, cat: cat}
}

func (e *iceEnv) run(t *testing.T, sql string) (*athena.QueryExecution, [][]string) {
	t.Helper()

	id := start(t, e.b, sql)
	qe := waitDone(t, e.b, id)

	if qe.Status.State != "SUCCEEDED" {
		return qe, nil
	}

	return qe, resultRows(t, e.b, id)
}

func (e *iceEnv) mustRun(t *testing.T, sql string) [][]string {
	t.Helper()

	qe, rows := e.run(t, sql)
	require.Equal(t, "SUCCEEDED", qe.Status.State, "%s: %s", sql, qe.Status.StateChangeReason)

	return rows
}

func (e *iceEnv) failure(t *testing.T, sql string) string {
	t.Helper()

	qe, _ := e.run(t, sql)
	require.Equal(t, "FAILED", qe.Status.State, sql)

	return qe.Status.StateChangeReason
}

func (e *iceEnv) metadata(t *testing.T, table string) (*iceberg.TableMetadata, string) {
	t.Helper()

	loc := e.cat.defs["shop/"+table].Parameters["metadata_location"]
	require.NotEmpty(t, loc)

	return parseMetadataAt(t, e.store, loc), loc
}

func (e *iceEnv) objectAt(t *testing.T, uri string) []byte {
	t.Helper()

	body, ok := e.store.objects[strings.TrimPrefix(uri, "s3://")]
	require.True(t, ok, "missing object %s", uri)

	return body
}

func parseMetadataAt(t *testing.T, store *engineS3, uri string) *iceberg.TableMetadata {
	t.Helper()

	body, ok := store.objects[strings.TrimPrefix(uri, "s3://")]
	require.True(t, ok, "missing metadata %s", uri)

	m, err := iceberg.ParseMetadata(body)
	require.NoError(t, err)

	return m
}

func TestIceberg_CreateInsertSelectRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		partition string
		wantTrans []string
		wantParts int
	}{
		{name: "unpartitioned", partition: "", wantParts: 0},
		{
			name: "partitioned", partition: "PARTITIONED BY (category, bucket(4, id))", wantParts: 2,
			wantTrans: []string{"identity", "bucket[4]"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newIceEnv(t)

			e.mustRun(t, "CREATE TABLE events (id bigint, category string, note string) "+tc.partition+
				" LOCATION 's3://lake/events/' TBLPROPERTIES ('table_type'='ICEBERG')")

			def := e.cat.defs["shop/events"]
			require.NotNil(t, def)
			assert.Equal(t, "ICEBERG", def.Parameters["table_type"])
			assert.Empty(t, def.PartitionKeys)

			meta, _ := e.metadata(t, "events")
			assert.Equal(t, 2, meta.FormatVersion)
			assert.Equal(t, iceberg.NoSnapshot, meta.CurrentSnapshotID)
			assert.Len(t, meta.DefaultSpec().Fields, tc.wantParts)

			for i, tr := range tc.wantTrans {
				assert.Equal(t, tr, meta.DefaultSpec().Fields[i].Transform)
			}

			assert.Empty(t, e.mustRun(t, "SELECT * FROM events"))

			e.mustRun(t, "INSERT INTO events VALUES (1, 'a', 'one'), (2, 'b', 'two'), (3, 'a', NULL)")
			e.mustRun(t, "INSERT INTO events VALUES (4, 'c', 'four')")

			assert.Equal(t, [][]string{{"1", "a", "one"}, {"2", "b", "two"}, {"3", "a", ""}, {"4", "c", "four"}},
				e.mustRun(t, "SELECT id, category, note FROM events ORDER BY id"))
			assert.Equal(t, [][]string{{"2"}}, e.mustRun(t, "SELECT count(*) FROM events WHERE category = 'a'"))

			meta, loc := e.metadata(t, "events")
			require.Len(t, meta.Snapshots, 2)
			assert.Len(t, meta.MetadataLog, 2)
			assert.Contains(t, loc, "/metadata/00002-")
			assert.Equal(t, "4", meta.Snapshots[1].Summary["total-records"])

			cur, ok := meta.CurrentSnapshot()
			require.True(t, ok)

			list, err := iceberg.ReadManifestList(e.objectAt(t, cur.ManifestList))
			require.NoError(t, err)
			require.Len(t, list, 2)

			var files int

			for _, mf := range list {
				entries, merr := iceberg.ReadManifest(e.objectAt(t, mf.Path))
				require.NoError(t, merr)

				for _, en := range entries {
					files++

					pf, perr := parquetio.Open(e.objectAt(t, en.DataFile.Path))
					require.NoError(t, perr)
					assert.Equal(t, en.DataFile.RecordCount, pf.NumRows())
					assert.Equal(t, map[int]string{1: "id", 2: "category", 3: "note"}, pf.ColumnIDs())
				}
			}

			assert.GreaterOrEqual(t, files, 2)
		})
	}
}

func TestIceberg_TimeTravelAndMetadataTables(t *testing.T) {
	t.Parallel()

	e := newIceEnv(t)
	e.mustRun(t, "CREATE TABLE t (id int, v string) LOCATION 's3://lake/t' TBLPROPERTIES ('table_type'='ICEBERG')")
	e.mustRun(t, "INSERT INTO t VALUES (1, 'a')")
	e.mustRun(t, "INSERT INTO t VALUES (2, 'b')")
	e.mustRun(t, "INSERT INTO t VALUES (3, 'c')")

	meta, _ := e.metadata(t, "t")
	require.Len(t, meta.Snapshots, 3)

	ts := func(s iceberg.Snapshot, plus time.Duration) string {
		return time.UnixMilli(s.TimestampMS).UTC().Add(plus).Format("2006-01-02 15:04:05.000")
	}

	cases := []struct {
		name string
		sql  string
		want [][]string
	}{
		{
			"version first",
			fmt.Sprintf("SELECT id FROM t FOR VERSION AS OF %d ORDER BY id", meta.Snapshots[0].SnapshotID),
			[][]string{{"1"}},
		},
		{
			"version second",
			fmt.Sprintf("SELECT id FROM t FOR VERSION AS OF %d ORDER BY id", meta.Snapshots[1].SnapshotID),
			[][]string{{"1"}, {"2"}},
		},
		{
			"timestamp between",
			"SELECT id FROM t FOR TIMESTAMP AS OF TIMESTAMP '" + ts(meta.Snapshots[1], 500*time.Millisecond) +
				" UTC' ORDER BY id",
			[][]string{{"1"}, {"2"}},
		},
		{
			"timestamp literal string", "SELECT count(*) FROM t FOR TIMESTAMP AS OF '" + ts(meta.Snapshots[0], 0) + "'",
			[][]string{{"1"}},
		},
		{
			"timestamp expression", "SELECT count(*) FROM t FOR TIMESTAMP AS OF (current_timestamp + interval '1' day)",
			[][]string{{"3"}},
		},
		{
			"time and version join",
			fmt.Sprintf(
				"SELECT a.id, b.id FROM t FOR VERSION AS OF %d a, t b WHERE b.id = 3",
				meta.Snapshots[0].SnapshotID,
			),
			[][]string{{"1", "3"}},
		},
		{"snapshots", "SELECT count(*), min(operation) FROM \"t$snapshots\"", [][]string{{"3", "append"}}},
		{"history", "SELECT count(*), sum(is_current_ancestor) FROM \"t$history\"", [][]string{{"3", "3"}}},
		{"files", "SELECT count(*), sum(record_count) FROM \"t$files\"", [][]string{{"3", "3"}}},
		{"manifests", "SELECT count(*) FROM \"t$manifests\"", [][]string{{"3"}}},
		{"refs", "SELECT name, type FROM \"t$refs\"", [][]string{{"main", "BRANCH"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, e.mustRun(t, tc.sql))
		})
	}

	fails := []struct {
		name string
		sql  string
		want string
	}{
		{"unknown version", "SELECT * FROM t FOR VERSION AS OF 42", "Iceberg snapshot ID does not exists: 42"},
		{
			"before history", "SELECT * FROM t FOR TIMESTAMP AS OF TIMESTAMP '2001-01-01 00:00:00'",
			"No version history table shop.t at or before",
		},
	}

	for _, tc := range fails {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Contains(t, e.failure(t, tc.sql), tc.want)
		})
	}
}

func TestIceberg_ErrorsAndValidation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		setup func(e *iceEnv)
		sql   string
		want  string
	}{
		{
			name: "metadata file missing",
			setup: func(e *iceEnv) {
				e.cat.defs["shop/gone"] = &athena.GlueTableDef{
					Name:     "gone",
					Location: "s3://lake/gone/",
					Columns:  []athena.Column{{Name: "id", Type: "int"}},
					Parameters: map[string]string{
						"table_type":        "ICEBERG",
						"metadata_location": "s3://lake/gone/metadata/0.json",
					},
				}
			},
			sql:  "SELECT * FROM gone",
			want: "ICEBERG_MISSING_METADATA: Metadata not found in metadata location for table shop.gone",
		},
		{
			name: "metadata location absent",
			setup: func(e *iceEnv) {
				e.cat.defs["shop/bare"] = &athena.GlueTableDef{
					Name: "bare", Location: "s3://lake/bare/", Parameters: map[string]string{"table_type": "ICEBERG"},
				}
			},
			sql: "SELECT * FROM bare", want: "ICEBERG_INVALID_METADATA: Table is missing [metadata_location] property",
		},
		{
			name: "metadata corrupt",
			setup: func(e *iceEnv) {
				e.store.objects["lake/bad/metadata/0.json"] = []byte("{not json")
				e.cat.defs["shop/bad"] = &athena.GlueTableDef{
					Name:     "bad",
					Location: "s3://lake/bad/",
					Parameters: map[string]string{
						"table_type":        "ICEBERG",
						"metadata_location": "s3://lake/bad/metadata/0.json",
					},
				}
			},
			sql: "SELECT * FROM bad", want: "ICEBERG_INVALID_METADATA: Invalid metadata file for table shop.bad",
		},
		{
			name: "external keyword", sql: "CREATE EXTERNAL TABLE x (id int) LOCATION 's3://lake/x/' " +
				"TBLPROPERTIES ('table_type'='ICEBERG')",
			want: "External keyword not supported for table type ICEBERG",
		},
		{
			name: "unknown property", sql: "CREATE TABLE x (id int) LOCATION 's3://lake/x/' " +
				"TBLPROPERTIES ('table_type'='ICEBERG', 'bogus'='1')",
			want: "Unsupported table property key: bogus",
		},
		{
			name: "orc format", sql: "CREATE TABLE x (id int) LOCATION 's3://lake/x/' " +
				"TBLPROPERTIES ('table_type'='ICEBERG', 'format'='orc')",
			want: "NOT_SUPPORTED",
		},
		{
			name: "partition column missing",
			sql: "CREATE TABLE x (id int) PARTITIONED BY (nope) LOCATION 's3://lake/x/' " +
				"TBLPROPERTIES ('table_type'='ICEBERG')",
			want: "COLUMN_NOT_FOUND",
		},
		{
			name: "travel on hive table",
			setup: func(e *iceEnv) {
				e.cat.defs["shop/h"] = salesDef(serdeCSV, nil)
				e.cat.defs["shop/h"].Name = "h"
			},
			sql: "SELECT * FROM h FOR VERSION AS OF 1", want: "does not support versioned tables",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newIceEnv(t)
			if tc.setup != nil {
				tc.setup(e)
			}

			assert.Contains(t, e.failure(t, tc.sql), tc.want)
		})
	}
}

func putParquet(t *testing.T, store *engineS3, key string, cols []parquetio.Column, rows [][]any) int {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, parquetio.Write(&buf, cols, rows, "SNAPPY"))

	_, err := store.PutObject(context.Background(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("lake"), Key: aws.String(key), Body: bytes.NewReader(buf.Bytes()),
	})
	require.NoError(t, err)

	return buf.Len()
}

func putObj(t *testing.T, store *engineS3, uri string, body []byte) {
	t.Helper()

	bucket, key, _ := strings.Cut(strings.TrimPrefix(uri, "s3://"), "/")
	_, err := store.PutObject(context.Background(), &sdk_s3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body),
	})
	require.NoError(t, err)
}

// seedExternalIceberg lays out a table the way another engine would: renamed
// Parquet columns matched by field id, an identity-partition column kept out of
// the data file, and a position delete removing one row.
func seedExternalIceberg(t *testing.T, e *iceEnv) {
	t.Helper()

	str, _ := iceberg.FromHive("string")
	lng, _ := iceberg.FromHive("bigint")
	schema := iceberg.Schema{Fields: []iceberg.Field{
		{ID: 10, Name: "id", Type: []byte(`"` + lng + `"`)},
		{ID: 20, Name: "label", Type: []byte(`"` + str + `"`)},
		{ID: 30, Name: "region", Type: []byte(`"` + str + `"`)},
	}}
	spec := iceberg.PartitionSpec{Fields: []iceberg.PartitionField{
		{Name: "region", Transform: "identity", SourceID: 30, FieldID: 1000},
	}}
	now := time.Now().UTC()
	meta := iceberg.NewTableMetadata(iceberg.NewParams{Location: "s3://lake/ext", Schema: schema, Spec: spec, Now: now})

	dataKey := "ext/data/region=eu/f1.parquet"
	size := putParquet(t, e.store, dataKey,
		[]parquetio.Column{{Name: "id_old", Type: "bigint", FieldID: 10}, {Name: "lbl", Type: "string", FieldID: 20}},
		[][]any{{int64(1), "keep"}, {int64(2), "drop"}, {int64(3), "keep"}})

	delKey := "ext/data/deletes.parquet"
	delSize := putParquet(t, e.store, delKey,
		[]parquetio.Column{{Name: "file_path", Type: "string"}, {Name: "pos", Type: "bigint"}},
		[][]any{{"s3://lake/" + dataKey, int64(1)}})

	snapID, seq := int64(111), int64(1)
	dataEntry := iceberg.ManifestEntry{
		Status:         iceberg.StatusAdded,
		SnapshotID:     &snapID,
		SequenceNumber: &seq,
		DataFile: iceberg.DataFile{
			Path: "s3://lake/" + dataKey, Format: "PARQUET", Partition: map[string]any{"region": "eu"},
			RecordCount: 3, FileSize: int64(size),
		},
	}
	delSeq := int64(2)
	delEntry := iceberg.ManifestEntry{
		Status:         iceberg.StatusAdded,
		SnapshotID:     &snapID,
		SequenceNumber: &delSeq,
		DataFile: iceberg.DataFile{
			Path: "s3://lake/" + delKey, Format: "PARQUET", Partition: map[string]any{"region": "eu"},
			Content: iceberg.ContentPositionDeletes, RecordCount: 1, FileSize: int64(delSize),
		},
	}

	dataManifest, err := iceberg.EncodeManifest(meta, spec, iceberg.ContentData, []iceberg.ManifestEntry{dataEntry})
	require.NoError(t, err)

	delManifest, err := iceberg.EncodeManifest(
		meta,
		spec,
		iceberg.ContentPositionDeletes,
		[]iceberg.ManifestEntry{delEntry},
	)
	require.NoError(t, err)

	putObj(t, e.store, "s3://lake/ext/metadata/m-data.avro", dataManifest)
	putObj(t, e.store, "s3://lake/ext/metadata/m-del.avro", delManifest)

	list, err := iceberg.EncodeManifestList(snapID, nil, 2, []iceberg.ManifestFile{
		{
			Path:              "s3://lake/ext/metadata/m-data.avro",
			Length:            int64(len(dataManifest)),
			Content:           iceberg.ContentData,
			SequenceNumber:    1,
			MinSequenceNumber: 1,
			AddedSnapshotID:   snapID,
			AddedFiles:        1,
			AddedRows:         3,
		},
		{
			Path:              "s3://lake/ext/metadata/m-del.avro",
			Length:            int64(len(delManifest)),
			Content:           1,
			SequenceNumber:    2,
			MinSequenceNumber: 2,
			AddedSnapshotID:   snapID,
			AddedFiles:        1,
			AddedRows:         1,
		},
	})
	require.NoError(t, err)
	putObj(t, e.store, "s3://lake/ext/metadata/snap-111.avro", list)

	next := meta.AppendSnapshot(iceberg.Snapshot{
		SnapshotID: snapID, SequenceNumber: 2, TimestampMS: now.UnixMilli(),
		ManifestList: "s3://lake/ext/metadata/snap-111.avro", Summary: map[string]string{"operation": "overwrite"},
	}, "", now)

	body, err := iceberg.EncodeMetadata(next)
	require.NoError(t, err)

	putObj(t, e.store, "s3://lake/ext/metadata/00001.metadata.json", body)

	e.cat.defs["shop/ext"] = &athena.GlueTableDef{
		Name:     "ext",
		Location: "s3://lake/ext",
		Parameters: map[string]string{
			"table_type":        "ICEBERG",
			"metadata_location": "s3://lake/ext/metadata/00001.metadata.json",
		},
	}
}

func TestIceberg_ReadsTablesWrittenByOtherEngines(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		sql  string
		want [][]string
	}{
		{"field ids partition constants and position deletes", "SELECT id, label, region FROM ext ORDER BY id",
			[][]string{{"1", "keep", "eu"}, {"3", "keep", "eu"}}},
		{"files metadata", "SELECT count(*) FROM \"ext$files\"", [][]string{{"2"}}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newIceEnv(t)
			seedExternalIceberg(t, e)

			assert.Equal(t, tc.want, e.mustRun(t, tc.sql))
		})
	}
}

func TestIceberg_InsertRetriesLostCommit(t *testing.T) {
	t.Parallel()

	e := newIceEnv(t)
	e.mustRun(t, "CREATE TABLE r (id int) LOCATION 's3://lake/r' TBLPROPERTIES ('table_type'='ICEBERG')")

	conflicts := &conflictOnce{iceCatalog: e.cat}
	e.b.SetGlueMetadataSource(conflicts)

	e.mustRun(t, "INSERT INTO r VALUES (1)")
	assert.Equal(t, 1, conflicts.hits)
	assert.Equal(t, [][]string{{"1"}}, e.mustRun(t, "SELECT id FROM r"))
}

type conflictOnce struct {
	*iceCatalog
	hits int
}

func (c *conflictOnce) CommitTableMetadata(db, name, expected, next string) error {
	if c.hits == 0 {
		c.hits++

		return athena.ErrCatalogCommitConflict
	}

	return c.iceCatalog.CommitTableMetadata(db, name, expected, next)
}
