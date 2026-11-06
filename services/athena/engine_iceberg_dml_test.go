package athena_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

func newOrders(t *testing.T, partition string) *iceEnv {
	t.Helper()

	e := newIceEnv(t)

	e.mustRun(t, "CREATE DATABASE shop")
	e.mustRun(t, "CREATE TABLE orders (id bigint, category string, qty int, price double) "+partition+
		" LOCATION 's3://lake/orders/' TBLPROPERTIES ('table_type'='ICEBERG')")
	e.mustRun(t, "INSERT INTO orders VALUES (1, 'a', 1, 10.5), (2, 'b', 2, 20.0), (3, 'a', 3, 30.0)")
	e.mustRun(t, "INSERT INTO orders VALUES (4, 'c', 4, 40.0), (5, 'b', 5, 50.0)")

	return e
}

func TestIceberg_DeleteWritesPositionDeletes(t *testing.T) {
	t.Parallel()

	for _, part := range []string{"", "PARTITIONED BY (category)"} {
		t.Run("part="+part, func(t *testing.T) {
			t.Parallel()

			e := newOrders(t, part)
			e.mustRun(t, "DELETE FROM orders WHERE category = 'a' OR id = 5")

			assert.Equal(t, [][]string{{"2"}, {"4"}},
				e.mustRun(t, "SELECT id FROM orders ORDER BY id"))

			meta, _ := e.metadata(t, "orders")
			cur, _ := meta.CurrentSnapshot()
			assert.Equal(t, "delete", cur.Summary["operation"])
			assert.Equal(t, "3", cur.Summary["added-position-deletes"])
			assert.Equal(t, "5", cur.Summary["total-records"])

			list, err := iceberg.ReadManifestList(e.objectAt(t, cur.ManifestList))
			require.NoError(t, err)

			var deleteFiles int

			for _, mf := range list {
				if mf.Content != iceberg.ContentPositionDeletes {
					continue
				}

				entries, merr := iceberg.ReadManifest(e.objectAt(t, mf.Path))
				require.NoError(t, merr)

				for _, en := range entries {
					deleteFiles++

					pf, perr := parquetio.Open(e.objectAt(t, en.DataFile.Path))
					require.NoError(t, perr)
					assert.Equal(t, map[int]string{2147483546: "file_path", 2147483545: "pos"}, pf.ColumnIDs())
					assert.Equal(t, iceberg.ContentPositionDeletes, en.DataFile.Content)
				}
			}

			assert.GreaterOrEqual(t, deleteFiles, 1)

			e.mustRun(t, "DELETE FROM orders")
			assert.Equal(t, [][]string{{"0"}}, e.mustRun(t, "SELECT count(*) FROM orders"))
		})
	}
}

func TestIceberg_UpdateAndMerge(t *testing.T) {
	t.Parallel()

	e := newOrders(t, "PARTITIONED BY (category)")

	e.mustRun(t, "UPDATE orders SET qty = qty + 10, price = price * 2 WHERE category = 'a'")
	assert.Equal(
		t,
		[][]string{
			{"1", "11", "21.0"},
			{"2", "2", "20.0"},
			{"3", "13", "60.0"},
			{"4", "4", "40.0"},
			{"5", "5", "50.0"},
		},
		e.mustRun(t, "SELECT id, qty, price FROM orders ORDER BY id"),
	)

	meta, _ := e.metadata(t, "orders")
	cur, _ := meta.CurrentSnapshot()
	assert.Equal(t, "overwrite", cur.Summary["operation"])

	e.mustRun(t, "CREATE TABLE changes (id bigint, category string, qty int, price double) "+
		"LOCATION 's3://lake/changes/' TBLPROPERTIES ('table_type'='ICEBERG')")
	e.mustRun(t, "INSERT INTO changes VALUES (2, 'b', 0, 0.0), (4, 'c', 99, 1.0), (6, 'd', 6, 60.0), (7, 'e', -1, 0.0)")

	e.mustRun(t, `MERGE INTO orders t USING changes s ON t.id = s.id
		WHEN MATCHED AND s.qty = 0 THEN DELETE
		WHEN MATCHED THEN UPDATE SET qty = s.qty, price = t.price + s.price
		WHEN NOT MATCHED AND s.qty > 0 THEN INSERT (id, category, qty, price) VALUES (s.id, s.category, s.qty, s.price)`)

	assert.Equal(t, [][]string{
		{"1", "a", "11", "21.0"}, {"3", "a", "13", "60.0"}, {"4", "c", "99", "41.0"},
		{"5", "b", "5", "50.0"}, {"6", "d", "6", "60.0"},
	}, e.mustRun(t, "SELECT id, category, qty, price FROM orders ORDER BY id"))

	e.mustRun(t, "INSERT INTO changes VALUES (4, 'c', 1, 1.0)")
	assert.Contains(t, e.failure(t, `MERGE INTO orders t USING changes s ON t.id = s.id
		WHEN MATCHED THEN DELETE`), "MERGE_TARGET_ROW_MULTIPLE_MATCHES")
}

func TestIceberg_OptimizeCompactsAndDropsDeletes(t *testing.T) {
	t.Parallel()

	e := newIceEnv(t)
	e.mustRun(t, "CREATE TABLE t (id bigint, cat string) PARTITIONED BY (cat) LOCATION 's3://lake/t/' "+
		"TBLPROPERTIES ('table_type'='ICEBERG', 'optimize_rewrite_data_file_threshold'='3')")

	for i := range 4 {
		e.mustRun(t, "INSERT INTO t VALUES ("+strings.Repeat("1", i+1)+", 'x'), (100, 'y')")
	}

	e.mustRun(t, "DELETE FROM t WHERE id = 11")
	before, _ := e.metadata(t, "t")
	cur, _ := before.CurrentSnapshot()
	assert.Equal(t, "8", cur.Summary["total-data-files"])

	assert.Contains(t, e.failure(t, "OPTIMIZE t REWRITE DATA USING BIN_PACK WHERE id > 1"), "partition columns")

	e.mustRun(t, "OPTIMIZE t REWRITE DATA USING BIN_PACK WHERE cat = 'x'")

	assert.Equal(t, [][]string{{"1"}, {"100"}, {"100"}, {"100"}, {"100"}, {"111"}, {"1111"}},
		e.mustRun(t, "SELECT id FROM t ORDER BY id"))

	after, _ := e.metadata(t, "t")
	cur, _ = after.CurrentSnapshot()
	assert.Equal(t, "replace", cur.Summary["operation"])
	assert.Equal(t, "5", cur.Summary["total-data-files"])
	assert.Equal(t, "0", cur.Summary["total-delete-files"])
}

func TestIceberg_CTAS(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		with  string
		files string
		parts []string
	}{
		{name: "plain", with: "table_type='ICEBERG', location='s3://lake/c1/', is_external=false", files: ".parquet"},
		{
			name: "partitioned",
			with: "table_type='ICEBERG', location='s3://lake/c2/', is_external=false, " +
				"partitioning=ARRAY['category', 'bucket(id, 4)']",
			parts: []string{"identity", "bucket[4]"}, files: ".parquet",
		},
		{
			name:  "avro",
			with:  "table_type='ICEBERG', location='s3://lake/c3/', format='AVRO', write_compression='snappy'",
			files: ".avro",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newOrders(t, "")
			e.mustRun(
				t,
				"CREATE TABLE copy WITH ("+tc.with+") AS SELECT id, category, qty > 2 AS big, price FROM orders",
			)

			assert.Equal(
				t,
				[][]string{
					{"1", "a", "false"},
					{"2", "b", "false"},
					{"3", "a", "true"},
					{"4", "c", "true"},
					{"5", "b", "true"},
				},
				e.mustRun(t, "SELECT id, category, big FROM copy ORDER BY id"),
			)

			meta, _ := e.metadata(t, "copy")
			assert.Equal(t, "boolean", string(meta.CurrentSchema().Fields[2].Type[1:8]))
			require.Len(t, meta.DefaultSpec().Fields, len(tc.parts))

			for i, tr := range tc.parts {
				assert.Equal(t, tr, meta.DefaultSpec().Fields[i].Transform)
			}

			cur, _ := meta.CurrentSnapshot()
			assert.Equal(t, "5", cur.Summary["total-records"])

			var sawExt bool

			for k := range e.store.objects {
				if strings.Contains(k, "/data/") && strings.HasSuffix(k, tc.files) {
					sawExt = true
				}
			}

			assert.True(t, sawExt)

			e.mustRun(t, "DELETE FROM copy WHERE big")
			assert.Equal(t, [][]string{{"2"}}, e.mustRun(t, "SELECT count(*) FROM copy"))
		})
	}

	e := newOrders(t, "")
	e.mustRun(
		t,
		"CREATE TABLE empty WITH (table_type='ICEBERG', location='s3://lake/e/') AS SELECT id FROM orders WITH NO DATA",
	)
	assert.Equal(t, [][]string{{"0"}}, e.mustRun(t, "SELECT count(*) FROM empty"))
	assert.Contains(
		t,
		e.failure(
			t,
			"CREATE TABLE bad WITH (table_type='ICEBERG', format='ORC', location='s3://lake/b/') AS SELECT id FROM orders",
		),
		"ORC",
	)
}

func TestIceberg_AlterTable(t *testing.T) {
	t.Parallel()

	e := newOrders(t, "PARTITIONED BY (category)")

	e.mustRun(t, "ALTER TABLE orders ADD COLUMNS (note string, weight double)")
	e.mustRun(t, "INSERT INTO orders VALUES (6, 'a', 6, 60.0, 'new', 1.5)")
	assert.Equal(t, [][]string{{"5", ""}, {"6", "new"}},
		e.mustRun(t, "SELECT id, note FROM orders WHERE id >= 5 ORDER BY id"))

	e.mustRun(t, "ALTER TABLE orders CHANGE COLUMN qty quantity bigint AFTER id")
	e.mustRun(t, "ALTER TABLE orders RENAME COLUMN note TO memo")
	e.mustRun(t, "ALTER TABLE orders DROP COLUMN weight")

	assert.Equal(t, [][]string{{"6", "6", "new"}},
		e.mustRun(t, "SELECT id, quantity, memo FROM orders WHERE id = 6"))
	assert.Equal(t, [][]string{{"1", "1", "a"}},
		e.mustRun(t, "SELECT id, quantity, category FROM orders WHERE id = 1"))

	meta, _ := e.metadata(t, "orders")
	assert.Len(t, meta.Schemas, 5)
	assert.Equal(t, 6, meta.LastColumnID)

	names := make([]string, 0, len(meta.CurrentSchema().Fields))
	for _, f := range meta.CurrentSchema().Fields {
		names = append(names, f.Name)
	}

	assert.Equal(t, []string{"id", "quantity", "category", "price", "memo"}, names)

	defCols := e.cat.defs["shop/orders"].Columns
	assert.Len(t, defCols, 5)

	assert.Contains(t, e.failure(t, "ALTER TABLE orders DROP COLUMN category"), "partition")
	assert.Contains(t, e.failure(t, "ALTER TABLE orders DROP COLUMN nope"), "COLUMN_NOT_FOUND")
	assert.Contains(t, e.failure(t, "ALTER TABLE orders ADD COLUMNS (id int)"), "already exists")
	assert.Contains(t, e.failure(t, "ALTER TABLE orders CHANGE COLUMN price price int"), "double to int")

	e.mustRun(t, "ALTER TABLE orders SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds'='60')")

	meta, _ = e.metadata(t, "orders")
	assert.Equal(t, "60000", meta.Properties["history.expire.max-snapshot-age-ms"])
	assert.Contains(t, e.failure(t, "ALTER TABLE orders SET TBLPROPERTIES ('bogus'='1')"), "bogus")
}

func TestIceberg_Vacuum(t *testing.T) {
	t.Parallel()

	e := newOrders(t, "")
	e.mustRun(t, "DELETE FROM orders WHERE id = 1")
	e.mustRun(
		t,
		"ALTER TABLE orders SET TBLPROPERTIES ('vacuum_max_snapshot_age_seconds'='1', 'vacuum_min_snapshots_to_keep'='1')",
	)

	before, _ := e.metadata(t, "orders")
	require.Len(t, before.Snapshots, 3)

	oldFiles := make([]string, 0, 2)

	for _, s := range before.Snapshots[:2] {
		oldFiles = append(oldFiles, s.ManifestList)
	}

	e.store.mu.Lock()
	e.store.objects["lake/orders/data/orphan.parquet"] = []byte("x")
	e.store.mu.Unlock()

	e.mustRun(t, "VACUUM orders")

	after, _ := e.metadata(t, "orders")
	require.Len(t, after.Snapshots, 1)
	assert.Equal(t, before.CurrentSnapshotID, after.CurrentSnapshotID)
	assert.Len(t, after.SnapshotLog, 1)

	e.store.mu.Lock()
	defer e.store.mu.Unlock()

	for _, f := range oldFiles {
		_, ok := e.store.objects[strings.TrimPrefix(f, "s3://")]
		assert.False(t, ok, f)
	}

	_, orphan := e.store.objects["lake/orders/data/orphan.parquet"]
	assert.False(t, orphan)
}

func TestIceberg_DMLErrors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name, sql, want string
	}{
		{"update unknown column", "UPDATE orders SET nope = 1", "COLUMN_NOT_FOUND"},
		{"missing table", "DELETE FROM ghost WHERE id = 1", "TABLE_NOT_FOUND"},
		{
			"merge arity",
			"MERGE INTO orders t USING orders s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (id) VALUES (1, 2)",
			"mismatched",
		},
		{"vacuum missing", "VACUUM ghost", "does not exist"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newOrders(t, "")
			assert.Contains(t, e.failure(t, tc.sql), tc.want)
		})
	}

	e := newOrders(t, "")
	e.mustRun(t, "CREATE TABLE hive (id int) LOCATION 's3://lake/hive/'")
	assert.Contains(t, e.failure(t, "DELETE FROM hive"), "transactional")
	assert.Contains(t, e.failure(t, "OPTIMIZE hive REWRITE DATA USING BIN_PACK"), "Iceberg")
	assert.Contains(t, e.failure(t, "VACUUM hive"), "Iceberg")
}
