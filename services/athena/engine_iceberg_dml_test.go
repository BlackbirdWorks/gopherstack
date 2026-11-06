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
	assert.Equal(t, [][]string{{"1", "11", "21.0"}, {"2", "2", "20.0"}, {"3", "13", "60.0"}, {"4", "4", "40.0"}, {"5", "5", "50.0"}},
		e.mustRun(t, "SELECT id, qty, price FROM orders ORDER BY id"))

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
