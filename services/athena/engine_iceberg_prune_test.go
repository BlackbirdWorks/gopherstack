package athena_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
)

func scannedBytes(t *testing.T, e *iceEnv, sql string) (int64, [][]string) {
	t.Helper()

	qe, rows := e.run(t, sql)
	require.Equal(t, "SUCCEEDED", qe.Status.State, "%s: %s", sql, qe.Status.StateChangeReason)

	return qe.Statistics.DataScannedInBytes, rows
}

func TestIceberg_ManifestBoundsPruneFiles(t *testing.T) {
	t.Parallel()

	file := func(lo, hi int64) seedFile {
		return seedFile{
			cols: dataCols(), seq: 1, bounds: map[int][2]int64{10: {lo, hi}},
			rows: [][]any{{lo, "a", "eu"}, {hi, "b", "eu"}},
		}
	}

	cases := []struct {
		name string
		sql  string
		want [][]string
		less bool
	}{
		{"equality in middle file", "SELECT id FROM t WHERE id = 15", nil, true},
		{"equality on bound", "SELECT id FROM t WHERE id = 11", [][]string{{"11"}}, true},
		{"greater than", "SELECT id FROM t WHERE id > 25 ORDER BY id", [][]string{{"30"}}, true},
		{"greater equal on bound", "SELECT id FROM t WHERE id >= 30", [][]string{{"30"}}, true},
		{"less than", "SELECT id FROM t WHERE id < 11", [][]string{{"1"}, {"10"}}, true},
		{"between", "SELECT id FROM t WHERE id BETWEEN 10 AND 11 ORDER BY id", [][]string{{"10"}, {"11"}}, true},
		{"in list", "SELECT id FROM t WHERE id IN (1, 30) ORDER BY id", [][]string{{"1"}, {"30"}}, true},
		{"qualified and conjunct", "SELECT t.id FROM t WHERE t.id > 15 AND label = 'b' ORDER BY id",
			[][]string{{"20"}, {"30"}}, true},
		{"reversed literal", "SELECT id FROM t WHERE 25 < id", [][]string{{"30"}}, false},
		{
			"or is never pruned",
			"SELECT id FROM t WHERE id = 1 OR id = 30 ORDER BY id",
			[][]string{{"1"}, {"30"}},
			false,
		},
		{
			"subquery is never pruned",
			"SELECT id FROM (SELECT id + 100 AS id FROM t) WHERE id = 101",
			[][]string{{"101"}},
			false,
		},
		{"not equal", "SELECT count(*) FROM t WHERE id <> 15", [][]string{{"6"}}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newIceEnv(t)
			seedIceberg(t, e, "t", false, []seedFile{file(1, 10), file(11, 20), file(21, 30)})

			all, _ := scannedBytes(t, e, "SELECT id FROM t")
			got, rows := scannedBytes(t, e, tc.sql)

			if tc.want != nil {
				assert.Equal(t, tc.want, rows)
			} else {
				assert.Empty(t, rows)
			}

			if tc.less {
				assert.Less(t, got, all)
			}
		})
	}
}

func TestIceberg_WrittenFilesCarryBoundsAndArePruned(t *testing.T) {
	t.Parallel()

	e := newIceEnv(t)
	e.mustRun(t, "CREATE DATABASE shop")
	e.mustRun(t, "CREATE TABLE b (id bigint, name string, day date, ts timestamp, score double) "+
		"LOCATION 's3://lake/b/' TBLPROPERTIES ('table_type'='ICEBERG')")
	e.mustRun(t, "INSERT INTO b VALUES (1, 'ann', DATE '2024-01-01', TIMESTAMP '2024-01-01 00:00:00', 1.5), "+
		"(2, 'bob', DATE '2024-01-09', TIMESTAMP '2024-01-09 12:00:00', 2.5)")
	e.mustRun(t, "INSERT INTO b VALUES (100, 'zed', DATE '2025-06-01', TIMESTAMP '2025-06-01 00:00:00', 9.5), "+
		"(200, 'zoe', NULL, NULL, NULL)")

	meta, _ := e.metadata(t, "b")
	cur, _ := meta.CurrentSnapshot()
	list, err := iceberg.ReadManifestList(e.objectAt(t, cur.ManifestList))
	require.NoError(t, err)

	var bounds []map[int][]byte

	for _, mf := range list {
		entries, merr := iceberg.ReadManifest(e.objectAt(t, mf.Path))
		require.NoError(t, merr)

		for _, en := range entries {
			bounds = append(bounds, en.DataFile.LowerBounds)
			assert.Len(t, en.DataFile.UpperBounds, 5)
		}
	}

	require.Len(t, bounds, 2)

	all, _ := scannedBytes(t, e, "SELECT id FROM b")

	for col, sql := range map[string]string{
		"id":    "SELECT id FROM b WHERE id = 200",
		"name":  "SELECT id, name FROM b WHERE name = 'zed'",
		"day":   "SELECT id, day FROM b WHERE day >= DATE '2025-01-01'",
		"ts":    "SELECT id, ts FROM b WHERE ts < TIMESTAMP '2024-02-01 00:00:00'",
		"score": "SELECT id, score FROM b WHERE score > 9.0",
	} {
		base, _ := scannedBytes(t, e, "SELECT id, "+col+" FROM b")
		if col == "id" {
			base = all
		}

		got, rows := scannedBytes(t, e, sql)
		assert.NotEmpty(t, rows, sql)
		assert.Less(t, got, base, sql)
	}

	_, rows := scannedBytes(t, e, "SELECT id FROM b WHERE day IS NULL")
	assert.Equal(t, [][]string{{"200"}}, rows)

	none, rows := scannedBytes(t, e, "SELECT id FROM b WHERE id > 1000")
	assert.Empty(t, rows)
	assert.Less(t, none, all)
}

func TestIceberg_PartitionsMetadataTable(t *testing.T) {
	t.Parallel()

	e := newOrders(t, "PARTITIONED BY (category)")

	got := e.mustRun(
		t,
		`SELECT partition.category, record_count, file_count, data.id.min, data.id.max, data.qty.null_count `+
			`FROM "orders$partitions" ORDER BY partition.category`,
	)
	assert.Equal(t, [][]string{
		{"a", "2", "1", "1", "3", "0"}, {"b", "2", "2", "2", "5", "0"}, {"c", "1", "1", "4", "4", "0"},
	}, got)

	assert.Equal(
		t,
		[][]string{{"5", "4"}},
		e.mustRun(t, `SELECT sum(record_count), sum(file_count) FROM "orders$partitions"`),
	)

	flat := newOrders(t, "")
	assert.Equal(t, [][]string{{"5", "2"}}, flat.mustRun(t, `SELECT record_count, file_count FROM "orders$partitions"`))
}

func TestIceberg_TargetFileSizeSplitsOutput(t *testing.T) {
	t.Parallel()

	values := func(from, to int) string {
		parts := make([]string, 0, to-from)
		for i := from; i < to; i++ {
			parts = append(parts, fmt.Sprintf("(%d, 'row-%d-padding-padding')", i, i))
		}

		return strings.Join(parts, ", ")
	}

	dataFiles := func(e *iceEnv) int {
		meta, _ := e.metadata(t, "s")
		cur, _ := meta.CurrentSnapshot()
		n, _ := strconv.Atoi(cur.Summary["total-data-files"])

		return n
	}

	e := newIceEnv(t)
	e.mustRun(t, "CREATE DATABASE shop")
	e.mustRun(t, "CREATE TABLE s (id bigint, label string) LOCATION 's3://lake/s/' TBLPROPERTIES ("+
		"'table_type'='ICEBERG', 'write_target_data_file_size_bytes'='2500', 'optimize_rewrite_data_file_threshold'='2')")

	e.mustRun(t, "INSERT INTO s VALUES "+values(0, 600))
	assert.Greater(t, dataFiles(e), 1)

	before := dataFiles(e)
	e.mustRun(t, "INSERT INTO s VALUES "+values(1000, 1003))
	e.mustRun(t, "INSERT INTO s VALUES "+values(2000, 2003))
	e.mustRun(t, "OPTIMIZE s REWRITE DATA USING BIN_PACK")

	assert.Equal(t, [][]string{{"606"}}, e.mustRun(t, "SELECT count(*) FROM s"))
	assert.LessOrEqual(t, dataFiles(e), before+1)
}

func TestIceberg_DMLRetriesLostCommit(t *testing.T) {
	t.Parallel()

	for _, stmt := range []string{
		"DELETE FROM orders WHERE id = 1",
		"UPDATE orders SET qty = 99 WHERE id = 1",
		"MERGE INTO orders t USING orders s ON t.id = s.id AND s.id = 1 WHEN MATCHED THEN DELETE",
	} {
		t.Run(stmt[:6], func(t *testing.T) {
			t.Parallel()

			e := newOrders(t, "")
			conflicts := &conflictOnce{iceCatalog: e.cat}
			e.b.SetGlueMetadataSource(conflicts)

			e.mustRun(t, stmt)
			assert.Equal(t, 1, conflicts.hits)
			assert.Equal(t, [][]string{{"0"}}, e.mustRun(t, "SELECT count(*) FROM orders WHERE id = 1 AND qty <> 99"))
		})
	}
}
