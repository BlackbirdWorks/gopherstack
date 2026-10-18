package app

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

func TestAthenaEngine_ParquetCTASRoundTrip(t *testing.T) {
	t.Parallel()

	f := newAthenaEngineFx(t)
	f.salesTable(t)

	f.ok(t, "CREATE TABLE shop.pq WITH (format='PARQUET', parquet_compression='GZIP', "+
		"external_location='s3://lake/pq/') AS SELECT id, category, amount FROM shop.sales WHERE amount > 4")

	keys := f.keys(t, "pq/")
	require.Len(t, keys, 1)

	pf, err := parquetio.Open(f.body(t, keys[0]))
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "category", "amount"}, pf.Columns())
	assert.Equal(t, int64(5), pf.NumRows())

	tbl, err := f.glue.GetTable(t.Context(), &glue.GetTableInput{
		DatabaseName: aws.String("shop"), Name: aws.String("pq"),
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(tbl.Table.StorageDescriptor.SerdeInfo.SerializationLibrary), "ParquetHiveSerDe")
	assert.Contains(t, aws.ToString(tbl.Table.StorageDescriptor.InputFormat), "MapredParquetInputFormat")

	id := f.ok(t, "SELECT category, sum(amount) FROM shop.pq GROUP BY category ORDER BY category")
	assert.Equal(t, [][]string{{"category", "_col1"}, {"books", "30.0"}, {"games", "55.0"}, {"toys", "7.0"}}, f.rows(t, id))
}

func TestAthenaEngine_ParquetDefaultFormatAndPartitions(t *testing.T) {
	t.Parallel()

	f := newAthenaEngineFx(t)
	f.salesTable(t)

	f.ok(t, "CREATE TABLE shop.pqp WITH (partitioned_by=ARRAY['category'], external_location='s3://lake/pqp/') "+
		"AS SELECT id, amount, category FROM shop.sales")
	assert.Len(t, f.keys(t, "pqp/category=books/"), 1)
	assert.Len(t, f.keys(t, "pqp/category=games/"), 1)

	id := f.ok(t, "SELECT count(*) FROM shop.pqp WHERE category = 'games'")
	assert.Equal(t, [][]string{{"_col0"}, {"2"}}, f.rows(t, id))

	f.ok(t, "INSERT INTO shop.pqp SELECT id + 100, amount, category FROM shop.sales WHERE id = 1")
	id = f.ok(t, "SELECT count(*) FROM shop.pqp")
	assert.Equal(t, [][]string{{"_col0"}, {"7"}}, f.rows(t, id))
}

func TestAthenaEngine_ParquetUnload(t *testing.T) {
	t.Parallel()

	f := newAthenaEngineFx(t)
	f.salesTable(t)

	f.ok(t, "UNLOAD (SELECT id, category FROM shop.sales) TO 's3://lake/unl-pq/' WITH (compression='ZSTD')")

	keys := f.keys(t, "unl-pq/")
	require.Len(t, keys, 1)

	pf, err := parquetio.Open(f.body(t, keys[0]))
	require.NoError(t, err)
	assert.Equal(t, int64(6), pf.NumRows())
}
