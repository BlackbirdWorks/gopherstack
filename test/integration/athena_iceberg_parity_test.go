package integration_test

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
	athenatypes "github.com/aws/aws-sdk-go-v2/service/athena/types"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3tablessdk "github.com/aws/aws-sdk-go-v2/service/s3tables"
	s3tablestypes "github.com/aws/aws-sdk-go-v2/service/s3tables/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/iceberg"
	"github.com/blackbirdworks/gopherstack/pkgs/parquetio"
)

type athenaLake struct {
	athena *athenasdk.Client
	glue   *gluesdk.Client
	s3     *s3.Client
	bucket string
	db     string
}

func newAthenaLake(t *testing.T, prefix string) *athenaLake {
	t.Helper()

	id := strings.ReplaceAll(uuid.NewString()[:8], "-", "")
	l := &athenaLake{
		athena: createAthenaClient(t), glue: createGlueClient(t), s3: createS3Client(t),
		bucket: prefix + "-" + id, db: prefix + "_" + id,
	}
	ctx := t.Context()

	_, err := l.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String(l.bucket)})
	require.NoError(t, err)

	_, err = l.glue.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{Name: aws.String(l.db)},
	})
	require.NoError(t, err)

	return l
}

func (l *athenaLake) put(t *testing.T, key string, body []byte) {
	t.Helper()

	_, err := l.s3.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String(l.bucket), Key: aws.String(key), Body: bytes.NewReader(body),
	})
	require.NoError(t, err)
}

func (l *athenaLake) get(t *testing.T, uri string) []byte {
	t.Helper()

	bucket, key, _ := strings.Cut(strings.TrimPrefix(uri, "s3://"), "/")
	out, err := l.s3.GetObject(t.Context(), &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	require.NoError(t, err)

	defer func() { _ = out.Body.Close() }()

	body, err := io.ReadAll(out.Body)
	require.NoError(t, err)

	return body
}

// query runs sql and returns the terminal state, the reason and the data rows.
func (l *athenaLake) query(t *testing.T, catalog, sql string) (athenatypes.QueryExecutionState, string, [][]string) {
	t.Helper()

	ctx := t.Context()
	qctx := &athenatypes.QueryExecutionContext{Database: aws.String(l.db)}

	if catalog != "" {
		qctx.Catalog = aws.String(catalog)
	}

	start, err := l.athena.StartQueryExecution(ctx, &athenasdk.StartQueryExecutionInput{
		QueryString:           aws.String(sql),
		QueryExecutionContext: qctx,
		ResultConfiguration: &athenatypes.ResultConfiguration{
			OutputLocation: aws.String("s3://" + l.bucket + "/results/"),
		},
	})
	require.NoError(t, err)

	var status *athenatypes.QueryExecutionStatus

	require.Eventually(t, func() bool {
		got, gerr := l.athena.GetQueryExecution(ctx, &athenasdk.GetQueryExecutionInput{
			QueryExecutionId: start.QueryExecutionId,
		})
		if gerr != nil || got.QueryExecution == nil {
			return false
		}

		status = got.QueryExecution.Status

		return status.State != athenatypes.QueryExecutionStateQueued &&
			status.State != athenatypes.QueryExecutionStateRunning
	}, 30*time.Second, 50*time.Millisecond, sql)

	reason := aws.ToString(status.StateChangeReason)
	if status.State != athenatypes.QueryExecutionStateSucceeded {
		return status.State, reason, nil
	}

	res, err := l.athena.GetQueryResults(ctx, &athenasdk.GetQueryResultsInput{QueryExecutionId: start.QueryExecutionId})
	require.NoError(t, err)

	var rows [][]string

	for i, r := range res.ResultSet.Rows {
		if i == 0 && len(res.ResultSet.ResultSetMetadata.ColumnInfo) > 0 {
			continue
		}

		cells := make([]string, len(r.Data))
		for j, d := range r.Data {
			cells[j] = aws.ToString(d.VarCharValue)
		}

		rows = append(rows, cells)
	}

	return status.State, reason, rows
}

func (l *athenaLake) must(t *testing.T, catalog, sql string) [][]string {
	t.Helper()

	state, reason, rows := l.query(t, catalog, sql)
	require.Equal(t, athenatypes.QueryExecutionStateSucceeded, state, "%s: %s", sql, reason)

	return rows
}

func TestIntegration_AthenaIceberg_CreateInsertSelectTimeTravel(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	l := newAthenaLake(t, "itice")
	ctx := t.Context()

	l.must(t, "", "CREATE TABLE events (id bigint, category string, ts timestamp) "+
		"PARTITIONED BY (category, bucket(4, id)) LOCATION 's3://"+l.bucket+"/events/' "+
		"TBLPROPERTIES ('table_type'='ICEBERG')")

	tbl, err := l.glue.GetTable(ctx, &gluesdk.GetTableInput{DatabaseName: aws.String(l.db), Name: aws.String("events")})
	require.NoError(t, err)
	assert.Equal(t, "ICEBERG", tbl.Table.Parameters["table_type"])
	require.NotEmpty(t, tbl.Table.Parameters["metadata_location"])

	l.must(
		t,
		"",
		"INSERT INTO events VALUES (1, 'a', TIMESTAMP '2024-01-01 10:00:00'), (2, 'b', TIMESTAMP '2024-01-02 11:30:00')",
	)
	l.must(t, "", "INSERT INTO events VALUES (3, 'a', TIMESTAMP '2024-01-03 12:00:00')")

	assert.Equal(
		t,
		[][]string{
			{"1", "a", "2024-01-01 10:00:00.000"},
			{"2", "b", "2024-01-02 11:30:00.000"},
			{"3", "a", "2024-01-03 12:00:00.000"},
		},
		l.must(t, "", "SELECT id, category, ts FROM events ORDER BY id"),
	)

	tbl, err = l.glue.GetTable(ctx, &gluesdk.GetTableInput{DatabaseName: aws.String(l.db), Name: aws.String("events")})
	require.NoError(t, err)

	meta, err := iceberg.ParseMetadata(l.get(t, tbl.Table.Parameters["metadata_location"]))
	require.NoError(t, err)
	require.Len(t, meta.Snapshots, 2)
	assert.NotEmpty(t, tbl.Table.Parameters["previous_metadata_location"])

	first := meta.Snapshots[0].SnapshotID
	assert.Equal(t, [][]string{{"2"}},
		l.must(t, "", fmt.Sprintf("SELECT count(*) FROM events FOR VERSION AS OF %d", first)))
	assert.Equal(t, [][]string{{"2"}},
		l.must(t, "", "SELECT count(*) FROM events FOR TIMESTAMP AS OF TIMESTAMP '"+
			time.UnixMilli(meta.Snapshots[0].TimestampMS).UTC().Format("2006-01-02 15:04:05.000")+" UTC'"))
	assert.Equal(t, [][]string{{"2"}}, l.must(t, "", `SELECT count(*) FROM "events$snapshots"`))

	state, reason, _ := l.query(t, "", "SELECT * FROM events FOR VERSION AS OF 42")
	assert.Equal(t, athenatypes.QueryExecutionStateFailed, state)
	assert.Contains(t, reason, "Iceberg snapshot ID does not exists")

	l.put(t, "events/metadata/gone", nil)
	_, err = l.glue.UpdateTable(ctx, &gluesdk.UpdateTableInput{
		DatabaseName: aws.String(l.db),
		TableInput: &gluetypes.TableInput{
			Name: aws.String("events"), TableType: tbl.Table.TableType, StorageDescriptor: tbl.Table.StorageDescriptor,
			Parameters: map[string]string{
				"table_type":        "ICEBERG",
				"metadata_location": "s3://" + l.bucket + "/events/metadata/missing.json",
			},
		},
	})
	require.NoError(t, err)

	state, reason, _ = l.query(t, "", "SELECT * FROM events")
	assert.Equal(t, athenatypes.QueryExecutionStateFailed, state)
	assert.Contains(t, reason, "ICEBERG_MISSING_METADATA")
}

func TestIntegration_AthenaS3TablesCatalog_QueryAndAppend(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	l := newAthenaLake(t, "its3t")
	tables := createS3TablesClient(t)
	ctx := t.Context()

	bucketName := "its3t-" + strings.ReplaceAll(uuid.NewString()[:8], "_", "")
	tb, err := tables.CreateTableBucket(ctx, &s3tablessdk.CreateTableBucketInput{Name: aws.String(bucketName)})
	require.NoError(t, err)

	_, err = tables.CreateNamespace(ctx, &s3tablessdk.CreateNamespaceInput{
		TableBucketARN: tb.Arn, Namespace: []string{"sales"},
	})
	require.NoError(t, err)

	_, err = tables.CreateTable(ctx, &s3tablessdk.CreateTableInput{
		TableBucketARN: tb.Arn,
		Namespace:      aws.String("sales"),
		Name:           aws.String("orders"),
		Format:         s3tablestypes.OpenTableFormatIceberg,
		Metadata: &s3tablestypes.TableMetadataMemberIceberg{Value: s3tablestypes.IcebergMetadata{
			Schema: &s3tablestypes.IcebergSchema{Fields: []s3tablestypes.SchemaField{
				{Name: aws.String("id"), Type: aws.String("long"), Required: false},
				{Name: aws.String("item"), Type: aws.String("string"), Required: false},
			}},
		}},
	})
	require.NoError(t, err)

	before, err := tables.GetTableMetadataLocation(ctx, &s3tablessdk.GetTableMetadataLocationInput{
		TableBucketARN: tb.Arn, Namespace: aws.String("sales"), Name: aws.String("orders"),
	})
	require.NoError(t, err)

	cat := "s3tablescatalog/" + bucketName

	assert.Empty(t, l.must(t, cat, `SELECT * FROM "`+cat+`"."sales"."orders"`))
	l.must(t, cat, `INSERT INTO "`+cat+`"."sales"."orders" VALUES (1, 'pen'), (2, 'ink')`)
	assert.Equal(
		t,
		[][]string{{"1", "pen"}, {"2", "ink"}},
		l.must(t, cat, `SELECT id, item FROM sales.orders ORDER BY id`),
	)

	after, err := tables.GetTableMetadataLocation(ctx, &s3tablessdk.GetTableMetadataLocationInput{
		TableBucketARN: tb.Arn, Namespace: aws.String("sales"), Name: aws.String("orders"),
	})
	require.NoError(t, err)
	assert.NotEqual(t, aws.ToString(before.MetadataLocation), aws.ToString(after.MetadataLocation))

	meta, err := iceberg.ParseMetadata(l.get(t, aws.ToString(after.MetadataLocation)))
	require.NoError(t, err)
	assert.Len(t, meta.Snapshots, 1)

	state, reason, _ := l.query(t, cat, `SELECT * FROM "`+cat+`"."sales"."nope"`)
	assert.Equal(t, athenatypes.QueryExecutionStateFailed, state)
	assert.Contains(t, reason, "TABLE_NOT_FOUND")
}

func TestIntegration_AthenaDelta_ReadsTransactionLog(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	l := newAthenaLake(t, "itdelta")

	var buf bytes.Buffer

	require.NoError(t, parquetio.Write(&buf,
		[]parquetio.Column{{Name: "id", Type: "bigint"}, {Name: "name", Type: "string"}},
		[][]any{{int64(1), "ann"}, {int64(2), "bob"}}, "SNAPPY"))
	l.put(t, "dl/part-0.parquet", buf.Bytes())

	schema := `{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},` +
		`{\"name\":\"name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}`
	l.put(t, "dl/_delta_log/00000000000000000000.json", []byte(strings.Join([]string{
		`{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}`,
		`{"metaData":{"id":"t","format":{"provider":"parquet"},"schemaString":"` + schema +
			`","partitionColumns":[],"configuration":{}}}`,
		`{"add":{"path":"part-0.parquet","partitionValues":{},"size":1,"dataChange":true}}`,
	}, "\n")+"\n"))

	l.must(t, "", "CREATE EXTERNAL TABLE people LOCATION 's3://"+l.bucket+"/dl/' TBLPROPERTIES ('table_type'='DELTA')")

	tbl, err := l.glue.GetTable(
		t.Context(),
		&gluesdk.GetTableInput{DatabaseName: aws.String(l.db), Name: aws.String("people")},
	)
	require.NoError(t, err)
	assert.Len(t, tbl.Table.StorageDescriptor.Columns, 2)

	assert.Equal(t, [][]string{{"1", "ann"}, {"2", "bob"}}, l.must(t, "", "SELECT id, name FROM people ORDER BY id"))

	state, reason, _ := l.query(t, "", "INSERT INTO people VALUES (3, 'cy')")
	assert.Equal(t, athenatypes.QueryExecutionStateFailed, state)
	assert.Contains(t, reason, "read-only")
}
