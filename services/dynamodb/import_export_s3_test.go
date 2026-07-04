package dynamodb_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// mockS3 is an in-memory S3Accessor for exercising ImportTable / Export.
type mockS3 struct {
	objects map[string][]byte // "bucket/key" -> bytes
}

func newMockS3() *mockS3 { return &mockS3{objects: map[string][]byte{}} }

func (m *mockS3) put(bucket, key string, data []byte) { m.objects[bucket+"/"+key] = data }

func (m *mockS3) GetObject(
	_ context.Context, in *s3sdk.GetObjectInput,
) (*s3sdk.GetObjectOutput, error) {
	data, ok := m.objects[aws.ToString(in.Bucket)+"/"+aws.ToString(in.Key)]
	if !ok {
		return nil, &s3types.NoSuchKey{}
	}

	return &s3sdk.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(data))}, nil
}

func (m *mockS3) ListObjectsV2(
	_ context.Context, in *s3sdk.ListObjectsV2Input,
) (*s3sdk.ListObjectsV2Output, error) {
	bucket := aws.ToString(in.Bucket)
	prefix := aws.ToString(in.Prefix)

	var contents []s3types.Object
	for k := range m.objects {
		b, key, _ := strings.Cut(k, "/")
		if b == bucket && strings.HasPrefix(key, prefix) {
			contents = append(contents, s3types.Object{Key: aws.String(key)})
		}
	}
	sort.Slice(contents, func(i, j int) bool {
		return aws.ToString(contents[i].Key) < aws.ToString(contents[j].Key)
	})

	return &s3sdk.ListObjectsV2Output{Contents: contents, IsTruncated: aws.Bool(false)}, nil
}

func (m *mockS3) PutObject(
	_ context.Context, in *s3sdk.PutObjectInput,
) (*s3sdk.PutObjectOutput, error) {
	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}
	m.put(aws.ToString(in.Bucket), aws.ToString(in.Key), data)

	return &s3sdk.PutObjectOutput{}, nil
}

func gzipBytes(t *testing.T, raw string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err := gz.Write([]byte(raw))
	require.NoError(t, err)
	require.NoError(t, gz.Close())

	return buf.Bytes()
}

func importCreationParams(name string) *ddbtypes.TableCreationParameters {
	return &ddbtypes.TableCreationParameters{
		TableName: aws.String(name),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	}
}

func waitForImport(t *testing.T, db *dynamodb.InMemoryDB, arn string) *sdk.DescribeImportOutput {
	t.Helper()

	for range 50 {
		out, err := db.DescribeImport(t.Context(), &sdk.DescribeImportInput{ImportArn: aws.String(arn)})
		require.NoError(t, err)
		if out.ImportTableDescription.ImportStatus != ddbtypes.ImportStatusInProgress {
			return out
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("import %s did not complete", arn)

	return nil
}

func waitForExport(t *testing.T, h *dynamodb.DynamoDBHandler, arn string) {
	t.Helper()

	for range 50 {
		code, res := invokeOp(t, h, "DescribeExport", map[string]any{"ExportArn": arn})
		require.Equal(t, 200, code)
		desc := res["ExportDescription"].(map[string]any)
		if desc["ExportStatus"] != "IN_PROGRESS" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("export %s did not complete", arn)
}

// TestImportTable_FromS3_DynamoDBJSON verifies ImportTable creates the table and
// ingests gzipped DynamoDB-JSON objects, reporting accurate counts.
func TestImportTable_FromS3_DynamoDBJSON(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	s3 := newMockS3()
	db.SetS3Backend(s3)

	s3.put("src", "data/part-1.json.gz", gzipBytes(t,
		`{"Item":{"pk":{"S":"a"},"v":{"N":"1"}}}`+"\n"+
			`{"Item":{"pk":{"S":"b"},"v":{"N":"2"}}}`+"\n"))

	out, err := db.ImportTable(t.Context(), &sdk.ImportTableInput{
		S3BucketSource: &ddbtypes.S3BucketSource{
			S3Bucket:    aws.String("src"),
			S3KeyPrefix: aws.String("data/"),
		},
		InputFormat:             ddbtypes.InputFormatDynamodbJson,
		InputCompressionType:    ddbtypes.InputCompressionTypeGzip,
		TableCreationParameters: importCreationParams("ImportedJSON"),
	})
	require.NoError(t, err)

	importDesc := waitForImport(t, db, aws.ToString(out.ImportTableDescription.ImportArn))

	assert.Equal(t, ddbtypes.ImportStatusCompleted, importDesc.ImportTableDescription.ImportStatus)
	assert.Equal(t, int64(2), importDesc.ImportTableDescription.ImportedItemCount)
	assert.Equal(t, int64(2), importDesc.ImportTableDescription.ProcessedItemCount)

	got, err := db.GetItem(t.Context(), &sdk.GetItemInput{
		TableName: aws.String("ImportedJSON"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "a"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, got.Item)
	assert.Equal(t, "1", got.Item["v"].(*ddbtypes.AttributeValueMemberN).Value)
}

// TestImportTable_FromS3_CSV verifies CSV ingestion with a header row.
func TestImportTable_FromS3_CSV(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	s3 := newMockS3()
	db.SetS3Backend(s3)

	s3.put("src", "csv/rows.csv", []byte("pk,name\na,Alice\nb,Bob\n"))

	out, err := db.ImportTable(t.Context(), &sdk.ImportTableInput{
		S3BucketSource: &ddbtypes.S3BucketSource{
			S3Bucket:    aws.String("src"),
			S3KeyPrefix: aws.String("csv/"),
		},
		InputFormat:             ddbtypes.InputFormatCsv,
		TableCreationParameters: importCreationParams("ImportedCSV"),
	})
	require.NoError(t, err)
	importDesc := waitForImport(t, db, aws.ToString(out.ImportTableDescription.ImportArn))
	assert.Equal(t, ddbtypes.ImportStatusCompleted, importDesc.ImportTableDescription.ImportStatus)
	assert.Equal(t, int64(2), importDesc.ImportTableDescription.ImportedItemCount)

	got, err := db.GetItem(t.Context(), &sdk.GetItemInput{
		TableName: aws.String("ImportedCSV"),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "b"},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, got.Item)
	assert.Equal(t, "Bob", got.Item["name"].(*ddbtypes.AttributeValueMemberS).Value)
}

// TestImportTable_ION_Unsupported verifies that ION input fails the import cleanly.
func TestImportTable_ION_Unsupported(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	s3 := newMockS3()
	db.SetS3Backend(s3)
	s3.put("src", "ion/data.ion", []byte("{pk: \"a\"}"))

	out, err := db.ImportTable(t.Context(), &sdk.ImportTableInput{
		S3BucketSource: &ddbtypes.S3BucketSource{
			S3Bucket:    aws.String("src"),
			S3KeyPrefix: aws.String("ion/"),
		},
		InputFormat:             ddbtypes.InputFormatIon,
		TableCreationParameters: importCreationParams("ImportedION"),
	})
	require.NoError(t, err)
	importDesc := waitForImport(t, db, aws.ToString(out.ImportTableDescription.ImportArn))
	assert.Equal(t, ddbtypes.ImportStatusFailed, importDesc.ImportTableDescription.ImportStatus)
	assert.NotEmpty(t, aws.ToString(importDesc.ImportTableDescription.FailureCode))
}

// TestExportImport_RoundTrip exports a populated table to S3 and re-imports it.
func TestExportImport_RoundTrip(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	s3 := newMockS3()
	db.SetS3Backend(s3)
	h := dynamodb.NewHandler(db)

	createTableHelper(t, db, "SourceTbl", "pk")
	for _, id := range []string{"x", "y", "z"} {
		_, err := db.PutItem(t.Context(), &sdk.PutItemInput{
			TableName: aws.String("SourceTbl"),
			Item: map[string]ddbtypes.AttributeValue{
				"pk": &ddbtypes.AttributeValueMemberS{Value: id},
			},
		})
		require.NoError(t, err)
	}

	tbl, ok := db.GetTable("SourceTbl")
	require.True(t, ok)

	// Export to S3 via the handler.
	code, res := invokeOp(t, h, "ExportTableToPointInTime", map[string]any{
		"TableArn": tbl.TableArn,
		"S3Bucket": "exb",
		"S3Prefix": "out",
	})
	require.Equal(t, 200, code)
	waitForExport(t, h, res["ExportDescription"].(map[string]any)["ExportArn"].(string))

	// Re-import the exported data into a new table from the data/ prefix.
	var dataPrefix string
	for k := range s3.objects {
		if strings.Contains(k, "/data/") {
			_, key, _ := strings.Cut(k, "/")
			dataPrefix = strings.TrimSuffix(key, "00000.json.gz")
		}
	}
	require.NotEmpty(t, dataPrefix, "export must write a data object")

	out, err := db.ImportTable(t.Context(), &sdk.ImportTableInput{
		S3BucketSource: &ddbtypes.S3BucketSource{
			S3Bucket:    aws.String("exb"),
			S3KeyPrefix: aws.String(dataPrefix),
		},
		InputFormat:             ddbtypes.InputFormatDynamodbJson,
		InputCompressionType:    ddbtypes.InputCompressionTypeGzip,
		TableCreationParameters: importCreationParams("RoundTripTbl"),
	})
	require.NoError(t, err)
	importDesc := waitForImport(t, db, aws.ToString(out.ImportTableDescription.ImportArn))
	assert.Equal(t, int64(3), importDesc.ImportTableDescription.ImportedItemCount)
}
