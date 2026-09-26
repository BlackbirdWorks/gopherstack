package stepfunctions_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	s3pkg "github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// itemReaderMapDef wraps an ItemReader clause in a plain (INLINE) Map state
// whose ItemProcessor echoes each item back via a Pass state, so the Map's
// own output is the exact array of items the ItemReader produced.
func itemReaderMapDef(itemReaderJSON string) string {
	return `{
		"StartAt": "M",
		"States": {
			"M": {
				"Type": "Map",
				"ItemReader": ` + itemReaderJSON + `,
				"ItemProcessor": {
					"StartAt": "P",
					"States": {"P": {"Type": "Pass", "End": true}}
				},
				"End": true
			}
		}
	}`
}

// putS3Object writes data to bucket/key on s3Bk, failing the test on error.
func putS3Object(t *testing.T, s3Bk *s3pkg.InMemoryBackend, bucket, key string, data []byte) {
	t.Helper()

	_, err := s3Bk.PutObject(context.Background(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	require.NoError(t, err)
}

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()

	var buf bytes.Buffer

	w := gzip.NewWriter(&buf)
	_, err := w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	return buf.Bytes()
}

// runItemReaderExecution wires s3Bk into a fresh stepfunctions backend, runs
// itemReaderJSON's Map state through the real SFN SDK client, and returns
// the terminal execution's status/output/error/cause.
func runItemReaderExecution(
	t *testing.T, s3Bk *s3pkg.InMemoryBackend, itemReaderJSON string,
) (sfntypes.ExecutionStatus, string, string, string) {
	t.Helper()

	backend := stepfunctions.NewInMemoryBackend()
	backend.SetS3Reader(stepfunctions.NewS3Integration(s3Bk))
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	createSM, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String("item-reader-" + uuid.NewString()[:8]),
		Definition: aws.String(itemReaderMapDef(itemReaderJSON)),
		RoleArn:    aws.String(validRoleARN),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	startOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: createSM.StateMachineArn,
		Input:           aws.String(`{}`),
	})
	require.NoError(t, err)

	waitTerminal(ctx, t, client, aws.ToString(startOut.ExecutionArn))

	desc, err := client.DescribeExecution(ctx, &sfnsdk.DescribeExecutionInput{
		ExecutionArn: startOut.ExecutionArn,
	})
	require.NoError(t, err)

	return desc.Status, aws.ToString(desc.Output), aws.ToString(desc.Error), aws.ToString(desc.Cause)
}

// TestItemReader_S3ListObjectsV2 covers the Resource arn:aws:states:::s3:listObjectsV2,
// both its default object-metadata mode and its LOAD_AND_FLATTEN transformation
// (AWS docs: input-output-itemreader.html).
func TestItemReader_S3ListObjectsV2(t *testing.T) {
	t.Parallel()

	t.Run("default lists object metadata", func(t *testing.T) {
		t.Parallel()

		const bucket = "list-meta-bucket"

		s3Bk := newBucketBackedS3(t, bucket)
		putS3Object(t, s3Bk, bucket, "data/a.txt", []byte("hello"))
		putS3Object(t, s3Bk, bucket, "data/b.txt", []byte("world!!"))

		itemReader := `{
			"Resource": "arn:aws:states:::s3:listObjectsV2",
			"Parameters": {"Bucket": "` + bucket + `", "Prefix": "data/"}
		}`

		status, output, errCode, cause := runItemReaderExecution(t, s3Bk, itemReader)
		require.Equal(t, sfntypes.ExecutionStatusSucceeded, status, "error=%s cause=%s", errCode, cause)

		var items []map[string]any
		require.NoError(t, json.Unmarshal([]byte(output), &items))
		require.Len(t, items, 2)

		assert.Equal(t, "data/a.txt", items[0]["Key"])
		assert.InDelta(t, 5.0, items[0]["Size"], 0)
		assert.Equal(t, "STANDARD", items[0]["StorageClass"])
		assert.NotEmpty(t, items[0]["Etag"])
		assert.Positive(t, items[0]["LastModified"])

		assert.Equal(t, "data/b.txt", items[1]["Key"])
		assert.InDelta(t, 7.0, items[1]["Size"], 0)
	})

	t.Run("LOAD_AND_FLATTEN JSON reads and flattens object contents", func(t *testing.T) {
		t.Parallel()

		const bucket = "list-flatten-json-bucket"

		s3Bk := newBucketBackedS3(t, bucket)
		putS3Object(t, s3Bk, bucket, "flat/a.json", []byte(`[{"n":1},{"n":2}]`))
		putS3Object(t, s3Bk, bucket, "flat/b.json", []byte(`[{"n":3}]`))

		itemReader := `{
			"Resource": "arn:aws:states:::s3:listObjectsV2",
			"ReaderConfig": {"InputType": "JSON", "Transformation": "LOAD_AND_FLATTEN"},
			"Parameters": {"Bucket": "` + bucket + `", "Prefix": "flat/"}
		}`

		status, output, errCode, cause := runItemReaderExecution(t, s3Bk, itemReader)
		require.Equal(t, sfntypes.ExecutionStatusSucceeded, status, "error=%s cause=%s", errCode, cause)
		assert.JSONEq(t, `[{"n":1},{"n":2},{"n":3}]`, output)
	})

	t.Run("LOAD_AND_FLATTEN CSV reads and flattens object contents", func(t *testing.T) {
		t.Parallel()

		const bucket = "list-flatten-csv-bucket"

		s3Bk := newBucketBackedS3(t, bucket)
		putS3Object(t, s3Bk, bucket, "flat/a.csv", []byte("col\nx\ny\n"))
		putS3Object(t, s3Bk, bucket, "flat/b.csv", []byte("col\nz\n"))

		itemReader := `{
			"Resource": "arn:aws:states:::s3:listObjectsV2",
			"ReaderConfig": {"InputType": "CSV", "Transformation": "LOAD_AND_FLATTEN"},
			"Parameters": {"Bucket": "` + bucket + `", "Prefix": "flat/"}
		}`

		status, output, errCode, cause := runItemReaderExecution(t, s3Bk, itemReader)
		require.Equal(t, sfntypes.ExecutionStatusSucceeded, status, "error=%s cause=%s", errCode, cause)
		assert.JSONEq(t, `[{"col":"x"},{"col":"y"},{"col":"z"}]`, output)
	})

	t.Run("missing bucket fails with States.ItemReaderFailed", func(t *testing.T) {
		t.Parallel()

		s3Bk := newBucketBackedS3(t, "unrelated-bucket")

		itemReader := `{
			"Resource": "arn:aws:states:::s3:listObjectsV2",
			"Parameters": {"Bucket": "does-not-exist", "Prefix": ""}
		}`

		status, _, errCode, cause := runItemReaderExecution(t, s3Bk, itemReader)
		assert.Equal(t, sfntypes.ExecutionStatusFailed, status)
		assert.Equal(t, "States.ItemReaderFailed", errCode)
		assert.Contains(t, cause, "NoSuchBucket")
	})
}

// TestItemReader_S3Manifest covers ReaderConfig ManifestType S3_INVENTORY and
// the legacy InputType=MANIFEST alias, including a gzip-compressed data file
// (AWS docs: input-output-itemreader.html, "Amazon S3 inventory").
func TestItemReader_S3Manifest(t *testing.T) {
	t.Parallel()

	const inventoryCSV = `"src-bucket","csvDataset/titles.csv","3399671","2022-11-16T00:29:32.000Z"` + "\n" +
		`"src-bucket","imageDataset/pic.jpg","27034","2022-11-15T20:02:16.000Z"` + "\n"

	buildManifest := func(t *testing.T, dataKey string) string {
		t.Helper()

		manifest := map[string]any{
			"sourceBucket":      "src-bucket",
			"destinationBucket": "arn:aws:s3:::inv-bucket",
			"version":           "2016-11-30",
			"fileFormat":        "CSV",
			"fileSchema":        "Bucket, Key, Size, LastModifiedDate",
			"files":             []map[string]any{{"key": dataKey, "size": len(inventoryCSV)}},
		}

		b, err := json.Marshal(manifest)
		require.NoError(t, err)

		return string(b)
	}

	t.Run("ManifestType S3_INVENTORY, gzip data file", func(t *testing.T) {
		t.Parallel()

		const bucket = "inv-bucket"

		s3Bk := newBucketBackedS3(t, bucket)
		putS3Object(t, s3Bk, bucket, "inv/data0.csv.gz", gzipBytes(t, []byte(inventoryCSV)))
		putS3Object(t, s3Bk, bucket, "inv/manifest.json", []byte(buildManifest(t, "inv/data0.csv.gz")))

		itemReader := `{
			"Resource": "arn:aws:states:::s3:getObject",
			"ReaderConfig": {"ManifestType": "S3_INVENTORY"},
			"Parameters": {"Bucket": "` + bucket + `", "Key": "inv/manifest.json"}
		}`

		status, output, errCode, cause := runItemReaderExecution(t, s3Bk, itemReader)
		require.Equal(t, sfntypes.ExecutionStatusSucceeded, status, "error=%s cause=%s", errCode, cause)

		var items []map[string]any
		require.NoError(t, json.Unmarshal([]byte(output), &items))
		require.Len(t, items, 2)
		assert.Equal(t, map[string]any{
			"Bucket": "src-bucket", "Key": "csvDataset/titles.csv",
			"Size": "3399671", "LastModifiedDate": "2022-11-16T00:29:32.000Z",
		}, items[0])
	})

	t.Run("legacy InputType MANIFEST, plain data file", func(t *testing.T) {
		t.Parallel()

		const bucket = "inv-bucket-legacy"

		s3Bk := newBucketBackedS3(t, bucket)
		putS3Object(t, s3Bk, bucket, "inv/data0.csv", []byte(inventoryCSV))
		putS3Object(t, s3Bk, bucket, "inv/manifest.json", []byte(buildManifest(t, "inv/data0.csv")))

		itemReader := `{
			"Resource": "arn:aws:states:::s3:getObject",
			"ReaderConfig": {"InputType": "MANIFEST"},
			"Parameters": {"Bucket": "` + bucket + `", "Key": "inv/manifest.json"}
		}`

		status, output, errCode, cause := runItemReaderExecution(t, s3Bk, itemReader)
		require.Equal(t, sfntypes.ExecutionStatusSucceeded, status, "error=%s cause=%s", errCode, cause)

		var items []map[string]any
		require.NoError(t, json.Unmarshal([]byte(output), &items))
		require.Len(t, items, 2)
		assert.Equal(t, "imageDataset/pic.jpg", items[1]["Key"])
	})

	t.Run("ManifestType ATHENA_DATA is a recorded gap", func(t *testing.T) {
		t.Parallel()

		const bucket = "athena-bucket"

		s3Bk := newBucketBackedS3(t, bucket)
		putS3Object(t, s3Bk, bucket, "athena/manifest.csv", []byte("s3://athena-bucket/data/f1.csv\n"))

		itemReader := `{
			"Resource": "arn:aws:states:::s3:getObject",
			"ReaderConfig": {"ManifestType": "ATHENA_DATA", "InputType": "CSV"},
			"Parameters": {"Bucket": "` + bucket + `", "Key": "athena/manifest.csv"}
		}`

		status, _, errCode, cause := runItemReaderExecution(t, s3Bk, itemReader)
		assert.Equal(t, sfntypes.ExecutionStatusFailed, status)
		assert.Equal(t, "States.ItemReaderFailed", errCode)
		assert.Contains(t, cause, "ATHENA_DATA")
	})
}

// TestItemReader_S3GetObject_Errors covers ItemReader failure behavior for
// the s3:getObject Resource: a missing key, and the recorded PARQUET gap.
func TestItemReader_S3GetObject_Errors(t *testing.T) {
	t.Parallel()

	t.Run("missing key fails with States.ItemReaderFailed", func(t *testing.T) {
		t.Parallel()

		const bucket = "getobject-bucket"
		s3Bk := newBucketBackedS3(t, bucket)

		itemReader := `{
			"Resource": "arn:aws:states:::s3:getObject",
			"Parameters": {"Bucket": "` + bucket + `", "Key": "does-not-exist.json"}
		}`

		status, _, errCode, cause := runItemReaderExecution(t, s3Bk, itemReader)
		assert.Equal(t, sfntypes.ExecutionStatusFailed, status)
		assert.Equal(t, "States.ItemReaderFailed", errCode)
		assert.Contains(t, cause, "NoSuchKey")
	})

	t.Run("PARQUET InputType is a recorded gap", func(t *testing.T) {
		t.Parallel()

		const bucket = "parquet-bucket"
		s3Bk := newBucketBackedS3(t, bucket)
		putS3Object(t, s3Bk, bucket, "data.parquet", []byte("not really parquet"))

		itemReader := `{
			"Resource": "arn:aws:states:::s3:getObject",
			"ReaderConfig": {"InputType": "PARQUET"},
			"Parameters": {"Bucket": "` + bucket + `", "Key": "data.parquet"}
		}`

		status, _, errCode, cause := runItemReaderExecution(t, s3Bk, itemReader)
		assert.Equal(t, sfntypes.ExecutionStatusFailed, status)
		assert.Equal(t, "States.ItemReaderFailed", errCode)
		assert.Contains(t, cause, "PARQUET")
	})
}
