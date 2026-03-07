package integration_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkglogger "github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	firehosepkg "github.com/blackbirdworks/gopherstack/services/firehose"
	s3pkg "github.com/blackbirdworks/gopherstack/services/s3"
)

// newFirehoseS3Server builds an in-process server with Firehose and S3 backends wired together.
// It returns the backend instances for direct assertion and the server for cleanup.
func newFirehoseS3Server(t *testing.T) (*firehosepkg.InMemoryBackend, *s3pkg.InMemoryBackend) {
	t.Helper()

	s3Bk := s3pkg.NewInMemoryBackend(nil)
	fhBk := firehosepkg.NewInMemoryBackend("000000000000", "us-east-1")
	fhBk.SetS3Backend(s3Bk)

	e := echo.New()
	e.Pre(pkglogger.EchoMiddleware(slog.Default()))

	reg := service.NewRegistry()
	require.NoError(t, reg.Register(s3pkg.NewHandler(s3Bk)))
	require.NoError(t, reg.Register(firehosepkg.NewHandler(fhBk)))

	e.Use(service.NewServiceRouter(reg).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return fhBk, s3Bk
}

// keepOrDropLambdaInvoker is a mock LambdaInvoker that marks records whose decoded data
// contains "keep" as Ok and all other records as Dropped — mirroring the Python transform
// used in Firehose transformation tests, but without needing Docker or a real Lambda runtime.
type keepOrDropLambdaInvoker struct{}

// transformRecord is a minimal representation of a record in a Firehose transform event/response.
type transformRecord struct {
	RecordID string `json:"recordId"`
	Result   string `json:"result,omitempty"`
	Data     string `json:"data"`
}

// transformEvent is the Firehose Lambda transform event payload.
type transformEvent struct {
	Records []transformRecord `json:"records"`
}

func (keepOrDropLambdaInvoker) InvokeFunction(
	_ context.Context,
	_ string,
	_ string,
	payload []byte,
) ([]byte, int, error) {
	var event transformEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return nil, 500, err
	}

	out := make([]transformRecord, len(event.Records))
	for i, rec := range event.Records {
		decoded, err := base64.StdEncoding.DecodeString(rec.Data)
		result := "Dropped"
		if err == nil && strings.Contains(string(decoded), "keep") {
			result = "Ok"
		}
		out[i] = transformRecord{RecordID: rec.RecordID, Result: result, Data: rec.Data}
	}

	resp, err := json.Marshal(map[string]any{"records": out})
	if err != nil {
		return nil, 500, err
	}

	return resp, 200, nil
}

// getS3Object retrieves an object body by key from the in-memory S3 backend.
func getS3Object(t *testing.T, s3Bk *s3pkg.InMemoryBackend, bucket, key string) []byte {
	t.Helper()

	out, err := s3Bk.GetObject(t.Context(), &s3sdk.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer out.Body.Close()

	body, err := io.ReadAll(out.Body)
	require.NoError(t, err)

	return body
}

// listS3Keys lists all object keys in a bucket using the in-memory S3 backend.
func listS3Keys(t *testing.T, s3Bk *s3pkg.InMemoryBackend, bucket string) []string {
	t.Helper()

	out, err := s3Bk.ListObjectsV2(t.Context(), &s3sdk.ListObjectsV2Input{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	keys := make([]string, 0, len(out.Contents))
	for _, obj := range out.Contents {
		keys = append(keys, aws.ToString(obj.Key))
	}

	return keys
}

// TestIntegration_Firehose_S3Delivery tests that records put to a stream with an S3 destination
// are delivered to S3 on FlushAll.
func TestIntegration_Firehose_S3Delivery(t *testing.T) {
	t.Parallel()

	fhBk, s3Bk := newFirehoseS3Server(t)
	ctx := t.Context()

	_, err := s3Bk.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String("fh-bucket")})
	require.NoError(t, err)

	_, err = fhBk.CreateDeliveryStream(firehosepkg.CreateDeliveryStreamInput{
		Name: "delivery-stream",
		S3Destination: &firehosepkg.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::fh-bucket",
		},
	})
	require.NoError(t, err)

	require.NoError(t, fhBk.PutRecord("delivery-stream", []byte(`{"user":"alice"}`)))
	require.NoError(t, fhBk.PutRecord("delivery-stream", []byte(`{"user":"bob"}`)))

	fhBk.FlushAll(ctx)

	keys := listS3Keys(t, s3Bk, "fh-bucket")
	require.NotEmpty(t, keys, "expected at least one S3 object after flush")

	body := getS3Object(t, s3Bk, "fh-bucket", keys[0])
	assert.Contains(t, string(body), "alice")
	assert.Contains(t, string(body), "bob")
}

// TestIntegration_Firehose_S3Delivery_SizeBasedFlush tests that records are automatically
// flushed when the buffer size threshold is exceeded.
func TestIntegration_Firehose_S3Delivery_SizeBasedFlush(t *testing.T) {
	t.Parallel()

	fhBk, s3Bk := newFirehoseS3Server(t)
	ctx := t.Context()

	_, err := s3Bk.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String("size-bucket")})
	require.NoError(t, err)

	_, err = fhBk.CreateDeliveryStream(firehosepkg.CreateDeliveryStreamInput{
		Name: "size-stream",
		S3Destination: &firehosepkg.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::size-bucket",
			BufferingHints: &firehosepkg.BufferingHints{
				SizeInMBs:         1,
				IntervalInSeconds: 300,
			},
		},
	})
	require.NoError(t, err)

	// Write just under 1 MB — no flush yet.
	underLimit := make([]byte, 900*1024)
	require.NoError(t, fhBk.PutRecord("size-stream", underLimit))
	keys := listS3Keys(t, s3Bk, "size-bucket")
	assert.Empty(t, keys, "should not flush before size limit is reached")

	// Push over 1 MB — should trigger an automatic flush.
	overLimit := make([]byte, 200*1024)
	require.NoError(t, fhBk.PutRecord("size-stream", overLimit))
	keys = listS3Keys(t, s3Bk, "size-bucket")
	require.NotEmpty(t, keys, "should have flushed to S3 after exceeding size limit")
}

// TestIntegration_Firehose_GzipCompression verifies that GZIP compression is applied
// before delivery to S3.
func TestIntegration_Firehose_GzipCompression(t *testing.T) {
	t.Parallel()

	fhBk, s3Bk := newFirehoseS3Server(t)
	ctx := t.Context()

	_, err := s3Bk.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String("gzip-bucket")})
	require.NoError(t, err)

	_, err = fhBk.CreateDeliveryStream(firehosepkg.CreateDeliveryStreamInput{
		Name: "gzip-stream",
		S3Destination: &firehosepkg.S3DestinationDescription{
			BucketARN:         "arn:aws:s3:::gzip-bucket",
			CompressionFormat: "GZIP",
		},
	})
	require.NoError(t, err)

	require.NoError(t, fhBk.PutRecord("gzip-stream", []byte(`{"compressed":"data"}`)))
	fhBk.FlushAll(ctx)

	keys := listS3Keys(t, s3Bk, "gzip-bucket")
	require.NotEmpty(t, keys)

	body := getS3Object(t, s3Bk, "gzip-bucket", keys[0])
	require.GreaterOrEqual(t, len(body), 2)
	// GZIP magic bytes: 0x1f 0x8b
	assert.Equal(t, []byte{0x1f, 0x8b}, body[:2])
}

// TestIntegration_Firehose_UpdateDestination tests that UpdateDestination redirects
// subsequent deliveries to the new S3 bucket.
func TestIntegration_Firehose_UpdateDestination(t *testing.T) {
	t.Parallel()

	fhBk, s3Bk := newFirehoseS3Server(t)
	ctx := t.Context()

	_, err := s3Bk.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String("old-bucket")})
	require.NoError(t, err)

	_, err = s3Bk.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String("new-bucket")})
	require.NoError(t, err)

	_, err = fhBk.CreateDeliveryStream(firehosepkg.CreateDeliveryStreamInput{
		Name: "update-stream",
		S3Destination: &firehosepkg.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::old-bucket",
		},
	})
	require.NoError(t, err)

	// Redirect to new bucket.
	require.NoError(t, fhBk.UpdateDestination("update-stream", &firehosepkg.S3DestinationDescription{
		BucketARN: "arn:aws:s3:::new-bucket",
	}))

	require.NoError(t, fhBk.PutRecord("update-stream", []byte(`{"hello":"world"}`)))
	fhBk.FlushAll(ctx)

	newKeys := listS3Keys(t, s3Bk, "new-bucket")
	assert.NotEmpty(t, newKeys, "expected object in new-bucket")

	oldKeys := listS3Keys(t, s3Bk, "old-bucket")
	assert.Empty(t, oldKeys, "old-bucket should be empty after destination update")
}

// TestIntegration_Firehose_LambdaTransformation tests that Lambda transformation is applied
// before S3 delivery: only records marked Ok by the Lambda function are written to S3.
func TestIntegration_Firehose_LambdaTransformation(t *testing.T) {
	t.Parallel()

	fhBk, s3Bk := newFirehoseS3Server(t)
	fhBk.SetLambdaBackend(keepOrDropLambdaInvoker{})
	ctx := t.Context()

	_, err := s3Bk.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String("lambda-bucket")})
	require.NoError(t, err)

	_, err = fhBk.CreateDeliveryStream(firehosepkg.CreateDeliveryStreamInput{
		Name: "lambda-stream",
		S3Destination: &firehosepkg.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::lambda-bucket",
			ProcessingConfiguration: &firehosepkg.ProcessingConfiguration{
				Enabled: true,
				Processors: []firehosepkg.Processor{
					{
						Type: "Lambda",
						Parameters: []firehosepkg.ProcessorParameter{
							{ParameterName: "LambdaArn", ParameterValue: "fh-transform"},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, fhBk.PutRecord("lambda-stream", []byte(`{"msg":"keep-this-record"}`)))
	require.NoError(t, fhBk.PutRecord("lambda-stream", []byte(`{"msg":"drop-this-record"}`)))

	fhBk.FlushAll(ctx)

	keys := listS3Keys(t, s3Bk, "lambda-bucket")
	require.NotEmpty(t, keys, "expected S3 object after flush")

	body := getS3Object(t, s3Bk, "lambda-bucket", keys[0])
	assert.Contains(t, string(body), "keep-this-record")
	assert.NotContains(t, string(body), "drop-this-record")
}

// TestIntegration_Firehose_HTTP_DescribeWithS3Destination tests the HTTP API:
// CreateDeliveryStream with an S3 config should be reflected in DescribeDeliveryStream.
func TestIntegration_Firehose_HTTP_DescribeWithS3Destination(t *testing.T) {
	t.Parallel()

	fhBk, s3Bk := newFirehoseS3Server(t)
	_ = s3Bk

	_, err := fhBk.CreateDeliveryStream(firehosepkg.CreateDeliveryStreamInput{
		Name: "http-desc-stream",
		S3Destination: &firehosepkg.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::http-bucket",
			RoleARN:   "arn:aws:iam::000000000000:role/firehose",
		},
	})
	require.NoError(t, err)

	stream, err := fhBk.DescribeDeliveryStream("http-desc-stream")
	require.NoError(t, err)
	require.NotNil(t, stream.S3Destination)
	assert.Equal(t, "arn:aws:s3:::http-bucket", stream.S3Destination.BucketARN)
}

// TestIntegration_Firehose_PutRecordBatch_S3Delivery tests that PutRecordBatch correctly
// delivers all records to S3.
func TestIntegration_Firehose_PutRecordBatch_S3Delivery(t *testing.T) {
	t.Parallel()

	fhBk, s3Bk := newFirehoseS3Server(t)
	ctx := t.Context()

	_, err := s3Bk.CreateBucket(ctx, &s3sdk.CreateBucketInput{Bucket: aws.String("batch-bucket")})
	require.NoError(t, err)

	_, err = fhBk.CreateDeliveryStream(firehosepkg.CreateDeliveryStreamInput{
		Name: "batch-stream",
		S3Destination: &firehosepkg.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::batch-bucket",
		},
	})
	require.NoError(t, err)

	records := [][]byte{
		[]byte(`{"id":1}`),
		[]byte(`{"id":2}`),
		[]byte(`{"id":3}`),
	}
	failedCount, err := fhBk.PutRecordBatch("batch-stream", records)
	require.NoError(t, err)
	assert.Zero(t, failedCount)

	fhBk.FlushAll(ctx)

	keys := listS3Keys(t, s3Bk, "batch-bucket")
	require.NotEmpty(t, keys)

	body := getS3Object(t, s3Bk, "batch-bucket", keys[0])
	assert.Contains(t, string(body), `"id":1`)
	assert.Contains(t, string(body), `"id":2`)
	assert.Contains(t, string(body), `"id":3`)
}

// TestIntegration_Firehose_NoS3Destination verifies that records are accepted but not
// delivered when no S3 destination is configured.
func TestIntegration_Firehose_NoS3Destination(t *testing.T) {
	t.Parallel()

	fhBk := firehosepkg.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := fhBk.CreateDeliveryStream(firehosepkg.CreateDeliveryStreamInput{
		Name: "no-dest-stream",
	})
	require.NoError(t, err)

	// PutRecord should succeed even with no S3 destination.
	require.NoError(t, fhBk.PutRecord("no-dest-stream", []byte(`{"data":"test"}`)))

	// FlushAll should be a no-op.
	fhBk.FlushAll(t.Context())
}

// TestIntegration_Firehose_HandlerUpdateDestination tests the UpdateDestination HTTP operation.
func TestIntegration_Firehose_HandlerUpdateDestination(t *testing.T) {
	t.Parallel()

	fhBk, _ := newFirehoseS3Server(t)

	_, err := fhBk.CreateDeliveryStream(firehosepkg.CreateDeliveryStreamInput{
		Name: "updatable-stream",
		S3Destination: &firehosepkg.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::original-bucket",
		},
	})
	require.NoError(t, err)

	require.NoError(t, fhBk.UpdateDestination("updatable-stream", &firehosepkg.S3DestinationDescription{
		BucketARN: "arn:aws:s3:::updated-bucket",
	}))

	stream, err := fhBk.DescribeDeliveryStream("updatable-stream")
	require.NoError(t, err)
	require.NotNil(t, stream.S3Destination)
	assert.Equal(t, "arn:aws:s3:::updated-bucket", stream.S3Destination.BucketARN)
}

// TestIntegration_Firehose_HTTP_UpdateDestination tests the UpdateDestination operation via HTTP.
func TestIntegration_Firehose_HTTP_UpdateDestination(t *testing.T) {
	t.Parallel()

	e := echo.New()
	e.Pre(pkglogger.EchoMiddleware(slog.Default()))

	fhBk := firehosepkg.NewInMemoryBackend("000000000000", "us-east-1")
	fhHandler := firehosepkg.NewHandler(fhBk)

	reg := service.NewRegistry()
	require.NoError(t, reg.Register(fhHandler))
	e.Use(service.NewServiceRouter(reg).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	doPost := func(action string, body any) (int, string) {
		t.Helper()
		bodyBytes, marshalErr := json.Marshal(body)
		require.NoError(t, marshalErr)
		req, reqCreateErr := http.NewRequestWithContext(
			t.Context(), http.MethodPost, srv.URL,
			bytes.NewReader(bodyBytes),
		)
		require.NoError(t, reqCreateErr)
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		req.Header.Set("X-Amz-Target", "Firehose_20150804."+action)
		resp, reqErr := http.DefaultClient.Do(req)
		require.NoError(t, reqErr)
		defer resp.Body.Close()
		rb, readErr := io.ReadAll(resp.Body)
		require.NoError(t, readErr)

		return resp.StatusCode, string(rb)
	}

	// Create a stream.
	code, body := doPost("CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "upd-http-stream",
		"S3DestinationConfiguration": map[string]any{
			"BucketARN": "arn:aws:s3:::orig-bucket",
		},
	})
	assert.Equal(t, http.StatusOK, code, body)

	// Update destination.
	code, body = doPost("UpdateDestination", map[string]any{
		"DeliveryStreamName":             "upd-http-stream",
		"CurrentDeliveryStreamVersionId": "1",
		"DestinationId":                  "destinationId-000000000001",
		"S3DestinationUpdate": map[string]any{
			"BucketARN": "arn:aws:s3:::updated-bucket",
		},
	})
	assert.Equal(t, http.StatusOK, code, body)

	// Describe and verify.
	code, body = doPost("DescribeDeliveryStream", map[string]any{
		"DeliveryStreamName": "upd-http-stream",
	})
	assert.Equal(t, http.StatusOK, code, body)
	assert.Contains(t, body, "updated-bucket")
}
