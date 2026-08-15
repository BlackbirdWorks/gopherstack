package s3_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// newRealS3ClientTest stands up the real aws-sdk-go-v2 S3 client against an httptest
// server running this package's Handler, wired through the same pkgs/service
// registry/router used in production.
func newRealS3ClientTest(t *testing.T) *sdk_s3.Client {
	t.Helper()

	handler, _ := newTestHandler(t)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(handler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return sdk_s3.NewFromConfig(cfg, func(o *sdk_s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetBucketAbac_RealClient is a regression test for gopherstack-ob1g: the real
// PutBucketAbac request root is AbacStatus (s3@v1.106.5 serializers.go:
// awsRestxml_serializeOpPutBucketAbac's payloadRoot.Local), not AbacConfiguration.
// PutBucketAbac stores the raw request body verbatim, so GetBucketAbac's re-parse of
// that stored body is really parsing the original client request -- a handler
// expecting the wrong root discards the whole thing via xml.Unmarshal's error, so
// the status silently comes back empty. Driving both ends through the real SDK
// client is what catches it, since the SDK writes/reads the exact wire shape
// regardless of what the handler expects.
func TestGetBucketAbac_RealClient(t *testing.T) {
	t.Parallel()

	client := newRealS3ClientTest(t)
	bucket := "abac-real-client-bucket"

	_, err := client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)

	_, err = client.PutBucketAbac(t.Context(), &sdk_s3.PutBucketAbacInput{
		Bucket: aws.String(bucket),
		AbacStatus: &types.AbacStatus{
			Status: types.BucketAbacStatusEnabled,
		},
	})
	require.NoError(t, err)

	got, err := client.GetBucketAbac(t.Context(), &sdk_s3.GetBucketAbacInput{Bucket: aws.String(bucket)})
	require.NoError(t, err)
	require.NotNil(t, got.AbacStatus)
	assert.Equal(t, types.BucketAbacStatusEnabled, got.AbacStatus.Status)
}

// TestGetBucketAbac_MalformedStoredBodyHandled verifies GetBucketAbac rejects a
// stored body that isn't well-formed XML with 400 MalformedXML instead of silently
// returning an empty status (gopherstack-ob1g: the previous handler discarded
// xml.Unmarshal's error). PutBucketAbac stores the raw body verbatim without
// validating it, so a malformed PUT succeeds and only surfaces on the next Get.
func TestGetBucketAbac_MalformedStoredBodyHandled(t *testing.T) {
	t.Parallel()

	handler, _ := newTestHandler(t)
	bucket := "abac-malformed-bucket"
	mustCreateBucket(t, handler.Backend, bucket)

	putReq := httptest.NewRequest(http.MethodPut, "/"+bucket+"?abac", strings.NewReader("<<<not xml"))
	putRec := httptest.NewRecorder()
	serveS3Handler(handler, putRec, putReq)
	require.Equal(t, http.StatusOK, putRec.Code, putRec.Body.String())

	getReq := httptest.NewRequest(http.MethodGet, "/"+bucket+"?abac", nil)
	getRec := httptest.NewRecorder()
	serveS3Handler(handler, getRec, getReq)

	assert.Equal(t, http.StatusBadRequest, getRec.Code)
	assert.Contains(t, getRec.Body.String(), "MalformedXML")
}

// TestXMLUnmarshalErrorHandled is a regression suite for gopherstack-ob1g: handlers
// that discarded xml.Unmarshal's error via `_ = xml.Unmarshal(...)` proceeded on a
// zeroed request struct instead of rejecting the malformed body. Each case posts an
// unparsable body and asserts the handler now returns 400 MalformedXML instead of
// silently succeeding on zero values.
func TestXMLUnmarshalErrorHandled(t *testing.T) {
	t.Parallel()

	const malformed = "<<<not xml"

	tests := []struct {
		name   string
		bucket string
		query  string
	}{
		{"put_bucket_request_payment", "xml-error-request-payment", "requestPayment"},
		{"put_bucket_accelerate", "xml-error-accelerate", "accelerate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, _ := newTestHandler(t)
			bucket := tt.bucket
			mustCreateBucket(t, handler.Backend, bucket)

			req := httptest.NewRequest(http.MethodPut, "/"+bucket+"?"+tt.query, strings.NewReader(malformed))
			rec := httptest.NewRecorder()
			serveS3Handler(handler, rec, req)

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), "MalformedXML")
		})
	}
}

// TestRestoreObject_MalformedBodyHandled verifies RestoreObject rejects a malformed
// body with 400 MalformedXML instead of silently restoring with zeroed Days
// (gopherstack-ob1g: the previous handler discarded xml.Unmarshal's error).
func TestRestoreObject_MalformedBodyHandled(t *testing.T) {
	t.Parallel()

	handler, backend := newTestHandler(t)
	bucket := "restore-malformed-bucket"
	mustCreateBucket(t, backend, bucket)
	mustPutObject(t, backend, bucket, "archived.txt", []byte("data"))

	req := httptest.NewRequest(http.MethodPost, "/"+bucket+"/archived.txt?restore", strings.NewReader("<<<not xml"))
	rec := httptest.NewRecorder()
	serveS3Handler(handler, rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MalformedXML")
}
