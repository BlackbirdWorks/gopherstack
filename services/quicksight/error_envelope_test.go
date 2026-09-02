package quicksight_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	quicksightsdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	"github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestErrorEnvelope_DescribeDataSetNotFoundDecodesToTypedError drives
// DescribeDataSet for a nonexistent dataset through the real
// aws-sdk-go-v2 quicksight client and asserts errors.As unwraps to the
// concrete *types.ResourceNotFoundException -- not merely that an error
// occurred. quicksight is restjson1
// (aws-sdk-go-v2/service/quicksight@v1.123.1: awsRestjson1_ prefix,
// verified 277-of-277 deserializeOpError functions in deserializers.go
// identically read the X-Amzn-ErrorType response header first, falling
// back to restjson.GetErrorInfo's JSON body "Code"/"__type" key -- "Code"
// is checked BEFORE "__type"). This backend's writeError
// (handler_paths.go) writes {"Code":..., "Message":...} with no header,
// which satisfies the body "Code" fallback directly (the untagged Go field
// `Code string` in aws-sdk-go-v2's errInfo struct exact-matches the JSON
// key "Code").
//
// Also asserts on the raw response bytes for the same case, to pin the
// exact envelope rather than trust the SDK's own leniency
// (parity-principles.md).
func TestErrorEnvelope_DescribeDataSetNotFoundDecodesToTypedError(t *testing.T) {
	t.Parallel()

	backend := quicksight.NewInMemoryBackend("000000000000", "us-east-1")
	h := quicksight.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
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

	client := quicksightsdk.NewFromConfig(cfg, func(o *quicksightsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	_, err = client.DescribeDataSet(t.Context(), &quicksightsdk.DescribeDataSetInput{
		AwsAccountId: aws.String("000000000000"),
		DataSetId:    aws.String("does-not-exist"),
	})
	require.Error(t, err)

	var notFound *types.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound,
		"expected *types.ResourceNotFoundException via errors.As, got %T: %v", err, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		srv.URL+"/accounts/000000000000/data-sets/does-not-exist", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/quicksight/aws4_request, "+
			"SignedHeaders=host, Signature=deadbeef")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var envelope map[string]any
	require.NoError(t, json.Unmarshal(raw, &envelope))
	require.Equal(t, "ResourceNotFoundException", envelope["Code"],
		"raw body must carry the Code key restjson.GetErrorInfo checks first: %s", raw)

	_, hasMessage := envelope["Message"]
	require.True(t, hasMessage, "raw body must carry a Message key: %s", raw)
}
