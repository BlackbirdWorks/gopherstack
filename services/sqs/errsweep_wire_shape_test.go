package sqs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// TestSDK_GetQueueUrl_NonExistentQueue_TypedError drives the real
// aws-sdk-go-v2 sqs client (pinned v1.46.4, which always speaks the
// awsjson10 protocol -- see TestRouteMatcher_OversizedQueryProtocolBodyRoutesInsteadOf404's
// doc comment) and asserts errors.As decodes the specific
// *types.QueueDoesNotExist exception, not merely that an error occurred.
//
// awsjson10's deserializeOpError (aws-sdk-go-v2/service/sqs@v1.46.4/deserializers.go,
// e.g. awsAwsjson10_deserializeOpErrorGetQueueUrl) resolves the error type from
// the response body's "__type" JSON field (or the X-Amzn-ErrorType header, which
// takes priority when present) via resolveProtocolErrorType/getProtocolErrorInfo,
// then strips any "namespace#" prefix with restjson.SanitizeErrorCode before
// switching on the sanitized code. It never reads a bare "code" key.
func TestSDK_GetQueueUrl_NonExistentQueue_TypedError(t *testing.T) {
	t.Parallel()

	client, rawURL := newSQSTestServer(t)

	_, err := client.GetQueueUrl(t.Context(), &sqssdk.GetQueueUrlInput{
		QueueName: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	var target *sqstypes.QueueDoesNotExist
	require.ErrorAs(t, err, &target,
		"expected a real QueueDoesNotExist from the SDK deserializer, got %T: %v", err, err)

	// Raw-bytes check: the JSON body must carry "__type" (the key
	// awsjson10's getProtocolErrorInfo actually reads), not a bare
	// "code"/"error" key -- a lenient client-side decode of the wrong key
	// would still pass the errors.As check above via smithy.GenericAPIError's
	// header fallback, masking a body-shape bug.
	rawBody := rawSQSErrorBody(t, rawURL, "GetQueueUrl", `{"QueueName":"does-not-exist"}`)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(rawBody, &decoded))
	require.Equal(t, "com.amazonaws.sqs#QueueDoesNotExist", decoded["__type"],
		"raw response body must carry __type; got: %s", rawBody)
	_, hasBareCode := decoded["code"]
	require.False(t, hasBareCode, "must not also emit a bare 'code' key: %s", rawBody)
}

func newSQSTestServer(t *testing.T) (*sqssdk.Client, string) {
	t.Helper()

	backend := sqs.NewInMemoryBackend()
	t.Cleanup(backend.Close)
	h := sqs.NewHandler(backend)
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

	client := sqssdk.NewFromConfig(cfg, func(o *sqssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return client, srv.URL
}

// rawSQSErrorBody sends a raw JSON-RPC request matching what the pinned SDK
// sends (X-Amz-Target header, JSON body) and returns the raw response bytes,
// bypassing SDK-side decoding entirely.
func rawSQSErrorBody(t *testing.T, url, action, jsonBody string) []byte {
	t.Helper()

	req, err := http.NewRequestWithContext(
		context.Background(), http.MethodPost, url+"/", bytes.NewReader([]byte(jsonBody)),
	)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS."+action)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	require.NotEqual(t, http.StatusOK, resp.StatusCode)

	return body
}
