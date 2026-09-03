package pinpoint_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	pinpointsdk "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	"github.com/aws/aws-sdk-go-v2/service/pinpoint/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/pinpoint"
)

// TestErrorEnvelope_GetAppNotFoundDecodesToTypedError drives GetApp for a
// nonexistent application through the real aws-sdk-go-v2 pinpoint client
// and asserts errors.As unwraps to the concrete *types.NotFoundException --
// not merely that an error occurred. aws-sdk-go-v2/service/pinpoint's
// restjson1 deserializeOpError functions (verified 122-of-122 identical
// boilerplate in deserializers.go) read the X-Amzn-ErrorType response
// header first, falling back to a JSON body "code"/"__type" key, with
// "message"/"Message" for the message -- this backend's writeErrorResponse
// (handler.go) writes {"message":..., "__type":...} with no header, which
// satisfies the body fallback path.
func TestErrorEnvelope_GetAppNotFoundDecodesToTypedError(t *testing.T) {
	t.Parallel()

	backend := pinpoint.NewInMemoryBackend("000000000000", "us-east-1")

	h := pinpoint.NewHandler(backend)

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

	client := pinpointsdk.NewFromConfig(cfg, func(o *pinpointsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	_, err = client.GetApp(t.Context(), &pinpointsdk.GetAppInput{
		ApplicationId: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	var notFound *types.NotFoundException
	require.ErrorAs(t, err, &notFound,
		"expected *types.NotFoundException via errors.As, got %T: %v", err, err)

	// Also assert on the raw response bytes to pin the exact envelope shape
	// (parity-principles.md: a lenient client tolerating a near-miss shape
	// would hide a real bug).
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
		srv.URL+"/v1/apps/does-not-exist", nil)
	require.NoError(t, err)
	// Pinpoint's SigV4 signing name is "mobiletargeting", not "pinpoint"
	// (handler.go's pinpointService const) -- ExtractServiceFromRequest
	// reads this from the Authorization header's credential scope.
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/mobiletargeting/aws4_request, "+
			"SignedHeaders=host, Signature=deadbeef")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var envelope map[string]any
	require.NoError(t, json.Unmarshal(raw, &envelope))
	require.Equal(t, "NotFoundException", envelope["__type"],
		"raw body must carry __type key restjson.GetErrorInfo's fallback reads: %s", raw)

	_, hasMessage := envelope["message"]
	require.True(t, hasMessage, "raw body must carry a message key: %s", raw)
}
