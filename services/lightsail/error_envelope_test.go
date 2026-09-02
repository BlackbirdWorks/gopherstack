package lightsail_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lightsailsdk "github.com/aws/aws-sdk-go-v2/service/lightsail"
	"github.com/aws/aws-sdk-go-v2/service/lightsail/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lightsail"
)

// TestErrorEnvelope_NotFoundDecodesToTypedError drives the real
// aws-sdk-go-v2 lightsail client against GetInstance for a nonexistent
// instance and asserts errors.As unwraps to the specific
// *types.NotFoundException -- not merely that an error occurred. Also
// asserts on the raw response bytes to pin the exact {"__type","message"}
// envelope the awsAwsjson11 protocol's deserializeOpError functions require
// (aws-sdk-go-v2/service/lightsail@v1.58.4/deserializers.go, via
// getProtocolErrorInfo: JSON body key "__type", case-insensitive
// "message"), confirmed identical across all 161 deserializeOpError
// functions in that file.
func TestErrorEnvelope_NotFoundDecodesToTypedError(t *testing.T) {
	t.Parallel()

	backend := lightsail.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(backend.Close)

	h := lightsail.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	rawBody, rawStatus := rawErrorResponse(t, srv.URL, "Lightsail_20161128.GetInstance",
		`{"instanceName":"does-not-exist"}`)

	require.Equal(t, http.StatusBadRequest, rawStatus)

	var envelope map[string]any
	require.NoError(t, json.Unmarshal(rawBody, &envelope))
	require.Equal(t, "NotFoundException", envelope["__type"],
		"raw response must carry the JSON body key __type the SDK's getProtocolErrorInfo reads")
	msg, ok := envelope["message"]
	require.True(t, ok, "raw response must carry a message key")
	require.Contains(t, msg, "does-not-exist")

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	client := lightsailsdk.NewFromConfig(cfg, func(o *lightsailsdk.Options) {
		o.BaseEndpoint = &srv.URL
	})

	_, err = client.GetInstance(t.Context(), &lightsailsdk.GetInstanceInput{
		InstanceName: aws.String("does-not-exist"),
	})
	require.Error(t, err)

	var notFound *types.NotFoundException
	require.ErrorAs(t, err, &notFound,
		"expected *types.NotFoundException via errors.As, got %T: %v", err, err)
	require.Contains(t, notFound.ErrorMessage(), "does-not-exist")
}

func rawErrorResponse(t *testing.T, baseURL, target, body string) ([]byte, int) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, baseURL, bytes.NewBufferString(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", target)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return raw, resp.StatusCode
}
