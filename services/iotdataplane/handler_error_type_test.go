package iotdataplane_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	iotdataplanesdk "github.com/aws/aws-sdk-go-v2/service/iotdataplane"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/iotdataplane"
)

// newTestIoTDataPlaneClient stands up the real aws-sdk-go-v2 IoT Data Plane
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestIoTDataPlaneClient(t *testing.T, h *iotdataplane.Handler) *iotdataplanesdk.Client {
	t.Helper()

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

	return iotdataplanesdk.NewFromConfig(cfg, func(o *iotdataplanesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetThingShadow_UnknownThingSurfacesResourceNotFoundException drives
// GetThingShadow for a thing with no shadow through a real SDK client.
// Before this fix, the body carried the modeled type under the "error" JSON
// key, which restjson.GetErrorInfo (aws-sdk-go-v2/aws/protocol/restjson/
// decoder_util.go) does not read -- it only reads "code"/"__type"/"message"
// -- and no X-Amzn-Errortype header was set either, so every error,
// including this one, deserialized client-side as a generic UnknownError
// instead of the modeled ResourceNotFoundException (gopherstack-aitg).
func TestGetThingShadow_UnknownThingSurfacesResourceNotFoundException(t *testing.T) {
	t.Parallel()

	backend := iotdataplane.NewInMemoryBackend()
	client := newTestIoTDataPlaneClient(t, iotdataplane.NewHandler(backend))

	_, err := client.GetThingShadow(t.Context(), &iotdataplanesdk.GetThingShadowInput{
		ThingName: aws.String("no-such-thing"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

// TestUpdateThingShadow_MalformedDocumentSurfacesInvalidRequestException
// covers the ErrValidation path through the shared handleError function.
func TestUpdateThingShadow_MalformedDocumentSurfacesInvalidRequestException(t *testing.T) {
	t.Parallel()

	backend := iotdataplane.NewInMemoryBackend()
	client := newTestIoTDataPlaneClient(t, iotdataplane.NewHandler(backend))

	_, err := client.UpdateThingShadow(t.Context(), &iotdataplanesdk.UpdateThingShadowInput{
		ThingName: aws.String("thing-1"),
		Payload:   []byte("not json"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InvalidRequestException", apiErr.ErrorCode())
}
