package securityhub_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

// newTestSecurityHubClient stands up the real aws-sdk-go-v2 SecurityHub
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestSecurityHubClient(t *testing.T, h *securityhub.Handler) *securityhubsdk.Client {
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

	return securityhubsdk.NewFromConfig(cfg, func(o *securityhubsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetSecurityControlDefinition_UnknownControlSurfacesResourceNotFoundException
// drives GetSecurityControlDefinition for a control that doesn't exist
// through a real SDK client. Before this fix, securityhub had no central
// error handler at all -- 20+ call sites inline a message-only map with no
// X-Amzn-Errortype header and no body "code"/"__type" field, so
// restjson.GetErrorInfo had nothing to read and every error, including this
// one, deserialized client-side as a generic UnknownError instead of the
// modeled ResourceNotFoundException (gopherstack-aitg).
func TestGetSecurityControlDefinition_UnknownControlSurfacesResourceNotFoundException(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	_, err := client.GetSecurityControlDefinition(t.Context(), &securityhubsdk.GetSecurityControlDefinitionInput{
		SecurityControlId: aws.String("no-such-control"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

// TestEnableSecurityHub_AlreadyEnabledSurfacesResourceConflictException
// guards the per-call-site fix: EnableSecurityHub models
// ResourceConflictException for "already enabled" (securityhub@v1.75.4
// deserializers.go), the only conflict-shaped code in its error list.
func TestEnableSecurityHub_AlreadyEnabledSurfacesResourceConflictException(t *testing.T) {
	t.Parallel()

	backend := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSecurityHubClient(t, securityhub.NewHandler(backend))

	_, err := client.EnableSecurityHub(t.Context(), &securityhubsdk.EnableSecurityHubInput{})
	require.NoError(t, err)

	_, err = client.EnableSecurityHub(t.Context(), &securityhubsdk.EnableSecurityHubInput{})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceConflictException", apiErr.ErrorCode())
}
