package appconfig_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	appconfigsdk "github.com/aws/aws-sdk-go-v2/service/appconfig"
	"github.com/aws/aws-sdk-go-v2/service/appconfig/types"
	smithy "github.com/aws/smithy-go"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

// newTestAppConfigClient stands up the real aws-sdk-go-v2 AppConfig client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestAppConfigClient(t *testing.T, h *appconfig.Handler) *appconfigsdk.Client {
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

	return appconfigsdk.NewFromConfig(cfg, func(o *appconfigsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestGetApplication_UnknownApplicationSurfacesResourceNotFoundException
// drives GetApplication for an application that doesn't exist through a real
// SDK client. Before this fix, notFoundResponse/badRequestResponse/
// conflictResponse never set X-Amzn-Errortype nor a body code/__type field,
// so restjson.GetErrorInfo had nothing to read and every error -- including
// this one -- deserialized client-side as a generic UnknownError instead of
// the modeled ResourceNotFoundException (gopherstack-aitg).
func TestGetApplication_UnknownApplicationSurfacesResourceNotFoundException(t *testing.T) {
	t.Parallel()

	backend := appconfig.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestAppConfigClient(t, appconfig.NewHandler(backend))

	_, err := client.GetApplication(t.Context(), &appconfigsdk.GetApplicationInput{
		ApplicationId: aws.String("no-such-app"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

// TestCreateApplication_DuplicateNameSurfacesBadRequestException guards the
// per-call-site fix: CreateApplication models only BadRequestException,
// InternalServerException and ServiceQuotaExceededException
// (appconfig@v1.48.4 deserializers.go:87) -- unlike CreateExtension or
// CreateHostedConfigurationVersion, it does not model ConflictException. A
// shared conflict-response helper naively wired to every "already exists"
// call site would emit an unmodelled code here.
func TestCreateApplication_DuplicateNameSurfacesBadRequestException(t *testing.T) {
	t.Parallel()

	backend := appconfig.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestAppConfigClient(t, appconfig.NewHandler(backend))

	name := aws.String("dup-app")
	_, err := client.CreateApplication(t.Context(), &appconfigsdk.CreateApplicationInput{Name: name})
	require.NoError(t, err)

	_, err = client.CreateApplication(t.Context(), &appconfigsdk.CreateApplicationInput{Name: name})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "BadRequestException", apiErr.ErrorCode())
}

// TestCreateExtension_DuplicateNameSurfacesConflictException is the
// contrasting case: CreateExtension does model ConflictException
// (appconfig@v1.48.4 deserializers.go:1276), so the shared conflictResponse
// helper is correct on this call site.
func TestCreateExtension_DuplicateNameSurfacesConflictException(t *testing.T) {
	t.Parallel()

	backend := appconfig.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestAppConfigClient(t, appconfig.NewHandler(backend))

	name := aws.String("dup-extension")
	input := &appconfigsdk.CreateExtensionInput{
		Name:    name,
		Actions: map[string][]types.Action{},
	}
	_, err := client.CreateExtension(t.Context(), input)
	require.NoError(t, err)

	_, err = client.CreateExtension(t.Context(), input)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ConflictException", apiErr.ErrorCode())
}
