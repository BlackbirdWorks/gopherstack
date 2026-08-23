package lakeformation_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lakeformationsdk "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

// mangleLakeFormationMethod rewrites the outgoing request's HTTP method to
// PUT after signing. Handler.RouteMatcher (services/lakeformation/
// handler.go) never inspects the HTTP method -- only the URL path and the
// SigV4 credential scope's service component -- so the request still routes
// to this package's Handler; Handler()'s own method-not-allowed check is
// what's meant to reject it. This is the sanctioned "smithy middleware
// corrupting the request after signing" proof technique for a path no
// legitimately-constructed aws-sdk-go-v2 request can otherwise reach
// (gopherstack-wlo1): the real LakeFormation client always POSTs.
func mangleLakeFormationMethod() func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Finalize.Add(
			middleware.FinalizeMiddlewareFunc("MangleLakeFormationMethod", func(
				ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
			) (middleware.FinalizeOutput, middleware.Metadata, error) {
				if req, ok := in.Request.(*smithyhttp.Request); ok {
					req.Method = http.MethodPut
				}

				return next.HandleFinalize(ctx, in)
			}),
			middleware.Before,
		)
	}
}

// TestHandler_WrongMethodSurfacesInvalidInputException drives a real
// LakeFormation client's GetDataLakeSettings through
// mangleLakeFormationMethod, which rewrites the request's HTTP method to PUT
// post-signing. Before this fix, Handler()'s method-not-allowed branch
// (handler.go) wrote a bare "Method not allowed" text/plain body --
// LakeFormation is restjson1 (services/_PROTOCOLS.md), whose deserializer
// (aws-sdk-go-v2@v1.43.4 aws/protocol/restjson.GetErrorInfo) parses __type/
// code/message from a JSON body; plain text doesn't decode, so a real client
// saw smithy.GenericAPIError{Code:"UnknownError"} instead of a typed error
// (gopherstack-wlo1).
func TestHandler_WrongMethodSurfacesInvalidInputException(t *testing.T) {
	t.Parallel()

	h := lakeformation.NewHandler(lakeformation.NewInMemoryBackend())
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

	client := lakeformationsdk.NewFromConfig(cfg, func(o *lakeformationsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, mangleLakeFormationMethod())
	})

	_, err = client.GetDataLakeSettings(t.Context(), &lakeformationsdk.GetDataLakeSettingsInput{},
		func(o *lakeformationsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InvalidInputException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
