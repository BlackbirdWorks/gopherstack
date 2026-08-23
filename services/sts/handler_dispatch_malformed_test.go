package sts_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	stssdk "github.com/aws/aws-sdk-go-v2/service/sts"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

// mangleSTSMethod rewrites the outgoing request's HTTP method to PUT after
// signing, keeping the form-encoded body and Content-Type intact. PUT (not
// GET) is used because Handler() special-cases GET into a 200
// GetSupportedOperations response before its method-not-allowed check.
// Handler.RouteMatcher (services/sts/handler.go) never inspects the HTTP
// method -- only Content-Type and whether the body contains "Version=" --
// so the request still routes to this package's Handler; Handler()'s own
// method-not-allowed check is what's meant to reject it. This is the
// sanctioned "smithy middleware corrupting the request after signing" proof
// technique for a path no legitimately-constructed aws-sdk-go-v2 request can
// otherwise reach (gopherstack-wlo1): the real STS client always POSTs.
func mangleSTSMethod() func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		return stack.Finalize.Add(
			middleware.FinalizeMiddlewareFunc("MangleSTSMethod", func(
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

// TestHandler_WrongMethodSurfacesInvalidParameterValue drives a real STS
// client's GetCallerIdentity through mangleSTSMethod, which rewrites the
// request's HTTP method to GET post-signing. Before this fix, Handler()'s
// method-not-allowed branch (handler.go) wrote a bare "Method not allowed"
// text/plain body -- STS is AWS Query/XML (confirmed from
// sts@v1.45.4 deserializers.go's awsAwsquery_deserializeOpError* prefix),
// whose deserializer expects a wrapped <ErrorResponse><Error> XML document;
// plain text doesn't decode through it, so a real client got a raw decode
// failure instead of a typed API error (gopherstack-wlo1).
func TestHandler_WrongMethodSurfacesInvalidParameterValue(t *testing.T) {
	t.Parallel()

	h := sts.NewHandler(sts.NewInMemoryBackend())
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

	client := stssdk.NewFromConfig(cfg, func(o *stssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, mangleSTSMethod())
	})

	_, err = client.GetCallerIdentity(t.Context(), &stssdk.GetCallerIdentityInput{},
		func(o *stssdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InvalidParameterValue", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
