package databrew_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	databrewsdk "github.com/aws/aws-sdk-go-v2/service/databrew"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// mangleListRecipesMethod rewrites ListRecipes' outgoing HTTP method to PUT
// after signing, keeping its "/recipes" path (with no name segment)
// unchanged. Handler.RouteMatcher (services/databrew/handler.go) matches
// "recipes" as the first path segment unconditionally, without ever
// inspecting the HTTP method, so the request still routes to this
// package's Handler. Inside parseRecipeOp (handler_recipes.go),
// method==PUT with an empty name segment matches none of its cases and
// falls through to opUnknown, landing in Handler()'s own dispatch-miss
// fallback -- the same white-box category as securityhub's analogous fix
// (a98561767). (GET, the method ListRecipes really uses, can't reach
// opUnknown here: parseRecipeOp maps every non-empty-name GET to
// DescribeRecipe and every empty-name GET to ListRecipes, so path
// corruption alone doesn't work for this branch -- only a method a real
// client never sends does.)
func mangleListRecipesMethod(stack *middleware.Stack) error {
	return stack.Finalize.Add(
		middleware.FinalizeMiddlewareFunc("MangleListRecipesMethod", func(
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

// TestHandler_WrongMethodSurfacesResourceNotFoundException drives a real
// DataBrew client's ListRecipes through mangleListRecipesMethod, which
// rewrites the request's HTTP method to PUT post-signing. Before this fix,
// Handler()'s dispatch-miss fallback (handler.go) wrote a bare "not found"
// text/plain body -- databrew is restjson1 (services/_PROTOCOLS.md), whose
// deserializer (aws-sdk-go-v2@v1.42.4 aws/protocol/restjson.GetErrorInfo)
// parses __type/code/message from a JSON body; plain text doesn't decode,
// so a real client saw smithy.GenericAPIError{Code:"UnknownError"} instead
// of a typed error (gopherstack-wlo1).
func TestHandler_WrongMethodSurfacesResourceNotFoundException(t *testing.T) {
	t.Parallel()

	h := databrew.NewHandler(databrew.NewInMemoryBackend("123456789012", "us-east-1"))
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

	client := databrewsdk.NewFromConfig(cfg, func(o *databrewsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, mangleListRecipesMethod)
	})

	_, err = client.ListRecipes(t.Context(), &databrewsdk.ListRecipesInput{},
		func(o *databrewsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
