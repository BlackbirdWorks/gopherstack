package scheduler_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	schedulersdk "github.com/aws/aws-sdk-go-v2/service/scheduler"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

// mangleListSchedulesMethod rewrites ListSchedules' outgoing HTTP method to
// PATCH after signing, keeping its "/schedules" path unchanged.
// Handler.RouteMatcher (services/scheduler/handler.go) matches any
// "/schedules"-prefixed path unconditionally, without ever inspecting the
// HTTP method, so the request still routes to this package's Handler.
// parseScheduleRESTPath's own switch has no PATCH case, so it falls through
// to restOpUnknown, landing in handleREST's own dispatch-miss fallback --
// the same white-box category as securityhub's analogous fix (a98561767).
func mangleListSchedulesMethod(stack *middleware.Stack) error {
	return stack.Finalize.Add(
		middleware.FinalizeMiddlewareFunc("MangleListSchedulesMethod", func(
			ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler,
		) (middleware.FinalizeOutput, middleware.Metadata, error) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				req.Method = http.MethodPatch
			}

			return next.HandleFinalize(ctx, in)
		}),
		middleware.Before,
	)
}

// TestHandleREST_WrongMethodSurfacesResourceNotFoundException drives a real
// Scheduler client's ListSchedules through mangleListSchedulesMethod, which
// rewrites the request's HTTP method to PATCH post-signing. Before this
// fix, handleREST's dispatch-miss fallback (handler.go) wrote a bare
// "not found" text/plain body -- scheduler is restjson1
// (services/_PROTOCOLS.md), whose deserializer (aws-sdk-go-v2@v1.20.4
// aws/protocol/restjson.GetErrorInfo) parses __type/code/message from a
// JSON body; plain text doesn't decode, so a real client saw
// smithy.GenericAPIError{Code:"UnknownError"} instead of a typed error
// (gopherstack-wlo1).
func TestHandleREST_WrongMethodSurfacesResourceNotFoundException(t *testing.T) {
	t.Parallel()

	h := scheduler.NewHandler(scheduler.NewInMemoryBackend("123456789012", "us-east-1"))
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

	client := schedulersdk.NewFromConfig(cfg, func(o *schedulersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
		o.APIOptions = append(o.APIOptions, mangleListSchedulesMethod)
	})

	_, err = client.ListSchedules(t.Context(), &schedulersdk.ListSchedulesInput{},
		func(o *schedulersdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}
