package scheduler_test

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	schedulersdk "github.com/aws/aws-sdk-go-v2/service/scheduler"
	"github.com/aws/aws-sdk-go-v2/service/scheduler/types"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

// newTestSchedulerClient stands up the real aws-sdk-go-v2 Scheduler client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestSchedulerClient(t *testing.T, h *scheduler.Handler) *schedulersdk.Client {
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

	return schedulersdk.NewFromConfig(cfg, func(o *schedulersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// wrongTypeScheduleExpressionBody replaces the outgoing request body with one
// where ScheduleExpression is a JSON number instead of a string, in a
// Finalize middleware, after the SDK's own client-side input validation has
// already passed a well-formed input through serialization. A spec-compliant
// generated client can never organically send a wire-type mismatch (it only
// ever serializes its own typed input, and Go's *string field can't hold a
// number), so this stands in for the wire-level malformation the server's
// json.UnmarshalTypeError branch handles.
//
// A body with syntactically-invalid JSON does NOT reach that branch for
// scheduler: CreateSchedule is REST-bound (SetURI("Name"), no X-Amz-Target),
// so it is routed through handleREST -> enrichRESTBody -> unmarshalOrEmpty,
// which silently discards any json.Unmarshal error and falls back to an
// empty (but syntactically valid) map -- re-serializing a clean body that
// then fails ordinary field-required validation instead. A syntactically
// valid body with one wrongly-typed value survives that map-shaped
// round-trip untouched (json.RawMessage preserves it byte-for-byte) and only
// fails when the real CreateScheduleInput struct is unmarshaled.
func wrongTypeScheduleExpressionBody(stack *middleware.Stack) error {
	return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc(
		"wrongTypeScheduleExpressionBody",
		func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
			middleware.FinalizeOutput, middleware.Metadata, error,
		) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				bad := []byte(`{"ScheduleExpression": 12345}`)

				corrupted, err := req.SetStream(bytes.NewReader(bad))
				if err == nil {
					corrupted.ContentLength = int64(len(bad))
					in.Request = corrupted
				}
			}

			return next.HandleFinalize(ctx, in)
		},
	), middleware.After)
}

// TestCreateSchedule_WrongTypeBodySurfacesValidationException drives
// CreateSchedule with a well-formed input, then swaps the outgoing JSON body
// for one where ScheduleExpression is a number via
// wrongTypeScheduleExpressionBody. Before this fix, the errInvalidRequest/
// errUnknownAction/json.SyntaxError/json.UnmarshalTypeError branch in
// handleError returned a bare message with no body __type field, so
// restjson.GetErrorInfo had nothing to read and the error deserialized as a
// generic UnknownError instead of the modeled ValidationException
// (gopherstack-he80; scheduler@v1.20.4 models ValidationException on every
// operation).
func TestCreateSchedule_WrongTypeBodySurfacesValidationException(t *testing.T) {
	t.Parallel()

	backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestSchedulerClient(t, scheduler.NewHandler(backend))

	_, err := client.CreateSchedule(t.Context(), &schedulersdk.CreateScheduleInput{
		Name:               aws.String("test-schedule"),
		ScheduleExpression: aws.String("rate(5 minutes)"),
		Target: &types.Target{
			Arn:     aws.String("arn:aws:sqs:us-east-1:000000000000:test-queue"),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/test-role"),
		},
		FlexibleTimeWindow: &types.FlexibleTimeWindow{Mode: types.FlexibleTimeWindowModeOff},
	}, func(o *schedulersdk.Options) {
		o.APIOptions = append(o.APIOptions, wrongTypeScheduleExpressionBody)
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ValidationException", apiErr.ErrorCode())
}
