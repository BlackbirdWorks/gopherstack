package ce_test

import (
	"bytes"
	"context"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	costexplorersdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ce"
)

// newTestCEClient stands up the real aws-sdk-go-v2 Cost Explorer client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestCEClient(t *testing.T, h *ce.Handler) *costexplorersdk.Client {
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

	return costexplorersdk.NewFromConfig(cfg, func(o *costexplorersdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// corruptJSONBody replaces the outgoing request body with syntactically invalid
// JSON in a Finalize middleware, after the SDK's own client-side input
// validation has already passed a well-formed input through serialization. A
// spec-compliant generated client can never organically send malformed JSON
// (it only ever serializes its own valid typed input), so this stands in for
// the wire-level malformation the server's json.SyntaxError branch handles.
func corruptJSONBody(stack *middleware.Stack) error {
	return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc(
		"corruptJSONBody",
		func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
			middleware.FinalizeOutput, middleware.Metadata, error,
		) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				bad := []byte(`{"MaxResults": not-valid-json`)

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

// TestGetAnomalyMonitors_MalformedBodySurfacesValidationError drives
// GetAnomalyMonitors (no required input members, so nothing blocks the call
// client-side) then corrupts the outgoing JSON body to invalid syntax via
// corruptJSONBody. Before this fix, the errInvalidRequest/errUnknownAction/
// json.SyntaxError/json.UnmarshalTypeError branch in handleError returned a
// bare message with no body __type field, so restjson.GetErrorInfo had
// nothing to read and the error deserialized as a generic UnknownError
// instead of "ValidationError" -- CE's documented CommonErrors type for
// malformed/missing-required-member requests (see ce/errors.go's
// ErrValidation, gopherstack-he80).
func TestGetAnomalyMonitors_MalformedBodySurfacesValidationError(t *testing.T) {
	t.Parallel()

	backend := ce.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCEClient(t, ce.NewHandler(backend))

	_, err := client.GetAnomalyMonitors(
		t.Context(),
		&costexplorersdk.GetAnomalyMonitorsInput{},
		func(o *costexplorersdk.Options) {
			o.APIOptions = append(o.APIOptions, corruptJSONBody)
		},
	)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "ValidationError", apiErr.ErrorCode())
}
