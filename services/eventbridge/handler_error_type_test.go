package eventbridge_test

import (
	"bytes"
	"context"
	"testing"

	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	smithy "github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// corruptEventBridgeJSONBody replaces the outgoing request body with
// syntactically invalid JSON in a Finalize middleware, after the SDK's own
// client-side input validation has already passed a well-formed input
// through serialization. A spec-compliant generated client can never
// organically send malformed JSON, so this stands in for the wire-level
// malformation that reaches handleError's default branch: eventbridge's
// handleError has no json.SyntaxError/json.UnmarshalTypeError case of its
// own, so any body the dispatch layer can't unmarshal falls straight to
// default.
func corruptEventBridgeJSONBody(stack *middleware.Stack) error {
	return stack.Finalize.Add(middleware.FinalizeMiddlewareFunc(
		"corruptEventBridgeJSONBody",
		func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (
			middleware.FinalizeOutput, middleware.Metadata, error,
		) {
			if req, ok := in.Request.(*smithyhttp.Request); ok {
				bad := []byte(`{"NamePrefix": not-valid-json`)

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

// TestListEventBuses_MalformedBodySurfacesInternalException drives
// ListEventBuses (no required input members) then corrupts the outgoing
// JSON body to invalid syntax via corruptEventBridgeJSONBody, forcing
// dispatch's json.Unmarshal to fail with an error none of handleError's
// sentinel cases match. Before this fix, the default branch wrote
// "InternalServerError", but eventbridge@v1.48.4 types/errors.go:109 models
// "InternalException" (ErrorFault: FaultServer) as the real service's 5xx
// fault -- wired into all 57 of eventbridge's operation deserializers,
// including ListEventBuses's own -- so a real client's
// errors.As(&types.InternalException{}) never matched (gopherstack-o7gx).
func TestListEventBuses_MalformedBodySurfacesInternalException(t *testing.T) {
	t.Parallel()

	backend := eventbridge.NewInMemoryBackend()
	client := newTestEventBridgeClient(t, eventbridge.NewHandler(backend))

	_, err := client.ListEventBuses(
		t.Context(),
		&eventbridgesdk.ListEventBusesInput{},
		func(o *eventbridgesdk.Options) {
			o.APIOptions = append(o.APIOptions, corruptEventBridgeJSONBody)
			o.RetryMaxAttempts = 1
		},
	)
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalException", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())

	var ie *ebtypes.InternalException
	require.ErrorAs(t, err, &ie, "client must map this to the modeled InternalException type")
	assert.Equal(t, smithy.FaultServer, ie.ErrorFault())
}
