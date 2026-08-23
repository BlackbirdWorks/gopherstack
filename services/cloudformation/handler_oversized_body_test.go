package cloudformation_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// TestHandler_OversizedBodySurfacesInternalFailure drives a real
// CloudFormation client's ListStacks with a NextToken large enough to push
// the request body (form-urlencoded, Query/XML protocol) past
// httputils.MaxRequestBodyBytes, through the same registry/router used in
// production (service.NewRegistry + service.NewServiceRouter via
// newTestHandlerAndClient).
//
// Before this fix, RouteMatcher's own httputils.ReadBody call swallowed this
// same read failure as a plain "false", so the router found no owner and
// answered a generic 404 -- masking Handler()'s already-typed InternalFailure.
// RouteMatcher now falls back to the api/cloudformation User-Agent marker
// (set by every aws-sdk-go-v2 cloudformation client) when the body can't be
// read, claims the request, and lets Handler() produce the typed error.
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.ListStacks(t.Context(), &cfnsdk.ListStacksInput{
		NextToken: huge,
	}, func(o *cfnsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

// TestHandler_NormalSizedBodyStillRoutes is the regression guard: a normal
// request must still reach Handler() and succeed now that RouteMatcher's
// read-failure branch has changed.
func TestHandler_NormalSizedBodyStillRoutes(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	out, err := client.ListStacks(t.Context(), &cfnsdk.ListStacksInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
