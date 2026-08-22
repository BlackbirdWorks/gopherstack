package elbv2_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestHandler_OversizedBodySurfacesInternalFailure drives a real ELBv2
// client's DescribeLoadBalancers with a Marker large enough to push the
// request body (form-urlencoded, Query/XML protocol) past
// httputils.MaxRequestBodyBytes, through the same registry/router used in
// production (service.NewRegistry + service.NewServiceRouter via
// newTestELBv2Client).
//
// Before this fix, RouteMatcher's own httputils.ReadBody call swallowed this
// same read failure as a plain "false", so the router found no owner and
// answered a generic 404 -- masking Handler()'s already-typed InternalFailure.
// RouteMatcher now falls back to the api/elasticloadbalancingv2 User-Agent
// marker (set by every aws-sdk-go-v2 elbv2 client) when the body can't be
// read, claims the request, and lets Handler() produce the typed error.
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	h := elbv2.NewHandler(elbv2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestELBv2Client(t, h)

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.DescribeLoadBalancers(t.Context(), &elbv2sdk.DescribeLoadBalancersInput{
		Marker: huge,
	}, func(o *elbv2sdk.Options) { o.RetryMaxAttempts = 1 })
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

	h := elbv2.NewHandler(elbv2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestELBv2Client(t, h)

	out, err := client.DescribeLoadBalancers(t.Context(), &elbv2sdk.DescribeLoadBalancersInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
