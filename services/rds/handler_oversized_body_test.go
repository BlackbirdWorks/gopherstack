package rds_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdssdk "github.com/aws/aws-sdk-go-v2/service/rds"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// TestHandler_OversizedBodySurfacesInternalFailure drives a real RDS
// client's DescribeDBInstances with a Marker large enough to push the
// request body (form-urlencoded, Query/XML protocol) past
// httputils.MaxRequestBodyBytes, through the same registry/router used in
// production (service.NewRegistry + service.NewServiceRouter via
// newTestRDSClient).
//
// Before this fix, RouteMatcher's own httputils.ReadBody call swallowed this
// same read failure as a plain "false", so the router found no owner and
// answered a generic 404 -- masking Handler()'s already-typed InternalFailure.
// RouteMatcher now falls back to the api/rds User-Agent marker (set by
// every aws-sdk-go-v2 rds client) when the body can't be read, claims the
// request, and lets Handler() produce the typed error.
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.DescribeDBInstances(t.Context(), &rdssdk.DescribeDBInstancesInput{
		Marker: huge,
	}, func(o *rdssdk.Options) { o.RetryMaxAttempts = 1 })
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

	h := newTestRDSHandler()
	client := newTestRDSClient(t, h)

	out, err := client.DescribeDBInstances(t.Context(), &rdssdk.DescribeDBInstancesInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
