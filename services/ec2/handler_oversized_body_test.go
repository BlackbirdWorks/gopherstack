package ec2_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestHandler_OversizedBodySurfacesInternalFailure drives a real EC2
// client's DescribeInstances with a NextToken large enough to push the
// request body (form-urlencoded, Query/XML protocol) past
// httputils.MaxRequestBodyBytes, through the same registry/router used in
// production (service.NewRegistry + service.NewServiceRouter via
// newTestEC2Client).
//
// Before this fix, RouteMatcher's own httputils.ReadBody call swallowed this
// same read failure as a plain "false", so the router found no owner and
// answered a generic 404 -- masking Handler()'s already-typed InternalFailure.
// RouteMatcher now falls back to the api/ec2 User-Agent marker (set by
// every aws-sdk-go-v2 ec2 client) when the body can't be read, claims the
// request, and lets Handler() produce the typed error.
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.DescribeInstances(t.Context(), &ec2sdk.DescribeInstancesInput{
		NextToken: huge,
	}, func(o *ec2sdk.Options) { o.RetryMaxAttempts = 1 })
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

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	out, err := client.DescribeInstances(t.Context(), &ec2sdk.DescribeInstancesInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
