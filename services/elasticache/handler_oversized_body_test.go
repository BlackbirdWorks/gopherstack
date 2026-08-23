package elasticache_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// TestHandler_OversizedBodySurfacesInternalFailure drives a real ElastiCache
// client's DescribeCacheClusters with a Marker large enough to push the
// request body (form-urlencoded, Query/XML protocol) past
// httputils.MaxRequestBodyBytes, through the same registry/router used in
// production (service.NewRegistry + service.NewServiceRouter via
// newTestStack) rather than mounting Handler() directly.
//
// Before gopherstack-3a8t, RouteMatcher's own httputils.ReadBody call
// swallowed this same read failure as a plain "false", so the router found
// no owner and answered a generic 404 -- masking gopherstack-o7gx's fix,
// which only ever ran once Handler() was reached. RouteMatcher now falls
// back to the api/elasticache User-Agent marker (set by every
// aws-sdk-go-v2 elasticache client) when the body can't be read, claims the
// request, and lets Handler() produce the typed InternalFailure.
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.DescribeCacheClusters(t.Context(), &elasticachesdk.DescribeCacheClustersInput{
		Marker: huge,
	}, func(o *elasticachesdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "UnknownError", apiErr.ErrorCode())
}

// TestHandler_NormalSizedBodyStillRoutes is the regression guard for the
// above: a normal request must still reach Handler() and succeed now that
// RouteMatcher's read-failure branch has changed.
func TestHandler_NormalSizedBodyStillRoutes(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeCacheClusters(t.Context(), &elasticachesdk.DescribeCacheClustersInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
