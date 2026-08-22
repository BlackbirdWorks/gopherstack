package docdb_test

import (
	"bytes"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	docdbsdk "github.com/aws/aws-sdk-go-v2/service/docdb"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/docdb"
)

// TestHandler_OversizedBodySurfacesInternalFailure drives a real DocDB
// client's DescribeDBClusters with a Marker large enough to push the request
// body past httputils.MaxRequestBodyBytes, through the same registry/router
// used in production.
//
// RouteMatcher already gates on service.MatchesUserAgentMarker(r.Header,
// "api/docdb") -- verified against the pinned docdb@v1.51.4 api_client.go's
// AddSDKAgentKeyValue call -- before ever reading the body, so a read failure
// past that point still belongs to this service; the matcher now claims it
// instead of 404ing. ExtractOperation/ExtractResource/Handler() were also
// migrated off r.ParseForm() onto httputils.ReadBody + url.ParseQuery: net/http's
// ParseForm caches an empty-but-non-nil r.PostForm after its first failed
// call, and the telemetry wrapper's ExtractOperation runs before Handler(),
// so Handler()'s own ParseForm call used to silently "succeed" empty on the
// second call, producing MissingAction instead of InternalFailure.
// httputils.ReadBody caches a read failure instead, so every call sees the
// same real error.
func TestHandler_OversizedBodySurfacesInternalFailure(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)

	huge := aws.String(string(bytes.Repeat([]byte("x"), int(httputils.MaxRequestBodyBytes+1))))

	_, err := client.DescribeDBClusters(t.Context(), &docdbsdk.DescribeDBClustersInput{
		Marker: huge,
	}, func(o *docdbsdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr, "SDK must surface a typed API error, not an opaque one")
	assert.Equal(t, "InternalFailure", apiErr.ErrorCode())
	assert.NotEqual(t, "MissingAction", apiErr.ErrorCode())
}

// TestHandler_NormalSizedBodyStillRoutes is the regression guard: a normal
// request must still reach Handler() and succeed now that both RouteMatcher's
// read-failure branch and the ParseForm call sites have changed.
func TestHandler_NormalSizedBodyStillRoutes(t *testing.T) {
	t.Parallel()

	backend := docdb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := docdb.NewHandler(backend)
	client := newTestDocDBClient(t, h)

	out, err := client.DescribeDBClusters(t.Context(), &docdbsdk.DescribeDBClustersInput{})
	require.NoError(t, err)
	assert.NotNil(t, out)
}
