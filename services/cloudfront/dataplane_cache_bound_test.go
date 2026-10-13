package cloudfront_test

import (
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfsdk "github.com/aws/aws-sdk-go-v2/service/cloudfront"
	"github.com/aws/aws-sdk-go-v2/service/cloudfront/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDataPlane_CacheStaysWithinByteLimit(t *testing.T) {
	t.Parallel()

	const limit = 32 << 10

	f := newDPFixture(t)
	f.h.SetCacheLimit(limit)

	origin := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("z", 1024))
	})

	d := f.dist(t, distSpec{
		origins: []types.Origin{customOrigin("api", origin.hostPort())},
		def:     behavior("api", f.cachePolicy(t, 3600)),
	})

	for i := range 300 {
		resp := f.viewer(t, d, http.MethodGet, "/obj/"+strconv.Itoa(i), nil)
		require.Equal(t, http.StatusOK, resp.status)
	}

	n, size := f.h.CacheStats()
	assert.Positive(t, n)
	assert.Less(t, n, 300, "entries must have been evicted")
	assert.LessOrEqual(t, size, int64(limit))

	last := f.viewer(t, d, http.MethodGet, "/obj/299", nil)
	assert.Equal(t, "Hit from cloudfront", last.xcache(), "most recent entry survives eviction")
}

func TestDataPlane_DeleteDistributionPurgesCache(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	origin := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "x") })

	d := f.dist(t, distSpec{
		origins: []types.Origin{customOrigin("api", origin.hostPort())},
		def:     behavior("api", f.cachePolicy(t, 3600)),
	})

	f.viewer(t, d, http.MethodGet, "/a", nil)

	n, _ := f.h.CacheStats()
	require.Equal(t, 1, n)

	cfg, err := f.cf.GetDistributionConfig(t.Context(), &cfsdk.GetDistributionConfigInput{Id: aws.String(d.id)})
	require.NoError(t, err)

	cfg.DistributionConfig.Enabled = aws.Bool(false)
	upd, err := f.cf.UpdateDistribution(t.Context(), &cfsdk.UpdateDistributionInput{
		Id: aws.String(d.id), IfMatch: cfg.ETag, DistributionConfig: cfg.DistributionConfig,
	})
	require.NoError(t, err)

	gone := f.viewer(t, d, http.MethodGet, "/a", nil)
	assert.Equal(t, http.StatusForbidden, gone.status, "disabled distribution must not serve cached content")

	_, err = f.cf.DeleteDistribution(
		t.Context(),
		&cfsdk.DeleteDistributionInput{Id: aws.String(d.id), IfMatch: upd.ETag},
	)
	require.NoError(t, err)

	n, size := f.h.CacheStats()
	assert.Zero(t, n)
	assert.Zero(t, size)
}

func TestDataPlane_ResetClearsCache(t *testing.T) {
	t.Parallel()

	f := newDPFixture(t)
	origin := newOriginServer(t, func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "x") })

	d := f.dist(t, distSpec{
		origins: []types.Origin{customOrigin("api", origin.hostPort())},
		def:     behavior("api", f.cachePolicy(t, 3600)),
	})

	f.viewer(t, d, http.MethodGet, "/a", nil)
	f.h.Reset()

	n, size := f.h.CacheStats()
	assert.Zero(t, n)
	assert.Zero(t, size)
}
