package gcpcompute_test

import (
	"net/http"
	"testing"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestURLMapInvalidateCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rule   *computepb.CacheInvalidationRule
		name   string
		um     string
		reason string
		status int
	}{
		{name: "path", um: "um2", rule: &computepb.CacheInvalidationRule{Path: s("/img/*"), Host: s("a.example.com")}},
		{name: "tags", um: "um2", rule: &computepb.CacheInvalidationRule{CacheTags: []string{"t1"}}},
		{
			name: "empty", um: "um2", rule: &computepb.CacheInvalidationRule{},
			status: http.StatusBadRequest, reason: "required",
		},
		{
			name: "no slash", um: "um2", rule: &computepb.CacheInvalidationRule{Path: s("img")},
			status: http.StatusBadRequest, reason: "invalid",
		},
		{
			name: "inner wildcard", um: "um2", rule: &computepb.CacheInvalidationRule{Path: s("/a*/b")},
			status: http.StatusBadRequest, reason: "invalid",
		},
		{
			name: "missing map", um: "ghost", rule: &computepb.CacheInvalidationRule{Path: s("/")},
			status: http.StatusNotFound, reason: "notFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			e.lbStack(t)
			require.NoError(t, e.lbBS(t, &computepb.BackendService{Name: s("api")}))
			require.NoError(t, e.lbMap(t, twoBackendMap("um2")))

			c := client(t, e, compute.NewUrlMapsRESTClient)
			op, err := c.InvalidateCache(t.Context(), &computepb.InvalidateCacheUrlMapRequest{
				Project: project, UrlMap: tt.um, CacheInvalidationRuleResource: tt.rule,
			})

			err = e.finish(t, op, err)
			if tt.status != 0 {
				assertAPIError(t, err, tt.status, tt.reason)

				return
			}

			require.NoError(t, err)
			assert.Contains(t, op.Proto().GetOperationType(), "invalidateCache")
		})
	}
}
