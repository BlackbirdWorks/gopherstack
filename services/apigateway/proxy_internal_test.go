package apigateway

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthorizerCacheSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*authorizerCache)
		name    string
		key     string
		ttl     time.Duration
		wantHit bool
		wantVal bool
	}{
		{
			name:    "stores_and_reads_entry",
			key:     "k1",
			ttl:     time.Minute,
			wantHit: true,
			wantVal: true,
		},
		{
			name: "evicts_lru_when_max_entries_reached",
			setup: func(cache *authorizerCache) {
				cache.set("a", true, time.Minute)
				cache.set("b", false, time.Minute)
				_, _ = cache.get("a")
			},
			key:     "c",
			ttl:     time.Minute,
			wantHit: true,
			wantVal: true,
		},
		{
			name: "zero_ttl_is_not_cached",
			key:  "k2",
			ttl:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cache := newAuthorizerCacheWithMaxEntries(2)
			if tt.setup != nil {
				tt.setup(cache)
			}

			cache.set(tt.key, tt.wantVal, tt.ttl)
			gotVal, gotHit := cache.get(tt.key)
			assert.Equal(t, tt.wantHit, gotHit)
			assert.Equal(t, tt.wantVal, gotVal)

			if tt.name == "evicts_lru_when_max_entries_reached" {
				_, hitA := cache.get("a")
				_, hitB := cache.get("b")
				require.True(t, hitA)
				assert.False(t, hitB)
			}
		})
	}
}

func TestFindMatchingResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "exact_match_preferred_over_greedy",
			run: func(t *testing.T) {
				t.Helper()

				resource, params := findMatchingResource([]Resource{
					{ID: "greedy", Path: "/items/{proxy+}"},
					{ID: "exact", Path: "/items/special"},
				}, "/items/special", "prod")

				require.NotNil(t, resource)
				assert.Equal(t, "exact", resource.ID)
				assert.Empty(t, params)
			},
		},
		{
			name: "single_segment_parameter",
			run: func(t *testing.T) {
				t.Helper()

				resource, params := findMatchingResource([]Resource{
					{ID: "param", Path: "/items/{id}"},
				}, "/items/42", "prod")

				require.NotNil(t, resource)
				require.NotNil(t, params)
				assert.Equal(t, "param", resource.ID)
				assert.Equal(t, "42", params["id"])
			},
		},
		{
			name: "stage_prefix_is_stripped",
			run: func(t *testing.T) {
				t.Helper()

				resource, params := findMatchingResource([]Resource{
					{ID: "param", Path: "/items/{id}"},
				}, "/prod/items/42", "prod")

				require.NotNil(t, resource)
				require.NotNil(t, params)
				assert.Equal(t, "param", resource.ID)
				assert.Equal(t, "42", params["id"])
			},
		},
		{
			name: "non_terminal_greedy_pattern_is_ignored",
			run: func(t *testing.T) {
				t.Helper()

				resource, params := findMatchingResource([]Resource{
					{ID: "bad", Path: "/{proxy+}/suffix"},
				}, "/anything/suffix", "prod")

				assert.Nil(t, resource)
				assert.Nil(t, params)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
