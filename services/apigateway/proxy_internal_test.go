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
		setup         func(*authorizerCache)
		name          string
		key           string
		retainedKey   string
		evictedKey    string
		ttl           time.Duration
		wantHit       bool
		wantVal       bool
		checkEviction bool
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
				cache.set("a", true, nil, time.Minute)
				cache.set("b", false, nil, time.Minute)
				_, _, _ = cache.get("a")
			},
			key:           "c",
			ttl:           time.Minute,
			wantHit:       true,
			wantVal:       true,
			checkEviction: true,
			retainedKey:   "a",
			evictedKey:    "b",
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

			cache.set(tt.key, tt.wantVal, nil, tt.ttl)
			gotVal, _, gotHit := cache.get(tt.key)
			assert.Equal(t, tt.wantHit, gotHit)
			assert.Equal(t, tt.wantVal, gotVal)

			if tt.checkEviction {
				_, _, retainedHit := cache.get(tt.retainedKey)
				_, _, evictedHit := cache.get(tt.evictedKey)
				require.True(t, retainedHit)
				assert.False(t, evictedHit)
			}
		})
	}
}

func TestFindMatchingResource(t *testing.T) {
	t.Parallel()

	type findMatchingResourceArgs struct {
		requestPath string
		stageName   string
		resources   []Resource
	}

	type findMatchingResourceWant struct {
		params     map[string]string
		resourceID string
		wantNil    bool
	}

	tests := []struct {
		name string
		args findMatchingResourceArgs
		want findMatchingResourceWant
	}{
		{
			name: "exact_match_preferred_over_greedy",
			args: findMatchingResourceArgs{
				resources: []Resource{
					{ID: "greedy", Path: "/items/{proxy+}"},
					{ID: "exact", Path: "/items/special"},
				},
				requestPath: "/items/special",
				stageName:   "prod",
			},
			want: findMatchingResourceWant{
				resourceID: "exact",
				params:     map[string]string{},
			},
		},
		{
			name: "single_segment_parameter",
			args: findMatchingResourceArgs{
				resources: []Resource{
					{ID: "param", Path: "/items/{id}"},
				},
				requestPath: "/items/42",
				stageName:   "prod",
			},
			want: findMatchingResourceWant{
				resourceID: "param",
				params:     map[string]string{"id": "42"},
			},
		},
		{
			name: "stage_prefix_is_stripped",
			args: findMatchingResourceArgs{
				resources: []Resource{
					{ID: "param", Path: "/items/{id}"},
				},
				requestPath: "/prod/items/42",
				stageName:   "prod",
			},
			want: findMatchingResourceWant{
				resourceID: "param",
				params:     map[string]string{"id": "42"},
			},
		},
		{
			name: "non_terminal_greedy_pattern_is_ignored",
			args: findMatchingResourceArgs{
				resources: []Resource{
					{ID: "bad", Path: "/{proxy+}/suffix"},
				},
				requestPath: "/anything/suffix",
				stageName:   "prod",
			},
			want: findMatchingResourceWant{wantNil: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resource, params := findMatchingResource(tt.args.resources, tt.args.requestPath, tt.args.stageName)
			if tt.want.wantNil {
				assert.Nil(t, resource)
				assert.Nil(t, params)

				return
			}

			require.NotNil(t, resource)
			assert.Equal(t, tt.want.resourceID, resource.ID)
			assert.Equal(t, tt.want.params, params)
		})
	}
}
