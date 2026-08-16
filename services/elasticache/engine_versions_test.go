package elasticache_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
)

func TestBackend_DescribeCacheEngineVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check      func(t *testing.T, v elasticache.CacheEngineVersion)
		name       string
		engine     string
		family     string
		minResults int
	}{
		{
			name:       "redis",
			engine:     "redis",
			minResults: 2,
			check: func(t *testing.T, v elasticache.CacheEngineVersion) {
				t.Helper()
				assert.Equal(t, "redis", v.Engine)
			},
		},
		{
			name:       "memcached",
			engine:     "memcached",
			minResults: 1,
			check: func(t *testing.T, v elasticache.CacheEngineVersion) {
				t.Helper()
				assert.Equal(t, "memcached", v.Engine)
			},
		},
		{
			name:       "valkey",
			engine:     "valkey",
			minResults: 2,
			check: func(t *testing.T, v elasticache.CacheEngineVersion) {
				t.Helper()
				assert.Equal(t, "valkey", v.Engine)
			},
		},
		{
			name:       "filter_by_family",
			family:     "valkey8",
			minResults: 1,
			check: func(t *testing.T, v elasticache.CacheEngineVersion) {
				t.Helper()
				assert.Equal(t, "valkey8", v.CacheParameterGroupFamily)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

			p, err := b.DescribeCacheEngineVersions(context.Background(), tt.engine, tt.family, "", "", 0)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(p.Data), tt.minResults)
			for _, v := range p.Data {
				tt.check(t, v)
			}
		})
	}
}
