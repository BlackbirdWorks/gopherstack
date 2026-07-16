package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeCacheEngineVersions_Valkey(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeCacheEngineVersions(t.Context(), &elasticachesdk.DescribeCacheEngineVersionsInput{
		Engine: aws.String("valkey"),
	})

	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.CacheEngineVersions), 2)

	for _, v := range out.CacheEngineVersions {
		assert.Equal(t, "valkey", aws.ToString(v.Engine))
	}
}

func TestDescribeCacheEngineVersions_ValkeyFamily(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeCacheEngineVersions(t.Context(), &elasticachesdk.DescribeCacheEngineVersionsInput{
		CacheParameterGroupFamily: aws.String("valkey8"),
	})

	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.CacheEngineVersions), 1)

	for _, v := range out.CacheEngineVersions {
		assert.Equal(t, "valkey8", aws.ToString(v.CacheParameterGroupFamily))
	}
}

// ----------------------------------------
// ClusterMode enforcement tests
// ----------------------------------------

func TestDescribeCacheEngineVersions_Memcached(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.DescribeCacheEngineVersions(t.Context(), &elasticachesdk.DescribeCacheEngineVersionsInput{
		Engine: aws.String("memcached"),
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(out.CacheEngineVersions), 1)

	for _, v := range out.CacheEngineVersions {
		assert.Equal(t, "memcached", aws.ToString(v.Engine))
	}
}

// ----------------------------------------
// CacheSubnetGroup — modify
// ----------------------------------------

func TestDescribeCacheEngineVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		engine       string
		wantMinCount int
	}{
		{
			name:         "all_versions",
			wantMinCount: 4,
		},
		{
			name:         "redis_only",
			engine:       "redis",
			wantMinCount: 4,
		},
		{
			name:         "memcached_only",
			engine:       "memcached",
			wantMinCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			input := &elasticachesdk.DescribeCacheEngineVersionsInput{}
			if tt.engine != "" {
				input.Engine = aws.String(tt.engine)
			}

			out, err := client.DescribeCacheEngineVersions(t.Context(), input)

			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(out.CacheEngineVersions), tt.wantMinCount)
		})
	}
}
