package elasticache_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_CreateParameterGroup_Redis7(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	pg, err := b.CreateParameterGroup(context.Background(), "redis7-pg", "redis7", "Redis 7 group")
	require.NoError(t, err)
	assert.Equal(t, "redis7-pg", pg.Name)
	assert.Equal(t, "redis7", pg.Family)
	assert.Contains(t, pg.ARN, "arn:aws:elasticache:us-east-1:000000000000:parametergroup:redis7-pg")
}

func TestBackend_CreateParameterGroup_Valkey(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	pg, err := b.CreateParameterGroup(context.Background(), "valkey8-pg", "valkey8", "Valkey 8 group")
	require.NoError(t, err)
	assert.Equal(t, "valkey8", pg.Family)
}

func TestBackend_DescribeParameterGroups_FilterByName(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateParameterGroup(context.Background(), "pg-1", "redis7", "group 1")
	require.NoError(t, err)
	_, err = b.CreateParameterGroup(context.Background(), "pg-2", "redis7", "group 2")
	require.NoError(t, err)

	p, err := b.DescribeParameterGroups(context.Background(), "pg-1", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "pg-1", p.Data[0].Name)
}

func TestBackend_ModifyParameterGroup_SetValue(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateParameterGroup(context.Background(), "mod-pg", "redis7", "param group to modify")
	require.NoError(t, err)

	pg, err := b.ModifyParameterGroup(context.Background(), "mod-pg", map[string]string{
		"maxmemory-policy": "allkeys-lru",
	})
	require.NoError(t, err)
	assert.Equal(t, "allkeys-lru", pg.Parameters["maxmemory-policy"])
}

func TestBackend_ResetParameterGroup_All(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateParameterGroup(context.Background(), "reset-all-pg", "redis7", "for reset")
	require.NoError(t, err)

	_, err = b.ModifyParameterGroup(context.Background(), "reset-all-pg", map[string]string{
		"maxmemory-policy": "volatile-lru",
	})
	require.NoError(t, err)

	pg, err := b.ResetParameterGroup(context.Background(), "reset-all-pg", nil, true)
	require.NoError(t, err)
	assert.Equal(t, "reset-all-pg", pg.Name)
	// After reset, the custom parameter should be cleared.
	assert.Empty(t, pg.Parameters["maxmemory-policy"])
}

func TestBackend_ResetParameterGroup_Specific(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateParameterGroup(context.Background(), "reset-spec-pg", "redis7", "for selective reset")
	require.NoError(t, err)

	_, err = b.ModifyParameterGroup(context.Background(), "reset-spec-pg", map[string]string{
		"maxmemory-policy": "allkeys-lru",
		"activerehashing":  "yes",
	})
	require.NoError(t, err)

	_, err = b.ResetParameterGroup(context.Background(), "reset-spec-pg", []string{"maxmemory-policy"}, false)
	require.NoError(t, err)

	p, err := b.DescribeParameters(context.Background(), "reset-spec-pg", "", 0)
	require.NoError(t, err)
	// maxmemory-policy should be reset; activerehashing should remain.
	paramMap := make(map[string]string)
	for _, param := range p.Data {
		if param.Value != "" {
			paramMap[param.Name] = param.Value
		}
	}
	assert.Empty(t, paramMap["maxmemory-policy"])
	assert.Equal(t, "yes", paramMap["activerehashing"])
}

// ----------------------------------------
// CacheSubnetGroup CRUD (issue)
// ----------------------------------------

func TestBackend_DescribeEngineDefaultParameters_Redis7(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	p, err := b.DescribeEngineDefaultParameters(context.Background(), "redis7", "", 0)
	require.NoError(t, err)
	assert.NotNil(t, p.Data)
}

func TestBackend_DescribeEngineDefaultParameters_Valkey8(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	p, err := b.DescribeEngineDefaultParameters(context.Background(), "valkey8", "", 0)
	require.NoError(t, err)
	assert.NotNil(t, p.Data)
}

// ----------------------------------------
// CacheEngineVersions — all engines
// ----------------------------------------

func TestBackend_DescribeEngineDefaultParameters_Redis(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	p, err := b.DescribeEngineDefaultParameters(context.Background(), "redis7", "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 3)

	names := make(map[string]bool)
	for _, param := range p.Data {
		names[param.Name] = true
	}

	assert.True(t, names["maxmemory-policy"])
	assert.True(t, names["hz"])
}

func TestBackend_DescribeEngineDefaultParameters_Memcached(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	p, err := b.DescribeEngineDefaultParameters(context.Background(), "memcached1.6", "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 2)

	names := make(map[string]bool)
	for _, param := range p.Data {
		names[param.Name] = true
	}

	assert.True(t, names["max_item_size"])
}

func TestBackend_DescribeEngineDefaultParameters_Valkey(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	p, err := b.DescribeEngineDefaultParameters(context.Background(), "valkey8", "", 0)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(p.Data), 3)
}
