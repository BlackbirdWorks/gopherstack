package elasticache

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func builtinCacheEngineVersions() []CacheEngineVersion {
	return []CacheEngineVersion{
		{
			Engine:                        engineRedis,
			EngineVersion:                 versionRedis710,
			CacheParameterGroupFamily:     familyRedis7,
			CacheEngineDescription:        engineRedisCap,
			CacheEngineVersionDescription: "Redis 7.1.0",
		},
		{
			Engine:                        engineRedis,
			EngineVersion:                 "7.0.7",
			CacheParameterGroupFamily:     familyRedis7,
			CacheEngineDescription:        engineRedisCap,
			CacheEngineVersionDescription: "Redis 7.0.7",
		},
		{
			Engine:                        engineRedis,
			EngineVersion:                 "6.2.6",
			CacheParameterGroupFamily:     "redis6.x",
			CacheEngineDescription:        engineRedisCap,
			CacheEngineVersionDescription: "Redis 6.2.6",
		},
		{
			Engine:                        engineRedis,
			EngineVersion:                 "5.0.6",
			CacheParameterGroupFamily:     "redis5.0",
			CacheEngineDescription:        engineRedisCap,
			CacheEngineVersionDescription: "Redis 5.0.6",
		},
		{
			Engine:                        engineMemcached,
			EngineVersion:                 "1.6.17",
			CacheParameterGroupFamily:     "memcached1.6",
			CacheEngineDescription:        "Memcached",
			CacheEngineVersionDescription: "Memcached 1.6.17",
		},
		{
			Engine:                        engineMemcached,
			EngineVersion:                 "1.5.16",
			CacheParameterGroupFamily:     "memcached1.5",
			CacheEngineDescription:        "Memcached",
			CacheEngineVersionDescription: "Memcached 1.5.16",
		},
		{
			Engine:                        engineValkey,
			EngineVersion:                 versionValkey82,
			CacheParameterGroupFamily:     familyValkey8,
			CacheEngineDescription:        engineValkeyCap,
			CacheEngineVersionDescription: "Valkey 8.2.0",
		},
		{
			Engine:                        engineValkey,
			EngineVersion:                 "8.0.1",
			CacheParameterGroupFamily:     familyValkey8,
			CacheEngineDescription:        engineValkeyCap,
			CacheEngineVersionDescription: "Valkey 8.0.1",
		},
		{
			Engine:                        engineValkey,
			EngineVersion:                 "7.2.7",
			CacheParameterGroupFamily:     familyValkey7,
			CacheEngineDescription:        engineValkeyCap,
			CacheEngineVersionDescription: "Valkey 7.2.7",
		},
	}
}

// DescribeCacheEngineVersions returns engine versions, optionally filtered.
func (b *InMemoryBackend) DescribeCacheEngineVersions(
	_ context.Context,
	engine, family, engineVersion, marker string,
	maxRecords int,
) (page.Page[CacheEngineVersion], error) {
	b.mu.RLock("DescribeCacheEngineVersions")
	defer b.mu.RUnlock()

	all := builtinCacheEngineVersions()
	filtered := make([]CacheEngineVersion, 0, len(all))

	for _, v := range all {
		if engine != "" && v.Engine != engine {
			continue
		}

		if family != "" && v.CacheParameterGroupFamily != family {
			continue
		}

		if engineVersion != "" && v.EngineVersion != engineVersion {
			continue
		}

		filtered = append(filtered, v)
	}

	return page.New(filtered, marker, maxRecords, elasticacheDefaultMaxRecords), nil
}
