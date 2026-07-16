package memorydb

import (
	"context"
)

// defaultEngineVersions returns the built-in list of supported engine versions.
func defaultEngineVersions() []*EngineVersion {
	return []*EngineVersion{
		{
			Engine:               "valkey",
			EngineVersion:        engineVersion80,
			EnginePatchVersion:   "8.0.1",
			ParameterGroupFamily: familyValkey8,
			Description:          "Valkey 8.0",
		},
		{
			Engine:               "valkey",
			EngineVersion:        engineVersion72,
			EnginePatchVersion:   "7.2.4",
			ParameterGroupFamily: familyValkey7,
			Description:          "Valkey 7.2",
		},
		{
			Engine:               engineRedis,
			EngineVersion:        engineVersion71,
			EnginePatchVersion:   "7.1.0",
			ParameterGroupFamily: familyRedis7,
			Description:          "Redis 7.1",
		},
		{
			Engine:               engineRedis,
			EngineVersion:        engineVersion70,
			EnginePatchVersion:   "7.0.7",
			ParameterGroupFamily: familyRedis7,
			Description:          "Redis 7.0",
		},
		{
			Engine:               engineRedis,
			EngineVersion:        engineVersion62,
			EnginePatchVersion:   "6.2.6",
			ParameterGroupFamily: familyRedis6,
			Description:          "Redis 6.2",
		},
	}
}

// DescribeEngineVersions returns supported engine versions, optionally filtered.
func (b *InMemoryBackend) DescribeEngineVersions(
	_ context.Context,
	req *describeEngineVersionsRequest,
) ([]*EngineVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	all := defaultEngineVersions()

	result := make([]*EngineVersion, 0, len(all))

	for _, ev := range all {
		if req.ParameterGroupFamily != "" && ev.ParameterGroupFamily != req.ParameterGroupFamily {
			continue
		}

		if req.Engine != "" && ev.Engine != req.Engine {
			continue
		}

		cp := *ev
		result = append(result, &cp)
	}

	if req.DefaultOnly && len(result) > 0 {
		result = result[:1]
	}

	return result, nil
}

// -- Event operations -----------------------------------------------------------
