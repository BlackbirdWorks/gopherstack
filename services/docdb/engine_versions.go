package docdb

import "context"

// DescribeDBEngineVersions returns available engine versions, optionally filtered.
func (b *InMemoryBackend) DescribeDBEngineVersions(_ context.Context, engine, engineVersion string) []DBEngineVersion {
	all := []DBEngineVersion{
		{Engine: docDBEngine, EngineVersion: docDBEngineVersion36, DBEngineDescription: docDBEngineDescription},
		{Engine: docDBEngine, EngineVersion: defaultEngineVersion, DBEngineDescription: docDBEngineDescription},
		{Engine: docDBEngine, EngineVersion: docDBEngineVersion5, DBEngineDescription: docDBEngineDescription},
	}
	result := make([]DBEngineVersion, 0, len(all))
	for _, v := range all {
		if engine != "" && v.Engine != engine {
			continue
		}
		if engineVersion != "" && v.EngineVersion != engineVersion {
			continue
		}
		result = append(result, v)
	}

	return result
}
