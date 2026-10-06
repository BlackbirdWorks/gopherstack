package docdb

import "context"

// DescribeDBEngineVersions returns available engine versions, optionally filtered.
func (b *InMemoryBackend) DescribeDBEngineVersions(
	_ context.Context,
	engine, engineVersion string,
	defaultOnly bool,
) []DBEngineVersion {
	logTypes := []string{"audit", "profiler"}
	all := []DBEngineVersion{
		{Engine: docDBEngine, EngineVersion: docDBEngineVersion36, DBEngineDescription: docDBEngineDescription,
			ExportableLogTypes: logTypes},
		{Engine: docDBEngine, EngineVersion: defaultEngineVersion, DBEngineDescription: docDBEngineDescription,
			ExportableLogTypes: logTypes},
		{Engine: docDBEngine, EngineVersion: docDBEngineVersion5, DBEngineDescription: docDBEngineDescription,
			ExportableLogTypes: logTypes},
	}
	result := make([]DBEngineVersion, 0, len(all))
	for _, v := range all {
		if engine != "" && v.Engine != engine {
			continue
		}
		if engineVersion != "" && v.EngineVersion != engineVersion {
			continue
		}
		if defaultOnly && v.EngineVersion != defaultEngineVersion {
			continue
		}
		result = append(result, v)
	}

	return result
}
