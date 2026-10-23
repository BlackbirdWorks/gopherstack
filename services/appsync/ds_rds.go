package appsync

import (
	"context"
	"encoding/json"
	"fmt"
)

// invokeRDSDataSource runs the mapped statements through the RDS Data API and
// hands the response mapping the JSON string AppSync produces.
func invokeRDSDataSource(
	ctx context.Context,
	backend *InMemoryBackend,
	mu mappingUnit,
	ds *DataSource,
	args map[string]any,
	prev any,
) (any, error) {
	if backend.rdsExec == nil {
		return nil, ErrRDSNotConfigured
	}

	cfg := rdsEndpointConfig(ds)
	if cfg == nil || cfg.DBClusterIdentifier == "" {
		return nil, fmt.Errorf("%w: relational data source missing rdsHttpEndpointConfig", ErrValidation)
	}

	req, err := requestObject(mu, args, prev, "RDS")
	if err != nil {
		return nil, err
	}

	stmts := listOf(req["statements"])
	if len(stmts) == 0 {
		return nil, fmt.Errorf("%w: RDS request requires a non-empty statements list", ErrValidation)
	}

	vars, _ := req["variableMap"].(map[string]any)
	hints := stringMap(req["variableTypeHintMap"])
	results := make([]any, 0, len(stmts))

	for _, st := range stmts {
		out, execErr := backend.rdsExec.ExecuteRDSStatement(withRegion(ctx, backend.region), RDSStatement{
			ClusterID: cfg.DBClusterIdentifier, Database: cfg.DatabaseName,
			SQL: vrender(st), Variables: vars, Hints: hints,
		})
		if execErr != nil {
			return nil, fmt.Errorf("rds statement failed: %w", execErr)
		}

		results = append(results, out)
	}

	raw, err := json.Marshal(map[string]any{"sqlStatementResults": results})
	if err != nil {
		return nil, fmt.Errorf("rds result is not JSON-serializable: %w", err)
	}

	return evalResponseMapping(mu, args, string(raw))
}

func rdsEndpointConfig(ds *DataSource) *RDSHTTPEndpointConfig {
	if ds.RelationalDatabaseConfig == nil {
		return nil
	}

	return ds.RelationalDatabaseConfig.RDSHTTPEndpointConfig
}

func stringMap(v any) map[string]string {
	m, _ := v.(map[string]any)
	out := make(map[string]string, len(m))

	for k, x := range m {
		out[k] = vrender(x)
	}

	return out
}
