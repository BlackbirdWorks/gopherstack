package rdsdata

import "context"

// ExecuteSQL executes one or more SQL statements against the cluster.
// This is a deprecated operation; use ExecuteStatement or BatchExecuteStatement instead.
func (b *InMemoryBackend) ExecuteSQL(
	ctx context.Context,
	resourceARN, sqlStatements string,
) ([]SQLStatementResult, error) {
	b.mu.Lock("ExecuteSql")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.defaultRegion)
	b.appendStatementLocked(region, resourceARN, sqlStatements, "")

	// Execute for real so the deprecated entry point still mutates state; the
	// reported update count reflects the engine result when available.
	_, _, updated, err := b.engine.execute(ctx, region, resourceARN, sqlStatements, "", nil)
	if err != nil {
		return []SQLStatementResult{{NumberOfRecordsUpdated: 0}}, nil
	}

	return []SQLStatementResult{{NumberOfRecordsUpdated: updated}}, nil
}
