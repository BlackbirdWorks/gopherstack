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
	records, columns, updated, _, err := b.engine.execute(ctx, region, resourceARN, sqlStatements, "", nil)
	if err != nil {
		return []SQLStatementResult{{NumberOfRecordsUpdated: 0}}, nil
	}

	result := SQLStatementResult{NumberOfRecordsUpdated: updated}
	if isQuery(sqlStatements) {
		result.ResultFrame = buildResultFrame(records, columns)
	}

	return []SQLStatementResult{result}, nil
}

// buildResultFrame converts the row/column data ExecuteStatement's engine
// path already produces (see engine.go's scanRows) into the legacy
// ExecuteSql wire shape.
func buildResultFrame(records [][]Field, columns []ColumnMetadata) *ResultFrame {
	rows := make([]Record, len(records))
	for i, rec := range records {
		values := make([]Value, len(rec))
		for j, f := range rec {
			values[j] = legacyValueFromField(f)
		}

		rows[i] = Record{Values: values}
	}

	return &ResultFrame{
		Records: rows,
		ResultSetMetadata: &ResultSetMetadata{
			ColumnCount:    int64(len(columns)),
			ColumnMetadata: columns,
		},
	}
}

// legacyValueFromField converts a Field (ExecuteStatement's result shape)
// to a Value (ExecuteSql's older union) at the wire boundary -- see Value's
// doc comment in models.go for why the member names differ.
func legacyValueFromField(f Field) Value {
	isNull := true

	switch {
	case f.IsNull != nil && *f.IsNull:
		return Value{IsNull: &isNull}
	case f.BooleanValue != nil:
		return Value{BitValue: f.BooleanValue}
	case f.LongValue != nil:
		return Value{BigIntValue: f.LongValue}
	case f.DoubleValue != nil:
		return Value{DoubleValue: f.DoubleValue}
	case f.StringValue != nil:
		return Value{StringValue: f.StringValue}
	case f.BlobValue != nil:
		return Value{BlobValue: f.BlobValue}
	default:
		return Value{IsNull: &isNull}
	}
}
