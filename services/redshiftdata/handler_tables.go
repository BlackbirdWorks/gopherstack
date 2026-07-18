package redshiftdata

import (
	"context"
	"encoding/json"
	"fmt"
)

func (h *Handler) handleListTables(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Database          string `json:"Database"`
		WorkgroupName     string `json:"WorkgroupName"`
		ClusterIdentifier string `json:"ClusterIdentifier"`
		SecretArn         string `json:"SecretArn"`
		DBUser            string `json:"DbUser"`
		SchemaPattern     string `json:"SchemaPattern"`
		TablePattern      string `json:"TablePattern"`
		TableType         string `json:"TableType"`
		NextToken         string `json:"NextToken"`
		MaxResults        int    `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MaxResults > maxListTablesResults {
		return nil, fmt.Errorf("%w: MaxResults must be ≤ %d", ErrValidation, maxListTablesResults)
	}

	tables := filterDemoTables(buildDemoTables(), req.TableType, req.SchemaPattern, req.TablePattern)
	page, next := paginateMaps(tables, req.NextToken, req.MaxResults, defaultListTablesResults)
	resp := map[string]any{"Tables": page}

	if next != "" {
		resp[keyNextToken] = next
	}

	return json.Marshal(resp)
}

// filterDemoTables applies TableType, SchemaPattern, and TablePattern filters to the demo table list.
func filterDemoTables(tables []map[string]any, tableType, schemaPattern, tablePattern string) []map[string]any {
	if tableType != "" {
		tables = filterMapsByField(tables, keyType, func(v string) bool { return v == tableType })
	}

	if schemaPattern != "" {
		tables = filterMapsByField(tables, keySchema, func(v string) bool { return matchSQLLike(v, schemaPattern) })
	}

	if tablePattern != "" {
		tables = filterMapsByField(tables, keyName, func(v string) bool { return matchSQLLike(v, tablePattern) })
	}

	return tables
}

// filterMapsByField returns entries where the string value at field satisfies match.
func filterMapsByField(all []map[string]any, field string, match func(string) bool) []map[string]any {
	out := make([]map[string]any, 0, len(all))

	for _, m := range all {
		if v, ok := m[field].(string); ok && match(v) {
			out = append(out, m)
		}
	}

	return out
}

func (h *Handler) handleDescribeTable(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Database          string `json:"Database"`
		WorkgroupName     string `json:"WorkgroupName"`
		ClusterIdentifier string `json:"ClusterIdentifier"`
		SecretArn         string `json:"SecretArn"`
		DBUser            string `json:"DbUser"`
		Schema            string `json:"Schema"`
		Table             string `json:"Table"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	// DescribeTableOutput.TableName is a plain string in the real API (see
	// aws-sdk-go-v2/service/redshiftdata's DescribeTableOutput), not a nested
	// schema/name/type object. Sending an object here would be silently
	// ignored by the SDK's deserializer (unknown-field default case),
	// leaving TableName unset for callers.
	return json.Marshal(map[string]any{
		"ColumnList": buildDemoColumns(),
		"TableName":  req.Table,
	})
}

// paginateMaps applies cursor-based pagination to a slice of maps keyed by "name".
// Returns the page and the next-page token (empty when no more pages).
func paginateMaps(all []map[string]any, token string, maxResults, defaultMax int) ([]map[string]any, string) {
	start := 0

	if token != "" {
		for i, m := range all {
			if nv, ok := m[keyName].(string); ok && nv == token {
				start = i + 1

				break
			}
		}
	}

	page := all[start:]
	limit := maxResults

	if limit <= 0 {
		limit = defaultMax
	}

	if len(page) <= limit {
		return page, ""
	}

	nextName, _ := page[limit][keyName].(string)

	return page[:limit], nextName
}

// buildDemoTables returns a realistic demo list of table descriptors.
func buildDemoTables() []map[string]any {
	return []map[string]any{
		{keyName: "users", keySchema: schemaPublic, keyType: valTABLE},
		{keyName: "orders", keySchema: schemaPublic, keyType: valTABLE},
		{keyName: "events", keySchema: schemaTypeRaw, keyType: valTABLE},
		{keyName: "sessions", keySchema: schemaTypeRaw, keyType: valTABLE},
		{keyName: "daily_summary", keySchema: valCurated, keyType: "VIEW"},
		{keyName: "user_metrics", keySchema: valCurated, keyType: "VIEW"},
		{keyName: "columns", keySchema: schemaInfo, keyType: valTABLE},
		{keyName: "tables", keySchema: schemaInfo, keyType: valTABLE},
	}
}

// buildDemoColumns returns a realistic demo list of column descriptors for DescribeTable.
func buildDemoColumns() []map[string]any {
	const (
		sizeInt       = int64(10)
		sizeVarchar   = int64(255)
		sizeTimestamp = int64(29)
		notNull       = int64(0)
		nullable      = int64(1)
	)

	return []map[string]any{
		{
			keyName:       "id",
			keyTypeName:   "int4",
			keyLength:     sizeInt,
			keyNullable:   notNull,
			keySchemaName: schemaPublic,
		},
		{
			keyName:       keyName,
			keyTypeName:   typeVarchar,
			keyLength:     sizeVarchar,
			keyNullable:   nullable,
			keySchemaName: schemaPublic,
		},
		{
			keyName:       "email",
			keyTypeName:   typeVarchar,
			keyLength:     sizeVarchar,
			keyNullable:   nullable,
			keySchemaName: schemaPublic,
		},
		{
			keyName:       "created_at",
			keyTypeName:   "timestamp",
			keyLength:     sizeTimestamp,
			keyNullable:   nullable,
			keySchemaName: schemaPublic,
		},
		{
			keyName:       "updated_at",
			keyTypeName:   "timestamp",
			keyLength:     sizeTimestamp,
			keyNullable:   nullable,
			keySchemaName: schemaPublic,
		},
	}
}
