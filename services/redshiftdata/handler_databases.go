package redshiftdata

import (
	"context"
	"encoding/json"
	"fmt"
)

func (h *Handler) handleListDatabases(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Database          string `json:"Database"`
		WorkgroupName     string `json:"WorkgroupName"`
		ClusterIdentifier string `json:"ClusterIdentifier"`
		SecretArn         string `json:"SecretArn"`
		DBUser            string `json:"DbUser"`
		NextToken         string `json:"NextToken"`
		MaxResults        int    `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MaxResults > maxListDatabasesResults {
		return nil, fmt.Errorf("%w: MaxResults must be ≤ %d", ErrValidation, maxListDatabasesResults)
	}

	page, next := paginateStrings(buildDemoDatabases(), req.NextToken, req.MaxResults, defaultListDatabasesResults)
	resp := map[string]any{"Databases": page, keyNextToken: next}

	return json.Marshal(resp)
}

func (h *Handler) handleListSchemas(_ context.Context, body []byte) ([]byte, error) {
	var req struct {
		Database          string `json:"Database"`
		WorkgroupName     string `json:"WorkgroupName"`
		ClusterIdentifier string `json:"ClusterIdentifier"`
		SecretArn         string `json:"SecretArn"`
		DBUser            string `json:"DbUser"`
		SchemaPattern     string `json:"SchemaPattern"`
		NextToken         string `json:"NextToken"`
		MaxResults        int    `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.MaxResults > maxListSchemasResults {
		return nil, fmt.Errorf("%w: MaxResults must be ≤ %d", ErrValidation, maxListSchemasResults)
	}

	schemas := buildDemoSchemas()
	if req.SchemaPattern != "" {
		schemas = filterByPattern(schemas, req.SchemaPattern)
	}

	page, next := paginateStrings(schemas, req.NextToken, req.MaxResults, defaultListSchemasResults)
	resp := map[string]any{"Schemas": page}

	if next != "" {
		resp[keyNextToken] = next
	}

	return json.Marshal(resp)
}

// paginateStrings applies cursor-based pagination to a sorted string slice.
// Returns the page and the next-page token (empty when no more pages).
func paginateStrings(all []string, token string, maxResults, defaultMax int) ([]string, string) {
	start := 0

	if token != "" {
		for i, s := range all {
			if s == token {
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

	return page[:limit], page[limit]
}

// filterByPattern returns strings that match the SQL LIKE pattern.
// % matches any sequence of characters, _ matches any single character.
func filterByPattern(all []string, pattern string) []string {
	out := make([]string, 0, len(all))

	for _, s := range all {
		if matchSQLLike(s, pattern) {
			out = append(out, s)
		}
	}

	return out
}

// listDemoData holds the static demo payloads for metadata list operations.
// These are package-level functions to avoid gochecknoglobals violations.

// buildDemoDatabases returns a realistic demo list of database names.
func buildDemoDatabases() []string {
	return []string{"dev", "prod", "staging", "analytics"}
}

// buildDemoSchemas returns a realistic demo list of schema names.
func buildDemoSchemas() []string {
	return []string{schemaPublic, schemaInfo, "pg_catalog", schemaTypeRaw, valCurated}
}
