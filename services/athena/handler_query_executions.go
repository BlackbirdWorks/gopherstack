package athena

import (
	"encoding/json"
	"fmt"
)

type startQueryExecutionInput struct { //nolint:govet // field order mirrors AWS API shape, not alignment
	QueryString              string                    `json:"QueryString"`
	WorkGroup                string                    `json:"WorkGroup"`
	QueryExecutionContext    QueryExecutionContext     `json:"QueryExecutionContext"`
	ResultConfiguration      ResultConfiguration       `json:"ResultConfiguration"`
	ExecutionParameters      []string                  `json:"ExecutionParameters"`
	ResultReuseConfiguration *ResultReuseConfiguration `json:"ResultReuseConfiguration,omitempty"`
}

type stopQueryExecutionInput struct {
	QueryExecutionID string `json:"QueryExecutionId"`
}

type getQueryExecutionInput struct {
	QueryExecutionID string `json:"QueryExecutionId"`
}

type listQueryExecutionsInput struct {
	WorkGroup  string `json:"WorkGroup"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

// maxListQueryExecutionsPageSize is the AWS upper bound (and default) for the
// MaxResults parameter on ListQueryExecutions.
const maxListQueryExecutionsPageSize = 50

type batchGetQueryExecutionInput struct {
	QueryExecutionIDs []string `json:"QueryExecutionIds"`
}

type getQueryRuntimeStatsInput struct {
	QueryExecutionID string `json:"QueryExecutionId"`
}

func (h *Handler) queryExecutionOps() map[string]athenaActionFn {
	return map[string]athenaActionFn{
		"StartQueryExecution": func(b []byte) (any, error) {
			var input startQueryExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			id, err := h.Backend.StartQueryExecution(
				input.QueryString,
				input.WorkGroup,
				input.QueryExecutionContext,
				input.ResultConfiguration,
				input.ExecutionParameters,
				input.ResultReuseConfiguration,
			)
			if err != nil {
				return nil, err
			}

			return map[string]any{"QueryExecutionId": id}, nil
		},
		"StopQueryExecution": func(b []byte) (any, error) {
			var input stopQueryExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.StopQueryExecution(input.QueryExecutionID)
		},
		"GetQueryExecution": func(b []byte) (any, error) {
			var input getQueryExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			qe, err := h.Backend.GetQueryExecution(input.QueryExecutionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{"QueryExecution": qe}, nil
		},
		"ListQueryExecutions": h.handleListQueryExecutions,
		"BatchGetQueryExecution": func(b []byte) (any, error) {
			var input batchGetQueryExecutionInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			const maxBatchGetQueryExecution = 50
			if len(input.QueryExecutionIDs) > maxBatchGetQueryExecution {
				return nil, fmt.Errorf(
					"%w: BatchGetQueryExecution accepts at most 50 IDs",
					ErrValidation,
				)
			}

			found, unprocessed := h.Backend.BatchGetQueryExecution(input.QueryExecutionIDs)

			return map[string]any{
				"QueryExecutions":              found,
				"UnprocessedQueryExecutionIds": unprocessed,
			}, nil
		},
		"GetQueryResults": h.handleGetQueryResults,
		"GetQueryRuntimeStatistics": func(b []byte) (any, error) {
			var input getQueryRuntimeStatsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			stats, err := h.Backend.GetQueryRuntimeStatistics(input.QueryExecutionID)
			if err != nil {
				return nil, err
			}

			return map[string]any{"QueryRuntimeStatistics": stats}, nil
		},
	}
}

// athenaMaxQueryResultsPageSize matches the AWS-documented maximum page size
// for Athena GetQueryResults. The minimum is 1.
const athenaMaxQueryResultsPageSize = 1000

// handleListQueryExecutions lists query-execution IDs for a workgroup with
// opaque-token pagination.
func (h *Handler) handleListQueryExecutions(b []byte) (any, error) {
	var input listQueryExecutionsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}

	ids, err := h.Backend.ListQueryExecutions(input.WorkGroup)
	if err != nil {
		return nil, err
	}

	ids, nextToken, err := h.tokens.paginateQueryExecutionIDs(
		ids, input.MaxResults, input.NextToken,
	)
	if err != nil {
		return nil, err
	}

	out := map[string]any{"QueryExecutionIds": ids}
	if nextToken != "" {
		out["NextToken"] = nextToken
	}

	return out, nil
}

type getQueryResultsInput struct {
	QueryExecutionID string `json:"QueryExecutionId"`
	NextToken        string `json:"NextToken,omitempty"`
	MaxResults       int    `json:"MaxResults,omitempty"`
}

// handleGetQueryResults returns the stored result set for a query execution.
// Real rows are returned for SELECT queries against registered in-memory tables.
func (h *Handler) handleGetQueryResults(b []byte) (any, error) {
	var input getQueryResultsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrValidation, err)
	}

	if input.QueryExecutionID == "" {
		return nil, fmt.Errorf("%w: QueryExecutionId is required", ErrValidation)
	}

	if input.MaxResults < 0 || input.MaxResults > athenaMaxQueryResultsPageSize {
		return nil, fmt.Errorf(
			"%w: MaxResults must be between 1 and %d",
			ErrValidation, athenaMaxQueryResultsPageSize,
		)
	}

	qe, err := h.Backend.GetQueryExecution(input.QueryExecutionID)
	if err != nil {
		return nil, err
	}

	if qe.Status.State != stateSucceeded {
		return nil, fmt.Errorf(
			"%w: query has not yet finished. Current state: %s",
			ErrValidation, qe.Status.State,
		)
	}

	page, err := h.Backend.GetQueryResults(
		input.QueryExecutionID,
		input.NextToken,
		input.MaxResults,
	)
	if err != nil {
		return nil, err
	}

	// Build ColumnInfo list.
	columnInfo := make([]map[string]any, 0, len(page.Columns))
	for _, c := range page.Columns {
		columnInfo = append(columnInfo, map[string]any{
			"Name":          c.name,
			"Type":          c.typ,
			"Label":         c.name,
			"CatalogName":   "hive",
			"SchemaName":    "",
			"TableName":     "",
			"Nullable":      "UNKNOWN",
			"CaseSensitive": false,
			"Precision":     0,
			"Scale":         0,
		})
	}

	// Build Rows: first row is header (column names), subsequent rows are data.
	rows := make([]map[string]any, 0)

	// Real AWS only includes the header row on the first page
	if len(page.Columns) > 0 && input.NextToken == "" {
		header := make([]map[string]any, 0, len(page.Columns))
		for _, c := range page.Columns {
			header = append(header, map[string]any{"VarCharValue": c.name})
		}
		rows = append(rows, map[string]any{"Data": header})
	}

	for _, row := range page.Rows {
		data := make([]map[string]any, 0, len(row))
		for _, cell := range row {
			data = append(data, map[string]any{"VarCharValue": cell})
		}
		rows = append(rows, map[string]any{"Data": data})
	}

	resp := map[string]any{
		"ResultSet": map[string]any{
			"Rows": rows,
			"ResultSetMetadata": map[string]any{
				"ColumnInfo": columnInfo,
			},
		},
		"UpdateCount": 0,
	}

	if page.NextToken != "" {
		resp["NextToken"] = page.NextToken
	}

	return resp, nil
}
