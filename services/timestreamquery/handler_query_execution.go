package timestreamquery

import (
	"context"
	"encoding/json"
	"fmt"
)

func (h *Handler) handleQuery(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		QueryInsights *struct {
			Mode string `json:"Mode"`
		} `json:"QueryInsights"`
		QueryString string `json:"QueryString"`
		ClientToken string `json:"ClientToken"`
		NextToken   string `json:"NextToken"`
		MaxRows     int32  `json:"MaxRows"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	insightsMode := ""
	if req.QueryInsights != nil {
		insightsMode = req.QueryInsights.Mode
	}

	if req.NextToken == "" && req.QueryString == "" {
		return nil, fmt.Errorf("%w: QueryString is required", ErrValidation)
	}

	page, err := h.Backend.QueryWithOptions(ctx, QueryOptions{
		QueryString:  req.QueryString,
		ClientToken:  req.ClientToken,
		NextToken:    req.NextToken,
		MaxRows:      req.MaxRows,
		InsightsMode: insightsMode,
	})
	if err != nil {
		return nil, err
	}

	resp := map[string]any{
		"QueryId":    page.QueryID,
		"Rows":       marshalRows(page.Rows),
		"ColumnInfo": marshalColumnInfos(page.Columns),
		"QueryStatus": map[string]any{
			"ProgressPercentage":     page.QueryStatus.ProgressPercentage,
			"CumulativeBytesScanned": page.QueryStatus.CumulativeBytesScanned,
			"CumulativeBytesMetered": page.QueryStatus.CumulativeBytesMetered,
		},
	}
	if page.NextToken != "" {
		resp["NextToken"] = page.NextToken
	}
	if page.Insights != nil {
		resp["QueryInsightsResponse"] = page.Insights
	}

	return json.Marshal(resp)
}

// marshalRows converts []Row to JSON-serialisable form.
func marshalRows(rows []Row) []map[string]any {
	out := make([]map[string]any, len(rows))
	for i, r := range rows {
		data := make([]any, len(r.Data))
		for j, d := range r.Data {
			data[j] = d
		}
		out[i] = map[string]any{"Data": data}
	}

	return out
}

// marshalColumnInfos converts []ColumnInfo to JSON-serialisable form.
// ColumnInfo.Type already carries the correct wire field names/omitempty
// tags for the full nested union (ScalarType | ArrayColumnInfo |
// RowColumnInfo | TimeSeriesMeasureValueColumnInfo, see types.Type), so it is
// passed straight through rather than hand-picking only ScalarType -- an
// earlier version dropped the other three union members entirely.
func marshalColumnInfos(cols []ColumnInfo) []map[string]any {
	out := make([]map[string]any, len(cols))
	for i, c := range cols {
		entry := map[string]any{"Type": c.Type}
		if c.Name != "" {
			entry["Name"] = c.Name
		}
		out[i] = entry
	}

	return out
}

func (h *Handler) handleCancelQuery(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		QueryID string `json:"QueryId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.QueryID == "" {
		return nil, fmt.Errorf("%w: QueryId is required", ErrValidation)
	}

	if err := h.Backend.CancelQuery(ctx, req.QueryID); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"CancellationMessage": "Query has been successfully cancelled.",
	})
}
