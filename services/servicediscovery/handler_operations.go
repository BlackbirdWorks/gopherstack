package servicediscovery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

type getOperationRequest struct {
	OperationID string `json:"OperationId"`
}

func (h *Handler) handleGetOperation(_ context.Context, body []byte) ([]byte, error) {
	var req getOperationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.OperationID == "" {
		return nil, fmt.Errorf("%w: OperationId is required", errInvalidRequest)
	}

	op, err := h.Backend.GetOperation(req.OperationID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Operation": operationToMap(op),
	})
}

type operationFilter struct {
	Name      string   `json:"Name"`
	Condition string   `json:"Condition"`
	Values    []string `json:"Values"`
}

type listOperationsRequest struct {
	MaxResults *int              `json:"MaxResults"`
	NextToken  string            `json:"NextToken"`
	Filters    []operationFilter `json:"Filters"`
}

func buildOperationsFilter(filters []operationFilter) ListOperationsFilter {
	f := ListOperationsFilter{}

	for _, entry := range filters {
		if len(entry.Values) == 0 {
			continue
		}

		switch entry.Name {
		case "STATUS":
			f.Status = entry.Values[0]
		case "TYPE":
			f.Type = entry.Values[0]
		}
	}

	return f
}

func (h *Handler) handleListOperations(_ context.Context, body []byte) ([]byte, error) {
	var req listOperationsRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	page, nextToken := applyPaginationOperations(
		h.Backend.ListOperations(buildOperationsFilter(req.Filters)),
		req.NextToken,
		resolveMaxResults(req.MaxResults),
	)

	items := make([]map[string]any, 0, len(page))
	for i := range page {
		items = append(items, operationToMap(&page[i]))
	}

	return marshalPagedResponse("Operations", items, nextToken)
}

// operationToMap converts an Operation to a JSON-serialisable map with full fields.
func operationToMap(op *Operation) map[string]any {
	m := map[string]any{
		"Id":           op.ID,
		keyType:        op.Type,
		keyStatusField: op.Status,
		keyCreateDate:  awstime.Epoch(op.CreateDate),
		"UpdateDate":   awstime.Epoch(op.UpdateDate),
	}

	if len(op.Targets) > 0 {
		m["Targets"] = op.Targets
	}

	if op.ErrorCode != "" {
		m["ErrorCode"] = op.ErrorCode
	}

	if op.ErrorMessage != "" {
		m["ErrorMessage"] = op.ErrorMessage
	}

	return m
}

func applyPaginationOperations(items []Operation, nextToken string, maxResults int) ([]Operation, string) {
	if maxResults <= 0 || maxResults > maxResultsCap {
		maxResults = maxResultsDefault
	}

	offset := decodeCursor(nextToken)
	if offset >= len(items) {
		return nil, ""
	}

	end := offset + maxResults

	var newToken string

	if end < len(items) {
		newToken = encodeCursor(end)
	} else {
		end = len(items)
	}

	return items[offset:end], newToken
}
