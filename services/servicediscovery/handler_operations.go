package servicediscovery

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

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

// parseEpochSeconds parses a decimal (optionally fractional) Unix-seconds
// string, the wire format OperationFilter's UPDATE_DATE Values use (per its
// doc comment: "Specify a start date and an end date in Unix date/time
// format"). Returns ok=false for unparseable input, letting the caller treat
// that bound as unset rather than erroring the whole request.
func parseEpochSeconds(s string) (time.Time, bool) {
	secs, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return time.Time{}, false
	}

	whole := int64(secs)
	nanos := int64((secs - float64(whole)) * float64(time.Second))

	return time.Unix(whole, nanos).UTC(), true
}

// buildOperationsFilter converts the wire-level filter entries into a
// ListOperationsFilter, per OperationFilter's documented Name values:
// NAMESPACE_ID, SERVICE_ID, STATUS, TYPE, UPDATE_DATE (types.OperationFilter
// doc comment). UPDATE_DATE only recognizes the documented BETWEEN condition
// with exactly a [start, end] Values pair; anything else leaves the date
// bounds unset (no filtering on that dimension).
func buildOperationsFilter(filters []operationFilter) ListOperationsFilter {
	f := ListOperationsFilter{}

	for _, entry := range filters {
		if len(entry.Values) == 0 {
			continue
		}

		fv := FilterValue{Condition: entry.Condition, Values: entry.Values}

		switch entry.Name {
		case "NAMESPACE_ID":
			f.NamespaceID = fv
		case "SERVICE_ID":
			f.ServiceID = fv
		case "STATUS":
			f.Status = fv
		case "TYPE":
			f.Type = fv
		case "UPDATE_DATE":
			if entry.Condition == "BETWEEN" && len(entry.Values) >= 2 {
				if start, ok := parseEpochSeconds(entry.Values[0]); ok {
					f.UpdateDateStart = &start
				}

				if end, ok := parseEpochSeconds(entry.Values[1]); ok {
					f.UpdateDateEnd = &end
				}
			}
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
		items = append(items, operationSummaryToMap(&page[i]))
	}

	return marshalPagedResponse("Operations", items, nextToken)
}

// operationSummaryToMap converts an Operation to the lightweight shape real
// Cloud Map's ListOperations returns: types.OperationSummary, which has only
// Id and Status (api_op_ListOperations.go: "Operations []types.OperationSummary").
// Unlike GetOperation, ListOperations never includes Type/CreateDate/UpdateDate/
// Targets/ErrorCode/ErrorMessage.
func operationSummaryToMap(op *Operation) map[string]any {
	return map[string]any{
		"Id":           op.ID,
		keyStatusField: op.Status,
	}
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
