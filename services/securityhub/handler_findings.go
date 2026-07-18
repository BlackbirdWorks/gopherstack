package securityhub

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

func classifyFindingsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/findings":
		return opGetFindings, ""
	case method == http.MethodPost && path == "/findings/import":
		return opBatchImportFindings, ""
	case method == http.MethodPatch && path == "/findings/batchupdate":
		return opBatchUpdateFindings, ""
	case method == http.MethodPatch && path == "/findings":
		return opUpdateFindings, ""
	case method == http.MethodPost && path == "/findingHistory/get":
		return opGetFindingHistory, ""
	}

	return opUnknown, ""
}

func (h *Handler) handleGetFindings(c *echo.Context, body map[string]any) error {
	filters, _ := body["Filters"].(map[string]any)
	sortCriteria, _ := body["SortCriteria"].([]any)
	nextToken, _ := body["NextToken"].(string)
	maxResults := intFromBody(body)

	var sortMaps []map[string]any

	for _, s := range sortCriteria {
		if m, ok := s.(map[string]any); ok {
			sortMaps = append(sortMaps, m)
		}
	}

	findings, nextOut := h.Backend.GetFindings(filters, sortMaps, nextToken, maxResults)

	resp := map[string]any{"Findings": findings}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleBatchImportFindings(c *echo.Context, body map[string]any) error {
	rawFindings, _ := body["Findings"].([]any)

	var findings []map[string]any

	for _, f := range rawFindings {
		if m, ok := f.(map[string]any); ok {
			findings = append(findings, m)
		}
	}

	const maxImportFindings = 100
	if len(findings) > maxImportFindings {
		return c.JSON(http.StatusBadRequest, map[string]any{
			keyMessage: fmt.Sprintf("Findings list must not exceed %d entries", maxImportFindings),
		})
	}

	successCount, failedCount, failedFindings := h.Backend.ImportFindings(findings)

	return c.JSON(http.StatusOK, map[string]any{
		"SuccessCount":   successCount,
		"FailedCount":    failedCount,
		"FailedFindings": failedFindings,
	})
}

func (h *Handler) handleBatchUpdateFindings(c *echo.Context, body map[string]any) error {
	rawIdents, _ := body["FindingIdentifiers"].([]any)

	var identifiers []map[string]any

	for _, i := range rawIdents {
		if m, ok := i.(map[string]any); ok {
			identifiers = append(identifiers, m)
		}
	}

	// Collect updates (all body fields except FindingIdentifiers)
	updates := make(map[string]any)

	for k, v := range body {
		if k != "FindingIdentifiers" {
			updates[k] = v
		}
	}

	processed, unprocessed := h.Backend.BatchUpdateFindings(identifiers, updates)

	return c.JSON(http.StatusOK, map[string]any{
		"ProcessedFindings":   processed,
		"UnprocessedFindings": unprocessed,
	})
}

func (h *Handler) handleUpdateFindings(c *echo.Context, body map[string]any) error {
	filters, _ := body["Filters"].(map[string]any)
	note, _ := body["Note"].(map[string]any)
	recordState, _ := body["RecordState"].(string)

	if err := h.Backend.UpdateFindings(filters, note, recordState); err != nil {
		if errors.Is(err, ErrHubNotEnabled) {
			return c.JSON(http.StatusBadRequest, map[string]any{
				keyMessage: msgHubNotEnabled,
			})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleGetFindingHistory(c *echo.Context, body map[string]any) error {
	ident, _ := body["FindingIdentifier"].(map[string]any)
	startTime, _ := body["StartTime"].(string)
	endTime, _ := body["EndTime"].(string)
	nextToken, _ := body["NextToken"].(string)
	maxResults := intFromBody(body)

	records, nextOut := h.Backend.GetFindingHistory(ident, startTime, endTime, nextToken, maxResults)

	resp := map[string]any{"Records": records}
	if nextOut != "" {
		resp["NextToken"] = nextOut
	}

	return c.JSON(http.StatusOK, resp)
}

func classifyFindingsV2Path(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == "/findingsv2":
		return opGetFindingsV2, ""
	case method == http.MethodPatch && path == "/findingsv2/batchupdatev2":
		return opBatchUpdateFindingsV2, ""
	case method == http.MethodPost && path == "/findingsv2/statistics":
		return opGetFindingStatisticsV2, ""
	case method == http.MethodPost && path == "/findingsTrendsv2":
		return opGetFindingsTrendsV2, ""
	}

	return opUnknown, ""
}

func (h *Handler) handleGetFindingsV2(c *echo.Context, body map[string]any) error {
	var filters map[string]any

	if f, ok := body["Filters"].(map[string]any); ok {
		filters = f
	}

	var sortCriteria []map[string]any

	if raw, ok := body["SortCriteria"].([]any); ok {
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok { //nolint:govet // existing issue.
				sortCriteria = append(sortCriteria, m)
			}
		}
	}

	nextToken, _ := body["NextToken"].(string)
	maxResults := 0

	if v, ok := body[keyMaxResults].(float64); ok {
		maxResults = int(v)
	}

	findings, next := h.Backend.GetFindingsV2(filters, sortCriteria, nextToken, maxResults)

	if findings == nil {
		findings = []map[string]any{}
	}

	resp := map[string]any{"Findings": findings}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleBatchUpdateFindingsV2(c *echo.Context, body map[string]any) error {
	var findingIdentifiers []map[string]any

	if raw, ok := body["FindingIdentifiers"].([]any); ok {
		for _, item := range raw {
			if m, ok := item.(map[string]any); ok { //nolint:govet // existing issue.
				findingIdentifiers = append(findingIdentifiers, m)
			}
		}
	}

	var updates map[string]any

	if u, ok := body["FindingFieldsUpdate"].(map[string]any); ok {
		updates = u
	}

	processed, unprocessed := h.Backend.BatchUpdateFindingsV2(findingIdentifiers, updates)

	if processed == nil {
		processed = []map[string]any{}
	}

	if unprocessed == nil {
		unprocessed = []map[string]any{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ProcessedFindings":   processed,
		"UnprocessedFindings": unprocessed,
	})
}

func (h *Handler) handleGetFindingStatisticsV2(c *echo.Context, body map[string]any) error {
	var groupByAttributes []string

	if raw, ok := body["GroupByAttributes"].([]any); ok {
		for _, v := range raw {
			if s, ok := v.(string); ok { //nolint:govet // existing issue.
				groupByAttributes = append(groupByAttributes, s)
			}
		}
	}

	stats := h.Backend.GetFindingStatisticsV2(groupByAttributes)

	if stats == nil {
		stats = []map[string]any{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FindingStatistics": stats,
	})
}

func (h *Handler) handleGetFindingsTrendsV2(c *echo.Context, body map[string]any) error {
	groupByAttribute, _ := body["GroupByAttribute"].(string)
	startTime, _ := body["StartTime"].(string)
	endTime, _ := body["EndTime"].(string)

	trends := h.Backend.GetFindingsTrendsV2(groupByAttribute, startTime, endTime)

	if trends == nil {
		trends = []map[string]any{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"FindingsTrends": trends,
	})
}

// findingsOpHandlers returns the Findings (V1 + V2) operation dispatch table
// for handleREST.
func (h *Handler) findingsOpHandlers(c *echo.Context, body map[string]any) map[string]func() error {
	return map[string]func() error{
		opGetFindings:            func() error { return h.handleGetFindings(c, body) },
		opBatchImportFindings:    func() error { return h.handleBatchImportFindings(c, body) },
		opBatchUpdateFindings:    func() error { return h.handleBatchUpdateFindings(c, body) },
		opUpdateFindings:         func() error { return h.handleUpdateFindings(c, body) },
		opGetFindingHistory:      func() error { return h.handleGetFindingHistory(c, body) },
		opGetFindingsV2:          func() error { return h.handleGetFindingsV2(c, body) },
		opBatchUpdateFindingsV2:  func() error { return h.handleBatchUpdateFindingsV2(c, body) },
		opGetFindingStatisticsV2: func() error { return h.handleGetFindingStatisticsV2(c, body) },
		opGetFindingsTrendsV2:    func() error { return h.handleGetFindingsTrendsV2(c, body) },
	}
}
