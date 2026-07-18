package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opCreateFindingsReport    = "CreateFindingsReport"
	opCancelFindingsReport    = "CancelFindingsReport"
	opGetFindingsReportStatus = "GetFindingsReportStatus"

	opListFindingAggregations = "ListFindingAggregations"

	opSearchVulnerabilities = "SearchVulnerabilities"

	opBatchGetCodeSnippet    = "BatchGetCodeSnippet"
	opBatchGetFindingDetails = "BatchGetFindingDetails"

	pathReportingCreate    = "/reporting/create"
	pathReportingCancel    = "/reporting/cancel"
	pathReportingStatusGet = "/reporting/status/get"

	pathFindingAggregationList = "/findings/aggregation/list"

	pathVulnerabilitiesSearch = "/vulnerabilities/search"

	pathCodeSnippetBatchGet    = "/codesnippet/batchget"
	pathFindingDetailsBatchGet = "/findings/details/batch/get"

	keyReportID = "reportId"
)

// handleListFindings handles POST /findings/list.
func (h *Handler) handleListFindings(c *echo.Context) error {
	req, ok := decodeFilterListRequest(c)
	if !ok {
		return nil
	}

	findings, nextToken, findErr := h.Backend.ListFindings(req.MaxResults, req.NextToken, req.FilterCriteria)
	if findErr != nil {
		return h.mapError(c, findErr)
	}

	wire := make([]map[string]any, 0, len(findings))
	for _, f := range findings {
		wire = append(wire, findingToWire(f))
	}

	resp := map[string]any{"findings": wire}

	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// findingToWire renders a Finding in its Inspector2 wire shape. Finding's
// timestamps are Go time.Time internally (for easy backend arithmetic), but
// the restjson1 DateTimeTimestamp shape requires epoch-seconds JSON numbers
// (see pkgs/awstime) -- marshaling the struct directly via encoding/json
// would emit RFC3339 strings and break every real SDK client's
// ListFindings deserializer once a finding carries a populated timestamp
// (e.g. after SeedFinding).
func findingToWire(f *Finding) map[string]any {
	entry := map[string]any{
		"awsAccountId":    f.AccountID,
		"description":     f.Description,
		"findingArn":      f.FindingArn,
		"firstObservedAt": awstime.Epoch(f.FirstObservedAt),
		"lastObservedAt":  awstime.Epoch(f.LastObservedAt),
		keyUpdatedAt:      awstime.Epoch(f.UpdatedAt),
		"severity":        severityToWire(f.Severity),
		keyStatus:         f.Status,
		keyType:           f.Type,
	}

	if f.Title != "" {
		entry["title"] = f.Title
	}

	if f.FixAvailable != "" {
		entry["fixAvailable"] = f.FixAvailable
	}

	if len(f.Resources) > 0 {
		resources := make([]map[string]any, 0, len(f.Resources))
		for _, r := range f.Resources {
			resources = append(resources, map[string]any{keyType: r.Type, "id": r.ID})
		}

		entry["resources"] = resources
	}

	return entry
}

// severityToWire renders a FindingSeverity, omitting a zero score to match
// the domain struct's existing "score,omitempty" contract.
func severityToWire(s FindingSeverity) map[string]any {
	entry := map[string]any{"label": s.Label}

	if s.Score != 0 {
		entry["score"] = s.Score
	}

	return entry
}

func (h *Handler) handleCreateFindingsReport(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		S3Destination map[string]any `json:"s3Destination"`
		ReportFormat  string         `json:"reportFormat"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	report, createErr := h.Backend.CreateFindingsReport(req.S3Destination)
	if createErr != nil {
		return h.mapError(c, createErr)
	}

	return c.JSON(
		http.StatusOK,
		map[string]any{keyReportID: report.ReportID},
	)
}

func (h *Handler) handleCancelFindingsReport(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ReportID string `json:"reportId"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	if cancelErr := h.Backend.CancelFindingsReport(req.ReportID); cancelErr != nil {
		return h.mapError(c, cancelErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyReportID: req.ReportID})
}

func (h *Handler) handleGetFindingsReportStatus(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		ReportID string `json:"reportId"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	report, getErr := h.Backend.GetFindingsReportStatus(req.ReportID)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyReportID:  report.ReportID,
		keyStatus:    report.Status,
		keyErrorCode: report.ErrorCode,
	})
}

func (h *Handler) handleListFindingAggregations(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		AggregationRequest map[string]any `json:"aggregationRequest"`
		AggregationType    string         `json:"aggregationType"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	result, aggErr := h.Backend.ListFindingAggregations(req.AggregationType, req.AggregationRequest)
	if aggErr != nil {
		return h.mapError(c, aggErr)
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleSearchVulnerabilities(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FilterCriteria map[string]any `json:"filterCriteria"`
		NextToken      string         `json:"nextToken"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	vulns, nextToken, searchErr := h.Backend.SearchVulnerabilities(
		req.FilterCriteria,
		req.NextToken,
	)
	if searchErr != nil {
		return h.mapError(c, searchErr)
	}

	resp := map[string]any{"vulnerabilities": vulns}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleBatchGetCodeSnippet(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FindingArns []string `json:"findingArns"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	result, getErr := h.Backend.BatchGetCodeSnippet(req.FindingArns)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, result)
}

func (h *Handler) handleBatchGetFindingDetails(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FindingArns []map[string]any `json:"findingArns"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid JSON"))
	}

	result, getErr := h.Backend.BatchGetFindingDetails(req.FindingArns)
	if getErr != nil {
		return h.mapError(c, getErr)
	}

	return c.JSON(http.StatusOK, result)
}
