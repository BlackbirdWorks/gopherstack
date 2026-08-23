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
// (e.g. after SeedFinding). Resources and Remediation are both required on
// the real Finding shape (confirmed via types.go): Resources was only
// emitted when non-empty, dropping the key entirely for any finding seeded
// with none (SeedFinding/AddFinding enforce nothing here, so this state is
// reachable); Remediation had no backing struct field at all. Real
// Remediation's own Recommendation member is optional, so an honestly empty
// object -- no text/URL gopherstack has any source for -- satisfies the
// required key without fabricating remediation advice. "severity" is a bare
// Severity string enum on the real wire (deserializers.go's
// awsRestjson1_deserializeDocumentFinding: case "severity" expects a JSON
// string, not an object) -- the prior {label,score} nested object caused a
// hard deserialization error on any real SDK client's ListFindings call once
// a finding existed, not merely a missing field. The numeric score belongs
// on the separate, optional, top-level InspectorScore member instead.
func findingToWire(f *Finding) map[string]any {
	resources := make([]map[string]any, 0, len(f.Resources))
	for _, r := range f.Resources {
		resources = append(resources, map[string]any{keyType: r.Type, "id": r.ID})
	}

	entry := map[string]any{
		"awsAccountId":    f.AccountID,
		"description":     f.Description,
		keyFindingArn:     f.FindingArn,
		"firstObservedAt": awstime.Epoch(f.FirstObservedAt),
		"lastObservedAt":  awstime.Epoch(f.LastObservedAt),
		keyUpdatedAt:      awstime.Epoch(f.UpdatedAt),
		"remediation":     map[string]any{},
		"resources":       resources,
		"severity":        f.Severity.Label,
		keyStatus:         f.Status,
		keyType:           f.Type,
	}

	if f.Severity.Score != 0 {
		entry["inspectorScore"] = f.Severity.Score
	}

	if f.Title != "" {
		entry["title"] = f.Title
	}

	if f.FixAvailable != "" {
		entry["fixAvailable"] = f.FixAvailable
	}

	return entry
}

func (h *Handler) handleCreateFindingsReport(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		S3Destination  map[string]any `json:"s3Destination"`
		FilterCriteria map[string]any `json:"filterCriteria"`
		ReportFormat   string         `json:"reportFormat"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	report, createErr := h.Backend.CreateFindingsReport(req.S3Destination, req.FilterCriteria, req.ReportFormat)
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

	resp := map[string]any{
		keyReportID:  report.ReportID,
		keyStatus:    report.Status,
		keyErrorCode: report.ErrorCode,
	}

	if report.ErrorMessage != "" {
		resp["errorMessage"] = report.ErrorMessage
	}

	if report.Destination != nil {
		resp["destination"] = report.Destination
	}

	if report.FilterCriteria != nil {
		resp["filterCriteria"] = report.FilterCriteria
	}

	return c.JSON(http.StatusOK, resp)
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

	resp := map[string]any{"vulnerabilities": vulnerabilitiesToWire(vulns)}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// vulnerabilitiesToWire renders Vulnerabilities in their Inspector2 wire
// shape. vendorCreatedAt/vendorUpdatedAt are DateTimeTimestamp members (see
// findingToWire's doc comment for why time.Time must never be marshaled
// directly for a restjson1 timestamp field).
func vulnerabilitiesToWire(vulns []*Vulnerability) []map[string]any {
	wire := make([]map[string]any, 0, len(vulns))

	for _, v := range vulns {
		entry := map[string]any{"id": v.ID}

		if v.Description != "" {
			entry["description"] = v.Description
		}

		if v.Source != "" {
			entry["source"] = v.Source
		}

		if v.SourceURL != "" {
			entry["sourceUrl"] = v.SourceURL
		}

		if v.VendorSeverity != "" {
			entry["vendorSeverity"] = v.VendorSeverity
		}

		if len(v.Cwes) > 0 {
			entry["cwes"] = v.Cwes
		}

		if len(v.ReferenceUrls) > 0 {
			entry["referenceUrls"] = v.ReferenceUrls
		}

		if len(v.RelatedVulnerabilities) > 0 {
			entry["relatedVulnerabilities"] = v.RelatedVulnerabilities
		}

		if !v.VendorCreatedAt.IsZero() {
			entry["vendorCreatedAt"] = awstime.Epoch(v.VendorCreatedAt)
		}

		if !v.VendorUpdatedAt.IsZero() {
			entry["vendorUpdatedAt"] = awstime.Epoch(v.VendorUpdatedAt)
		}

		wire = append(wire, entry)
	}

	return wire
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

	// Real BatchGetFindingDetailsInput.findingArns is a plain string array
	// (["arn1","arn2"]), not an array of objects -- decoding into
	// []map[string]any made every real request fail JSON unmarshaling with a
	// ValidationException.
	var req struct {
		FindingArns []string `json:"findingArns"`
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
