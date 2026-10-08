package inspector2

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	opListCoverage           = "ListCoverage"
	opListCoverageStatistics = "ListCoverageStatistics"

	pathCoverageList           = "/coverage/list"
	pathCoverageStatisticsList = "/coverage/statistics/list"
)

func (h *Handler) handleListCoverage(c *echo.Context) error {
	req, ok := decodeFilterListRequest(c)
	if !ok {
		return nil
	}

	entries, nextToken, listErr := h.Backend.ListCoverage(req.FilterCriteria, req.MaxResults, req.NextToken)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	wire := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		wire = append(wire, coverageEntryToWire(e))
	}

	resp := map[string]any{"coveredResources": wire}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// coverageEntryToWire renders a CoverageEntry in its real CoveredResource
// wire shape. lastScannedAt is a DateTimeTimestamp member (see
// findingToWire's doc comment for why time.Time must never be marshaled
// directly for a restjson1 timestamp field).
func coverageEntryToWire(e *CoverageEntry) map[string]any {
	entry := map[string]any{
		keyAccountID:   e.AccountID,
		keyResourceID:  e.ResourceID,
		"resourceType": e.ResourceType,
		"scanType":     e.ScanType,
	}

	if !e.LastScannedAt.IsZero() {
		entry["lastScannedAt"] = awstime.Epoch(e.LastScannedAt)
	}

	if e.ScanMode != "" {
		entry["scanMode"] = e.ScanMode
	}

	if e.ScanStatus != nil {
		status := map[string]any{"statusCode": e.ScanStatus.StatusCode}
		if e.ScanStatus.Reason != "" {
			status["reason"] = e.ScanStatus.Reason
		}

		entry["scanStatus"] = status
	}

	if md := coverageMetadataToWire(e.ResourceMetadata); md != nil {
		entry["resourceMetadata"] = md
	}

	return entry
}

func (h *Handler) handleListCoverageStatistics(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid body"))
	}

	var req struct {
		FilterCriteria map[string]any `json:"filterCriteria"`
		GroupBy        string         `json:"groupBy"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(
				http.StatusBadRequest,
				errorResponse("ValidationException", "invalid JSON"),
			)
		}
	}

	stats, statsErr := h.Backend.ListCoverageStatistics(req.FilterCriteria, req.GroupBy)
	if statsErr != nil {
		return h.mapError(c, statsErr)
	}

	return c.JSON(http.StatusOK, stats)
}

// coverageMetadataToWire renders the ResourceScanMetadata union members that are set.
func coverageMetadataToWire(md *CoverageResourceMetadata) map[string]any {
	if md == nil {
		return nil
	}

	out := map[string]any{}

	if e := md.Ec2; e != nil {
		m := map[string]any{}
		putNonEmpty(m, "amiId", e.AmiID)
		putNonEmpty(m, "platform", e.Platform)

		if len(e.Tags) > 0 {
			m["tags"] = e.Tags
		}

		out["ec2"] = m
	}

	if i := md.EcrImage; i != nil {
		m := map[string]any{"inUseCount": i.InUseCount}
		if !i.ImagePulledAt.IsZero() {
			m["imagePulledAt"] = awstime.Epoch(i.ImagePulledAt)
		}

		if !i.LastInUseAt.IsZero() {
			m["lastInUseAt"] = awstime.Epoch(i.LastInUseAt)
		}

		if len(i.Tags) > 0 {
			m["tags"] = i.Tags
		}

		out["ecrImage"] = m
	}

	if r := md.EcrRepository; r != nil {
		m := map[string]any{"name": r.Name}
		putNonEmpty(m, "scanFrequency", r.ScanFrequency)
		out["ecrRepository"] = m
	}

	if l := md.LambdaFunction; l != nil {
		m := map[string]any{}
		putNonEmpty(m, "functionName", l.FunctionName)
		putNonEmpty(m, "runtime", l.Runtime)

		if len(l.FunctionTags) > 0 {
			m["functionTags"] = l.FunctionTags
		}

		if len(l.Layers) > 0 {
			m["layers"] = l.Layers
		}

		out["lambdaFunction"] = m
	}

	if c := md.CodeRepository; c != nil {
		m := map[string]any{}
		putNonEmpty(m, "projectName", c.ProjectName)
		putNonEmpty(m, "providerType", c.ProviderType)
		putNonEmpty(m, "providerTypeVisibility", c.ProviderTypeVisibility)
		putNonEmpty(m, "integrationArn", c.IntegrationArn)
		putNonEmpty(m, "lastScannedCommitId", c.LastScannedCommitID)
		out["codeRepository"] = m
	}

	return out
}
