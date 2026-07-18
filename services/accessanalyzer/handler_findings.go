package accessanalyzer

import (
	"encoding/json"
	"net/http"
	"time"
)

const (
	opGetFinding                    = "GetFinding"
	opListFindings                  = "ListFindings"
	opUpdateFindings                = "UpdateFindings"
	opGetFindingV2                  = "GetFindingV2"
	opListFindingsV2                = "ListFindingsV2"
	opGetFindingsStatistics         = "GetFindingsStatistics"
	opGenerateFindingRecommendation = "GenerateFindingRecommendation"
	opGetFindingRecommendation      = "GetFindingRecommendation"

	pathFindingV2      = "findingv2"
	pathRecommendation = "recommendation"

	keyFinding           = "finding"
	keyFindings          = "findings"
	keyResource          = "resource"
	keyResourceOwnerAcct = "resourceOwnerAccount"
)

// dispatchFindingOps routes finding operations (v1, v2, statistics, and recommendations).
func (h *Handler) dispatchFindingOps(op, path, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetFinding:
		r, c, e := h.handleGetFinding(path, query)

		return r, c, true, e
	case opListFindings:
		r, c, e := h.handleListFindings(body)

		return r, c, true, e
	case opUpdateFindings:
		c, e := h.handleUpdateFindings(body)

		return nil, c, true, e
	case opGetFindingV2:
		r, c, e := h.handleGetFindingV2(path, query)

		return r, c, true, e
	case opListFindingsV2:
		r, c, e := h.handleListFindingsV2(body)

		return r, c, true, e
	case opGetFindingsStatistics:
		r, c, e := h.handleGetFindingsStatistics(body)

		return r, c, true, e
	case opGenerateFindingRecommendation:
		c, e := h.handleGenerateFindingRecommendation(path, body)

		return nil, c, true, e
	case opGetFindingRecommendation:
		r, c, e := h.handleGetFindingRecommendation(path, query)

		return r, c, true, e
	}

	return nil, 0, false, nil
}

// ---- operation handlers ----

// handleGetFinding serves GET /finding/{id}?analyzerArn=... . Unlike the
// analyzer/archive-rule/finding-family ops nested under /analyzer/{name}/...,
// the real GetFinding endpoint carries the owning analyzer as an ARN query
// parameter, not a path segment (see aws-sdk-go-v2's
// awsRestjson1_serializeOpHttpBindingsGetFindingInput).
func (h *Handler) handleGetFinding(path, query string) (any, int, error) {
	findingID := extractLastSegment(path, pathFinding)
	analyzerArn := queryParamValue(query, keyAnalyzerArn)
	analyzerName := analyzerNameFromArn(analyzerArn)

	f, err := h.Backend.GetFinding(analyzerName, findingID)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyFinding: findingToJSON(f, h.Backend.AccountID())}, http.StatusOK, nil
}

// handleListFindings serves POST /finding. The owning analyzer is carried as
// an ARN in the JSON body (analyzerArn), not a path segment.
func (h *Handler) handleListFindings(body []byte) (any, int, error) {
	var req struct {
		Filter      map[string]FilterCriterion `json:"filter"`
		AnalyzerArn string                     `json:"analyzerArn"`
		NextToken   string                     `json:"nextToken"`
		Status      string                     `json:"status"`
		MaxResults  int                        `json:"maxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if req.AnalyzerArn == "" {
		return nil, 0, ErrValidation
	}

	analyzerName := analyzerNameFromArn(req.AnalyzerArn)

	findings, nextToken, err := h.Backend.ListFindings(
		analyzerName, req.Filter, req.Status, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, 0, err
	}

	accountID := h.Backend.AccountID()
	list := make([]any, 0, len(findings))

	for _, f := range findings {
		list = append(list, findingToJSON(f, accountID))
	}

	resp := map[string]any{keyFindings: list}

	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}

// handleUpdateFindings serves PUT /finding. The owning analyzer is carried as
// an ARN in the JSON body (analyzerArn), not a path segment.
func (h *Handler) handleUpdateFindings(body []byte) (int, error) {
	var req struct {
		AnalyzerArn string   `json:"analyzerArn"`
		Status      string   `json:"status"`
		IDs         []string `json:"ids"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return 0, ErrValidation
	}

	if req.AnalyzerArn == "" {
		return 0, ErrValidation
	}

	analyzerName := analyzerNameFromArn(req.AnalyzerArn)

	if err := h.Backend.UpdateFindings(analyzerName, req.IDs, FindingStatus(req.Status)); err != nil {
		return 0, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetFindingV2(path, query string) (any, int, error) {
	findingID := extractLastSegment(path, pathFindingV2)
	analyzerArn := queryParamValue(query, keyAnalyzerArn)

	f, err := h.Backend.GetFindingV2(analyzerArn, findingID)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{
		"id":                 f.ID,
		keyAnalyzerArn:       f.AnalyzerArn,
		keyStatus:            string(f.Status),
		keyResourceType:      f.ResourceType,
		keyResource:          f.ResourceArn,
		keyResourceOwnerAcct: h.Backend.AccountID(),
		keyAnalyzedAt:        f.UpdatedAt.Format(time.RFC3339),
		"createdAt":          f.CreatedAt.Format(time.RFC3339),
		"updatedAt":          f.UpdatedAt.Format(time.RFC3339),
		"findingDetails":     []any{},
	}, http.StatusOK, nil
}

func (h *Handler) handleListFindingsV2(body []byte) (any, int, error) {
	var req struct {
		AnalyzerArn string `json:"analyzerArn"`
		NextToken   string `json:"nextToken"`
		Status      string `json:"status"`
		MaxResults  int    `json:"maxResults"`
	}

	_ = json.Unmarshal(body, &req)

	findings, nextToken, err := h.Backend.ListFindingsV2(
		req.AnalyzerArn, req.Status, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, 0, err
	}

	accountID := h.Backend.AccountID()
	list := make([]any, 0, len(findings))

	for _, f := range findings {
		list = append(list, map[string]any{
			"id":                 f.ID,
			keyAnalyzerArn:       f.AnalyzerArn,
			keyStatus:            string(f.Status),
			keyResourceType:      f.ResourceType,
			keyResource:          f.ResourceArn,
			keyResourceOwnerAcct: accountID,
			keyAnalyzedAt:        f.UpdatedAt.Format(time.RFC3339),
			keyUpdatedAt:         f.UpdatedAt.Format(time.RFC3339),
			keyCreatedAt:         f.CreatedAt.Format(time.RFC3339),
		})
	}

	resp := map[string]any{keyFindings: list}

	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleGetFindingsStatistics(body []byte) (any, int, error) {
	var req struct {
		AnalyzerArn string `json:"analyzerArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	counts, err := h.Backend.GetFindingsStatistics(req.AnalyzerArn)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{"findingsStatistics": []any{map[string]any{"externalAccessFindingsStatistics": map[string]any{
		"activeFindings": map[string]int{
			"total": counts[string(FindingStatusActive)], //nolint:goconst // existing issue.
		},
		"archivedFindings": map[string]int{"total": counts[string(FindingStatusArchived)]},
		"resolvedFindings": map[string]int{"total": counts[string(FindingStatusResolved)]},
	}}}}, http.StatusOK, nil
}

func (h *Handler) handleGenerateFindingRecommendation(path string, body []byte) (int, error) {
	findingID := extractLastSegment(path, pathRecommendation)

	var req struct {
		AnalyzerArn string `json:"analyzerArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return 0, ErrValidation
	}

	if err := h.Backend.GenerateFindingRecommendation(req.AnalyzerArn, findingID); err != nil {
		return 0, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetFindingRecommendation(path, query string) (any, int, error) {
	findingID := extractLastSegment(path, pathRecommendation)
	analyzerArn := queryParamValue(query, keyAnalyzerArn)

	rec, err := h.Backend.GetFindingRecommendation(analyzerArn, findingID)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{
		"recommendedSteps": []any{}, "recommendationType": rec.RecommendationType,
		keyStatus: rec.Status,
	}, http.StatusOK, nil
}

// ---- URL path parsing ----

// parseFindingPath parses GetFinding/ListFindings/UpdateFindings paths.
// Unlike the analyzer/archive-rule family, these live at the top-level
// /finding and /finding/{id} -- the owning analyzer is carried as an ARN in
// the query string (Get) or JSON body (List/Update), never as a path
// segment. See aws-sdk-go-v2's serializers.go for GetFinding ("/finding/{id}"
// GET), ListFindings ("/finding" POST), UpdateFindings ("/finding" PUT).
func parseFindingPath(method string, segments []string) (string, string) {
	switch len(segments) {
	case 1:
		switch method {
		case http.MethodPost:
			return opListFindings, ""
		case http.MethodPut:
			return opUpdateFindings, ""
		}
	case segmentDepthResource:
		if method == http.MethodGet {
			return opGetFinding, segments[1]
		}
	}

	return opUnknown, ""
}

func parseFindingV2Path(method string, segments []string) (string, string, bool) {
	switch len(segments) {
	case 1:
		if method == http.MethodPost {
			return opListFindingsV2, "", true
		}
	case segmentDepthResource:
		if method == http.MethodGet {
			return opGetFindingV2, segments[1], true
		}
	}

	return "", "", false
}

func parseRecommendationPath(method string, segments []string) (string, string, bool) {
	if len(segments) != segmentDepthResource {
		return "", "", false
	}

	switch method {
	case http.MethodPost:
		return opGenerateFindingRecommendation, segments[1], true
	case http.MethodGet:
		return opGetFindingRecommendation, segments[1], true
	}

	return "", "", false
}

// ---- JSON serialization ----

// findingToJSON builds the wire shape for the (v1) Finding type. The real API
// serializes the owning resource under "resource" (not "resourceArn" --
// that's only correct for AnalyzedResource) and requires
// "resourceOwnerAccount"/"analyzedAt", neither of which InMemoryBackend
// tracks per-finding; resourceOwnerAccount defaults to the backend's own
// account (emulated resources belong to the same test account) and
// analyzedAt mirrors updatedAt, matching the GetFindingV2/ListFindingsV2
// convention already used for the same data.
func findingToJSON(f *Finding, accountID string) map[string]any {
	m := map[string]any{
		"id":                 f.ID,
		keyAnalyzerArn:       f.AnalyzerArn,
		keyStatus:            string(f.Status),
		keyResourceType:      f.ResourceType,
		keyResource:          f.ResourceArn,
		keyResourceOwnerAcct: accountID,
		keyAnalyzedAt:        f.UpdatedAt.Format(time.RFC3339),
		keyUpdatedAt:         f.UpdatedAt.Format(time.RFC3339),
		keyCreatedAt:         f.CreatedAt.Format(time.RFC3339),
	}

	if len(f.Action) > 0 {
		m["action"] = f.Action
	}

	if len(f.Principal) > 0 {
		m["principal"] = f.Principal
	}

	if len(f.Condition) > 0 {
		m["condition"] = f.Condition
	}

	if f.IsPublic != nil {
		m["isPublic"] = *f.IsPublic
	}

	return m
}
