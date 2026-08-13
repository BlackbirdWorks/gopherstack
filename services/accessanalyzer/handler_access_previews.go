package accessanalyzer

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const (
	opCreateAccessPreview       = "CreateAccessPreview"
	opGetAccessPreview          = "GetAccessPreview"
	opListAccessPreviews        = "ListAccessPreviews"
	opListAccessPreviewFindings = "ListAccessPreviewFindings"

	pathAccessPreview = "access-preview"
)

// dispatchAccessPreviewOps routes access preview operations.
func (h *Handler) dispatchAccessPreviewOps(op, path, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateAccessPreview:
		r, c, e := h.handleCreateAccessPreview(body)

		return r, c, true, e
	case opGetAccessPreview:
		r, c, e := h.handleGetAccessPreview(path, query)

		return r, c, true, e
	case opListAccessPreviews:
		r, c, e := h.handleListAccessPreviews(query)

		return r, c, true, e
	case opListAccessPreviewFindings:
		r, c, e := h.handleListAccessPreviewFindings(path, body)

		return r, c, true, e
	}

	return nil, 0, false, nil
}

// ---- operation handlers ----

// handleCreateAccessPreview decodes Configurations, the access control
// configuration being previewed (api_op_CreateAccessPreview.go:39-43) --
// required, and per its doc comment must contain exactly one element.
// Each value is a 13-member union keyed by resource type (e.g. "s3Bucket",
// "iamRole"; awsRestjson1_serializeDocumentConfiguration in
// aws-sdk-go-v2/service/accessanalyzer@v1.51.4/serializers.go); gopherstack
// stores it opaquely as raw JSON rather than decoding the full union (see
// AccessPreview.Configurations godoc).
func (h *Handler) handleCreateAccessPreview(body []byte) (any, int, error) {
	var req struct {
		Configurations map[string]json.RawMessage `json:"configurations"`
		AnalyzerArn    string                     `json:"analyzerArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if req.AnalyzerArn == "" {
		return nil, 0, ErrValidation
	}

	ap, err := h.Backend.CreateAccessPreview(req.AnalyzerArn, req.Configurations)
	if err != nil {
		return nil, 0, err
	}

	return map[string]string{"id": ap.ID}, http.StatusOK, nil
}

func (h *Handler) handleGetAccessPreview(
	path, query string, //nolint:revive,unparam // existing issue.
) (any, int, error) {
	accessPreviewID := extractLastSegment(path, pathAccessPreview)

	ap, err := h.Backend.GetAccessPreview(accessPreviewID)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{"accessPreview": accessPreviewToJSON(ap, true)}, http.StatusOK, nil
}

func (h *Handler) handleListAccessPreviews(query string) (any, int, error) {
	var analyzerArn string

	for part := range strings.SplitSeq(query, "&") {
		if v, ok := strings.CutPrefix(part, "analyzerArn="); ok {
			analyzerArn = v
		}
	}

	previews, err := h.Backend.ListAccessPreviews(analyzerArn)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(previews))

	for _, ap := range previews {
		// ListAccessPreviews returns types.AccessPreviewSummary, which has no
		// Configurations member -- unlike GetAccessPreview's types.AccessPreview.
		list = append(list, accessPreviewToJSON(ap, false))
	}

	return map[string]any{"accessPreviews": list}, http.StatusOK, nil
}

// handleListAccessPreviewFindings serves POST /access-preview/{id}.
// ListAccessPreviewFindingsInput requires analyzerArn (validated below, same
// required-field contract as ListFindings/GetFindingsStatistics).
func (h *Handler) handleListAccessPreviewFindings(path string, body []byte) (any, int, error) {
	accessPreviewID := extractLastSegment(path, pathAccessPreview)

	var req struct {
		Filter      map[string]FilterCriterion `json:"filter"`
		AnalyzerArn string                     `json:"analyzerArn"`
		NextToken   string                     `json:"nextToken"`
		MaxResults  int                        `json:"maxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if req.AnalyzerArn == "" {
		return nil, 0, ErrValidation
	}

	findings, nextToken, err := h.Backend.ListAccessPreviewFindings(
		accessPreviewID, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, 0, err
	}

	accountID := h.Backend.AccountID()
	list := make([]any, 0, len(findings))

	for _, f := range findings {
		list = append(list, accessPreviewFindingToJSON(f, accountID))
	}

	resp := map[string]any{keyFindings: list}

	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}

// ---- URL path parsing ----

func parseAccessPreviewPath(method string, segments []string) (string, string, bool) {
	switch len(segments) {
	case 1:
		switch method {
		case http.MethodPut:
			return opCreateAccessPreview, "", true
		case http.MethodGet:
			return opListAccessPreviews, "", true
		}
	case segmentDepthResource:
		switch method {
		case http.MethodGet:
			return opGetAccessPreview, segments[1], true
		case http.MethodPost:
			return opListAccessPreviewFindings, segments[1], true
		}
	}

	return "", "", false
}

// ---- JSON serialization ----

// accessPreviewToJSON builds the wire shape for an access preview.
// includeConfigurations distinguishes GetAccessPreview's full
// types.AccessPreview (which has a Configurations member) from
// ListAccessPreviews' types.AccessPreviewSummary (which doesn't) -- same
// asymmetry convention as analyzerToJSON's includeConfiguration param.
func accessPreviewToJSON(ap *AccessPreview, includeConfigurations bool) map[string]any {
	m := map[string]any{
		"id":           ap.ID,
		keyAnalyzerArn: ap.AnalyzerArn,
		keyStatus:      string(ap.Status),
		keyCreatedAt:   ap.CreatedAt.Format(time.RFC3339),
	}

	if includeConfigurations {
		m["configurations"] = ap.Configurations
	}

	return m
}

// accessPreviewFindingToJSON builds the wire shape of types.AccessPreviewFinding
// for ListAccessPreviewFindings, which is NOT the same shape as v1
// Finding/FindingSummary despite gopherstack modeling both from the same
// underlying *Finding record: AccessPreviewFinding uses "id"/"changeType"
// instead of a bare finding id and has no analyzerArn member. Every finding
// InMemoryBackend can produce for a preview is reported as changeType "New"
// (a newly-introduced finding), since access previews here are not diffed
// against a prior finding set -- existingFindingId/existingFindingStatus are
// therefore never populated, matching an access preview with no prior
// findings to compare against.
func accessPreviewFindingToJSON(f *Finding, accountID string) map[string]any {
	m := map[string]any{
		"id":                 f.ID,
		"changeType":         "New",
		keyStatus:            string(f.Status),
		keyResourceType:      f.ResourceType,
		keyResource:          f.ResourceArn,
		keyResourceOwnerAcct: accountID,
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
