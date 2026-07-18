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

func (h *Handler) handleCreateAccessPreview(body []byte) (any, int, error) {
	var req struct {
		AnalyzerArn string `json:"analyzerArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if req.AnalyzerArn == "" {
		return nil, 0, ErrValidation
	}

	ap, err := h.Backend.CreateAccessPreview(req.AnalyzerArn)
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

	return map[string]any{"accessPreview": accessPreviewToJSON(ap)}, http.StatusOK, nil
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
		list = append(list, accessPreviewToJSON(ap))
	}

	return map[string]any{"accessPreviews": list}, http.StatusOK, nil
}

func (h *Handler) handleListAccessPreviewFindings(path string, body []byte) (any, int, error) {
	accessPreviewID := extractLastSegment(path, pathAccessPreview)

	var req struct {
		NextToken  string `json:"nextToken"`
		MaxResults int    `json:"maxResults"`
	}

	_ = json.Unmarshal(body, &req)

	findings, nextToken, err := h.Backend.ListAccessPreviewFindings(
		accessPreviewID, req.MaxResults, req.NextToken,
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

func accessPreviewToJSON(ap *AccessPreview) map[string]any {
	return map[string]any{
		"id":           ap.ID,
		keyAnalyzerArn: ap.AnalyzerArn,
		keyStatus:      string(ap.Status),
		keyCreatedAt:   ap.CreatedAt.Format(time.RFC3339),
	}
}
