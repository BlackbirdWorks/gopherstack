package accessanalyzer

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const (
	opGetAnalyzedResource   = "GetAnalyzedResource"
	opListAnalyzedResources = "ListAnalyzedResources"
	opStartResourceScan     = "StartResourceScan"

	pathAnalyzedResourceHyph = "analyzed-resource"
)

// dispatchAnalyzedResourceOps routes analyzed-resource and resource-scan
// operations. None of the handlers need the raw path (identity is carried in
// the query string or JSON body), so that parameter is unused here.
func (h *Handler) dispatchAnalyzedResourceOps(op, _, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetAnalyzedResource:
		r, c, e := h.handleGetAnalyzedResource(query)

		return r, c, true, e
	case opListAnalyzedResources:
		r, c, e := h.handleListAnalyzedResources(body)

		return r, c, true, e
	case opStartResourceScan:
		c, e := h.handleStartResourceScan(body)

		return nil, c, true, e
	}

	return nil, 0, false, nil
}

// ---- operation handlers ----

func (h *Handler) handleGetAnalyzedResource(query string) (any, int, error) {
	var analyzerArn, resourceArn string

	for part := range strings.SplitSeq(query, "&") {
		if v, ok := strings.CutPrefix(part, "analyzerArn="); ok {
			analyzerArn = v
		}

		if v, ok := strings.CutPrefix(part, "resourceArn="); ok {
			resourceArn = v
		}
	}

	ar, err := h.Backend.GetAnalyzedResource(analyzerArn, resourceArn)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{"resource": analyzedResourceToJSON(ar)}, http.StatusOK, nil
}

func (h *Handler) handleListAnalyzedResources(body []byte) (any, int, error) {
	var req struct {
		AnalyzerArn  string `json:"analyzerArn"`
		ResourceType string `json:"resourceType"`
		NextToken    string `json:"nextToken"`
		MaxResults   int    `json:"maxResults"`
	}

	_ = json.Unmarshal(body, &req)

	resources, nextToken, err := h.Backend.ListAnalyzedResources(
		req.AnalyzerArn, req.ResourceType, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(resources))

	for _, ar := range resources {
		list = append(list, map[string]any{
			"resourceArn":   ar.ResourceArn,
			keyResourceType: ar.ResourceType,
		})
	}

	resp := map[string]any{"analyzedResources": list}

	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleStartResourceScan(body []byte) (int, error) {
	var req struct {
		AnalyzerArn string `json:"analyzerArn"`
		ResourceArn string `json:"resourceArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return 0, ErrValidation
	}

	if err := h.Backend.StartResourceScan(req.AnalyzerArn, req.ResourceArn); err != nil {
		return 0, err
	}

	return http.StatusOK, nil
}

// ---- JSON serialization ----

func analyzedResourceToJSON(ar *AnalyzedResource) map[string]any {
	return map[string]any{
		"resourceArn":   ar.ResourceArn,
		keyResourceType: ar.ResourceType,
		keyAnalyzerArn:  ar.AnalyzerArn,
		"isPublic":      ar.IsPublic,
		keyCreatedAt:    ar.CreatedAt.Format(time.RFC3339),
		keyUpdatedAt:    ar.UpdatedAt.Format(time.RFC3339),
		keyAnalyzedAt:   ar.AnalyzedAt.Format(time.RFC3339),
	}
}
