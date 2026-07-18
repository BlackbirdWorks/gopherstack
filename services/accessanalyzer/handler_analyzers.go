package accessanalyzer

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const (
	opCreateAnalyzer              = "CreateAnalyzer"
	opGetAnalyzer                 = "GetAnalyzer"
	opListAnalyzers               = "ListAnalyzers"
	opDeleteAnalyzer              = "DeleteAnalyzer"
	opUpdateAnalyzer              = "UpdateAnalyzer"
	opCreateServiceLinkedAnalyzer = "CreateServiceLinkedAnalyzer"
	opDeleteServiceLinkedAnalyzer = "DeleteServiceLinkedAnalyzer"

	pathServiceLinkedAnalyzer = "service-linked-analyzer"

	keyARN      = "arn"
	keyAnalyzer = "analyzer"
)

// dispatchAnalyzerOps routes analyzer (and service-linked analyzer) operations.
func (h *Handler) dispatchAnalyzerOps(op, path, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateAnalyzer:
		r, c, e := h.handleCreateAnalyzer(body)

		return r, c, true, e
	case opGetAnalyzer:
		r, c, e := h.handleGetAnalyzer(path)

		return r, c, true, e
	case opListAnalyzers:
		r, c, e := h.handleListAnalyzers(query)

		return r, c, true, e
	case opDeleteAnalyzer:
		c, e := h.handleDeleteAnalyzer(path)

		return nil, c, true, e
	case opUpdateAnalyzer:
		r, c, e := h.handleUpdateAnalyzer(path)

		return r, c, true, e
	case opCreateServiceLinkedAnalyzer:
		r, c, e := h.handleCreateServiceLinkedAnalyzer(body)

		return r, c, true, e
	case opDeleteServiceLinkedAnalyzer:
		c, e := h.handleDeleteServiceLinkedAnalyzer(path)

		return nil, c, true, e
	}

	return nil, 0, false, nil
}

// ---- operation handlers ----

func (h *Handler) handleCreateAnalyzer(body []byte) (any, int, error) {
	var req struct {
		Tags         map[string]string `json:"tags"`
		AnalyzerName string            `json:"analyzerName"`
		Type         string            `json:"type"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if req.AnalyzerName == "" {
		return nil, 0, ErrValidation
	}

	analyzerType := AnalyzerType(req.Type)
	if analyzerType == "" {
		analyzerType = AnalyzerTypeAccount
	}

	a, err := h.Backend.CreateAnalyzer(req.AnalyzerName, analyzerType, req.Tags)
	if err != nil {
		return nil, 0, err
	}

	return map[string]string{keyARN: a.Arn}, http.StatusOK, nil
}

func (h *Handler) handleGetAnalyzer(path string) (any, int, error) {
	name := extractAnalyzerName(path)

	a, err := h.Backend.GetAnalyzer(name)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyAnalyzer: analyzerToJSON(a)}, http.StatusOK, nil
}

func (h *Handler) handleListAnalyzers(query string) (any, int, error) {
	analyzerType := ""

	for part := range strings.SplitSeq(query, "&") {
		if after, ok := strings.CutPrefix(part, "type="); ok {
			analyzerType = after
		}
	}

	analyzers, err := h.Backend.ListAnalyzers(analyzerType)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(analyzers))

	for _, a := range analyzers {
		list = append(list, analyzerToJSON(a))
	}

	return map[string]any{"analyzers": list}, http.StatusOK, nil
}

func (h *Handler) handleDeleteAnalyzer(path string) (int, error) {
	name := extractAnalyzerName(path)

	if err := h.Backend.DeleteAnalyzer(name); err != nil {
		return 0, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUpdateAnalyzer(path string) (any, int, error) {
	name := extractAnalyzerName(path)

	a, err := h.Backend.UpdateAnalyzer(name)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{"configuration": map[string]any{}, "arn": a.Arn}, http.StatusOK, nil
}

func (h *Handler) handleCreateServiceLinkedAnalyzer(body []byte) (any, int, error) {
	var req struct {
		Type string `json:"type"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	analyzerType := AnalyzerType(req.Type)
	if analyzerType == "" {
		analyzerType = AnalyzerTypeAccountUnusedAccess
	}

	a, err := h.Backend.CreateServiceLinkedAnalyzer(analyzerType)
	if err != nil {
		return nil, 0, err
	}

	return map[string]string{keyARN: a.Arn}, http.StatusOK, nil
}

func (h *Handler) handleDeleteServiceLinkedAnalyzer(path string) (int, error) {
	name := extractLastSegment(path, pathServiceLinkedAnalyzer)

	if err := h.Backend.DeleteAnalyzer(name); err != nil {
		return 0, err
	}

	return http.StatusOK, nil
}

// ---- URL path parsing ----

func parseAnalyzerPath(method string, segments []string) (string, string) {
	switch len(segments) {
	case 1:
		return parseAnalyzerCollection(method)
	case segmentDepthResource:
		return parseAnalyzerResource(method, segments[1])
	case segmentDepthSubResource:
		return parseAnalyzerSubResource(method, segments)
	case segmentDepthLeafResource:
		return parseAnalyzerLeafResource(method, segments)
	}

	return opUnknown, ""
}

func parseAnalyzerCollection(method string) (string, string) {
	switch method {
	case http.MethodPut:
		return opCreateAnalyzer, ""
	case http.MethodGet:
		return opListAnalyzers, ""
	}

	return opUnknown, ""
}

func parseAnalyzerResource(method, name string) (string, string) {
	switch method {
	case http.MethodGet:
		return opGetAnalyzer, name
	case http.MethodDelete:
		return opDeleteAnalyzer, name
	case http.MethodPut:
		return opUpdateAnalyzer, name
	}

	return opUnknown, ""
}

func parseServiceLinkedAnalyzerPath(method string, segments []string) (string, string, bool) {
	switch len(segments) {
	case 1:
		if method == http.MethodPut {
			return opCreateServiceLinkedAnalyzer, "", true
		}
	case segmentDepthResource:
		if method == http.MethodDelete {
			return opDeleteServiceLinkedAnalyzer, segments[1], true
		}
	}

	return "", "", false
}

// ---- JSON serialization ----

func analyzerToJSON(a *Analyzer) map[string]any {
	m := map[string]any{
		keyARN:       a.Arn,
		"name":       a.Name,
		"type":       string(a.Type),
		keyStatus:    string(a.Status),
		keyCreatedAt: a.CreatedAt.Format(time.RFC3339),
	}

	if a.Tags != nil {
		m[keyTags] = a.Tags
	}

	if a.LastResourceAnalyzedAt != nil {
		m["lastResourceAnalyzedAt"] = a.LastResourceAnalyzedAt.Format(time.RFC3339)
	}

	return m
}
