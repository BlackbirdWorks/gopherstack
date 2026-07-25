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
		r, c, e := h.handleUpdateAnalyzer(path, body)

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
		Tags          map[string]string   `json:"tags"`
		Configuration json.RawMessage     `json:"configuration"`
		AnalyzerName  string              `json:"analyzerName"`
		Type          string              `json:"type"`
		ArchiveRules  []inlineArchiveRule `json:"archiveRules"`
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

	a, err := h.Backend.CreateAnalyzer(req.AnalyzerName, analyzerType, req.Tags, req.Configuration)
	if err != nil {
		return nil, 0, err
	}

	if arErr := h.createInlineArchiveRules(a.Name, req.ArchiveRules); arErr != nil {
		return nil, 0, arErr
	}

	return map[string]string{keyARN: a.Arn}, http.StatusOK, nil
}

// inlineArchiveRule mirrors the wire shape of one element of
// CreateAnalyzer/CreateServiceLinkedAnalyzer's "archiveRules" array
// (types.InlineArchiveRule): the same {ruleName, filter} shape
// CreateArchiveRule itself takes.
type inlineArchiveRule struct {
	Filter   map[string]FilterCriterion `json:"filter"`
	RuleName string                     `json:"ruleName"`
}

// createInlineArchiveRules creates the archive rules an analyzer was
// created with (CreateAnalyzer/CreateServiceLinkedAnalyzer's optional
// "archiveRules" array), each of which also auto-archives any
// already-active findings, matching CreateArchiveRule's own behavior.
func (h *Handler) createInlineArchiveRules(analyzerName string, rules []inlineArchiveRule) error {
	for _, r := range rules {
		if _, err := h.Backend.CreateArchiveRule(analyzerName, r.RuleName, r.Filter); err != nil {
			return err
		}
	}

	return nil
}

func (h *Handler) handleGetAnalyzer(path string) (any, int, error) {
	name := extractAnalyzerName(path)

	a, err := h.Backend.GetAnalyzer(name)
	if err != nil {
		return nil, 0, err
	}

	// GetAnalyzer includes "configuration" in its response when the
	// analyzer has one; ListAnalyzers omits it (see analyzerToJSON's doc
	// comment).
	return map[string]any{keyAnalyzer: analyzerToJSON(a, true)}, http.StatusOK, nil
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
		list = append(list, analyzerToJSON(a, false))
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

// handleUpdateAnalyzer serves PUT /analyzer/{name}. UpdateAnalyzerOutput
// carries only "configuration" (the analyzer's AnalyzerConfiguration union
// after the update) -- unlike CreateAnalyzer/CreateServiceLinkedAnalyzer, it
// has no "arn" field.
func (h *Handler) handleUpdateAnalyzer(path string, body []byte) (any, int, error) {
	name := extractAnalyzerName(path)

	var req struct {
		Configuration json.RawMessage `json:"configuration"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	a, err := h.Backend.UpdateAnalyzer(name, req.Configuration)
	if err != nil {
		return nil, 0, err
	}

	resp := map[string]any{}
	if len(a.Configuration) > 0 {
		resp["configuration"] = a.Configuration
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleCreateServiceLinkedAnalyzer(body []byte) (any, int, error) {
	var req struct {
		Configuration json.RawMessage     `json:"configuration"`
		Type          string              `json:"type"`
		ArchiveRules  []inlineArchiveRule `json:"archiveRules"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	analyzerType := AnalyzerType(req.Type)
	if analyzerType == "" {
		analyzerType = AnalyzerTypeAccountUnusedAccess
	}

	a, err := h.Backend.CreateServiceLinkedAnalyzer(analyzerType, req.Configuration)
	if err != nil {
		return nil, 0, err
	}

	if arErr := h.createInlineArchiveRules(a.Name, req.ArchiveRules); arErr != nil {
		return nil, 0, arErr
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

// analyzerToJSON builds the AnalyzerSummary wire shape used by GetAnalyzer
// and ListAnalyzers. includeConfiguration controls whether "configuration"
// is included: per the real API, the GetAnalyzer action includes this
// property in its response if a configuration is specified, while the
// ListAnalyzers action omits it.
func analyzerToJSON(a *Analyzer, includeConfiguration bool) map[string]any {
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

	if includeConfiguration && len(a.Configuration) > 0 {
		m["configuration"] = a.Configuration
	}

	return m
}
