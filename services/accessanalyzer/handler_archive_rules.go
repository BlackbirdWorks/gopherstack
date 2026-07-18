package accessanalyzer

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const (
	opCreateArchiveRule = "CreateArchiveRule"
	opGetArchiveRule    = "GetArchiveRule"
	opListArchiveRules  = "ListArchiveRules"
	opDeleteArchiveRule = "DeleteArchiveRule"
	opUpdateArchiveRule = "UpdateArchiveRule"
	opApplyArchiveRule  = "ApplyArchiveRule"

	// pathArchiveRule is the sub-resource segment name under
	// /analyzer/{name}/archive-rule/..., while pathArchiveRuleRoot is the
	// unrelated top-level "/archive-rule" route used by ApplyArchiveRule.
	// Both happen to be the same literal ("archive-rule") but identify
	// different things on the wire, so they stay separate constants.
	pathArchiveRule     = "archive-rule"
	pathArchiveRuleRoot = "archive-rule"

	// pathStatistics only appears nested under /analyzer/findings/statistics
	// (see parseAnalyzerSubResource below); it is handled here because it
	// shares a switch with the archive-rule sub-resource paths, even though
	// the operation it maps to (GetFindingsStatistics) belongs to the
	// findings family.
	pathStatistics = "statistics"

	keyArchiveRule = "archiveRule"
)

// dispatchArchiveRuleOps routes archive rule operations. None of the archive
// rule handlers need the raw query string (analyzer/rule identity comes from
// the path or the request body), so that parameter is unused here.
func (h *Handler) dispatchArchiveRuleOps(op, path, _ string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateArchiveRule:
		c, e := h.handleCreateArchiveRule(path, body)

		return nil, c, true, e
	case opGetArchiveRule:
		r, c, e := h.handleGetArchiveRule(path)

		return r, c, true, e
	case opListArchiveRules:
		r, c, e := h.handleListArchiveRules(path)

		return r, c, true, e
	case opDeleteArchiveRule:
		c, e := h.handleDeleteArchiveRule(path)

		return nil, c, true, e
	case opUpdateArchiveRule:
		c, e := h.handleUpdateArchiveRule(path, body)

		return nil, c, true, e
	case opApplyArchiveRule:
		c, e := h.handleApplyArchiveRule(body)

		return nil, c, true, e
	}

	return nil, 0, false, nil
}

// ---- operation handlers ----

func (h *Handler) handleCreateArchiveRule(path string, body []byte) (int, error) {
	analyzerName := extractAnalyzerName(path)

	var req struct {
		Filter   map[string]FilterCriterion `json:"filter"`
		RuleName string                     `json:"ruleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return 0, ErrValidation
	}

	if _, err := h.Backend.CreateArchiveRule(analyzerName, req.RuleName, req.Filter); err != nil {
		return 0, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetArchiveRule(path string) (any, int, error) {
	analyzerName, ruleName := extractAnalyzerAndSubName(path, pathArchiveRule)

	rule, err := h.Backend.GetArchiveRule(analyzerName, ruleName)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyArchiveRule: archiveRuleToJSON(rule)}, http.StatusOK, nil
}

func (h *Handler) handleListArchiveRules(path string) (any, int, error) {
	analyzerName := extractAnalyzerName(path)

	rules, err := h.Backend.ListArchiveRules(analyzerName)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(rules))

	for _, r := range rules {
		list = append(list, archiveRuleToJSON(r))
	}

	return map[string]any{"archiveRules": list}, http.StatusOK, nil
}

func (h *Handler) handleDeleteArchiveRule(path string) (int, error) {
	analyzerName, ruleName := extractAnalyzerAndSubName(path, pathArchiveRule)

	if err := h.Backend.DeleteArchiveRule(analyzerName, ruleName); err != nil {
		return 0, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUpdateArchiveRule(path string, body []byte) (int, error) {
	analyzerName, ruleName := extractAnalyzerAndSubName(path, pathArchiveRule)

	var req struct {
		Filter map[string]FilterCriterion `json:"filter"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return 0, ErrValidation
	}

	if _, err := h.Backend.UpdateArchiveRule(analyzerName, ruleName, req.Filter); err != nil {
		return 0, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleApplyArchiveRule(body []byte) (int, error) {
	var req struct {
		AnalyzerArn string `json:"analyzerArn"`
		RuleName    string `json:"ruleName"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return 0, ErrValidation
	}

	if err := h.Backend.ApplyArchiveRule(req.AnalyzerArn, req.RuleName); err != nil {
		return 0, err
	}

	return http.StatusOK, nil
}

// ---- URL path parsing ----

// extractAnalyzerAndSubName extracts analyzer name and a sub-resource name from a path.
// For /analyzer/{name}/archive-rule/{ruleName} with subKey "archive-rule",
// returns (name, ruleName).
func extractAnalyzerAndSubName(path, subKey string) (string, string) {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	var first, second string

	for i, s := range segments {
		if s == pathAnalyzer && i+1 < len(segments) {
			first = segments[i+1]
		}

		if s == subKey && i+1 < len(segments) {
			second = segments[i+1]
		}
	}

	return first, second
}

func parseAnalyzerSubResource(method string, segments []string) (string, string) {
	name := segments[1]

	switch segments[2] {
	case pathArchiveRule:
		switch method {
		case http.MethodPut:
			return opCreateArchiveRule, name
		case http.MethodGet:
			return opListArchiveRules, name
		}
	case pathStatistics:
		// /analyzer/findings/statistics (name == "findings" here)
		if method == http.MethodPost {
			return opGetFindingsStatistics, ""
		}
	}

	return opUnknown, ""
}

func parseAnalyzerLeafResource(method string, segments []string) (string, string) {
	name := segments[1]

	if segments[2] == pathArchiveRule {
		switch method {
		case http.MethodGet:
			return opGetArchiveRule, name
		case http.MethodDelete:
			return opDeleteArchiveRule, name
		case http.MethodPut:
			return opUpdateArchiveRule, name
		}
	}

	return opUnknown, ""
}

// ---- JSON serialization ----

func archiveRuleToJSON(r *ArchiveRule) map[string]any {
	return map[string]any{
		"ruleName":   r.RuleName,
		"filter":     r.Filter,
		keyCreatedAt: r.CreatedAt.Format(time.RFC3339),
		keyUpdatedAt: r.UpdatedAt.Format(time.RFC3339),
	}
}
