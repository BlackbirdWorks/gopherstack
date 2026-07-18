package accessanalyzer

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const (
	opStartPolicyGeneration  = "StartPolicyGeneration"
	opGetGeneratedPolicy     = "GetGeneratedPolicy"
	opCancelPolicyGeneration = "CancelPolicyGeneration"
	opListPolicyGenerations  = "ListPolicyGenerations"

	pathGeneration = "generation"
)

// dispatchGeneratedPolicyOps routes policy generation job operations.
func (h *Handler) dispatchGeneratedPolicyOps(op, path, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opStartPolicyGeneration:
		r, c, e := h.handleStartPolicyGeneration(body)

		return r, c, true, e
	case opGetGeneratedPolicy:
		r, c, e := h.handleGetGeneratedPolicy(path)

		return r, c, true, e
	case opCancelPolicyGeneration:
		c, e := h.handleCancelPolicyGeneration(path)

		return nil, c, true, e
	case opListPolicyGenerations:
		r, c, e := h.handleListPolicyGenerations(query)

		return r, c, true, e
	}

	return nil, 0, false, nil
}

// ---- operation handlers ----

func (h *Handler) handleStartPolicyGeneration(body []byte) (any, int, error) {
	var req struct {
		PolicyGenerationDetails struct {
			PrincipalArn string `json:"principalArn"`
		} `json:"policyGenerationDetails"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	pg, err := h.Backend.StartPolicyGeneration(req.PolicyGenerationDetails.PrincipalArn)
	if err != nil {
		return nil, 0, err
	}

	return map[string]string{"jobId": pg.JobID}, http.StatusOK, nil
}

func (h *Handler) handleGetGeneratedPolicy(path string) (any, int, error) {
	jobID := extractLastSegment(path, pathGeneration)

	pg, err := h.Backend.GetPolicyGeneration(jobID)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{
		"generatedPolicyResult": map[string]any{
			"generatedPolicies": []any{},
			"properties": map[string]any{
				"principalArn": pg.PrincipalArn,
			},
		},
		"jobDetails": policyGenerationToJSON(pg),
	}, http.StatusOK, nil
}

func (h *Handler) handleCancelPolicyGeneration(path string) (int, error) {
	jobID := extractLastSegment(path, pathGeneration)

	if err := h.Backend.CancelPolicyGeneration(jobID); err != nil {
		return 0, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListPolicyGenerations(query string) (any, int, error) {
	var principalArn string

	for part := range strings.SplitSeq(query, "&") {
		if v, ok := strings.CutPrefix(part, "principalArn="); ok {
			principalArn = v
		}
	}

	pgs, err := h.Backend.ListPolicyGenerations(principalArn)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(pgs))

	for _, pg := range pgs {
		list = append(list, policyGenerationToJSON(pg))
	}

	return map[string]any{"policyGenerations": list}, http.StatusOK, nil
}

// ---- URL path parsing ----

// parsePolicyGenerationPath parses /policy/generation and
// /policy/generation/{jobId} paths, reached via parsePolicyPath's
// pathGeneration case.
func parsePolicyGenerationPath(method string, segments []string) (string, string, bool) {
	switch len(segments) {
	case segmentDepthResource:
		switch method {
		case http.MethodGet:
			return opListPolicyGenerations, "", true
		case http.MethodPut:
			return opStartPolicyGeneration, "", true
		}
	case 3: //nolint:mnd // existing issue.
		switch method {
		case http.MethodPut:
			return opCancelPolicyGeneration, segments[2], true
		case http.MethodGet:
			return opGetGeneratedPolicy, segments[2], true
		}
	}

	return "", "", false
}

// ---- JSON serialization ----

func policyGenerationToJSON(pg *PolicyGeneration) map[string]any {
	m := map[string]any{
		"jobId":        pg.JobID,
		"principalArn": pg.PrincipalArn,
		keyStatus:      string(pg.Status),
		"startedOn":    pg.StartedOn.Format(time.RFC3339),
	}

	if pg.CompletedOn != nil {
		m["completedOn"] = pg.CompletedOn.Format(time.RFC3339)
	}

	return m
}
