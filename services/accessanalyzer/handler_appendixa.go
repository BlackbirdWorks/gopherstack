package accessanalyzer

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

const (
	opApplyArchiveRule              = "ApplyArchiveRule"
	opCancelPolicyGeneration        = "CancelPolicyGeneration"
	opCheckAccessNotGranted         = "CheckAccessNotGranted"
	opCheckNoNewAccess              = "CheckNoNewAccess"
	opCheckNoPublicAccess           = "CheckNoPublicAccess"
	opCreateAccessPreview           = "CreateAccessPreview"
	opCreateServiceLinkedAnalyzer   = "CreateServiceLinkedAnalyzer"
	opDeleteServiceLinkedAnalyzer   = "DeleteServiceLinkedAnalyzer"
	opGenerateFindingRecommendation = "GenerateFindingRecommendation"
	opGetAccessPreview              = "GetAccessPreview"
	opGetAnalyzedResource           = "GetAnalyzedResource"
	opGetFindingRecommendation      = "GetFindingRecommendation"
	opGetFindingsStatistics         = "GetFindingsStatistics"
	opGetFindingV2                  = "GetFindingV2"
	opGetGeneratedPolicy            = "GetGeneratedPolicy"
	opListAccessPreviewFindings     = "ListAccessPreviewFindings"
	opListAccessPreviews            = "ListAccessPreviews"
	opListAnalyzedResources         = "ListAnalyzedResources"
	opListFindingsV2                = "ListFindingsV2"
	opListPolicyGenerations         = "ListPolicyGenerations"
	opStartPolicyGeneration         = "StartPolicyGeneration"
	opUpdateAnalyzer                = "UpdateAnalyzer"
	opValidatePolicy                = "ValidatePolicy"

	pathAccessPreview         = "access-preview"
	pathPolicy                = "policy"
	pathServiceLinkedAnalyzer = "service-linked-analyzer"
	pathRecommendation        = "recommendation"
	pathFindingV2             = "findingv2"
	pathAnalyzedResourceHyph  = "analyzed-resource"
	pathStatistics            = "statistics"
	pathGeneration            = "generation"
	pathValidation            = "validation"
	pathCheckAccessNotGranted = "check-access-not-granted"
	pathCheckNoNewAccess      = "check-no-new-access"
	pathCheckNoPublicAccess   = "check-no-public-access"
	pathArchiveRuleRoot       = "archive-rule"
)

// GetSupportedOperations returns all supported operations (original + Appendix A).
// This replaces the method defined in handler.go.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateAnalyzer,
		opGetAnalyzer,
		opListAnalyzers,
		opDeleteAnalyzer,
		opUpdateAnalyzer,
		opCreateServiceLinkedAnalyzer,
		opDeleteServiceLinkedAnalyzer,
		opCreateArchiveRule,
		opGetArchiveRule,
		opListArchiveRules,
		opDeleteArchiveRule,
		opUpdateArchiveRule,
		opApplyArchiveRule,
		opGetFinding,
		opListFindings,
		opUpdateFindings,
		opGetFindingV2,
		opListFindingsV2,
		opGetFindingsStatistics,
		opGenerateFindingRecommendation,
		opGetFindingRecommendation,
		opGetAnalyzedResource,
		opListAnalyzedResources,
		opStartResourceScan,
		opStartPolicyGeneration,
		opGetGeneratedPolicy,
		opCancelPolicyGeneration,
		opListPolicyGenerations,
		opCreateAccessPreview,
		opGetAccessPreview,
		opListAccessPreviews,
		opListAccessPreviewFindings,
		opCheckAccessNotGranted,
		opCheckNoNewAccess,
		opCheckNoPublicAccess,
		opValidatePolicy,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
	}
}

// dispatchAppendixA routes Appendix A operations.
func (h *Handler) dispatchAppendixA( //nolint:cyclop,funlen // existing issue.
	op, path, query string,
	body []byte,
) (any, int, bool, error) {
	switch op {
	case opApplyArchiveRule:
		c, e := h.handleApplyArchiveRule(body)

		return nil, c, true, e

	case opCancelPolicyGeneration:
		c, e := h.handleCancelPolicyGeneration(path)

		return nil, c, true, e

	case opCheckAccessNotGranted:
		r, c := h.handleCheckAccessNotGranted(body)
		return r, c, true, nil

	case opCheckNoNewAccess:
		r, c := h.handleCheckNoNewAccess(body)
		return r, c, true, nil

	case opCheckNoPublicAccess:
		r, c := h.handleCheckNoPublicAccess(body)
		return r, c, true, nil

	case opCreateAccessPreview:
		r, c, e := h.handleCreateAccessPreview(body)

		return r, c, true, e

	case opCreateServiceLinkedAnalyzer:
		r, c, e := h.handleCreateServiceLinkedAnalyzer(body)

		return r, c, true, e

	case opDeleteServiceLinkedAnalyzer:
		c, e := h.handleDeleteServiceLinkedAnalyzer(path)

		return nil, c, true, e

	case opGenerateFindingRecommendation:
		c, e := h.handleGenerateFindingRecommendation(path, body)

		return nil, c, true, e

	case opGetAccessPreview:
		r, c, e := h.handleGetAccessPreview(path, query)

		return r, c, true, e

	case opGetAnalyzedResource:
		r, c, e := h.handleGetAnalyzedResource(query)

		return r, c, true, e

	case opGetFindingRecommendation:
		r, c, e := h.handleGetFindingRecommendation(path, query)

		return r, c, true, e

	case opGetFindingsStatistics:
		r, c, e := h.handleGetFindingsStatistics(body)

		return r, c, true, e

	case opGetFindingV2:
		r, c, e := h.handleGetFindingV2(path, query)

		return r, c, true, e

	case opGetGeneratedPolicy:
		r, c, e := h.handleGetGeneratedPolicy(path)

		return r, c, true, e

	case opListAccessPreviewFindings:
		r, c, e := h.handleListAccessPreviewFindings(path, body)

		return r, c, true, e

	case opListAccessPreviews:
		r, c, e := h.handleListAccessPreviews(query)

		return r, c, true, e

	case opListAnalyzedResources:
		r, c, e := h.handleListAnalyzedResources(body)

		return r, c, true, e

	case opListFindingsV2:
		r, c, e := h.handleListFindingsV2(body)

		return r, c, true, e

	case opListPolicyGenerations:
		r, c, e := h.handleListPolicyGenerations(query)

		return r, c, true, e

	case opStartPolicyGeneration:
		r, c, e := h.handleStartPolicyGeneration(body)

		return r, c, true, e

	case opUpdateAnalyzer:
		r, c, e := h.handleUpdateAnalyzer(path)

		return r, c, true, e

	case opValidatePolicy:
		r, c := h.handleValidatePolicy(body)
		return r, c, true, nil

	}

	return nil, 0, false, nil
}

// ---- operation handlers ----

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

func (h *Handler) handleCancelPolicyGeneration(path string) (int, error) {
	jobID := extractLastSegment(path, pathGeneration)

	if err := h.Backend.CancelPolicyGeneration(jobID); err != nil {
		return 0, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleCheckAccessNotGranted(_ []byte) (any, int, error) { //nolint:unparam // existing issue.
	return map[string]any{
		"result":  "PASS",                                                      //nolint:goconst // existing issue.
		"message": "The specified policy does not grant the specified access.", //nolint:goconst // existing issue.
	}, http.StatusOK, nil
}

func (h *Handler) handleCheckNoNewAccess(_ []byte) (any, int, error) { //nolint:unparam // existing issue.
	return map[string]any{
		"result":  "PASS",
		"message": "The updated policy does not grant new access.",
	}, http.StatusOK
}

func (h *Handler) handleCheckNoPublicAccess(_ []byte) (any, int, error) { //nolint:unparam // existing issue.
	return map[string]any{
		"result":  "PASS",
		"message": "The policy does not grant public access.",
		"reasons": []any{},
	}, http.StatusOK
}

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

func (h *Handler) handleGetFindingRecommendation(path, query string) (any, int, error) {
	findingID := extractLastSegment(path, pathRecommendation)

	var analyzerArn string

	for part := range strings.SplitSeq(query, "&") {
		if v, ok := strings.CutPrefix(part, "analyzerArn="); ok {
			analyzerArn = v
		}
	}

	rec, err := h.Backend.GetFindingRecommendation(analyzerArn, findingID)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{
		"recommendedSteps": []any{}, "recommendationType": rec.RecommendationType,
		"status": rec.Status, //nolint:goconst // existing issue.
	}, http.StatusOK, nil
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

func (h *Handler) handleGetFindingV2(path, query string) (any, int, error) {
	findingID := extractLastSegment(path, pathFindingV2)

	var analyzerArn string

	for part := range strings.SplitSeq(query, "&") {
		if v, ok := strings.CutPrefix(part, "analyzerArn="); ok {
			analyzerArn = v
		}
	}

	f, err := h.Backend.GetFindingV2(analyzerArn, findingID)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{
		"id":             f.ID,
		"analyzerArn":    f.AnalyzerArn, //nolint:goconst // existing issue.
		"status":         string(f.Status),
		"resourceType":   f.ResourceType, //nolint:goconst // existing issue.
		"resourceArn":    f.ResourceArn,  //nolint:goconst // existing issue.
		"analyzedAt":     f.UpdatedAt.Format(time.RFC3339),
		"createdAt":      f.CreatedAt.Format(time.RFC3339),
		"updatedAt":      f.UpdatedAt.Format(time.RFC3339),
		"findingDetails": []any{},
	}, http.StatusOK, nil
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

	list := make([]any, 0, len(findings))

	for _, f := range findings {
		list = append(list, findingToJSON(f))
	}

	resp := map[string]any{"findings": list} //nolint:goconst // existing issue.

	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
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
			"resourceArn":  ar.ResourceArn,
			"resourceType": ar.ResourceType,
		})
	}

	resp := map[string]any{"analyzedResources": list}

	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
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

	list := make([]any, 0, len(findings))

	for _, f := range findings {
		list = append(list, map[string]any{
			"id":           f.ID,
			"analyzerArn":  f.AnalyzerArn,
			"status":       string(f.Status),
			"resourceType": f.ResourceType,
			"resourceArn":  f.ResourceArn,
			keyUpdatedAt:   f.UpdatedAt.Format(time.RFC3339),
			keyCreatedAt:   f.CreatedAt.Format(time.RFC3339),
		})
	}

	resp := map[string]any{"findings": list}

	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
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

func (h *Handler) handleUpdateAnalyzer(path string) (any, int, error) {
	name := extractAnalyzerName(path)

	a, err := h.Backend.UpdateAnalyzer(name)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{"configuration": map[string]any{}, "arn": a.Arn}, http.StatusOK, nil
}

func (h *Handler) handleValidatePolicy(_ []byte) (any, int, error) { //nolint:unparam // existing issue.
	return map[string]any{
		"findings":  []any{},
		"nextToken": "",
	}, http.StatusOK
}

// ---- JSON serialization helpers ----

func accessPreviewToJSON(ap *AccessPreview) map[string]any {
	return map[string]any{
		"id":          ap.ID,
		"analyzerArn": ap.AnalyzerArn,
		"status":      string(ap.Status),
		keyCreatedAt:  ap.CreatedAt.Format(time.RFC3339),
	}
}

func analyzedResourceToJSON(ar *AnalyzedResource) map[string]any {
	return map[string]any{
		"resourceArn":  ar.ResourceArn,
		"resourceType": ar.ResourceType,
		"analyzerArn":  ar.AnalyzerArn,
		"isPublic":     ar.IsPublic,
		keyCreatedAt:   ar.CreatedAt.Format(time.RFC3339),
		keyUpdatedAt:   ar.UpdatedAt.Format(time.RFC3339),
		"analyzedAt":   ar.AnalyzedAt.Format(time.RFC3339),
	}
}

func policyGenerationToJSON(pg *PolicyGeneration) map[string]any {
	m := map[string]any{
		"jobId":        pg.JobID,
		"principalArn": pg.PrincipalArn,
		"status":       string(pg.Status),
		"startedOn":    pg.StartedOn.Format(time.RFC3339),
	}

	if pg.CompletedOn != nil {
		m["completedOn"] = pg.CompletedOn.Format(time.RFC3339)
	}

	return m
}

// ---- URL path helpers ----

// extractLastSegment extracts the last path segment after the given prefix segment.
// For /access-preview/{id}, extractLastSegment(path, "access-preview") returns the id.
func extractLastSegment(path, prefix string) string {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	for i, s := range segments {
		if s == prefix && i+1 < len(segments) {
			return segments[i+1]
		}
	}

	return ""
}

// ---- extended route parsing ----

// parseRESTPathAppendixA parses Appendix A REST paths.
// Returns (op, resource, ok) — ok=true means the path was handled.
func parseRESTPathAppendixA(method, path string) (string, string, bool) {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(segments) == 0 {
		return "", "", false
	}

	switch segments[0] {
	case pathArchiveRuleRoot:
		if len(segments) == 1 && method == http.MethodPut {
			return opApplyArchiveRule, "", true
		}

	case pathAccessPreview:
		return parseAccessPreviewPath(method, segments)

	case pathServiceLinkedAnalyzer:
		return parseServiceLinkedAnalyzerPath(method, segments)

	case pathRecommendation:
		return parseRecommendationPath(method, segments)

	case pathAnalyzedResourceHyph:
		switch method {
		case http.MethodGet:
			return opGetAnalyzedResource, "", true
		case http.MethodPost:
			return opListAnalyzedResources, "", true
		}

	case pathFindingV2:
		return parseFindingV2Path(method, segments)

	case pathPolicy:
		return parsePolicyPath(method, segments)
	}

	return "", "", false
}

func parseAccessPreviewPath(method string, segments []string) (string, string, bool) {
	switch len(segments) {
	case 1:
		switch method {
		case http.MethodPut:
			return opCreateAccessPreview, "", true
		case http.MethodGet:
			return opListAccessPreviews, "", true
		}
	case 2: //nolint:mnd // existing issue.
		switch method {
		case http.MethodGet:
			return opGetAccessPreview, segments[1], true
		case http.MethodPost:
			return opListAccessPreviewFindings, segments[1], true
		}
	}

	return "", "", false
}

func parseServiceLinkedAnalyzerPath(method string, segments []string) (string, string, bool) {
	switch len(segments) {
	case 1:
		if method == http.MethodPut {
			return opCreateServiceLinkedAnalyzer, "", true
		}
	case 2: //nolint:mnd // existing issue.
		if method == http.MethodDelete {
			return opDeleteServiceLinkedAnalyzer, segments[1], true
		}
	}

	return "", "", false
}

func parseRecommendationPath(method string, segments []string) (string, string, bool) {
	if len(segments) != 2 { //nolint:mnd // existing issue.
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

func parseFindingV2Path(method string, segments []string) (string, string, bool) {
	switch len(segments) {
	case 1:
		if method == http.MethodPost {
			return opListFindingsV2, "", true
		}
	case 2: //nolint:mnd // existing issue.
		if method == http.MethodGet {
			return opGetFindingV2, segments[1], true
		}
	}

	return "", "", false
}

func parsePolicyPath(method string, segments []string) (string, string, bool) {
	if len(segments) < 2 { //nolint:mnd // existing issue.
		return "", "", false
	}

	switch segments[1] {
	case pathCheckAccessNotGranted:
		if method == http.MethodPost {
			return opCheckAccessNotGranted, "", true
		}
	case pathCheckNoNewAccess:
		if method == http.MethodPost {
			return opCheckNoNewAccess, "", true
		}
	case pathCheckNoPublicAccess:
		if method == http.MethodPost {
			return opCheckNoPublicAccess, "", true
		}
	case pathValidation:
		if method == http.MethodPost {
			return opValidatePolicy, "", true
		}
	case pathGeneration:
		return parsePolicyGenerationPath(method, segments)
	}

	return "", "", false
}

func parsePolicyGenerationPath(method string, segments []string) (string, string, bool) {
	switch len(segments) {
	case 2: //nolint:mnd // existing issue.
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
