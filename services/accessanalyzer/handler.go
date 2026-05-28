package accessanalyzer

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	accessAnalyzerService = "access-analyzer"
	matchPriority         = service.PriorityPathVersioned
	pathAnalyzer          = "analyzer"
	pathArchiveRule       = "archive-rule"
	pathFindings          = "findings"
	pathFinding           = "finding"
	pathTags              = "tags"
	pathResource          = "resource"
	pathScan              = "scan"
	pathEnable            = "enable"
	pathDisable           = "disable"
	pathAnalyzedResource  = "analyzedResource"

	opCreateAnalyzer      = "CreateAnalyzer"
	opGetAnalyzer         = "GetAnalyzer"
	opListAnalyzers       = "ListAnalyzers"
	opDeleteAnalyzer      = "DeleteAnalyzer"
	opCreateArchiveRule   = "CreateArchiveRule"
	opGetArchiveRule      = "GetArchiveRule"
	opListArchiveRules    = "ListArchiveRules"
	opDeleteArchiveRule   = "DeleteArchiveRule"
	opUpdateArchiveRule   = "UpdateArchiveRule"
	opGetFinding          = "GetFinding"
	opListFindings        = "ListFindings"
	opUpdateFindings      = "UpdateFindings"
	opStartResourceScan   = "StartResourceScan"
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"
	opUnknown             = "Unknown"

	keyCreatedAt  = "createdAt"
	keyUpdatedAt  = "updatedAt"
	keyAnalyzer   = "analyzer"
	keyArchiveRule = "archiveRule"
	keyFinding    = "finding"
	keyFindings   = "findings"
	keyARN        = "arn"
	keyTags       = "tags"
)

// Handler handles Access Analyzer HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "AccessAnalyzer" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateAnalyzer,
		opGetAnalyzer,
		opListAnalyzers,
		opDeleteAnalyzer,
		opCreateArchiveRule,
		opGetArchiveRule,
		opListArchiveRules,
		opDeleteArchiveRule,
		opUpdateArchiveRule,
		opGetFinding,
		opListFindings,
		opUpdateFindings,
		opStartResourceScan,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
	}
}

// RouteMatcher returns a function that matches Access Analyzer requests by path prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		return strings.HasPrefix(path, "/"+pathAnalyzer) ||
			strings.HasPrefix(path, "/"+pathTags+"/") ||
			path == "/"+pathResource+"/"+pathScan ||
			path == "/"+pathAnalyzedResource ||
			strings.HasPrefix(path, "/"+pathAnalyzedResource)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := parseRESTPath(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, resource := parseRESTPath(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return h.handleREST(c)
	}
}

// handleREST dispatches the incoming REST request.
func (h *Handler) handleREST(c *echo.Context) error {
	ctx := c.Request().Context()
	log := logger.Load(ctx)

	op, _ := parseRESTPath(c.Request().Method, c.Request().URL.Path)

	if op == opUnknown {
		return c.JSON(http.StatusNotFound, errorBody("ResourceNotFoundException", "not found"))
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errorBody("ValidationException", "failed to read body"))
	}

	result, statusCode, opErr := h.dispatch(ctx, op, c.Request().URL.Path, c.Request().URL.RawQuery, body)
	if opErr != nil {
		log.Error("access-analyzer operation error", "op", op, "err", opErr)

		return h.handleError(c, opErr)
	}

	if result == nil {
		return c.JSON(statusCode, struct{}{})
	}

	data, err := json.Marshal(result)
	if err != nil {
		return c.JSON(http.StatusInternalServerError,
			errorBody("InternalFailure", "failed to serialize response"))
	}

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(statusCode, data)
}

// dispatch routes to the appropriate operation handler.
func (h *Handler) dispatch(
	_ context.Context,
	op, path, query string,
	body []byte,
) (any, int, error) {
	if result, code, err, handled := h.dispatchAnalyzerOps(op, path, query, body); handled {
		return result, code, err
	}

	if result, code, err, handled := h.dispatchFindingOps(op, path, body); handled {
		return result, code, err
	}

	return h.dispatchTagOps(op, path, query, body)
}

func (h *Handler) dispatchAnalyzerOps(op, path, query string, body []byte) (any, int, error, bool) {
	switch op {
	case opCreateAnalyzer:
		r, c, e := h.handleCreateAnalyzer(body)
		return r, c, e, true
	case opGetAnalyzer:
		r, c, e := h.handleGetAnalyzer(path)
		return r, c, e, true
	case opListAnalyzers:
		r, c, e := h.handleListAnalyzers(query)
		return r, c, e, true
	case opDeleteAnalyzer:
		r, c, e := h.handleDeleteAnalyzer(path)
		return r, c, e, true
	case opCreateArchiveRule:
		r, c, e := h.handleCreateArchiveRule(path, body)
		return r, c, e, true
	case opGetArchiveRule:
		r, c, e := h.handleGetArchiveRule(path)
		return r, c, e, true
	case opListArchiveRules:
		r, c, e := h.handleListArchiveRules(path)
		return r, c, e, true
	case opDeleteArchiveRule:
		r, c, e := h.handleDeleteArchiveRule(path)
		return r, c, e, true
	case opUpdateArchiveRule:
		r, c, e := h.handleUpdateArchiveRule(path, body)
		return r, c, e, true
	}

	return nil, 0, nil, false
}

func (h *Handler) dispatchFindingOps(op, path string, body []byte) (any, int, error, bool) {
	switch op {
	case opGetFinding:
		r, c, e := h.handleGetFinding(path)
		return r, c, e, true
	case opListFindings:
		r, c, e := h.handleListFindings(path, body)
		return r, c, e, true
	case opUpdateFindings:
		r, c, e := h.handleUpdateFindings(path, body)
		return r, c, e, true
	case opStartResourceScan:
		r, c, e := h.handleStartResourceScan(body)
		return r, c, e, true
	}

	return nil, 0, nil, false
}

func (h *Handler) dispatchTagOps(op, path, query string, body []byte) (any, int, error) {
	switch op {
	case opTagResource:
		return h.handleTagResource(path, body)
	case opUntagResource:
		return h.handleUntagResource(path, query)
	case opListTagsForResource:
		return h.handleListTagsForResource(path)
	}

	return nil, http.StatusNotFound, nil
}

// ---- operation handlers ----

func (h *Handler) handleCreateAnalyzer(body []byte) (any, int, error) {
	var req struct {
		AnalyzerName string            `json:"analyzerName"`
		Type         string            `json:"type"`
		Tags         map[string]string `json:"tags"`
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
	name := extractPathParam(path, pathAnalyzer)

	a, err := h.Backend.GetAnalyzer(name)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyAnalyzer: analyzerToJSON(a)}, http.StatusOK, nil
}

func (h *Handler) handleListAnalyzers(query string) (any, int, error) {
	analyzerType := ""

	for _, part := range strings.Split(query, "&") {
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

func (h *Handler) handleDeleteAnalyzer(path string) (any, int, error) {
	name := extractPathParam(path, pathAnalyzer)

	if err := h.Backend.DeleteAnalyzer(name); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleCreateArchiveRule(path string, body []byte) (any, int, error) {
	analyzerName := extractPathParam(path, pathAnalyzer)

	var req struct {
		RuleName string                     `json:"ruleName"`
		Filter   map[string]FilterCriterion `json:"filter"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if _, err := h.Backend.CreateArchiveRule(analyzerName, req.RuleName, req.Filter); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleGetArchiveRule(path string) (any, int, error) {
	analyzerName, ruleName := extractTwoPathParams(path, pathAnalyzer, pathArchiveRule)

	rule, err := h.Backend.GetArchiveRule(analyzerName, ruleName)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyArchiveRule: archiveRuleToJSON(rule)}, http.StatusOK, nil
}

func (h *Handler) handleListArchiveRules(path string) (any, int, error) {
	analyzerName := extractPathParam(path, pathAnalyzer)

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

func (h *Handler) handleDeleteArchiveRule(path string) (any, int, error) {
	analyzerName, ruleName := extractTwoPathParams(path, pathAnalyzer, pathArchiveRule)

	if err := h.Backend.DeleteArchiveRule(analyzerName, ruleName); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleUpdateArchiveRule(path string, body []byte) (any, int, error) {
	analyzerName, ruleName := extractTwoPathParams(path, pathAnalyzer, pathArchiveRule)

	var req struct {
		Filter map[string]FilterCriterion `json:"filter"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if _, err := h.Backend.UpdateArchiveRule(analyzerName, ruleName, req.Filter); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleGetFinding(path string) (any, int, error) {
	analyzerName, findingID := extractTwoPathParams(path, pathAnalyzer, pathFinding)

	f, err := h.Backend.GetFinding(analyzerName, findingID)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyFinding: findingToJSON(f)}, http.StatusOK, nil
}

func (h *Handler) handleListFindings(path string, body []byte) (any, int, error) {
	analyzerName := extractPathParam(path, pathAnalyzer)

	var req struct {
		AnalyzerArn string                     `json:"analyzerArn"`
		Filter      map[string]FilterCriterion `json:"filter"`
		MaxResults  int                        `json:"maxResults"`
		NextToken   string                     `json:"nextToken"`
		Status      string                     `json:"status"`
	}

	_ = json.Unmarshal(body, &req)

	findings, nextToken, err := h.Backend.ListFindings(
		analyzerName, req.Filter, req.Status, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(findings))

	for _, f := range findings {
		list = append(list, findingToJSON(f))
	}

	resp := map[string]any{keyFindings: list}

	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleUpdateFindings(path string, body []byte) (any, int, error) {
	analyzerName := extractPathParam(path, pathAnalyzer)

	var req struct {
		AnalyzerArn string   `json:"analyzerArn"`
		Ids         []string `json:"ids"`
		Status      string   `json:"status"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if err := h.Backend.UpdateFindings(analyzerName, req.Ids, FindingStatus(req.Status)); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleStartResourceScan(body []byte) (any, int, error) {
	var req struct {
		AnalyzerArn string `json:"analyzerArn"`
		ResourceArn string `json:"resourceArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if err := h.Backend.StartResourceScan(req.AnalyzerArn, req.ResourceArn); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleTagResource(path string, body []byte) (any, int, error) {
	resourceARN := strings.TrimPrefix(path, "/"+pathTags+"/")

	var req struct {
		Tags map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if err := h.Backend.TagResource(resourceARN, req.Tags); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleUntagResource(path, query string) (any, int, error) {
	resourceARN := strings.TrimPrefix(path, "/"+pathTags+"/")

	var tagKeys []string

	for _, part := range strings.Split(query, "&") {
		if after, ok := strings.CutPrefix(part, "tagKeys="); ok {
			tagKeys = append(tagKeys, after)
		}
	}

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleListTagsForResource(path string) (any, int, error) {
	resourceARN := strings.TrimPrefix(path, "/"+pathTags+"/")

	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, 0, err
	}

	if tags == nil {
		tags = make(map[string]string)
	}

	return map[string]any{keyTags: tags}, http.StatusOK, nil
}

// handleError writes an error response.
func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrAnalyzerAlreadyExists), errors.Is(err, ErrArchiveRuleAlreadyExists):
		return c.JSON(http.StatusConflict, errorBody("ConflictException", err.Error()))
	case errors.Is(err, ErrAnalyzerNotFound), errors.Is(err, ErrArchiveRuleNotFound),
		errors.Is(err, ErrFindingNotFound):
		return c.JSON(http.StatusNotFound, errorBody("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errorBody("ValidationException", err.Error()))
	}

	return c.JSON(http.StatusInternalServerError, errorBody("InternalFailure", err.Error()))
}

// ---- URL path parsing ----

// parseRESTPath maps an HTTP method + path to an operation name and resource identifier.
func parseRESTPath(method, path string) (string, string) {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(segments) == 0 {
		return opUnknown, ""
	}

	switch segments[0] {
	case pathAnalyzer:
		return parseAnalyzerPath(method, segments)
	case pathTags:
		return parseTagsPath(method, segments)
	case pathResource:
		if len(segments) >= 2 && segments[1] == pathScan && method == http.MethodPost {
			return opStartResourceScan, ""
		}
	case pathAnalyzedResource:
		// skip — not in supported ops list but valid path
	}

	return opUnknown, ""
}

func parseAnalyzerPath(method string, segments []string) (string, string) {
	switch len(segments) {
	case 1:
		// /analyzer
		switch method {
		case http.MethodPut:
			return opCreateAnalyzer, ""
		case http.MethodGet:
			return opListAnalyzers, ""
		}
	case 2:
		// /analyzer/{name}
		name := segments[1]

		switch method {
		case http.MethodGet:
			return opGetAnalyzer, name
		case http.MethodDelete:
			return opDeleteAnalyzer, name
		}
	case 3:
		// /analyzer/{name}/archive-rule  or  /analyzer/{name}/findings  or  /analyzer/{name}/finding
		switch segments[2] {
		case pathArchiveRule:
			switch method {
			case http.MethodPut:
				return opCreateArchiveRule, segments[1]
			case http.MethodGet:
				return opListArchiveRules, segments[1]
			}
		case pathFindings:
			switch method {
			case http.MethodPost:
				return opListFindings, segments[1]
			case http.MethodPut:
				return opUpdateFindings, segments[1]
			}
		}
	case 4:
		// /analyzer/{name}/archive-rule/{rule}  or  /analyzer/{name}/finding/{id}
		switch segments[2] {
		case pathArchiveRule:
			switch method {
			case http.MethodGet:
				return opGetArchiveRule, segments[1]
			case http.MethodDelete:
				return opDeleteArchiveRule, segments[1]
			case http.MethodPut:
				return opUpdateArchiveRule, segments[1]
			}
		case pathFinding:
			if method == http.MethodGet {
				return opGetFinding, segments[1]
			}
		}
	}

	return opUnknown, ""
}

func parseTagsPath(method string, segments []string) (string, string) {
	if len(segments) < 2 || segments[1] == "" {
		return opUnknown, ""
	}

	resourceARN := strings.Join(segments[1:], "/")

	switch method {
	case http.MethodGet:
		return opListTagsForResource, resourceARN
	case http.MethodPost:
		return opTagResource, resourceARN
	case http.MethodDelete:
		return opUntagResource, resourceARN
	}

	return opUnknown, ""
}

// ---- path parameter extraction ----

// extractPathParam extracts the segment after the given key segment.
// For /analyzer/{name} with key "analyzer", returns name.
func extractPathParam(path, key string) string {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	for i, s := range segments {
		if s == key && i+1 < len(segments) {
			return segments[i+1]
		}
	}

	return ""
}

// extractTwoPathParams extracts path params after two key segments.
// For /analyzer/{name}/archive-rule/{ruleName} with keys "analyzer" and "archive-rule",
// returns (name, ruleName).
func extractTwoPathParams(path, key1, key2 string) (string, string) {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")
	var first, second string

	for i, s := range segments {
		if s == key1 && i+1 < len(segments) {
			first = segments[i+1]
		}

		if s == key2 && i+1 < len(segments) {
			second = segments[i+1]
		}
	}

	return first, second
}

// ---- JSON serialization ----

func analyzerToJSON(a *Analyzer) map[string]any {
	m := map[string]any{
		keyARN:       a.Arn,
		"name":       a.Name,
		"type":       string(a.Type),
		"status":     string(a.Status),
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

func archiveRuleToJSON(r *ArchiveRule) map[string]any {
	return map[string]any{
		"ruleName":   r.RuleName,
		"filter":     r.Filter,
		keyCreatedAt: r.CreatedAt.Format(time.RFC3339),
		keyUpdatedAt: r.UpdatedAt.Format(time.RFC3339),
	}
}

func findingToJSON(f *Finding) map[string]any {
	m := map[string]any{
		"id":           f.ID,
		"analyzerArn":  f.AnalyzerArn,
		"status":       string(f.Status),
		"resourceType": f.ResourceType,
		"resourceArn":  f.ResourceArn,
		keyUpdatedAt:   f.UpdatedAt.Format(time.RFC3339),
		keyCreatedAt:   f.CreatedAt.Format(time.RFC3339),
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

// errorBody constructs a JSON error payload.
func errorBody(code, message string) map[string]string {
	return map[string]string{
		"__type":  code,
		"message": message,
	}
}
