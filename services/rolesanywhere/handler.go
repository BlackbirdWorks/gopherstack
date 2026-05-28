package rolesanywhere

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
	rolesAnywhereService = "rolesanywhere"
	matchPriority        = service.PriorityPathVersioned

	pathTrustanchors  = "trustanchors"
	pathTrustanchor   = "trustanchor"
	pathProfiles      = "profiles"
	pathProfile       = "profile"
	pathEnable        = "enable"
	pathDisable       = "disable"
	pathTagResource   = "TagResource"
	pathUntagResource = "UntagResource"
	pathListTags      = "ListTagsForResource"

	opCreateTrustAnchor   = "CreateTrustAnchor"
	opGetTrustAnchor      = "GetTrustAnchor"
	opListTrustAnchors    = "ListTrustAnchors"
	opDeleteTrustAnchor   = "DeleteTrustAnchor"
	opUpdateTrustAnchor   = "UpdateTrustAnchor"
	opEnableTrustAnchor   = "EnableTrustAnchor"
	opDisableTrustAnchor  = "DisableTrustAnchor"
	opCreateProfile       = "CreateProfile"
	opGetProfile          = "GetProfile"
	opListProfiles        = "ListProfiles"
	opDeleteProfile       = "DeleteProfile"
	opUpdateProfile       = "UpdateProfile"
	opEnableProfile       = "EnableProfile"
	opDisableProfile      = "DisableProfile"
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
	opListTagsForResource = "ListTagsForResource"
	opUnknown             = "Unknown"

	keyTrustAnchor  = "trustAnchor"
	keyTrustAnchors = "trustAnchors"
	keyProfile      = "profile"
	keyProfiles     = "profiles"
	keyTags         = "tags"
)

// Handler handles Roles Anywhere HTTP requests.
type Handler struct {
	Backend StorageBackend
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	return &Handler{Backend: b}
}

// Name returns the service name.
func (h *Handler) Name() string { return "RolesAnywhere" }

// Reset resets the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		opCreateTrustAnchor,
		opGetTrustAnchor,
		opListTrustAnchors,
		opDeleteTrustAnchor,
		opUpdateTrustAnchor,
		opEnableTrustAnchor,
		opDisableTrustAnchor,
		opCreateProfile,
		opGetProfile,
		opListProfiles,
		opDeleteProfile,
		opUpdateProfile,
		opEnableProfile,
		opDisableProfile,
		opTagResource,
		opUntagResource,
		opListTagsForResource,
	}
}

// RouteMatcher returns a function that matches Roles Anywhere requests by path.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		return strings.HasPrefix(path, "/"+pathTrustanchors) ||
			strings.HasPrefix(path, "/"+pathTrustanchor+"/") ||
			strings.HasPrefix(path, "/"+pathProfiles) ||
			strings.HasPrefix(path, "/"+pathProfile+"/") ||
			path == "/"+pathTagResource ||
			path == "/"+pathUntagResource ||
			path == "/"+pathListTags
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

func (h *Handler) handleREST(c *echo.Context) error {
	ctx := c.Request().Context()
	log := logger.Load(ctx)

	op, _ := parseRESTPath(c.Request().Method, c.Request().URL.Path)

	if op == opUnknown {
		return c.JSON(http.StatusNotFound, errBody("ResourceNotFoundException", "not found"))
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errBody("ValidationException", "failed to read body"))
	}

	result, statusCode, opErr := h.dispatch(ctx, op, c.Request().URL.Path, c.Request().URL.RawQuery, body)
	if opErr != nil {
		log.Error("rolesanywhere operation error", "op", op, "err", opErr)

		return h.handleError(c, opErr)
	}

	if result == nil {
		return c.JSON(statusCode, struct{}{})
	}

	data, jsonErr := json.Marshal(result)
	if jsonErr != nil {
		return c.JSON(http.StatusInternalServerError, errBody("InternalFailure", "serialization failed"))
	}

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(statusCode, data)
}

func (h *Handler) dispatch(
	_ context.Context,
	op, path, query string,
	body []byte,
) (any, int, error) {
	if result, code, err, ok := h.dispatchTrustAnchorOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, err, ok := h.dispatchProfileOps(op, path, query, body); ok {
		return result, code, err
	}

	return h.dispatchTagOps(op, query, body)
}

func (h *Handler) dispatchTrustAnchorOps(op, path, query string, body []byte) (any, int, error, bool) {
	switch op {
	case opCreateTrustAnchor:
		r, c, e := h.handleCreateTrustAnchor(body)
		return r, c, e, true
	case opGetTrustAnchor:
		r, c, e := h.handleGetTrustAnchor(path)
		return r, c, e, true
	case opListTrustAnchors:
		r, c, e := h.handleListTrustAnchors(query)
		return r, c, e, true
	case opDeleteTrustAnchor:
		r, c, e := h.handleDeleteTrustAnchor(path)
		return r, c, e, true
	case opUpdateTrustAnchor:
		r, c, e := h.handleUpdateTrustAnchor(path, body)
		return r, c, e, true
	case opEnableTrustAnchor:
		r, c, e := h.handleEnableTrustAnchor(path)
		return r, c, e, true
	case opDisableTrustAnchor:
		r, c, e := h.handleDisableTrustAnchor(path)
		return r, c, e, true
	}

	return nil, 0, nil, false
}

func (h *Handler) dispatchProfileOps(op, path, query string, body []byte) (any, int, error, bool) {
	switch op {
	case opCreateProfile:
		r, c, e := h.handleCreateProfile(body)
		return r, c, e, true
	case opGetProfile:
		r, c, e := h.handleGetProfile(path)
		return r, c, e, true
	case opListProfiles:
		r, c, e := h.handleListProfiles(query)
		return r, c, e, true
	case opDeleteProfile:
		r, c, e := h.handleDeleteProfile(path)
		return r, c, e, true
	case opUpdateProfile:
		r, c, e := h.handleUpdateProfile(path, body)
		return r, c, e, true
	case opEnableProfile:
		r, c, e := h.handleEnableProfile(path)
		return r, c, e, true
	case opDisableProfile:
		r, c, e := h.handleDisableProfile(path)
		return r, c, e, true
	}

	return nil, 0, nil, false
}

func (h *Handler) dispatchTagOps(op, query string, body []byte) (any, int, error) {
	switch op {
	case opTagResource:
		return h.handleTagResource(body)
	case opUntagResource:
		return h.handleUntagResource(query)
	case opListTagsForResource:
		return h.handleListTagsForResource(query)
	}

	return nil, http.StatusNotFound, nil
}

// ---- Trust Anchor handlers ----

func (h *Handler) handleCreateTrustAnchor(body []byte) (any, int, error) {
	var req struct {
		Name   string            `json:"name"`
		Source TrustAnchorSource `json:"source"`
		Tags   []TagEntry        `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	ta, err := h.Backend.CreateTrustAnchor(req.Name, req.Source, req.Tags)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyTrustAnchor: trustAnchorToJSON(ta)}, http.StatusCreated, nil
}

func (h *Handler) handleGetTrustAnchor(path string) (any, int, error) {
	id := extractID(path, pathTrustanchor)

	ta, err := h.Backend.GetTrustAnchor(id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyTrustAnchor: trustAnchorToJSON(ta)}, http.StatusOK, nil
}

func (h *Handler) handleListTrustAnchors(query string) (any, int, error) {
	pageToken, maxResults := parsePageParams(query)

	all, next, err := h.Backend.ListTrustAnchors(pageToken, maxResults)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(all))

	for _, ta := range all {
		list = append(list, trustAnchorToJSON(ta))
	}

	resp := map[string]any{keyTrustAnchors: list}

	if next != "" {
		resp["nextToken"] = next
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleDeleteTrustAnchor(path string) (any, int, error) {
	id := extractID(path, pathTrustanchor)

	if err := h.Backend.DeleteTrustAnchor(id); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleUpdateTrustAnchor(path string, body []byte) (any, int, error) {
	id := extractID(path, pathTrustanchor)

	var req struct {
		Name   string             `json:"name"`
		Source *TrustAnchorSource `json:"source"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	ta, err := h.Backend.UpdateTrustAnchor(id, req.Name, req.Source)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyTrustAnchor: trustAnchorToJSON(ta)}, http.StatusOK, nil
}

func (h *Handler) handleEnableTrustAnchor(path string) (any, int, error) {
	id := extractID(path, pathTrustanchor)

	ta, err := h.Backend.EnableTrustAnchor(id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyTrustAnchor: trustAnchorToJSON(ta)}, http.StatusOK, nil
}

func (h *Handler) handleDisableTrustAnchor(path string) (any, int, error) {
	id := extractID(path, pathTrustanchor)

	ta, err := h.Backend.DisableTrustAnchor(id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyTrustAnchor: trustAnchorToJSON(ta)}, http.StatusOK, nil
}

// ---- Profile handlers ----

func (h *Handler) handleCreateProfile(body []byte) (any, int, error) {
	var req struct {
		Name                      string     `json:"name"`
		RoleArns                  []string   `json:"roleArns"`
		Tags                      []TagEntry `json:"tags"`
		DurationSeconds           *int32     `json:"durationSeconds"`
		ManagedPolicyArns         []string   `json:"managedPolicyArns"`
		SessionPolicy             string     `json:"sessionPolicy"`
		RequireInstanceProperties bool       `json:"requireInstanceProperties"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	p, err := h.Backend.CreateProfile(
		req.Name, req.RoleArns, req.Tags,
		req.DurationSeconds, req.ManagedPolicyArns,
		req.SessionPolicy, req.RequireInstanceProperties,
	)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyProfile: profileToJSON(p)}, http.StatusCreated, nil
}

func (h *Handler) handleGetProfile(path string) (any, int, error) {
	id := extractID(path, pathProfile)

	p, err := h.Backend.GetProfile(id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyProfile: profileToJSON(p)}, http.StatusOK, nil
}

func (h *Handler) handleListProfiles(query string) (any, int, error) {
	pageToken, maxResults := parsePageParams(query)

	all, next, err := h.Backend.ListProfiles(pageToken, maxResults)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(all))

	for _, p := range all {
		list = append(list, profileToJSON(p))
	}

	resp := map[string]any{keyProfiles: list}

	if next != "" {
		resp["nextToken"] = next
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleDeleteProfile(path string) (any, int, error) {
	id := extractID(path, pathProfile)

	if err := h.Backend.DeleteProfile(id); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleUpdateProfile(path string, body []byte) (any, int, error) {
	id := extractID(path, pathProfile)

	var req struct {
		Name                      string   `json:"name"`
		RoleArns                  []string `json:"roleArns"`
		DurationSeconds           *int32   `json:"durationSeconds"`
		ManagedPolicyArns         []string `json:"managedPolicyArns"`
		SessionPolicy             string   `json:"sessionPolicy"`
		RequireInstanceProperties *bool    `json:"requireInstanceProperties"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	p, err := h.Backend.UpdateProfile(
		id, req.Name, req.RoleArns,
		req.DurationSeconds, req.ManagedPolicyArns,
		req.SessionPolicy, req.RequireInstanceProperties,
	)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyProfile: profileToJSON(p)}, http.StatusOK, nil
}

func (h *Handler) handleEnableProfile(path string) (any, int, error) {
	id := extractID(path, pathProfile)

	p, err := h.Backend.EnableProfile(id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyProfile: profileToJSON(p)}, http.StatusOK, nil
}

func (h *Handler) handleDisableProfile(path string) (any, int, error) {
	id := extractID(path, pathProfile)

	p, err := h.Backend.DisableProfile(id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyProfile: profileToJSON(p)}, http.StatusOK, nil
}

// ---- Tag handlers ----

func (h *Handler) handleTagResource(body []byte) (any, int, error) {
	var req struct {
		ResourceArn string     `json:"resourceArn"`
		Tags        []TagEntry `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if err := h.Backend.TagResource(req.ResourceArn, req.Tags); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleUntagResource(query string) (any, int, error) {
	var resourceARN string

	var tagKeys []string

	for _, part := range strings.Split(query, "&") {
		if after, ok := strings.CutPrefix(part, "resourceArn="); ok {
			resourceARN = after
		}

		if after, ok := strings.CutPrefix(part, "tagKeys="); ok {
			tagKeys = append(tagKeys, after)
		}
	}

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleListTagsForResource(query string) (any, int, error) {
	var resourceARN string

	for _, part := range strings.Split(query, "&") {
		if after, ok := strings.CutPrefix(part, "resourceArn="); ok {
			resourceARN = after
		}
	}

	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return nil, 0, err
	}

	if tags == nil {
		tags = []TagEntry{}
	}

	return map[string]any{keyTags: tags}, http.StatusOK, nil
}

// handleError writes an error response.
func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrTrustAnchorAlreadyExists), errors.Is(err, ErrProfileAlreadyExists):
		return c.JSON(http.StatusConflict, errBody("ConflictException", err.Error()))
	case errors.Is(err, ErrTrustAnchorNotFound), errors.Is(err, ErrProfileNotFound):
		return c.JSON(http.StatusNotFound, errBody("ResourceNotFoundException", err.Error()))
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errBody("ValidationException", err.Error()))
	}

	return c.JSON(http.StatusInternalServerError, errBody("InternalFailure", err.Error()))
}

// ---- routing ----

func parseRESTPath(method, path string) (string, string) {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(segments) == 0 {
		return opUnknown, ""
	}

	switch segments[0] {
	case pathTrustanchors:
		// GET /trustanchors
		if method == http.MethodGet {
			return opListTrustAnchors, ""
		}

		// POST /trustanchors (CreateTrustAnchor)
		if method == http.MethodPost {
			return opCreateTrustAnchor, ""
		}
	case pathTrustanchor:
		return parseTrustAnchorPath(method, segments)
	case pathProfiles:
		if method == http.MethodGet {
			return opListProfiles, ""
		}

		if method == http.MethodPost {
			return opCreateProfile, ""
		}
	case pathProfile:
		return parseProfilePath(method, segments)
	case pathTagResource:
		if method == http.MethodPost {
			return opTagResource, ""
		}
	case pathUntagResource:
		if method == http.MethodPost {
			return opUntagResource, ""
		}
	case pathListTags:
		if method == http.MethodGet {
			return opListTagsForResource, ""
		}
	}

	return opUnknown, ""
}

func parseTrustAnchorPath(method string, segments []string) (string, string) {
	if len(segments) < 2 {
		return opUnknown, ""
	}

	id := segments[1]

	switch len(segments) {
	case 2:
		switch method {
		case http.MethodGet:
			return opGetTrustAnchor, id
		case http.MethodDelete:
			return opDeleteTrustAnchor, id
		case http.MethodPatch:
			return opUpdateTrustAnchor, id
		}
	case 3:
		switch segments[2] {
		case pathEnable:
			if method == http.MethodPost {
				return opEnableTrustAnchor, id
			}
		case pathDisable:
			if method == http.MethodPost {
				return opDisableTrustAnchor, id
			}
		}
	}

	return opUnknown, ""
}

func parseProfilePath(method string, segments []string) (string, string) {
	if len(segments) < 2 {
		return opUnknown, ""
	}

	id := segments[1]

	switch len(segments) {
	case 2:
		switch method {
		case http.MethodGet:
			return opGetProfile, id
		case http.MethodDelete:
			return opDeleteProfile, id
		case http.MethodPatch:
			return opUpdateProfile, id
		}
	case 3:
		switch segments[2] {
		case pathEnable:
			if method == http.MethodPost {
				return opEnableProfile, id
			}
		case pathDisable:
			if method == http.MethodPost {
				return opDisableProfile, id
			}
		}
	}

	return opUnknown, ""
}

// extractID extracts the ID segment from a path like /trustanchor/{id} or /trustanchor/{id}/enable.
func extractID(path, prefix string) string {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	for i, s := range segments {
		if s == prefix && i+1 < len(segments) {
			return segments[i+1]
		}
	}

	return ""
}

// parsePageParams extracts nextToken and maxResults from a query string.
func parsePageParams(query string) (string, int) {
	var nextToken string

	var maxResults int

	for _, part := range strings.Split(query, "&") {
		if after, ok := strings.CutPrefix(part, "nextToken="); ok {
			nextToken = after
		}

		if after, ok := strings.CutPrefix(part, "maxResults="); ok {
			var n int

			for _, c := range after {
				if c >= '0' && c <= '9' {
					n = n*10 + int(c-'0')
				}
			}

			maxResults = n
		}
	}

	return nextToken, maxResults
}

// ---- JSON serialization ----

func trustAnchorToJSON(ta *TrustAnchor) map[string]any {
	m := map[string]any{
		"trustAnchorId":  ta.TrustAnchorID,
		"trustAnchorArn": ta.TrustAnchorArn,
		"name":           ta.Name,
		"source":         ta.Source,
		"enabled":        ta.Enabled,
		"createdAt":      ta.CreatedAt.Format(time.RFC3339),
		"updatedAt":      ta.UpdatedAt.Format(time.RFC3339),
	}

	if len(ta.Tags) > 0 {
		m["tags"] = ta.Tags
	}

	return m
}

func profileToJSON(p *Profile) map[string]any {
	m := map[string]any{
		"profileId":  p.ProfileID,
		"profileArn": p.ProfileArn,
		"name":       p.Name,
		"roleArns":   p.RoleArns,
		"enabled":    p.Enabled,
		"createdAt":  p.CreatedAt.Format(time.RFC3339),
		"updatedAt":  p.UpdatedAt.Format(time.RFC3339),
	}

	if len(p.Tags) > 0 {
		m["tags"] = p.Tags
	}

	if p.DurationSeconds != nil {
		m["durationSeconds"] = *p.DurationSeconds
	}

	if len(p.ManagedPolicyArns) > 0 {
		m["managedPolicyArns"] = p.ManagedPolicyArns
	}

	if p.SessionPolicy != "" {
		m["sessionPolicy"] = p.SessionPolicy
	}

	if p.RequireInstanceProperties {
		m["requireInstanceProperties"] = true
	}

	return m
}

func errBody(code, message string) map[string]string {
	return map[string]string{
		"__type":  code,
		"message": message,
	}
}
