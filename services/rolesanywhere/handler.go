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

	pathTrustanchors       = "trustanchors"
	pathTrustanchor        = "trustanchor"
	pathProfiles           = "profiles"
	pathProfile            = "profile"
	pathCrls               = "crls"
	pathCrl                = "crl"
	pathSubjects           = "subjects"
	pathSubject            = "subject"
	pathMappings           = "mappings"
	pathPutNotifications   = "put-notifications-settings"
	pathResetNotifications = "reset-notifications-settings"
	pathEnable             = "enable"
	pathDisable            = "disable"
	pathTagResource        = "TagResource"
	pathUntagResource      = "UntagResource"
	pathListTags           = "ListTagsForResource"

	opCreateTrustAnchor         = "CreateTrustAnchor"
	opGetTrustAnchor            = "GetTrustAnchor"
	opListTrustAnchors          = "ListTrustAnchors"
	opDeleteTrustAnchor         = "DeleteTrustAnchor"
	opUpdateTrustAnchor         = "UpdateTrustAnchor"
	opEnableTrustAnchor         = "EnableTrustAnchor"
	opDisableTrustAnchor        = "DisableTrustAnchor"
	opCreateProfile             = "CreateProfile"
	opGetProfile                = "GetProfile"
	opListProfiles              = "ListProfiles"
	opDeleteProfile             = "DeleteProfile"
	opUpdateProfile             = "UpdateProfile"
	opEnableProfile             = "EnableProfile"
	opDisableProfile            = "DisableProfile"
	opImportCrl                 = "ImportCrl"
	opGetCrl                    = "GetCrl"
	opListCrls                  = "ListCrls"
	opUpdateCrl                 = "UpdateCrl"
	opDeleteCrl                 = "DeleteCrl"
	opEnableCrl                 = "EnableCrl"
	opDisableCrl                = "DisableCrl"
	opGetSubject                = "GetSubject"
	opListSubjects              = "ListSubjects"
	opPutAttributeMapping       = "PutAttributeMapping"
	opDeleteAttributeMapping    = "DeleteAttributeMapping"
	opPutNotificationSettings   = "PutNotificationSettings"
	opResetNotificationSettings = "ResetNotificationSettings"
	opTagResource               = "TagResource"
	opUntagResource             = "UntagResource"
	opListTagsForResource       = "ListTagsForResource"
	opUnknown                   = "Unknown"

	keyTrustAnchor  = "trustAnchor"
	keyTrustAnchors = "trustAnchors"
	keyProfile      = "profile"
	keyProfiles     = "profiles"
	keyCrl          = "crl"
	keyCrls         = "crls"
	keySubject      = "subject"
	keySubjects     = "subjects"
	keyTags         = "tags"

	// URL path segment depth constants.
	segmentDepthResource    = 2 // /prefix/{id}
	segmentDepthSubResource = 3 // /prefix/{id}/action
	segmentDepthMapping     = 3 // /profiles/{id}/mappings

	// minSegmentsForResource is the minimum number of path segments for a resource op.
	minSegmentsForResource = 2

	// base10 is the radix for integer parsing in query string parameters.
	base10 = 10
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
		opImportCrl,
		opGetCrl,
		opListCrls,
		opUpdateCrl,
		opDeleteCrl,
		opEnableCrl,
		opDisableCrl,
		opGetSubject,
		opListSubjects,
		opPutAttributeMapping,
		opDeleteAttributeMapping,
		opPutNotificationSettings,
		opResetNotificationSettings,
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
			strings.HasPrefix(path, "/"+pathCrls) ||
			strings.HasPrefix(path, "/"+pathCrl+"/") ||
			strings.HasPrefix(path, "/"+pathSubjects) ||
			strings.HasPrefix(path, "/"+pathSubject+"/") ||
			path == "/"+pathPutNotifications ||
			path == "/"+pathResetNotifications ||
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
	if result, code, ok, err := h.dispatchTrustAnchorOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchProfileOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchCrlOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchSubjectOps(op, path, query); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchMappingOps(op, path, query, body); ok {
		return result, code, err
	}

	if result, code, ok, err := h.dispatchNotificationOps(op, body); ok {
		return result, code, err
	}

	return h.dispatchTagOps(op, query, body)
}

func (h *Handler) dispatchTrustAnchorOps(op, path, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateTrustAnchor:
		r, c, e := h.handleCreateTrustAnchor(body)

		return r, c, true, e
	case opGetTrustAnchor:
		r, c, e := h.handleGetTrustAnchor(path)

		return r, c, true, e
	case opListTrustAnchors:
		r, c, e := h.handleListTrustAnchors(query)

		return r, c, true, e
	case opDeleteTrustAnchor:
		c, e := h.handleDeleteTrustAnchor(path)

		return nil, c, true, e
	case opUpdateTrustAnchor:
		r, c, e := h.handleUpdateTrustAnchor(path, body)

		return r, c, true, e
	case opEnableTrustAnchor:
		r, c, e := h.handleEnableTrustAnchor(path)

		return r, c, true, e
	case opDisableTrustAnchor:
		r, c, e := h.handleDisableTrustAnchor(path)

		return r, c, true, e
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchProfileOps(op, path, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateProfile:
		r, c, e := h.handleCreateProfile(body)

		return r, c, true, e
	case opGetProfile:
		r, c, e := h.handleGetProfile(path)

		return r, c, true, e
	case opListProfiles:
		r, c, e := h.handleListProfiles(query)

		return r, c, true, e
	case opDeleteProfile:
		c, e := h.handleDeleteProfile(path)

		return nil, c, true, e
	case opUpdateProfile:
		r, c, e := h.handleUpdateProfile(path, body)

		return r, c, true, e
	case opEnableProfile:
		r, c, e := h.handleEnableProfile(path)

		return r, c, true, e
	case opDisableProfile:
		r, c, e := h.handleDisableProfile(path)

		return r, c, true, e
	}

	return nil, 0, false, nil
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

func (h *Handler) handleDeleteTrustAnchor(path string) (int, error) {
	id := extractID(path, pathTrustanchor)

	if err := h.Backend.DeleteTrustAnchor(id); err != nil {
		return 0, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUpdateTrustAnchor(path string, body []byte) (any, int, error) {
	id := extractID(path, pathTrustanchor)

	var req struct {
		Source *TrustAnchorSource `json:"source"`
		Name   string             `json:"name"`
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
		DurationSeconds           *int32     `json:"durationSeconds"`
		Name                      string     `json:"name"`
		SessionPolicy             string     `json:"sessionPolicy"`
		RoleArns                  []string   `json:"roleArns"`
		Tags                      []TagEntry `json:"tags"`
		ManagedPolicyArns         []string   `json:"managedPolicyArns"`
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

func (h *Handler) handleDeleteProfile(path string) (int, error) {
	id := extractID(path, pathProfile)

	if err := h.Backend.DeleteProfile(id); err != nil {
		return 0, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUpdateProfile(path string, body []byte) (any, int, error) {
	id := extractID(path, pathProfile)

	var req struct {
		DurationSeconds           *int32   `json:"durationSeconds"`
		RequireInstanceProperties *bool    `json:"requireInstanceProperties"`
		Name                      string   `json:"name"`
		SessionPolicy             string   `json:"sessionPolicy"`
		RoleArns                  []string `json:"roleArns"`
		ManagedPolicyArns         []string `json:"managedPolicyArns"`
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

	for part := range strings.SplitSeq(query, "&") {
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

	for part := range strings.SplitSeq(query, "&") {
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

func (h *Handler) dispatchCrlOps(op, path, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opImportCrl:
		r, c, e := h.handleImportCrl(body)

		return r, c, true, e
	case opGetCrl:
		r, c, e := h.handleGetCrl(path)

		return r, c, true, e
	case opListCrls:
		r, c, e := h.handleListCrls(query)

		return r, c, true, e
	case opUpdateCrl:
		r, c, e := h.handleUpdateCrl(path, body)

		return r, c, true, e
	case opDeleteCrl:
		r, c, e := h.handleDeleteCrl(path)

		return r, c, true, e
	case opEnableCrl:
		r, c, e := h.handleEnableCrl(path)

		return r, c, true, e
	case opDisableCrl:
		r, c, e := h.handleDisableCrl(path)

		return r, c, true, e
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchSubjectOps(op, path, query string) (any, int, bool, error) {
	switch op {
	case opGetSubject:
		r, c, e := h.handleGetSubject(path)

		return r, c, true, e
	case opListSubjects:
		r, c, e := h.handleListSubjects(query)

		return r, c, true, e
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchMappingOps(op, path, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opPutAttributeMapping:
		r, c, e := h.handlePutAttributeMapping(path, body)

		return r, c, true, e
	case opDeleteAttributeMapping:
		r, c, e := h.handleDeleteAttributeMapping(path, query)

		return r, c, true, e
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchNotificationOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opPutNotificationSettings:
		r, c, e := h.handlePutNotificationSettings(body)

		return r, c, true, e
	case opResetNotificationSettings:
		r, c, e := h.handleResetNotificationSettings(body)

		return r, c, true, e
	}

	return nil, 0, false, nil
}

// ---- CRL handlers ----

func (h *Handler) handleImportCrl(body []byte) (any, int, error) {
	var req struct {
		Enabled        *bool      `json:"enabled"`
		Name           string     `json:"name"`
		TrustAnchorArn string     `json:"trustAnchorArn"`
		CrlData        []byte     `json:"crlData"`
		Tags           []TagEntry `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	enabled := true
	if req.Enabled != nil {
		enabled = *req.Enabled
	}

	crl, err := h.Backend.ImportCrl(req.Name, req.CrlData, req.TrustAnchorArn, enabled, req.Tags)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyCrl: crlToJSON(crl)}, http.StatusCreated, nil
}

func (h *Handler) handleGetCrl(path string) (any, int, error) {
	id := extractID(path, pathCrl)

	crl, err := h.Backend.GetCrl(id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyCrl: crlToJSON(crl)}, http.StatusOK, nil
}

func (h *Handler) handleListCrls(query string) (any, int, error) {
	pageToken, maxResults := parsePageParams(query)

	all, next, err := h.Backend.ListCrls(pageToken, maxResults)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(all))

	for _, c := range all {
		list = append(list, crlToJSON(c))
	}

	resp := map[string]any{keyCrls: list}

	if next != "" {
		resp["nextToken"] = next
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleUpdateCrl(path string, body []byte) (any, int, error) {
	id := extractID(path, pathCrl)

	var req struct {
		Name    string `json:"name"`
		CrlData []byte `json:"crlData"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	crl, err := h.Backend.UpdateCrl(id, req.Name, req.CrlData)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyCrl: crlToJSON(crl)}, http.StatusOK, nil
}

func (h *Handler) handleDeleteCrl(path string) (any, int, error) {
	id := extractID(path, pathCrl)

	crl, err := h.Backend.DeleteCrl(id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyCrl: crlToJSON(crl)}, http.StatusOK, nil
}

func (h *Handler) handleEnableCrl(path string) (any, int, error) {
	id := extractID(path, pathCrl)

	crl, err := h.Backend.EnableCrl(id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyCrl: crlToJSON(crl)}, http.StatusOK, nil
}

func (h *Handler) handleDisableCrl(path string) (any, int, error) {
	id := extractID(path, pathCrl)

	crl, err := h.Backend.DisableCrl(id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyCrl: crlToJSON(crl)}, http.StatusOK, nil
}

// ---- Subject handlers ----

func (h *Handler) handleGetSubject(path string) (any, int, error) {
	id := extractID(path, pathSubject)

	s, err := h.Backend.GetSubject(id)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keySubject: subjectToJSON(s)}, http.StatusOK, nil
}

func (h *Handler) handleListSubjects(query string) (any, int, error) {
	pageToken, maxResults := parsePageParams(query)

	all, next, err := h.Backend.ListSubjects(pageToken, maxResults)
	if err != nil {
		return nil, 0, err
	}

	list := make([]any, 0, len(all))

	for _, s := range all {
		list = append(list, subjectToJSON(s))
	}

	resp := map[string]any{keySubjects: list}

	if next != "" {
		resp["nextToken"] = next
	}

	return resp, http.StatusOK, nil
}

// ---- Attribute mapping handlers ----

func (h *Handler) handlePutAttributeMapping(path string, body []byte) (any, int, error) {
	profileID := extractProfileIDFromMappingPath(path)

	var req struct {
		CertificateField string        `json:"certificateField"`
		MappingRules     []MappingRule `json:"mappingRules"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	p, err := h.Backend.PutAttributeMapping(profileID, req.CertificateField, req.MappingRules)
	if err != nil {
		return nil, 0, err
	}

	mappings := h.Backend.GetAttributeMappings(profileID)

	return map[string]any{keyProfile: profileWithMappingsToJSON(p, mappings)}, http.StatusOK, nil
}

func (h *Handler) handleDeleteAttributeMapping(path, query string) (any, int, error) {
	profileID := extractProfileIDFromMappingPath(path)

	var certificateField string

	var specifiers []string

	for part := range strings.SplitSeq(query, "&") {
		if after, ok := strings.CutPrefix(part, "certificateField="); ok {
			certificateField = after
		}

		if after, ok := strings.CutPrefix(part, "specifiers="); ok {
			specifiers = append(specifiers, after)
		}
	}

	p, err := h.Backend.DeleteAttributeMapping(profileID, certificateField, specifiers)
	if err != nil {
		return nil, 0, err
	}

	mappings := h.Backend.GetAttributeMappings(profileID)

	return map[string]any{keyProfile: profileWithMappingsToJSON(p, mappings)}, http.StatusOK, nil
}

// ---- Notification settings handlers ----

func (h *Handler) handlePutNotificationSettings(body []byte) (any, int, error) {
	var req struct {
		TrustAnchorID        string                `json:"trustAnchorId"`
		NotificationSettings []NotificationSetting `json:"notificationSettings"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	ta, err := h.Backend.PutNotificationSettings(req.TrustAnchorID, req.NotificationSettings)
	if err != nil {
		return nil, 0, err
	}

	settings := h.Backend.GetNotificationSettings(req.TrustAnchorID)

	return map[string]any{keyTrustAnchor: trustAnchorWithSettingsToJSON(ta, settings)}, http.StatusOK, nil
}

func (h *Handler) handleResetNotificationSettings(body []byte) (any, int, error) {
	var req struct {
		TrustAnchorID           string                   `json:"trustAnchorId"`
		NotificationSettingKeys []NotificationSettingKey `json:"notificationSettingKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	ta, err := h.Backend.ResetNotificationSettings(req.TrustAnchorID, req.NotificationSettingKeys)
	if err != nil {
		return nil, 0, err
	}

	settings := h.Backend.GetNotificationSettings(req.TrustAnchorID)

	return map[string]any{keyTrustAnchor: trustAnchorWithSettingsToJSON(ta, settings)}, http.StatusOK, nil
}

// handleError writes an error response.
func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrTrustAnchorAlreadyExists),
		errors.Is(err, ErrProfileAlreadyExists),
		errors.Is(err, ErrCrlAlreadyExists):
		return c.JSON(http.StatusConflict, errBody("ConflictException", err.Error()))
	case errors.Is(err, ErrTrustAnchorNotFound),
		errors.Is(err, ErrProfileNotFound),
		errors.Is(err, ErrCrlNotFound),
		errors.Is(err, ErrSubjectNotFound):
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

	if op, id := parseEntityPath(method, segments); op != opUnknown {
		return op, id
	}

	return parseTagPaths(method, segments)
}

// parseEntityPath handles /trustanchors, /trustanchor/*, /profiles, /profile/*, /crls, /crl/*, etc.
func parseEntityPath(method string, segments []string) (string, string) {
	switch segments[0] {
	case pathTrustanchors:
		return parseTrustAnchorsCollection(method)
	case pathTrustanchor:
		return parseTrustAnchorPath(method, segments)
	case pathProfiles:
		if len(segments) >= segmentDepthMapping {
			return parseProfilesMappingPath(method, segments)
		}

		return parseProfilesCollection(method)
	case pathProfile:
		return parseProfilePath(method, segments)
	case pathCrls:
		return parseCrlsCollection(method)
	case pathCrl:
		return parseCrlPath(method, segments)
	case pathSubjects:
		return parseSubjectsCollection(method)
	case pathSubject:
		return parseSubjectPath(method, segments)
	}

	return opUnknown, ""
}

func parseTrustAnchorsCollection(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListTrustAnchors, ""
	case http.MethodPost:
		return opCreateTrustAnchor, ""
	}

	return opUnknown, ""
}

func parseProfilesCollection(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListProfiles, ""
	case http.MethodPost:
		return opCreateProfile, ""
	}

	return opUnknown, ""
}

func parseProfilesMappingPath(method string, segments []string) (string, string) {
	// /profiles/{id}/mappings
	if len(segments) != segmentDepthMapping || segments[2] != pathMappings {
		return opUnknown, ""
	}

	id := segments[1]

	switch method {
	case http.MethodPut:
		return opPutAttributeMapping, id
	case http.MethodDelete:
		return opDeleteAttributeMapping, id
	}

	return opUnknown, ""
}

// parseTagPaths handles /TagResource, /UntagResource, /ListTagsForResource, and notification routing.
func parseTagPaths(method string, segments []string) (string, string) {
	switch segments[0] {
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
	case pathPutNotifications:
		if method == http.MethodPatch {
			return opPutNotificationSettings, ""
		}
	case pathResetNotifications:
		if method == http.MethodPatch {
			return opResetNotificationSettings, ""
		}
	}

	return opUnknown, ""
}

func parseTrustAnchorPath(method string, segments []string) (string, string) {
	if len(segments) < minSegmentsForResource {
		return opUnknown, ""
	}

	id := segments[1]

	switch len(segments) {
	case segmentDepthResource:
		switch method {
		case http.MethodGet:
			return opGetTrustAnchor, id
		case http.MethodDelete:
			return opDeleteTrustAnchor, id
		case http.MethodPatch:
			return opUpdateTrustAnchor, id
		}
	case segmentDepthSubResource:
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
	if len(segments) < minSegmentsForResource {
		return opUnknown, ""
	}

	id := segments[1]

	switch len(segments) {
	case segmentDepthResource:
		switch method {
		case http.MethodGet:
			return opGetProfile, id
		case http.MethodDelete:
			return opDeleteProfile, id
		case http.MethodPatch:
			return opUpdateProfile, id
		}
	case segmentDepthSubResource:
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

func parseCrlsCollection(method string) (string, string) {
	switch method {
	case http.MethodGet:
		return opListCrls, ""
	case http.MethodPost:
		return opImportCrl, ""
	}

	return opUnknown, ""
}

func parseCrlPath(method string, segments []string) (string, string) {
	if len(segments) < minSegmentsForResource {
		return opUnknown, ""
	}

	id := segments[1]

	switch len(segments) {
	case segmentDepthResource:
		switch method {
		case http.MethodGet:
			return opGetCrl, id
		case http.MethodDelete:
			return opDeleteCrl, id
		case http.MethodPatch:
			return opUpdateCrl, id
		}
	case segmentDepthSubResource:
		switch segments[2] {
		case pathEnable:
			if method == http.MethodPost {
				return opEnableCrl, id
			}
		case pathDisable:
			if method == http.MethodPost {
				return opDisableCrl, id
			}
		}
	}

	return opUnknown, ""
}

func parseSubjectsCollection(method string) (string, string) {
	if method == http.MethodGet {
		return opListSubjects, ""
	}

	return opUnknown, ""
}

func parseSubjectPath(method string, segments []string) (string, string) {
	if len(segments) < minSegmentsForResource {
		return opUnknown, ""
	}

	id := segments[1]

	if len(segments) == segmentDepthResource && method == http.MethodGet {
		return opGetSubject, id
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

	for part := range strings.SplitSeq(query, "&") {
		if after, ok := strings.CutPrefix(part, "nextToken="); ok {
			nextToken = after
		}

		if after, ok := strings.CutPrefix(part, "maxResults="); ok {
			var n int

			for _, c := range after {
				if c >= '0' && c <= '9' {
					n = n*base10 + int(c-'0')
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
		"name":           ta.Name, //nolint:goconst // existing issue.
		"source":         ta.Source,
		"enabled":        ta.Enabled,                        //nolint:goconst // existing issue.
		"createdAt":      ta.CreatedAt.Format(time.RFC3339), //nolint:goconst // existing issue.
		"updatedAt":      ta.UpdatedAt.Format(time.RFC3339), //nolint:goconst // existing issue.
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

func crlToJSON(c *Crl) map[string]any {
	return map[string]any{
		"crlId":          c.CrlID,
		"crlArn":         c.CrlArn,
		"name":           c.Name,
		"crlData":        c.CrlData,
		"trustAnchorArn": c.TrustAnchorArn,
		"enabled":        c.Enabled,
		"createdAt":      c.CreatedAt.Format(time.RFC3339),
		"updatedAt":      c.UpdatedAt.Format(time.RFC3339),
	}
}

func subjectToJSON(s *Subject) map[string]any {
	m := map[string]any{
		"subjectId":   s.SubjectID,
		"subjectArn":  s.SubjectArn,
		"x509Subject": s.X509Subject,
		"enabled":     s.Enabled,
		"createdAt":   s.CreatedAt.Format(time.RFC3339),
		"updatedAt":   s.UpdatedAt.Format(time.RFC3339),
	}

	if !s.LastSeenAt.IsZero() {
		m["lastSeenAt"] = s.LastSeenAt.Format(time.RFC3339)
	}

	return m
}

func profileWithMappingsToJSON(p *Profile, mappings []AttributeMapping) map[string]any {
	m := profileToJSON(p)

	if len(mappings) > 0 {
		m["attributeMappings"] = mappings
	}

	return m
}

func trustAnchorWithSettingsToJSON(ta *TrustAnchor, settings []NotificationSetting) map[string]any {
	m := trustAnchorToJSON(ta)

	if len(settings) > 0 {
		m["notificationSettings"] = settings
	}

	return m
}

// extractProfileIDFromMappingPath extracts the profile ID from /profiles/{id}/mappings.
func extractProfileIDFromMappingPath(path string) string {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(segments) >= 2 { //nolint:mnd // existing issue.
		return segments[1]
	}

	return ""
}

func errBody(code, message string) map[string]string {
	return map[string]string{
		"__type":  code,
		"message": message,
	}
}
