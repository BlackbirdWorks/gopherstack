package pinpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// errInvalidRequestBody is returned by createTemplateByType when the request body cannot be parsed.
var errInvalidRequestBody = errors.New("invalid request body")

// errUnsupportedTemplateType is returned when an unknown template type is provided.
var errUnsupportedTemplateType = errors.New("unsupported template type")

const (
	pinpointService         = "mobiletargeting"
	pinpointMatchPriority   = 87
	appSubPathParts         = 2
	pinpointDefaultPageSize = 500

	templateSubPathParts   = 2
	campaignStatus         = "ACTIVE"
	journeyStateDraft      = "DRAFT"
	jobStatusCreated       = "CREATED"
	exportJobType          = "EXPORT"
	importJobType          = "IMPORT"
	segmentTypeDimensional = "DIMENSIONAL"
	unknownOperation       = "Unknown"
)

// Handler is the HTTP handler for the Amazon Pinpoint REST API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Pinpoint handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears the handler's backend state (used for test isolation).
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "Pinpoint" }

// GetSupportedOperations returns the list of supported Pinpoint operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApp",
		"CreateCampaign",
		"CreateEmailTemplate",
		"CreateExportJob",
		"CreateImportJob",
		"CreateInAppTemplate",
		"CreateJourney",
		"CreatePushTemplate",
		"CreateRecommenderConfiguration",
		"CreateSegment",
		"CreateSmsTemplate",
		"DeleteApp",
		"GetApp",
		"GetApplicationSettings",
		"GetApps",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		"UpdateApplicationSettings",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return pinpointService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Pinpoint API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		if httputils.ExtractServiceFromRequest(c.Request()) != pinpointService {
			return false
		}

		path := c.Request().URL.Path

		return strings.HasPrefix(path, "/v1/apps") ||
			strings.HasPrefix(path, "/v1/tags/") ||
			strings.HasPrefix(path, "/v1/templates/") ||
			strings.HasPrefix(path, "/v1/recommenders")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return pinpointMatchPriority }

// ExtractOperation extracts the operation name from the request path and method.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	method := c.Request().Method
	path := c.Request().URL.Path

	switch {
	case strings.HasPrefix(path, "/v1/tags/"):
		return extractTagOperation(method)
	case path == "/v1/apps" || path == "/v1/apps/":
		if method == http.MethodPost {
			return "CreateApp"
		}

		if method == http.MethodGet {
			return "GetApps"
		}
	case strings.HasPrefix(path, "/v1/apps/"):
		suffix := strings.TrimPrefix(path, "/v1/apps/")
		if strings.Contains(suffix, "/") {
			return h.extractAppSubOperation(method, suffix)
		}

		switch method {
		case http.MethodGet:
			return "GetApp"
		case http.MethodDelete:
			return "DeleteApp"
		}
	case path == "/v1/recommenders" || path == "/v1/recommenders/":
		if method == http.MethodPost {
			return "CreateRecommenderConfiguration"
		}
	case strings.HasPrefix(path, "/v1/templates/"):
		return h.extractTemplateOperation(method, path)
	}

	return unknownOperation
}

// extractTagOperation resolves the operation for tag-related paths.
func extractTagOperation(method string) string {
	switch method {
	case http.MethodGet:
		return "ListTagsForResource"
	case http.MethodPost:
		return "TagResource"
	case http.MethodDelete:
		return "UntagResource"
	}

	return unknownOperation
}

// extractAppSubOperation resolves the operation name for paths under /v1/apps/{id}/.
func (h *Handler) extractAppSubOperation(method, suffix string) string {
	parts := strings.SplitN(suffix, "/", appSubPathParts)
	if len(parts) != appSubPathParts {
		return unknownOperation
	}

	subPath := parts[1]

	switch {
	case subPath == "settings":
		switch method {
		case http.MethodGet:
			return "GetApplicationSettings"
		case http.MethodPut:
			return "UpdateApplicationSettings"
		}
	case subPath == "campaigns" && method == http.MethodPost:
		return "CreateCampaign"
	case subPath == "journeys" && method == http.MethodPost:
		return "CreateJourney"
	case subPath == "segments" && method == http.MethodPost:
		return "CreateSegment"
	case subPath == "jobs/export" && method == http.MethodPost:
		return "CreateExportJob"
	case subPath == "jobs/import" && method == http.MethodPost:
		return "CreateImportJob"
	}

	return unknownOperation
}

// extractTemplateOperation resolves the operation name for paths under /v1/templates/.
func (h *Handler) extractTemplateOperation(method, path string) string {
	suffix := strings.TrimPrefix(path, "/v1/templates/")
	parts := strings.SplitN(suffix, "/", templateSubPathParts)

	if len(parts) != templateSubPathParts || method != http.MethodPost {
		return unknownOperation
	}

	switch parts[1] {
	case "email":
		return "CreateEmailTemplate"
	case "inapp":
		return "CreateInAppTemplate"
	case "push":
		return "CreatePushTemplate"
	case "sms":
		return "CreateSmsTemplate"
	}

	return unknownOperation
}

// ExtractResource extracts the app ID or decoded ARN from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	switch {
	case strings.HasPrefix(path, "/v1/apps/"):
		return strings.TrimPrefix(path, "/v1/apps/")
	case strings.HasPrefix(path, "/v1/tags/"):
		escaped := strings.TrimPrefix(path, "/v1/tags/")
		decoded, err := url.PathUnescape(escaped)
		if err != nil {
			return escaped
		}

		return decoded
	case strings.HasPrefix(path, "/v1/templates/"):
		return strings.TrimPrefix(path, "/v1/templates/")
	case strings.HasPrefix(path, "/v1/recommenders"):
		return strings.TrimPrefix(path, "/v1/recommenders")
	}

	return ""
}

// Handler returns the echo.HandlerFunc for this service.
func (h *Handler) Handler() echo.HandlerFunc {
	return h.ServeHTTP
}

// ServeHTTP dispatches Pinpoint API requests.
func (h *Handler) ServeHTTP(c *echo.Context) error {
	path := c.Request().URL.Path

	switch {
	case strings.HasPrefix(path, "/v1/tags/"):
		return h.dispatchTags(c, path)
	case path == "/v1/apps" || path == "/v1/apps/":
		return h.dispatchApps(c)
	case strings.HasPrefix(path, "/v1/apps/"):
		suffix := strings.TrimPrefix(path, "/v1/apps/")
		if strings.Contains(suffix, "/") {
			return h.dispatchAppSubPath(c, suffix)
		}

		return h.dispatchApp(c, suffix)
	case path == "/v1/recommenders" || path == "/v1/recommenders/":
		return h.dispatchRecommenders(c)
	case strings.HasPrefix(path, "/v1/templates/"):
		return h.dispatchTemplates(c, path)
	}

	ctx := c.Request().Context()
	log := logger.Load(ctx)
	log.WarnContext(ctx, "pinpoint: unhandled request", "method", c.Request().Method, "path", path)

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
}

// dispatchTags routes tag-related requests, URL-decoding the resource ARN from the path.
func (h *Handler) dispatchTags(c *echo.Context, path string) error {
	escaped := strings.TrimPrefix(path, "/v1/tags/")

	resourceARN, err := url.PathUnescape(escaped)
	if err != nil || resourceARN == "" {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid resource ARN in path")
	}

	switch c.Request().Method {
	case http.MethodGet:
		return h.handleListTagsForResource(c, resourceARN)
	case http.MethodPost:
		return h.handleTagResource(c, resourceARN)
	case http.MethodDelete:
		return h.handleUntagResource(c, resourceARN)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchApps(c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.handleCreateApp(c)
	case http.MethodGet:
		return h.handleGetApps(c)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchApp(c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.handleGetApp(c, appID)
	case http.MethodDelete:
		return h.handleDeleteApp(c, appID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

// dispatchAppSubPath handles paths under /v1/apps/{appId}/ (e.g. settings).
func (h *Handler) dispatchAppSubPath(c *echo.Context, suffix string) error {
	parts := strings.SplitN(suffix, "/", appSubPathParts)
	if len(parts) != appSubPathParts {
		return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
	}

	appID, subPath := parts[0], parts[1]

	switch {
	case subPath == "settings":
		return h.dispatchAppSettings(c, appID)
	case subPath == "campaigns" && c.Request().Method == http.MethodPost:
		return h.handleCreateCampaign(c, appID)
	case subPath == "journeys" && c.Request().Method == http.MethodPost:
		return h.handleCreateJourney(c, appID)
	case subPath == "segments" && c.Request().Method == http.MethodPost:
		return h.handleCreateSegment(c, appID)
	case subPath == "jobs/export" && c.Request().Method == http.MethodPost:
		return h.handleCreateExportJob(c, appID)
	case subPath == "jobs/import" && c.Request().Method == http.MethodPost:
		return h.handleCreateImportJob(c, appID)
	}

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
}

// dispatchAppSettings handles GET/PUT /v1/apps/{appId}/settings.
func (h *Handler) dispatchAppSettings(c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.handleGetApplicationSettings(c, appID)
	case http.MethodPut:
		return h.handleUpdateApplicationSettings(c, appID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) handleCreateApp(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req createAppRequest

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if strings.TrimSpace(req.Name) == "" {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "Name is required")
	}

	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

	app, err := h.Backend.CreateApp(region, h.AccountID, req.Name, req.Tags)
	if err != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusCreated, toAppResponse(app))

	return nil
}

func (h *Handler) handleGetApp(c *echo.Context, appID string) error {
	app, err := h.Backend.GetApp(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toAppResponse(app))

	return nil
}

func (h *Handler) handleDeleteApp(c *echo.Context, appID string) error {
	app, err := h.Backend.DeleteApp(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, toAppResponse(app))

	return nil
}

func (h *Handler) handleGetApps(c *echo.Context) error {
	apps, err := h.Backend.GetApps()
	if err != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	items := make([]appResponse, 0, len(apps))

	for _, app := range apps {
		items = append(items, toAppResponse(app))
	}

	// Support pageSize and token query parameters for cursor-based pagination.
	// The Pinpoint REST API uses ?pageSize=N&token=<cursor>.
	q := c.Request().URL.Query()
	token := q.Get("token")

	var limit int

	if ps := q.Get("pageSize"); ps != "" {
		if n, parseErr := strconv.Atoi(ps); parseErr == nil && n > 0 {
			limit = n
		}
	}

	p := page.New(items, token, limit, pinpointDefaultPageSize)

	resp := appsResponse{Item: p.Data}
	if p.Next != "" {
		resp.NextToken = &p.Next
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleGetApplicationSettings handles GET /v1/apps/{appId}/settings.
func (h *Handler) handleGetApplicationSettings(c *echo.Context, appID string) error {
	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, newAppSettingsResponse(appID))

	return nil
}

// handleUpdateApplicationSettings handles PUT /v1/apps/{appId}/settings.
func (h *Handler) handleUpdateApplicationSettings(c *echo.Context, appID string) error {
	// Read and discard the body; no settings are persisted in the mock backend.
	_, _ = httputils.ReadBody(c.Request())

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, newAppSettingsResponse(appID))

	return nil
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req tagResourceRequest

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	tagErr := h.Backend.TagResource(resourceARN, req.Tags)
	if tagErr != nil {
		if errors.Is(tagErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", tagErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", tagErr.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	c.Response().WriteHeader(http.StatusNoContent)

	return nil
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, tagsModel{Tags: tags})

	return nil
}

// toAppResponse converts an App to the JSON wire format.
func toAppResponse(app *App) appResponse {
	return appResponse{
		ARN:          app.ARN,
		ID:           app.ID,
		Name:         app.Name,
		CreationDate: app.CreationDate,
		Tags:         app.Tags,
	}
}

// writeErrorResponse writes a JSON error response in the Pinpoint REST API format.
func writeErrorResponse(c *echo.Context, statusCode int, errorType, message string) error {
	httputils.WriteJSON(c.Request().Context(), c.Response(), statusCode, map[string]string{
		"message": message,
		"__type":  errorType,
	})

	return nil
}

// dispatchRecommenders routes POST /v1/recommenders requests.
func (h *Handler) dispatchRecommenders(c *echo.Context) error {
	if c.Request().Method == http.MethodPost {
		return h.handleCreateRecommenderConfiguration(c)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

// dispatchTemplates routes requests under /v1/templates/{templateName}/{type}.
func (h *Handler) dispatchTemplates(c *echo.Context, path string) error {
	suffix := strings.TrimPrefix(path, "/v1/templates/")
	parts := strings.SplitN(suffix, "/", templateSubPathParts)

	if len(parts) != templateSubPathParts {
		return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
	}

	templateName, templateType := parts[0], parts[1]

	if c.Request().Method != http.MethodPost {
		return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}

	return h.handleCreateTemplate(c, templateName, templateType)
}

// handleCreateTemplate handles creation of any template type (email, inapp, push, sms).
func (h *Handler) handleCreateTemplate(c *echo.Context, templateName, templateType string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

	templateARN, creationErr := h.createTemplateByType(body, region, templateName, templateType)
	if creationErr != nil {
		switch {
		case errors.Is(creationErr, errInvalidRequestBody):
			return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
		case errors.Is(creationErr, ErrAlreadyExists):
			return writeErrorResponse(c, http.StatusConflict, "ConflictException", creationErr.Error())
		default:
			return writeErrorResponse(
				c,
				http.StatusInternalServerError,
				"InternalServerErrorException",
				creationErr.Error(),
			)
		}
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusCreated, createTemplateMessageBody{
		ARN:     templateARN,
		Message: "Created",
	})

	return nil
}

// createTemplateByType creates a template based on templateType and returns its ARN.
func (h *Handler) createTemplateByType(body []byte, region, templateName, templateType string) (string, error) {
	switch templateType {
	case "email":
		var req createEmailTemplateRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return "", errInvalidRequestBody
		}

		t, err := h.Backend.CreateEmailTemplate(region, h.AccountID, templateName, req)
		if err != nil {
			return "", err
		}

		return t.ARN, nil

	case "inapp":
		var req createInAppTemplateRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return "", errInvalidRequestBody
		}

		t, err := h.Backend.CreateInAppTemplate(region, h.AccountID, templateName, req)
		if err != nil {
			return "", err
		}

		return t.ARN, nil

	case "push":
		var req createPushTemplateRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return "", errInvalidRequestBody
		}

		t, err := h.Backend.CreatePushTemplate(region, h.AccountID, templateName, req)
		if err != nil {
			return "", err
		}

		return t.ARN, nil

	case "sms":
		var req createSmsTemplateRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return "", errInvalidRequestBody
		}

		t, err := h.Backend.CreateSmsTemplate(region, h.AccountID, templateName, req)
		if err != nil {
			return "", err
		}

		return t.ARN, nil
	}

	return "", fmt.Errorf("%w: %s", errUnsupportedTemplateType, templateType)
}
func (h *Handler) handleCreateCampaign(c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req createCampaignRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if strings.TrimSpace(req.Name) == "" {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "Name is required")
	}

	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

	campaign, backendErr := h.Backend.CreateCampaign(region, h.AccountID, appID, req)
	if backendErr != nil {
		if errors.Is(backendErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", backendErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", backendErr.Error())
	}

	resp := campaignResponse{
		ApplicationID:    campaign.ApplicationID,
		ARN:              campaign.ARN,
		ID:               campaign.ID,
		Name:             campaign.Name,
		SegmentID:        campaign.SegmentID,
		SegmentVersion:   campaign.SegmentVersion,
		Tags:             campaign.Tags,
		CreationDate:     campaign.CreationDate,
		LastModifiedDate: campaign.LastModifiedDate,
		State:            campaignState{CampaignStatus: campaignStatus},
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusCreated, resp)

	return nil
}

// handleCreateExportJob handles POST /v1/apps/{appId}/jobs/export.
func (h *Handler) handleCreateExportJob(c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req createExportJobRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if strings.TrimSpace(req.RoleArn) == "" {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "RoleArn is required")
	}

	if strings.TrimSpace(req.S3UrlPrefix) == "" {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "S3UrlPrefix is required")
	}

	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

	job, backendErr := h.Backend.CreateExportJob(region, h.AccountID, appID, req)
	if backendErr != nil {
		if errors.Is(backendErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", backendErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", backendErr.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusCreated, exportJobResponse{
		ARN:           job.ARN,
		ApplicationID: job.ApplicationID,
		ID:            job.ID,
		RoleArn:       job.RoleArn,
		S3UrlPrefix:   job.S3UrlPrefix,
		JobStatus:     job.JobStatus,
		Type:          exportJobType,
		CreationDate:  job.CreationDate,
	})

	return nil
}

// handleCreateImportJob handles POST /v1/apps/{appId}/jobs/import.
func (h *Handler) handleCreateImportJob(c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req createImportJobRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if strings.TrimSpace(req.RoleArn) == "" {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "RoleArn is required")
	}

	if strings.TrimSpace(req.S3Url) == "" {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "S3Url is required")
	}

	if strings.TrimSpace(req.Format) == "" {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "Format is required")
	}

	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

	job, backendErr := h.Backend.CreateImportJob(region, h.AccountID, appID, req)
	if backendErr != nil {
		if errors.Is(backendErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", backendErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", backendErr.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusCreated, importJobResponse{
		ARN:           job.ARN,
		ApplicationID: job.ApplicationID,
		ID:            job.ID,
		RoleArn:       job.RoleArn,
		S3Url:         job.S3Url,
		Format:        job.Format,
		JobStatus:     job.JobStatus,
		Type:          importJobType,
		CreationDate:  job.CreationDate,
	})

	return nil
}

// errNameRequired is returned when a required Name field is missing.
var errNameRequired = errors.New("Name is required")

// namedResourceCreatorFn creates a named resource and returns the JSON-serialisable response or an error.
type namedResourceCreatorFn func(body []byte, region, appID string) (any, error)

// handleCreateNamedAppResource is a shared handler for app-scoped named resource creation.
func (h *Handler) handleCreateNamedAppResource(c *echo.Context, appID string, creator namedResourceCreatorFn) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	region := httputils.ExtractRegionFromRequest(c.Request(), h.DefaultRegion)

	resp, creationErr := creator(body, region, appID)
	if creationErr != nil {
		switch {
		case errors.Is(creationErr, errInvalidRequestBody):
			return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
		case errors.Is(creationErr, errNameRequired):
			return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "Name is required")
		case errors.Is(creationErr, awserr.ErrNotFound):
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", creationErr.Error())
		default:
			return writeErrorResponse(
				c,
				http.StatusInternalServerError,
				"InternalServerErrorException",
				creationErr.Error(),
			)
		}
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusCreated, resp)

	return nil
}

// handleCreateJourney handles POST /v1/apps/{appId}/journeys.
func (h *Handler) handleCreateJourney(c *echo.Context, appID string) error {
	return h.handleCreateNamedAppResource(c, appID, func(body []byte, region, appID string) (any, error) {
		var req createJourneyRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, errInvalidRequestBody
		}

		if strings.TrimSpace(req.Name) == "" {
			return nil, errNameRequired
		}

		journey, err := h.Backend.CreateJourney(region, h.AccountID, appID, req)
		if err != nil {
			return nil, err
		}

		return journeyResponse{
			ApplicationID:    journey.ApplicationID,
			ARN:              journey.ARN,
			ID:               journey.ID,
			Name:             journey.Name,
			State:            journey.State,
			Tags:             journey.Tags,
			CreationDate:     journey.CreationDate,
			LastModifiedDate: journey.LastModifiedDate,
		}, nil
	})
}

// handleCreateRecommenderConfiguration handles POST /v1/recommenders.
func (h *Handler) handleCreateRecommenderConfiguration(c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var req createRecommenderConfigRequest
	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if strings.TrimSpace(req.Name) == "" {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "Name is required")
	}

	r, backendErr := h.Backend.CreateRecommenderConfiguration(req)
	if backendErr != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", backendErr.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusCreated, recommenderConfigResponse{
		Attributes:                    r.Attributes,
		ID:                            r.ID,
		Name:                          r.Name,
		Description:                   r.Description,
		RecommendationProviderIDType:  r.RecommendationProviderIDType,
		RecommendationProviderRoleArn: r.RecommendationProviderRoleARN,
		RecommendationProviderURI:     r.RecommendationProviderURI,
		RecommendationsPerMessage:     r.RecommendationsPerMessage,
		CreationDate:                  r.CreationDate,
		LastModifiedDate:              r.LastModifiedDate,
	})

	return nil
}

// handleCreateSegment handles POST /v1/apps/{appId}/segments.
func (h *Handler) handleCreateSegment(c *echo.Context, appID string) error {
	return h.handleCreateNamedAppResource(c, appID, func(body []byte, region, appID string) (any, error) {
		var req createSegmentRequest
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, errInvalidRequestBody
		}

		if strings.TrimSpace(req.Name) == "" {
			return nil, errNameRequired
		}

		segment, err := h.Backend.CreateSegment(region, h.AccountID, appID, req)
		if err != nil {
			return nil, err
		}

		return segmentResponse{
			ApplicationID: segment.ApplicationID,
			ARN:           segment.ARN,
			ID:            segment.ID,
			Name:          segment.Name,
			SegmentType:   segment.SegmentType,
			Tags:          segment.Tags,
			CreationDate:  segment.CreationDate,
		}, nil
	})
}
