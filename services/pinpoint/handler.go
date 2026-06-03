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
	journeyStateDraft      = "DRAFT"
	jobStatusCreated       = "CREATED"
	exportJobType          = "EXPORT"
	importJobType          = "IMPORT"
	segmentTypeDimensional = "DIMENSIONAL"
	segmentTypeImport      = "IMPORT"

	campaignStatusScheduled = "SCHEDULED"
	campaignStatusPaused    = "PAUSED"
	campaignStatusCompleted = "COMPLETED"
	unknownOperation        = "Unknown"

	// sub-path segment constants used throughout dispatch helpers.
	subPathJobsExport       = "jobs/export"
	subPathJobsImport       = "jobs/import"
	subPathVersions         = "versions"
	subPathExecutionMetrics = "execution/metrics"
	phoneValidatePath       = "/v1/phone/number/validate"

	// dispatchSplitN is the split count used in journey/campaign/segment sub-path dispatch.
	dispatchSplitTwo   = 2
	dispatchSplitThree = 3

	// acceptedMessage is the standard response message for accepted operations.
	acceptedMessage = "Accepted"
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
//
//nolint:funlen // Long list of all supported operations; splitting would reduce clarity.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		// App
		"CreateApp",
		"DeleteApp",
		"GetApp",
		"GetApplicationSettings",
		"GetApps",
		"UpdateApplicationSettings",
		"GetApplicationDateRangeKpi",
		// Tags
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
		// Campaigns
		"CreateCampaign",
		"DeleteCampaign",
		"GetCampaign",
		"GetCampaigns",
		"UpdateCampaign",
		"GetCampaignActivities",
		"GetCampaignDateRangeKpi",
		"GetCampaignVersion",
		"GetCampaignVersions",
		// Segments
		"CreateSegment",
		"DeleteSegment",
		"GetSegment",
		"GetSegments",
		"UpdateSegment",
		"GetSegmentExportJobs",
		"GetSegmentImportJobs",
		"GetSegmentVersion",
		"GetSegmentVersions",
		// Journeys
		"CreateJourney",
		"DeleteJourney",
		"GetJourney",
		"ListJourneys",
		"UpdateJourney",
		"UpdateJourneyState",
		"GetJourneyDateRangeKpi",
		"GetJourneyExecutionMetrics",
		"GetJourneyExecutionActivityMetrics",
		"GetJourneyRuns",
		"GetJourneyRunExecutionMetrics",
		"GetJourneyRunExecutionActivityMetrics",
		// Templates
		"CreateEmailTemplate",
		"GetEmailTemplate",
		"UpdateEmailTemplate",
		"DeleteEmailTemplate",
		"CreateInAppTemplate",
		"GetInAppTemplate",
		"UpdateInAppTemplate",
		"DeleteInAppTemplate",
		"CreatePushTemplate",
		"GetPushTemplate",
		"UpdatePushTemplate",
		"DeletePushTemplate",
		"CreateSmsTemplate",
		"GetSmsTemplate",
		"UpdateSmsTemplate",
		"DeleteSmsTemplate",
		"CreateVoiceTemplate",
		"GetVoiceTemplate",
		"UpdateVoiceTemplate",
		"DeleteVoiceTemplate",
		"ListTemplates",
		"ListTemplateVersions",
		"UpdateTemplateActiveVersion",
		// Channels
		"GetAdmChannel",
		"UpdateAdmChannel",
		"DeleteAdmChannel",
		"GetApnsChannel",
		"UpdateApnsChannel",
		"DeleteApnsChannel",
		"GetApnsSandboxChannel",
		"UpdateApnsSandboxChannel",
		"DeleteApnsSandboxChannel",
		"GetApnsVoipChannel",
		"UpdateApnsVoipChannel",
		"DeleteApnsVoipChannel",
		"GetApnsVoipSandboxChannel",
		"UpdateApnsVoipSandboxChannel",
		"DeleteApnsVoipSandboxChannel",
		"GetBaiduChannel",
		"UpdateBaiduChannel",
		"DeleteBaiduChannel",
		"GetEmailChannel",
		"UpdateEmailChannel",
		"DeleteEmailChannel",
		"GetGcmChannel",
		"UpdateGcmChannel",
		"DeleteGcmChannel",
		"GetSmsChannel",
		"UpdateSmsChannel",
		"DeleteSmsChannel",
		"GetVoiceChannel",
		"UpdateVoiceChannel",
		"DeleteVoiceChannel",
		"GetChannels",
		// Endpoints
		"GetEndpoint",
		"UpdateEndpoint",
		"DeleteEndpoint",
		"GetUserEndpoints",
		"DeleteUserEndpoints",
		"UpdateEndpointsBatch",
		"RemoveAttributes",
		// EventStream
		"GetEventStream",
		"PutEventStream",
		"DeleteEventStream",
		// Messaging
		"SendMessages",
		"SendUsersMessages",
		"SendOTPMessage",
		"VerifyOTPMessage",
		// Events
		"PutEvents",
		"GetInAppMessages",
		// Phone
		"PhoneNumberValidate",
		// Jobs
		"CreateExportJob",
		"GetExportJob",
		"GetExportJobs",
		"CreateImportJob",
		"GetImportJob",
		"GetImportJobs",
		// Recommenders
		"CreateRecommenderConfiguration",
		"GetRecommenderConfiguration",
		"GetRecommenderConfigurations",
		"UpdateRecommenderConfiguration",
		"DeleteRecommenderConfiguration",
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
			strings.HasPrefix(path, "/v1/templates") ||
			strings.HasPrefix(path, "/v1/recommenders") ||
			strings.HasPrefix(path, "/v1/phone/") ||
			path == phoneValidatePath
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return pinpointMatchPriority }

// ExtractOperation extracts the operation name from the request path and method.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	method := c.Request().Method
	path := c.Request().URL.Path

	if op := extractTagOrAppsOp(h, method, path); op != unknownOperation {
		return op
	}

	return extractRecommenderTemplateOp(h, method, path)
}

// extractTagOrAppsOp handles tag and app operations.
func extractTagOrAppsOp(h *Handler, method, path string) string {
	switch {
	case strings.HasPrefix(path, "/v1/tags/"):
		return extractTagOperation(method)
	case path == "/v1/apps" || path == "/v1/apps/":
		return extractAppsCollectionOp(method)
	case strings.HasPrefix(path, "/v1/apps/"):
		return extractAppsResourceOp(h, method, path)
	}

	return unknownOperation
}

// extractAppsCollectionOp returns the operation for the apps collection.
func extractAppsCollectionOp(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateApp"
	case http.MethodGet:
		return "GetApps"
	}

	return unknownOperation
}

// extractAppsResourceOp returns the operation for a specific app resource.
func extractAppsResourceOp(h *Handler, method, path string) string {
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

	return unknownOperation
}

// extractRecommenderTemplateOp handles recommender and template operations.
func extractRecommenderTemplateOp(h *Handler, method, path string) string {
	switch {
	case path == "/v1/recommenders" || path == "/v1/recommenders/":
		return extractRecommendersCollectionOp(method)
	case strings.HasPrefix(path, "/v1/recommenders/"):
		return extractRecommenderResourceOp(method, path)
	case path == "/v1/templates" || path == "/v1/templates/":
		return "ListTemplates"
	case strings.HasPrefix(path, "/v1/templates/"):
		return h.extractTemplateOperation(method, path)
	case path == phoneValidatePath:
		return "PhoneNumberValidate"
	}

	return unknownOperation
}

// extractRecommendersCollectionOp returns the operation for the recommenders collection.
func extractRecommendersCollectionOp(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateRecommenderConfiguration"
	case http.MethodGet:
		return "GetRecommenderConfigurations"
	}

	return unknownOperation
}

// extractRecommenderResourceOp returns the operation for a specific recommender resource.
func extractRecommenderResourceOp(method, path string) string {
	recommenderID := strings.TrimPrefix(path, "/v1/recommenders/")
	if recommenderID == "" {
		return unknownOperation
	}

	switch method {
	case http.MethodGet:
		return "GetRecommenderConfiguration"
	case http.MethodPut:
		return "UpdateRecommenderConfiguration"
	case http.MethodDelete:
		return "DeleteRecommenderConfiguration"
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
//

func (h *Handler) extractAppSubOperation(method, suffix string) string {
	parts := strings.SplitN(suffix, "/", appSubPathParts)
	if len(parts) != appSubPathParts {
		return unknownOperation
	}

	subPath := parts[1]

	if op := h.extractAppCoreSubOp(method, subPath); op != unknownOperation {
		return op
	}

	return h.extractAppExtSubOp(method, subPath)
}

// extractAppCoreSubOp resolves settings, campaign, journey, segment, and job operations.
func (h *Handler) extractAppCoreSubOp(method, subPath string) string {
	switch {
	case subPath == "settings":
		return extractSettingsOp(method)
	case subPath == "campaigns":
		return extractCampaignsOp(method)
	case strings.HasPrefix(subPath, "campaigns/"):
		return h.extractCampaignSubOp(method, strings.TrimPrefix(subPath, "campaigns/"))
	case subPath == "journeys":
		return extractJourneysOp(method)
	case strings.HasPrefix(subPath, "journeys/"):
		return h.extractJourneySubOp(method, strings.TrimPrefix(subPath, "journeys/"))
	case subPath == "segments":
		return extractSegmentsOp(method)
	case strings.HasPrefix(subPath, "segments/"):
		return h.extractSegmentSubOp(method, strings.TrimPrefix(subPath, "segments/"))
	case subPath == subPathJobsExport:
		return extractExportJobsOp(method)
	case strings.HasPrefix(subPath, subPathJobsExport+"/"):
		return "GetExportJob"
	case subPath == subPathJobsImport:
		return extractImportJobsOp(method)
	case strings.HasPrefix(subPath, subPathJobsImport+"/"):
		return "GetImportJob"
	}

	return unknownOperation
}

// extractSettingsOp returns the settings operation name.
func extractSettingsOp(method string) string {
	switch method {
	case http.MethodGet:
		return "GetApplicationSettings"
	case http.MethodPut:
		return "UpdateApplicationSettings"
	}

	return unknownOperation
}

// extractCampaignsOp returns the campaigns collection operation name.
func extractCampaignsOp(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateCampaign"
	case http.MethodGet:
		return "GetCampaigns"
	}

	return unknownOperation
}

// extractJourneysOp returns the journeys collection operation name.
func extractJourneysOp(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateJourney"
	case http.MethodGet:
		return "ListJourneys"
	}

	return unknownOperation
}

// extractSegmentsOp returns the segments collection operation name.
func extractSegmentsOp(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateSegment"
	case http.MethodGet:
		return "GetSegments"
	}

	return unknownOperation
}

// extractExportJobsOp returns the export jobs collection operation name.
func extractExportJobsOp(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateExportJob"
	case http.MethodGet:
		return "GetExportJobs"
	}

	return unknownOperation
}

// extractImportJobsOp returns the import jobs collection operation name.
func extractImportJobsOp(method string) string {
	switch method {
	case http.MethodPost:
		return "CreateImportJob"
	case http.MethodGet:
		return "GetImportJobs"
	}

	return unknownOperation
}

// extractAppExtSubOp resolves messaging, KPI, channel, endpoint, and user operations.
func (h *Handler) extractAppExtSubOp(method, subPath string) string {
	switch {
	case subPath == "eventstream":
		return extractEventStreamOp(method)
	case subPath == "messages":
		return "SendMessages"
	case subPath == "users-messages":
		return "SendUsersMessages"
	case subPath == "otp":
		return "SendOTPMessage"
	case subPath == "verify-otp":
		return "VerifyOTPMessage"
	case subPath == "events":
		return "PutEvents"
	case strings.HasPrefix(subPath, "kpis/daterange/"):
		return "GetApplicationDateRangeKpi"
	case subPath == "channels":
		return "GetChannels"
	case strings.HasPrefix(subPath, "channels/"):
		return h.extractChannelOp(method, strings.TrimPrefix(subPath, "channels/"))
	case subPath == "endpoints":
		return "UpdateEndpointsBatch"
	case strings.HasPrefix(subPath, "endpoints/"):
		return h.extractEndpointSubOp(method, strings.TrimPrefix(subPath, "endpoints/"))
	case strings.HasPrefix(subPath, "users/"):
		userID := strings.TrimPrefix(subPath, "users/")
		if userID != "" {
			return extractUserEndpointsOp(method)
		}
	case strings.HasPrefix(subPath, "attributes/"):
		return "RemoveAttributes"
	}

	return unknownOperation
}

// extractEventStreamOp returns the event stream operation name.
func extractEventStreamOp(method string) string {
	switch method {
	case http.MethodGet:
		return "GetEventStream"
	case http.MethodPost:
		return "PutEventStream"
	case http.MethodDelete:
		return "DeleteEventStream"
	}

	return unknownOperation
}

// extractUserEndpointsOp returns the user endpoints operation name.
func extractUserEndpointsOp(method string) string {
	switch method {
	case http.MethodGet:
		return "GetUserEndpoints"
	case http.MethodDelete:
		return "DeleteUserEndpoints"
	}

	return unknownOperation
}

func (h *Handler) extractCampaignSubOp(method, rest string) string {
	parts := strings.SplitN(rest, "/", dispatchSplitTwo)
	subPath := ""

	if len(parts) == dispatchSplitTwo {
		subPath = parts[1]
	}

	switch {
	case subPath == "":
		switch method {
		case http.MethodGet:
			return "GetCampaign"
		case http.MethodPut:
			return "UpdateCampaign"
		case http.MethodDelete:
			return "DeleteCampaign"
		}
	case subPath == "activities":
		return "GetCampaignActivities"
	case strings.HasPrefix(subPath, "kpis/daterange/"):
		return "GetCampaignDateRangeKpi"
	case subPath == subPathVersions:
		return "GetCampaignVersions"
	case strings.HasPrefix(subPath, subPathVersions+"/"):
		return "GetCampaignVersion"
	}

	return unknownOperation
}

func (h *Handler) extractJourneySubOp(method, rest string) string {
	parts := strings.SplitN(rest, "/", dispatchSplitTwo)
	subPath := ""

	if len(parts) == dispatchSplitTwo {
		subPath = parts[1]
	}

	switch {
	case subPath == "":
		switch method {
		case http.MethodGet:
			return "GetJourney"
		case http.MethodPut:
			return "UpdateJourney"
		case http.MethodDelete:
			return "DeleteJourney"
		}
	case subPath == "state":
		return "UpdateJourneyState"
	case strings.HasPrefix(subPath, "kpis/daterange/"):
		return "GetJourneyDateRangeKpi"
	case subPath == subPathExecutionMetrics:
		return "GetJourneyExecutionMetrics"
	case strings.HasPrefix(subPath, "activities/") && strings.HasSuffix(subPath, "/execution/metrics"):
		return "GetJourneyExecutionActivityMetrics"
	case subPath == "runs":
		return "GetJourneyRuns"
	case strings.HasPrefix(subPath, "runs/"):
		runRest := strings.TrimPrefix(subPath, "runs/")
		if strings.HasSuffix(runRest, "/execution/metrics") {
			if strings.Contains(runRest, "/activities/") {
				return "GetJourneyRunExecutionActivityMetrics"
			}

			return "GetJourneyRunExecutionMetrics"
		}
	}

	return unknownOperation
}

func (h *Handler) extractSegmentSubOp(method, rest string) string {
	parts := strings.SplitN(rest, "/", dispatchSplitTwo)
	subPath := ""

	if len(parts) == dispatchSplitTwo {
		subPath = parts[1]
	}

	switch {
	case subPath == "":
		switch method {
		case http.MethodGet:
			return "GetSegment"
		case http.MethodPut:
			return "UpdateSegment"
		case http.MethodDelete:
			return "DeleteSegment"
		}
	case subPath == subPathJobsExport:
		return "GetSegmentExportJobs"
	case subPath == subPathJobsImport:
		return "GetSegmentImportJobs"
	case subPath == subPathVersions:
		return "GetSegmentVersions"
	case strings.HasPrefix(subPath, subPathVersions+"/"):
		return "GetSegmentVersion"
	}

	return unknownOperation
}

func (h *Handler) extractChannelOp(method, channelType string) string {
	if channelType == "" {
		return unknownOperation
	}

	switch method {
	case http.MethodGet:
		return "Get" + channelTypeOpName(channelType) + "Channel"
	case http.MethodPut:
		return "Update" + channelTypeOpName(channelType) + "Channel"
	case http.MethodDelete:
		return "Delete" + channelTypeOpName(channelType) + "Channel"
	}

	return unknownOperation
}

// channelTypeOpName converts a URL channel type segment to the AWS op suffix.
// e.g. "adm" → "Adm", "apns" → "Apns", "apns_sandbox" → "ApnsSandbox".
func channelTypeOpName(channelType string) string {
	switch strings.ToLower(channelType) {
	case "adm":
		return "Adm"
	case "apns":
		return "Apns"
	case "apns_sandbox":
		return "ApnsSandbox"
	case "apns_voip":
		return "ApnsVoip"
	case "apns_voip_sandbox":
		return "ApnsVoipSandbox"
	case "baidu":
		return "Baidu"
	case templateTypeEmail:
		return "Email"
	case "gcm":
		return "Gcm"
	case templateTypeSMS:
		return "Sms"
	case templateTypeVoice:
		return "Voice"
	}

	return channelType
}

func (h *Handler) extractEndpointSubOp(method, rest string) string {
	parts := strings.SplitN(rest, "/", dispatchSplitTwo)
	subPath := ""

	if len(parts) == dispatchSplitTwo {
		subPath = parts[1]
	}

	if subPath == "inappmessages" {
		return "GetInAppMessages"
	}

	switch method {
	case http.MethodGet:
		return "GetEndpoint"
	case http.MethodPut:
		return "UpdateEndpoint"
	case http.MethodDelete:
		return "DeleteEndpoint"
	}

	return unknownOperation
}

// extractTemplateOperation resolves the operation name for paths under /v1/templates/.
func (h *Handler) extractTemplateOperation(method, path string) string {
	suffix := strings.TrimPrefix(path, "/v1/templates/")
	parts := strings.SplitN(suffix, "/", dispatchSplitThree)

	if len(parts) < templateSubPathParts {
		return unknownOperation
	}

	templateType := parts[1]
	subPath := ""

	if len(parts) == dispatchSplitThree {
		subPath = parts[2]
	}

	switch subPath {
	case "versions":
		return "ListTemplateVersions"
	case "active-version":
		return "UpdateTemplateActiveVersion"
	case "":
		switch method {
		case http.MethodPost:
			return h.createTemplateOpName(templateType)
		case http.MethodGet:
			return h.getTemplateOpName(templateType)
		case http.MethodPut:
			return h.updateTemplateOpName(templateType)
		case http.MethodDelete:
			return h.deleteTemplateOpName(templateType)
		}
	}

	return unknownOperation
}

func (h *Handler) createTemplateOpName(t string) string {
	switch t {
	case templateTypeEmail:
		return "CreateEmailTemplate"
	case templateTypeInApp:
		return "CreateInAppTemplate"
	case templateTypePush:
		return "CreatePushTemplate"
	case templateTypeSMS:
		return "CreateSmsTemplate"
	case templateTypeVoice:
		return "CreateVoiceTemplate"
	}

	return unknownOperation
}

func (h *Handler) getTemplateOpName(t string) string {
	switch t {
	case templateTypeEmail:
		return "GetEmailTemplate"
	case templateTypeInApp:
		return "GetInAppTemplate"
	case templateTypePush:
		return "GetPushTemplate"
	case templateTypeSMS:
		return "GetSmsTemplate"
	case templateTypeVoice:
		return "GetVoiceTemplate"
	}

	return unknownOperation
}

func (h *Handler) updateTemplateOpName(t string) string {
	switch t {
	case templateTypeEmail:
		return "UpdateEmailTemplate"
	case templateTypeInApp:
		return "UpdateInAppTemplate"
	case templateTypePush:
		return "UpdatePushTemplate"
	case templateTypeSMS:
		return "UpdateSmsTemplate"
	case templateTypeVoice:
		return "UpdateVoiceTemplate"
	}

	return unknownOperation
}

func (h *Handler) deleteTemplateOpName(t string) string {
	switch t {
	case templateTypeEmail:
		return "DeleteEmailTemplate"
	case templateTypeInApp:
		return "DeleteInAppTemplate"
	case templateTypePush:
		return "DeletePushTemplate"
	case templateTypeSMS:
		return "DeleteSmsTemplate"
	case templateTypeVoice:
		return "DeleteVoiceTemplate"
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
	case strings.HasPrefix(path, "/v1/recommenders/"):
		return h.dispatchRecommenderByID(c, strings.TrimPrefix(path, "/v1/recommenders/"))
	case strings.HasPrefix(path, "/v1/templates"):
		return h.dispatchTemplates(c, path)
	case path == phoneValidatePath:
		return h.handlePhoneNumberValidate(c)
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
//

func (h *Handler) dispatchAppSubPath(c *echo.Context, suffix string) error {
	parts := strings.SplitN(suffix, "/", appSubPathParts)
	if len(parts) != appSubPathParts {
		return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
	}

	appID, subPath := parts[0], parts[1]

	if handled, err := h.tryAppCoreSubPath(c, appID, subPath); handled {
		return err
	}

	if handled, err := h.tryAppExtSubPath(c, appID, subPath); handled {
		return err
	}

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
}

// tryAppCoreSubPath handles settings, campaigns, journeys, segments, and job paths.
// Returns (true, err) if the path was handled.
func (h *Handler) tryAppCoreSubPath(c *echo.Context, appID, subPath string) (bool, error) {
	switch {
	case subPath == "settings":
		return true, h.dispatchAppSettings(c, appID)
	case subPath == "campaigns":
		return true, h.dispatchCampaigns(c, appID)
	case strings.HasPrefix(subPath, "campaigns/"):
		return true, h.dispatchCampaignByID(c, appID, strings.TrimPrefix(subPath, "campaigns/"))
	case subPath == "journeys":
		return true, h.dispatchJourneys(c, appID)
	case strings.HasPrefix(subPath, "journeys/"):
		return true, h.dispatchJourneyByID(c, appID, strings.TrimPrefix(subPath, "journeys/"))
	case subPath == "segments":
		return true, h.dispatchSegments(c, appID)
	case strings.HasPrefix(subPath, "segments/"):
		return true, h.dispatchSegmentByID(c, appID, strings.TrimPrefix(subPath, "segments/"))
	case subPath == subPathJobsExport:
		return true, h.dispatchExportJobs(c, appID, "")
	case strings.HasPrefix(subPath, subPathJobsExport+"/"):
		return true, h.handleGetExportJob(c, appID, strings.TrimPrefix(subPath, "jobs/export/"))
	case subPath == subPathJobsImport:
		return true, h.dispatchImportJobs(c, appID, "")
	case strings.HasPrefix(subPath, subPathJobsImport+"/"):
		return true, h.handleGetImportJob(c, appID, strings.TrimPrefix(subPath, "jobs/import/"))
	case subPath == "eventstream":
		return true, h.dispatchEventStream(c, appID)
	}

	return false, nil
}

// tryAppExtSubPath handles messaging, KPI, channel, endpoint, and user paths.
// Returns (true, err) if the path was handled.
func (h *Handler) tryAppExtSubPath(c *echo.Context, appID, subPath string) (bool, error) {
	switch {
	case subPath == "messages":
		return true, h.handleSendMessages(c, appID)
	case subPath == "users-messages":
		return true, h.handleSendUsersMessages(c, appID)
	case subPath == "otp":
		return true, h.handleSendOTPMessage(c, appID)
	case subPath == "verify-otp":
		return true, h.handleVerifyOTPMessage(c, appID)
	case subPath == "events":
		return true, h.handlePutEvents(c, appID)
	case strings.HasPrefix(subPath, "kpis/daterange/"):
		return true, h.handleGetApplicationDateRangeKpi(c, appID, strings.TrimPrefix(subPath, "kpis/daterange/"))
	case strings.HasPrefix(subPath, "channels/"):
		return true, h.dispatchChannelByType(c, appID, strings.TrimPrefix(subPath, "channels/"))
	case subPath == "channels":
		return true, h.handleGetChannels(c, appID)
	case strings.HasPrefix(subPath, "endpoints/"):
		return true, h.dispatchEndpointByID(c, appID, strings.TrimPrefix(subPath, "endpoints/"))
	case subPath == "endpoints":
		if c.Request().Method == http.MethodPut {
			return true, h.handleUpdateEndpointsBatch(c, appID)
		}
	case strings.HasPrefix(subPath, "users/"):
		return true, h.dispatchUserByID(c, appID, strings.TrimPrefix(subPath, "users/"))
	case strings.HasPrefix(subPath, "attributes/"):
		attrType := strings.TrimPrefix(subPath, "attributes/")
		if c.Request().Method == http.MethodPut {
			return true, h.handleRemoveAttributes(c, appID, attrType)
		}
	}

	return false, nil
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
	settings, err := h.Backend.GetApplicationSettings(appID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", err.Error())
	}

	resp := appSettingsResponse{
		ApplicationID:            appID,
		LastModifiedDate:         settings.LastModifiedDate,
		CampaignHook:             settings.CampaignHook,
		Limits:                   settings.Limits,
		QuietTime:                settings.QuietTime,
		CloudWatchMetricsEnabled: settings.CloudWatchMetrics,
		EventTaggingEnabled:      settings.EventTaggingEnabled,
	}

	if resp.CampaignHook == nil {
		resp.CampaignHook = map[string]any{}
	}

	if resp.Limits == nil {
		resp.Limits = map[string]any{}
	}

	if resp.QuietTime == nil {
		resp.QuietTime = map[string]any{}
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}

// handleUpdateApplicationSettings handles PUT /v1/apps/{appId}/settings.
func (h *Handler) handleUpdateApplicationSettings(c *echo.Context, appID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "failed to read request body")
	}

	var incoming struct {
		CampaignHook        map[string]any `json:"CampaignHook"`
		Limits              map[string]any `json:"Limits"`
		QuietTime           map[string]any `json:"QuietTime"`
		CloudWatchMetrics   bool           `json:"CloudWatchMetricsEnabled"`
		EventTaggingEnabled bool           `json:"EventTaggingEnabled"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &incoming); jsonErr != nil {
			return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
		}
	}

	settingsToStore := &storedAppSettings{
		CampaignHook:        incoming.CampaignHook,
		Limits:              incoming.Limits,
		QuietTime:           incoming.QuietTime,
		CloudWatchMetrics:   incoming.CloudWatchMetrics,
		EventTaggingEnabled: incoming.EventTaggingEnabled,
	}

	settings, updateErr := h.Backend.UpdateApplicationSettings(appID, settingsToStore)
	if updateErr != nil {
		if errors.Is(updateErr, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", updateErr.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerErrorException", updateErr.Error())
	}

	resp := appSettingsResponse{
		ApplicationID:            appID,
		LastModifiedDate:         settings.LastModifiedDate,
		CampaignHook:             settings.CampaignHook,
		Limits:                   settings.Limits,
		QuietTime:                settings.QuietTime,
		CloudWatchMetricsEnabled: settings.CloudWatchMetrics,
		EventTaggingEnabled:      settings.EventTaggingEnabled,
	}

	if resp.CampaignHook == nil {
		resp.CampaignHook = map[string]any{}
	}

	if resp.Limits == nil {
		resp.Limits = map[string]any{}
	}

	if resp.QuietTime == nil {
		resp.QuietTime = map[string]any{}
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

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

	if len(tagKeys) == 0 {
		return writeErrorResponse(c, http.StatusBadRequest, "BadRequestException", "tagKeys parameter is required")
	}

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

// dispatchRecommenders routes /v1/recommenders requests (POST=create, GET=list).
func (h *Handler) dispatchRecommenders(c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.handleCreateRecommenderConfiguration(c)
	case http.MethodGet:
		return h.handleGetRecommenderConfigurations(c)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

// dispatchTemplates routes requests under /v1/templates.
func (h *Handler) dispatchTemplates(c *echo.Context, path string) error {
	// /v1/templates (list all)
	if path == "/v1/templates" || path == "/v1/templates/" {
		return h.handleListTemplates(c)
	}

	suffix := strings.TrimPrefix(path, "/v1/templates/")
	// suffix format: {templateName}/{type} or {templateName}/{type}/versions or {templateName}/{type}/active-version
	parts := strings.SplitN(suffix, "/", dispatchSplitThree)

	if len(parts) < templateSubPathParts {
		return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
	}

	templateName, templateType := parts[0], parts[1]
	subPath := ""

	if len(parts) == dispatchSplitThree {
		subPath = parts[2]
	}

	switch subPath {
	case "versions":
		return h.handleListTemplateVersions(c, templateName, templateType)
	case "active-version":
		return h.handleUpdateTemplateActiveVersion(c, templateName, templateType)
	case "":
		switch c.Request().Method {
		case http.MethodPost:
			return h.handleCreateTemplate(c, templateName, templateType)
		case http.MethodGet:
			return h.handleGetTemplate(c, templateName, templateType)
		case http.MethodPut:
			return h.handleUpdateTemplate(c, templateName, templateType)
		case http.MethodDelete:
			return h.handleDeleteTemplateByType(c, templateName, templateType)
		}
	}

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
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
	case templateTypeEmail:
		return h.createEmailTemplateARN(body, region, templateName)
	case templateTypeInApp:
		return h.createInAppTemplateARN(body, region, templateName)
	case templateTypePush:
		return h.createPushTemplateARN(body, region, templateName)
	case templateTypeSMS:
		return h.createSMSTemplateARN(body, region, templateName)
	case templateTypeVoice:
		return h.createVoiceTemplateARN(body, region, templateName)
	}

	return "", fmt.Errorf("%w: %s", errUnsupportedTemplateType, templateType)
}

func (h *Handler) createEmailTemplateARN(body []byte, region, templateName string) (string, error) {
	var req createEmailTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", errInvalidRequestBody
	}

	t, err := h.Backend.CreateEmailTemplate(region, h.AccountID, templateName, req)
	if err != nil {
		return "", err
	}

	return t.ARN, nil
}

func (h *Handler) createInAppTemplateARN(body []byte, region, templateName string) (string, error) {
	var req createInAppTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", errInvalidRequestBody
	}

	t, err := h.Backend.CreateInAppTemplate(region, h.AccountID, templateName, req)
	if err != nil {
		return "", err
	}

	return t.ARN, nil
}

func (h *Handler) createPushTemplateARN(body []byte, region, templateName string) (string, error) {
	var req createPushTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", errInvalidRequestBody
	}

	t, err := h.Backend.CreatePushTemplate(region, h.AccountID, templateName, req)
	if err != nil {
		return "", err
	}

	return t.ARN, nil
}

func (h *Handler) createSMSTemplateARN(body []byte, region, templateName string) (string, error) {
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

func (h *Handler) createVoiceTemplateARN(body []byte, region, templateName string) (string, error) {
	var req createVoiceTemplateRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return "", errInvalidRequestBody
	}

	t, err := h.Backend.CreateVoiceTemplate(region, h.AccountID, templateName, req)
	if err != nil {
		return "", err
	}

	return t.ARN, nil
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

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusCreated, toCampaignResponse(campaign))

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

		return toJourneyResponse(journey), nil
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

// ──────────────────────────────────────────────────
// Sub-resource dispatch helpers
// ──────────────────────────────────────────────────

func (h *Handler) dispatchCampaigns(c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.handleCreateCampaign(c, appID)
	case http.MethodGet:
		return h.handleGetCampaigns(c, appID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchCampaignByID(c *echo.Context, appID, rest string) error {
	// rest can be: {id}, {id}/activities, {id}/kpis/daterange/{kpi}, {id}/versions, {id}/versions/{v}
	parts := strings.SplitN(rest, "/", dispatchSplitTwo)
	campaignID := parts[0]
	subPath := ""

	if len(parts) == dispatchSplitTwo {
		subPath = parts[1]
	}

	switch {
	case subPath == "":
		switch c.Request().Method {
		case http.MethodGet:
			return h.handleGetCampaign(c, appID, campaignID)
		case http.MethodPut:
			return h.handleUpdateCampaign(c, appID, campaignID)
		case http.MethodDelete:
			return h.handleDeleteCampaign(c, appID, campaignID)
		}
	case subPath == "activities":
		return h.handleGetCampaignActivities(c, appID, campaignID)
	case strings.HasPrefix(subPath, "kpis/daterange/"):
		kpiName := strings.TrimPrefix(subPath, "kpis/daterange/")

		return h.handleGetCampaignDateRangeKpi(c, appID, campaignID, kpiName)
	case subPath == subPathVersions:
		return h.handleGetCampaignVersions(c, appID, campaignID)
	case strings.HasPrefix(subPath, subPathVersions+"/"):
		versionStr := strings.TrimPrefix(subPath, "versions/")
		v, _ := parseVersionParam(versionStr)

		return h.handleGetCampaignVersion(c, appID, campaignID, v)
	}

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
}

func (h *Handler) dispatchJourneys(c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.handleCreateJourney(c, appID)
	case http.MethodGet:
		return h.handleListJourneys(c, appID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchJourneyByID(c *echo.Context, appID, rest string) error {
	// rest: {id}, {id}/state, {id}/kpis/daterange/{kpi},
	//       {id}/execution/metrics, {id}/activities/{aid}/execution/metrics,
	//       {id}/runs, {id}/runs/{rid}/execution/metrics,
	//       {id}/runs/{rid}/activities/{aid}/execution/metrics
	parts := strings.SplitN(rest, "/", dispatchSplitTwo)
	journeyID := parts[0]
	subPath := ""

	if len(parts) == dispatchSplitTwo {
		subPath = parts[1]
	}

	switch {
	case subPath == "":
		switch c.Request().Method {
		case http.MethodGet:
			return h.handleGetJourney(c, appID, journeyID)
		case http.MethodPut:
			return h.handleUpdateJourney(c, appID, journeyID)
		case http.MethodDelete:
			return h.handleDeleteJourney(c, appID, journeyID)
		}
	case subPath == "state":
		return h.handleUpdateJourneyState(c, appID, journeyID)
	case strings.HasPrefix(subPath, "kpis/daterange/"):
		kpiName := strings.TrimPrefix(subPath, "kpis/daterange/")

		return h.handleGetJourneyDateRangeKpi(c, appID, journeyID, kpiName)
	case subPath == subPathExecutionMetrics:
		return h.handleGetJourneyExecutionMetrics(c, appID, journeyID)
	case strings.HasPrefix(subPath, "activities/") && strings.HasSuffix(subPath, "/execution/metrics"):
		activityID := strings.TrimPrefix(subPath, "activities/")
		activityID = strings.TrimSuffix(activityID, "/execution/metrics")

		return h.handleGetJourneyExecutionActivityMetrics(c, appID, journeyID, activityID)
	case subPath == "runs":
		return h.handleGetJourneyRuns(c, appID, journeyID)
	case strings.HasPrefix(subPath, "runs/"):
		return h.dispatchJourneyRun(c, appID, journeyID, strings.TrimPrefix(subPath, "runs/"))
	}

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
}

func (h *Handler) dispatchJourneyRun(c *echo.Context, appID, journeyID, rest string) error {
	// rest: {runId}/execution/metrics, {runId}/activities/{aid}/execution/metrics
	parts := strings.SplitN(rest, "/", dispatchSplitTwo)
	runID := parts[0]
	subPath := ""

	if len(parts) == dispatchSplitTwo {
		subPath = parts[1]
	}

	switch {
	case subPath == subPathExecutionMetrics:
		return h.handleGetJourneyRunExecutionMetrics(c, appID, journeyID, runID)
	case strings.HasPrefix(subPath, "activities/") && strings.HasSuffix(subPath, "/execution/metrics"):
		activityID := strings.TrimPrefix(subPath, "activities/")
		activityID = strings.TrimSuffix(activityID, "/execution/metrics")

		return h.handleGetJourneyRunExecutionActivityMetrics(c, appID, journeyID, runID, activityID)
	}

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
}

func (h *Handler) dispatchSegments(c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.handleCreateSegment(c, appID)
	case http.MethodGet:
		return h.handleGetSegments(c, appID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchSegmentByID(c *echo.Context, appID, rest string) error {
	parts := strings.SplitN(rest, "/", dispatchSplitTwo)
	segmentID := parts[0]
	subPath := ""

	if len(parts) == dispatchSplitTwo {
		subPath = parts[1]
	}

	switch {
	case subPath == "":
		switch c.Request().Method {
		case http.MethodGet:
			return h.handleGetSegment(c, appID, segmentID)
		case http.MethodPut:
			return h.handleUpdateSegment(c, appID, segmentID)
		case http.MethodDelete:
			return h.handleDeleteSegment(c, appID, segmentID)
		}
	case subPath == subPathJobsExport:
		return h.handleGetSegmentExportJobs(c, appID, segmentID)
	case subPath == subPathJobsImport:
		return h.handleGetSegmentImportJobs(c, appID, segmentID)
	case subPath == subPathVersions:
		return h.handleGetSegmentVersions(c, appID, segmentID)
	case strings.HasPrefix(subPath, subPathVersions+"/"):
		versionStr := strings.TrimPrefix(subPath, "versions/")
		v, _ := parseVersionParam(versionStr)

		return h.handleGetSegmentVersion(c, appID, segmentID, v)
	}

	return writeErrorResponse(c, http.StatusNotFound, "NotFoundException", "resource not found")
}

func (h *Handler) dispatchExportJobs(c *echo.Context, appID, jobID string) error {
	if jobID != "" {
		return h.handleGetExportJob(c, appID, jobID)
	}

	switch c.Request().Method {
	case http.MethodPost:
		return h.handleCreateExportJob(c, appID)
	case http.MethodGet:
		return h.handleGetExportJobs(c, appID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchImportJobs(c *echo.Context, appID, jobID string) error {
	if jobID != "" {
		return h.handleGetImportJob(c, appID, jobID)
	}

	switch c.Request().Method {
	case http.MethodPost:
		return h.handleCreateImportJob(c, appID)
	case http.MethodGet:
		return h.handleGetImportJobs(c, appID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchEventStream(c *echo.Context, appID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.handleGetEventStream(c, appID)
	case http.MethodPost:
		return h.handlePutEventStream(c, appID)
	case http.MethodDelete:
		return h.handleDeleteEventStream(c, appID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchChannelByType(c *echo.Context, appID, channelType string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.handleGetChannel(c, appID, channelType)
	case http.MethodPut:
		return h.handleUpdateChannel(c, appID, channelType)
	case http.MethodDelete:
		return h.handleDeleteChannel(c, appID, channelType)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchEndpointByID(c *echo.Context, appID, rest string) error {
	// rest: {endpointId} or {endpointId}/inappmessages
	parts := strings.SplitN(rest, "/", dispatchSplitTwo)
	endpointID := parts[0]
	subPath := ""

	if len(parts) == dispatchSplitTwo {
		subPath = parts[1]
	}

	if subPath == "inappmessages" {
		return h.handleGetInAppMessages(c, appID, endpointID)
	}

	switch c.Request().Method {
	case http.MethodGet:
		return h.handleGetEndpoint(c, appID, endpointID)
	case http.MethodPut:
		return h.handleUpdateEndpoint(c, appID, endpointID)
	case http.MethodDelete:
		return h.handleDeleteEndpoint(c, appID, endpointID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchUserByID(c *echo.Context, appID, userID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.handleGetUserEndpoints(c, appID, userID)
	case http.MethodDelete:
		return h.handleDeleteUserEndpoints(c, appID, userID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
}

func (h *Handler) dispatchRecommenderByID(c *echo.Context, recommenderID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.handleGetRecommenderConfiguration(c, recommenderID)
	case http.MethodPut:
		return h.handleUpdateRecommenderConfiguration(c, recommenderID)
	case http.MethodDelete:
		return h.handleDeleteRecommenderConfiguration(c, recommenderID)
	}

	return writeErrorResponse(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
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

		return toSegmentResponse(segment), nil
	})
}
