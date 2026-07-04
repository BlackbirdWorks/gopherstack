package cloudtrail

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	cloudtrailMatchPriority = service.PriorityHeaderExact
	cloudtrailTargetPrefix  = "CloudTrail_20131101."
	keyTrailARN             = "TrailARN"
	keyName                 = "Name"
	keyQueryID              = "QueryId"
	keyQueryStatus          = "QueryStatus"
	keyChannelArn           = "ChannelArn"
	keySource               = "Source"
	keyDestinations         = "Destinations"
	keyDashboardArn         = "DashboardArn"
	keyStatus               = "Status"
	keyEDSArn               = "EventDataStoreArn"
	keyImportID             = "ImportId"
	keyImportStatus         = "ImportStatus"
	keyResourceArn          = "ResourceArn"
	statusEnabled           = "ENABLED"
	statusDisabled          = "DISABLED"
)

var errInvalidRequest = errors.New("invalid request")

// Handler is the Echo HTTP handler for AWS CloudTrail operations (JSON-1.1 protocol).
type Handler struct {
	ops     map[string]func(*echo.Context, []byte) error
	Backend *InMemoryBackend
}

// NewHandler creates a new CloudTrail handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "CloudTrail" }

// GetSupportedOperations returns the list of supported CloudTrail operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"AddTags",
		"CancelQuery",
		"CreateChannel",
		"CreateDashboard",
		"CreateEventDataStore",
		"CreateTrail",
		"DeleteChannel",
		"DeleteDashboard",
		"DeleteEventDataStore",
		"DeleteResourcePolicy",
		"DeleteTrail",
		"DeregisterOrganizationDelegatedAdmin",
		"DescribeQuery",
		"DescribeTrails",
		"DisableFederation",
		"EnableFederation",
		"GenerateQuery",
		"GetChannel",
		"GetDashboard",
		"GetEventConfiguration",
		"GetEventDataStore",
		"GetEventSelectors",
		"GetImport",
		"GetInsightSelectors",
		"GetQueryResults",
		"GetResourcePolicy",
		"GetTrail",
		"GetTrailStatus",
		"ListChannels",
		"ListDashboards",
		"ListEventDataStores",
		"ListImportFailures",
		"ListImports",
		"ListInsightsData",
		"ListInsightsMetricData",
		"ListPublicKeys",
		"ListQueries",
		"ListTags",
		"ListTrails",
		"LookupEvents",
		"PutEventConfiguration",
		"PutEventSelectors",
		"PutInsightSelectors",
		"PutResourcePolicy",
		"RegisterOrganizationDelegatedAdmin",
		"RemoveTags",
		"RestoreEventDataStore",
		"SearchSampleQueries",
		"StartDashboardRefresh",
		"StartEventDataStoreIngestion",
		"StartImport",
		"StartLogging",
		"StartQuery",
		"StopEventDataStoreIngestion",
		"StopImport",
		"StopLogging",
		"UpdateChannel",
		"UpdateDashboard",
		"UpdateEventDataStore",
		"UpdateTrail",
	}
}

// RecordManagementEvent implements service.CloudTrailRecorder, allowing the
// central service registry to reach this live backend directly (no second,
// disconnected CloudTrail backend is created).
func (h *Handler) RecordManagementEvent(ev service.CloudTrailEventInput) {
	h.Backend.RecordManagementEvent(ev)
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "cloudtrail" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CloudTrail instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS CloudTrail JSON requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, cloudtrailTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return cloudtrailMatchPriority }

// ExtractOperation extracts the CloudTrail operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, cloudtrailTargetPrefix)
}

// ExtractResource extracts the primary resource identifier from the request body.
func (h *Handler) ExtractResource(_ *echo.Context) string {
	return ""
}

// Handler returns the Echo handler function for CloudTrail requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		operation := h.ExtractOperation(c)

		log.Debug("cloudtrail request", "operation", operation)

		var body []byte
		if c.Request().Body != nil {
			decoder := json.NewDecoder(c.Request().Body)
			var raw json.RawMessage
			if err := decoder.Decode(&raw); err == nil {
				body = raw
			}
		}

		return h.dispatch(c, operation, body)
	}
}

// Reset clears the backend state (test helper).
func (h *Handler) Reset() {
	h.Backend.Reset()
}

func (h *Handler) dispatch(c *echo.Context, operation string, body []byte) error {
	if fn, ok := h.ops[operation]; ok {
		return fn(c, body)
	}

	return c.JSON(
		http.StatusBadRequest,
		errResp("InvalidParameterCombinationException", "unknown operation: "+operation),
	)
}

func (h *Handler) buildOps() map[string]func(*echo.Context, []byte) error {
	return map[string]func(*echo.Context, []byte) error{
		"AddTags":                              h.handleAddTags,
		"CancelQuery":                          h.handleCancelQuery,
		"CreateChannel":                        h.handleCreateChannel,
		"CreateDashboard":                      h.handleCreateDashboard,
		"CreateEventDataStore":                 h.handleCreateEventDataStore,
		"CreateTrail":                          h.handleCreateTrail,
		"DeleteChannel":                        h.handleDeleteChannel,
		"DeleteDashboard":                      h.handleDeleteDashboard,
		"DeleteEventDataStore":                 h.handleDeleteEventDataStore,
		"DeleteResourcePolicy":                 h.handleDeleteResourcePolicy,
		"DeleteTrail":                          h.handleDeleteTrail,
		"DeregisterOrganizationDelegatedAdmin": h.handleDeregisterOrganizationDelegatedAdmin,
		"DescribeQuery":                        h.handleDescribeQuery,
		"DescribeTrails":                       h.handleDescribeTrails,
		"DisableFederation":                    h.handleDisableFederation,
		"EnableFederation":                     h.handleEnableFederation,
		"GenerateQuery":                        h.handleGenerateQuery,
		"GetChannel":                           h.handleGetChannel,
		"GetDashboard":                         h.handleGetDashboard,
		"GetEventConfiguration":                h.handleGetEventConfiguration,
		"GetEventDataStore":                    h.handleGetEventDataStore,
		"GetEventSelectors":                    h.handleGetEventSelectors,
		"GetImport":                            h.handleGetImport,
		"GetInsightSelectors":                  h.handleGetInsightSelectors,
		"GetQueryResults":                      h.handleGetQueryResults,
		"GetResourcePolicy":                    h.handleGetResourcePolicy,
		"GetTrail":                             h.handleGetTrail,
		"GetTrailStatus":                       h.handleGetTrailStatus,
		"ListChannels":                         h.handleListChannels,
		"ListDashboards":                       h.handleListDashboards,
		"ListEventDataStores":                  h.handleListEventDataStores,
		"ListImportFailures":                   h.handleListImportFailures,
		"ListImports":                          h.handleListImports,
		"ListInsightsData":                     h.handleListInsightsData,
		"ListInsightsMetricData":               h.handleListInsightsMetricData,
		"ListPublicKeys":                       h.handleListPublicKeys,
		"ListQueries":                          h.handleListQueries,
		"ListTags":                             h.handleListTags,
		"ListTrails":                           h.handleListTrails,
		"LookupEvents":                         h.handleLookupEvents,
		"PutEventConfiguration":                h.handlePutEventConfiguration,
		"PutEventSelectors":                    h.handlePutEventSelectors,
		"PutInsightSelectors":                  h.handlePutInsightSelectors,
		"PutResourcePolicy":                    h.handlePutResourcePolicy,
		"RegisterOrganizationDelegatedAdmin":   h.handleRegisterOrganizationDelegatedAdmin,
		"RemoveTags":                           h.handleRemoveTags,
		"RestoreEventDataStore":                h.handleRestoreEventDataStore,
		"SearchSampleQueries":                  h.handleSearchSampleQueries,
		"StartDashboardRefresh":                h.handleStartDashboardRefresh,
		"StartEventDataStoreIngestion":         h.handleStartEventDataStoreIngestion,
		"StartImport":                          h.handleStartImport,
		"StartLogging":                         h.handleStartLogging,
		"StartQuery":                           h.handleStartQuery,
		"StopEventDataStoreIngestion":          h.handleStopEventDataStoreIngestion,
		"StopImport":                           h.handleStopImport,
		"StopLogging":                          h.handleStopLogging,
		"UpdateChannel":                        h.handleUpdateChannel,
		"UpdateDashboard":                      h.handleUpdateDashboard,
		"UpdateEventDataStore":                 h.handleUpdateEventDataStore,
		"UpdateTrail":                          h.handleUpdateTrail,
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errResp("TrailNotFoundException", err.Error()))
	case errors.Is(err, ErrChannelNotFound):
		return c.JSON(http.StatusNotFound, errResp("ChannelNotFoundException", err.Error()))
	case errors.Is(err, ErrDashboardNotFound):
		return c.JSON(http.StatusNotFound, errResp("DashboardNotFoundException", err.Error()))
	case errors.Is(err, ErrEventDataStoreNotFound):
		return c.JSON(http.StatusNotFound, errResp("EventDataStoreNotFoundException", err.Error()))
	case errors.Is(err, ErrQueryNotFound):
		return c.JSON(http.StatusNotFound, errResp("InactiveQueryException", err.Error()))
	case errors.Is(err, ErrTerminationProtected):
		return c.JSON(http.StatusConflict, errResp("EventDataStoreTerminationProtectedException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errResp("TrailAlreadyExistsException", err.Error()))
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterException", err.Error()))
	case errors.Is(err, errInvalidRequest):
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errResp("InternalFailure", err.Error()))
	}
}

func errResp(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

// --- CreateTrail ---

type createTrailBody struct {
	Name                      string `json:"Name"`
	S3BucketName              string `json:"S3BucketName"`
	S3KeyPrefix               string `json:"S3KeyPrefix"`
	SnsTopicName              string `json:"SnsTopicName"`
	CloudWatchLogsLogGroupArn string `json:"CloudWatchLogsLogGroupArn"`
	CloudWatchLogsRoleArn     string `json:"CloudWatchLogsRoleArn"`
	KMSKeyID                  string `json:"KMSKeyId"`
	TagsList                  []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"TagsList"`
	IncludeGlobalServiceEvents bool `json:"IncludeGlobalServiceEvents"`
	IsMultiRegionTrail         bool `json:"IsMultiRegionTrail"`
	EnableLogFileValidation    bool `json:"EnableLogFileValidation"`
}

func (h *Handler) handleCreateTrail(c *echo.Context, body []byte) error {
	var in createTrailBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "Name is required"))
	}
	if in.S3BucketName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidS3BucketNameException", "S3BucketName is required"))
	}

	kv := make(map[string]string, len(in.TagsList))
	for _, tag := range in.TagsList {
		kv[tag.Key] = tag.Value
	}

	t, err := h.Backend.CreateTrail(
		in.Name, in.S3BucketName, in.S3KeyPrefix, in.SnsTopicName,
		in.CloudWatchLogsLogGroupArn, in.CloudWatchLogsRoleArn, in.KMSKeyID,
		in.IncludeGlobalServiceEvents, in.IsMultiRegionTrail, in.EnableLogFileValidation,
		kv,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := trailToMap(t)

	return c.JSON(http.StatusOK, resp)
}

// --- GetTrail ---

type getTrailBody struct {
	Name string `json:"Name"`
}

func (h *Handler) handleGetTrail(c *echo.Context, body []byte) error {
	var in getTrailBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	t, err := h.Backend.GetTrail(in.Name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"Trail": trailToMap(t)})
}

// --- DescribeTrails ---

type describeTrailsBody struct {
	TrailNameList       []string `json:"trailNameList"`
	IncludeShadowTrails bool     `json:"includeShadowTrails"`
}

func (h *Handler) handleDescribeTrails(c *echo.Context, body []byte) error {
	var in describeTrailsBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterCombinationException", "invalid request body"),
			)
		}
	}

	trails := h.Backend.DescribeTrails(in.TrailNameList)
	items := make([]map[string]any, 0, len(trails))
	for _, t := range trails {
		items = append(items, trailToMap(t))
	}

	return c.JSON(http.StatusOK, map[string]any{"trailList": items})
}

// --- UpdateTrail ---

type updateTrailBody struct {
	IncludeGlobalServiceEvents *bool  `json:"IncludeGlobalServiceEvents"`
	IsMultiRegionTrail         *bool  `json:"IsMultiRegionTrail"`
	EnableLogFileValidation    *bool  `json:"EnableLogFileValidation"`
	Name                       string `json:"Name"`
	S3BucketName               string `json:"S3BucketName"`
	S3KeyPrefix                string `json:"S3KeyPrefix"`
	SnsTopicName               string `json:"SnsTopicName"`
	CloudWatchLogsLogGroupArn  string `json:"CloudWatchLogsLogGroupArn"`
	CloudWatchLogsRoleArn      string `json:"CloudWatchLogsRoleArn"`
	KMSKeyID                   string `json:"KMSKeyId"`
}

func (h *Handler) handleUpdateTrail(c *echo.Context, body []byte) error {
	var in updateTrailBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "Name is required"))
	}

	t, err := h.Backend.UpdateTrail(
		in.Name, in.S3BucketName, in.S3KeyPrefix, in.SnsTopicName,
		in.CloudWatchLogsLogGroupArn, in.CloudWatchLogsRoleArn, in.KMSKeyID,
		in.IncludeGlobalServiceEvents, in.IsMultiRegionTrail, in.EnableLogFileValidation,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, trailToMap(t))
}

// --- DeleteTrail ---

type deleteTrailBody struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteTrail(c *echo.Context, body []byte) error {
	var in deleteTrailBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.DeleteTrail(in.Name); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- StartLogging ---

type startLoggingBody struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStartLogging(c *echo.Context, body []byte) error {
	var in startLoggingBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "Name is required"))
	}

	if err := h.Backend.StartLogging(in.Name); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- StopLogging ---

type stopLoggingBody struct {
	Name string `json:"Name"`
}

func (h *Handler) handleStopLogging(c *echo.Context, body []byte) error {
	var in stopLoggingBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.Name == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "Name is required"))
	}

	if err := h.Backend.StopLogging(in.Name); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- GetTrailStatus ---

type getTrailStatusBody struct {
	Name string `json:"Name"`
}

func (h *Handler) handleGetTrailStatus(c *echo.Context, body []byte) error {
	var in getTrailStatusBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	t, err := h.Backend.GetTrailStatus(in.Name)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		"IsLogging": t.IsLogging,
	}
	if t.StartLoggingTime != nil {
		resp["StartLoggingTime"] = float64(t.StartLoggingTime.Unix())
	}
	if t.StopLoggingTime != nil {
		resp["StopLoggingTime"] = float64(t.StopLoggingTime.Unix())
	}
	if t.LatestDeliveryTime != nil {
		resp["LatestDeliveryTime"] = float64(t.LatestDeliveryTime.Unix())
	}

	return c.JSON(http.StatusOK, resp)
}

// --- PutEventSelectors ---

type putEventSelectorsBody struct {
	TrailName              string                  `json:"TrailName"`
	EventSelectors         []EventSelector         `json:"EventSelectors"`
	AdvancedEventSelectors []AdvancedEventSelector `json:"AdvancedEventSelectors"`
}

func (h *Handler) handlePutEventSelectors(c *echo.Context, body []byte) error {
	var in putEventSelectorsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.TrailName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "TrailName is required"))
	}

	t, err := h.Backend.PutEventSelectors(in.TrailName, in.EventSelectors, in.AdvancedEventSelectors)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyTrailARN: t.TrailARN,
	}
	if len(t.AdvancedEventSelectors) > 0 {
		resp["AdvancedEventSelectors"] = t.AdvancedEventSelectors
	} else {
		selectors := t.EventSelectors
		if selectors == nil {
			selectors = []EventSelector{}
		}
		resp["EventSelectors"] = selectors
	}

	return c.JSON(http.StatusOK, resp)
}

// --- GetEventSelectors ---

type getEventSelectorsBody struct {
	TrailName string `json:"TrailName"`
}

func (h *Handler) handleGetEventSelectors(c *echo.Context, body []byte) error {
	var in getEventSelectorsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	trailARN, selectors, advancedSelectors, err := h.Backend.GetEventSelectors(in.TrailName)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyTrailARN: trailARN,
	}
	if len(advancedSelectors) > 0 {
		resp["AdvancedEventSelectors"] = advancedSelectors
		resp["EventSelectors"] = []EventSelector{}
	} else {
		if selectors == nil {
			selectors = []EventSelector{}
		}
		resp["EventSelectors"] = selectors
	}

	return c.JSON(http.StatusOK, resp)
}

// --- AddTags ---

type addTagsBody struct {
	ResourceID string `json:"ResourceId"`
	TagsList   []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"TagsList"`
}

func (h *Handler) handleAddTags(c *echo.Context, body []byte) error {
	var in addTagsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	kv := make(map[string]string, len(in.TagsList))
	for _, tag := range in.TagsList {
		kv[tag.Key] = tag.Value
	}

	if err := h.Backend.AddTags(in.ResourceID, kv); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- RemoveTags ---

type removeTagsBody struct {
	ResourceID string `json:"ResourceId"`
	TagsList   []struct {
		Key string `json:"Key"`
	} `json:"TagsList"`
}

func (h *Handler) handleRemoveTags(c *echo.Context, body []byte) error {
	var in removeTagsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	keys := make([]string, 0, len(in.TagsList))
	for _, tag := range in.TagsList {
		keys = append(keys, tag.Key)
	}

	if err := h.Backend.RemoveTags(in.ResourceID, keys); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- ListTags ---

type listTagsBody struct {
	ResourceIDList []string `json:"ResourceIdList"`
}

func (h *Handler) handleListTags(c *echo.Context, body []byte) error {
	var in listTagsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	tagsByResource := h.Backend.ListTags(in.ResourceIDList)
	resourceTagList := make([]map[string]any, 0, len(tagsByResource))

	for resourceID, kv := range tagsByResource {
		tagList := make([]map[string]string, 0, len(kv))
		for k, v := range kv {
			tagList = append(tagList, map[string]string{"Key": k, "Value": v})
		}
		resourceTagList = append(resourceTagList, map[string]any{
			"ResourceId": resourceID,
			"TagsList":   tagList,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"ResourceTagList": resourceTagList,
	})
}

// --- ListTrails ---

func (h *Handler) handleListTrails(c *echo.Context, _ []byte) error {
	trails := h.Backend.ListTrails()
	items := make([]map[string]any, 0, len(trails))

	for _, t := range trails {
		items = append(items, map[string]any{
			keyTrailARN:  t.TrailARN,
			keyName:      t.Name,
			"HomeRegion": t.HomeRegion,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"Trails": items})
}

// --- StartQuery ---

type startQueryBody struct {
	QueryStatement string `json:"QueryStatement"`
	EventDataStore string `json:"EventDataStore"`
	DeliveryS3URI  string `json:"DeliveryS3Uri"`
}

func (h *Handler) handleStartQuery(c *echo.Context, body []byte) error {
	var in startQueryBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.QueryStatement == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "QueryStatement is required"),
		)
	}

	q, err := h.Backend.StartQuery(in.QueryStatement, in.EventDataStore, in.DeliveryS3URI)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyQueryID: q.QueryID})
}

// --- LookupEvents ---

type lookupEventsBody struct {
	StartTime        *int64            `json:"StartTime"`
	EndTime          *int64            `json:"EndTime"`
	NextToken        string            `json:"NextToken"`
	LookupAttributes []LookupAttribute `json:"LookupAttributes"`
	MaxResults       int32             `json:"MaxResults"`
}

func (h *Handler) handleLookupEvents(c *echo.Context, body []byte) error {
	var in lookupEventsBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(
				http.StatusBadRequest,
				errResp("InvalidParameterCombinationException", "invalid request body"),
			)
		}
	}

	input := LookupEventsInput{
		LookupAttributes: in.LookupAttributes,
		MaxResults:       in.MaxResults,
		NextToken:        in.NextToken,
	}
	if in.StartTime != nil {
		t := time.Unix(*in.StartTime, 0).UTC()
		input.StartTime = &t
	}
	if in.EndTime != nil {
		t := time.Unix(*in.EndTime, 0).UTC()
		input.EndTime = &t
	}

	out := h.Backend.LookupEvents(input)

	resp := map[string]any{"Events": out.Events}
	if out.NextToken != "" {
		resp["NextToken"] = out.NextToken
	}

	return c.JSON(http.StatusOK, resp)
}

// --- CreateChannel ---

type createChannelBody struct {
	Name         string        `json:"Name"`
	Source       string        `json:"Source"`
	Destinations []Destination `json:"Destinations"`
	Tags         []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
}

func (h *Handler) handleCreateChannel(c *echo.Context, body []byte) error {
	var in createChannelBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	kv := make(map[string]string, len(in.Tags))
	for _, tag := range in.Tags {
		kv[tag.Key] = tag.Value
	}

	ch, err := h.Backend.CreateChannel(in.Name, in.Source, in.Destinations, kv)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyChannelArn:   ch.ChannelARN,
		keyName:         ch.Name,
		keySource:       ch.Source,
		keyDestinations: ch.Destinations,
	})
}

// --- DeleteChannel ---

type deleteChannelBody struct {
	Channel string `json:"Channel"`
}

func (h *Handler) handleDeleteChannel(c *echo.Context, body []byte) error {
	var in deleteChannelBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.DeleteChannel(in.Channel); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- CreateDashboard ---

type createDashboardBody struct {
	Name string `json:"Name"`
	Type string `json:"Type"`
	Tags []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"Tags"`
}

func (h *Handler) handleCreateDashboard(c *echo.Context, body []byte) error {
	var in createDashboardBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	kv := make(map[string]string, len(in.Tags))
	for _, tag := range in.Tags {
		kv[tag.Key] = tag.Value
	}

	d, err := h.Backend.CreateDashboard(in.Name, in.Type, kv)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDashboardArn: d.DashboardARN,
		keyName:         d.Name,
		"Type":          d.Type,
		keyStatus:       d.Status,
	})
}

// --- DeleteDashboard ---

type deleteDashboardBody struct {
	DashboardID string `json:"DashboardId"`
}

func (h *Handler) handleDeleteDashboard(c *echo.Context, body []byte) error {
	var in deleteDashboardBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.DeleteDashboard(in.DashboardID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- CreateEventDataStore ---

type createEventDataStoreBody struct {
	Name                   string                  `json:"Name"`
	BillingMode            string                  `json:"BillingMode"`
	KMSKeyID               string                  `json:"KmsKeyId"`
	AdvancedEventSelectors []AdvancedEventSelector `json:"AdvancedEventSelectors"`
	Tags                   []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"TagsList"`
	RetentionPeriod              int32 `json:"RetentionPeriod"`
	MultiRegionEnabled           bool  `json:"MultiRegionEnabled"`
	OrganizationEnabled          bool  `json:"OrganizationEnabled"`
	TerminationProtectionEnabled bool  `json:"TerminationProtectionEnabled"`
}

func (h *Handler) handleCreateEventDataStore(c *echo.Context, body []byte) error {
	var in createEventDataStoreBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	kv := make(map[string]string, len(in.Tags))
	for _, tag := range in.Tags {
		kv[tag.Key] = tag.Value
	}

	eds, err := h.Backend.CreateEventDataStore(
		in.Name,
		in.MultiRegionEnabled,
		in.OrganizationEnabled,
		in.TerminationProtectionEnabled,
		in.RetentionPeriod,
		in.AdvancedEventSelectors,
		in.BillingMode,
		in.KMSKeyID,
		kv,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, edsToMap(eds))
}

// --- DeleteEventDataStore ---

type deleteEventDataStoreBody struct {
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleDeleteEventDataStore(c *echo.Context, body []byte) error {
	var in deleteEventDataStoreBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.DeleteEventDataStore(in.EventDataStore); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- DeleteResourcePolicy ---

type deleteResourcePolicyBody struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleDeleteResourcePolicy(c *echo.Context, body []byte) error {
	var in deleteResourcePolicyBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.DeleteResourcePolicy(in.ResourceArn); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- DeregisterOrganizationDelegatedAdmin ---

type deregisterOrgDelegatedAdminBody struct {
	DelegatedAdminAccountID string `json:"DelegatedAdminAccountId"`
}

func (h *Handler) handleDeregisterOrganizationDelegatedAdmin(c *echo.Context, body []byte) error {
	var in deregisterOrgDelegatedAdminBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.DeregisterOrganizationDelegatedAdmin(in.DelegatedAdminAccountID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- CancelQuery ---

type cancelQueryBody struct {
	QueryID        string `json:"QueryId"`
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleCancelQuery(c *echo.Context, body []byte) error {
	var in cancelQueryBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.QueryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "QueryId is required"))
	}

	q, err := h.Backend.CancelQuery(in.QueryID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyQueryID:     q.QueryID,
		keyQueryStatus: q.QueryStatus,
	})
}

// --- DescribeQuery ---

type describeQueryBody struct {
	QueryID        string `json:"QueryId"`
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleDescribeQuery(c *echo.Context, body []byte) error {
	var in describeQueryBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.QueryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "QueryId is required"))
	}

	q, err := h.Backend.DescribeQuery(in.QueryID)
	if err != nil {
		return h.handleError(c, err)
	}

	resp := map[string]any{
		keyQueryID:     q.QueryID,
		"QueryString":  q.QueryString,
		keyQueryStatus: q.QueryStatus,
	}
	if q.DeliveryS3URI != "" {
		resp["DeliveryS3Uri"] = q.DeliveryS3URI
	}
	if q.ErrorMessage != "" {
		resp["ErrorMessage"] = q.ErrorMessage
	}

	return c.JSON(http.StatusOK, resp)
}

// --- GetEventDataStore ---

type getEventDataStoreBody struct {
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleGetEventDataStore(c *echo.Context, body []byte) error {
	var in getEventDataStoreBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	eds, err := h.Backend.GetEventDataStore(in.EventDataStore)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, edsToMap(eds))
}

// --- UpdateEventDataStore ---

type updateEventDataStoreBody struct {
	RetentionPeriod              *int32                  `json:"RetentionPeriod"`
	MultiRegionEnabled           *bool                   `json:"MultiRegionEnabled"`
	OrganizationEnabled          *bool                   `json:"OrganizationEnabled"`
	TerminationProtectionEnabled *bool                   `json:"TerminationProtectionEnabled"`
	EventDataStore               string                  `json:"EventDataStore"`
	Name                         string                  `json:"Name"`
	BillingMode                  string                  `json:"BillingMode"`
	KMSKeyID                     string                  `json:"KmsKeyId"`
	AdvancedEventSelectors       []AdvancedEventSelector `json:"AdvancedEventSelectors"`
}

func (h *Handler) handleUpdateEventDataStore(c *echo.Context, body []byte) error {
	var in updateEventDataStoreBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	eds, err := h.Backend.UpdateEventDataStore(
		in.EventDataStore, in.Name,
		in.MultiRegionEnabled, in.OrganizationEnabled, in.TerminationProtectionEnabled,
		in.RetentionPeriod,
		in.AdvancedEventSelectors,
		in.BillingMode,
		in.KMSKeyID,
	)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, edsToMap(eds))
}

// --- ListEventDataStores ---

func (h *Handler) handleListEventDataStores(c *echo.Context, _ []byte) error {
	list := h.Backend.ListEventDataStores()
	items := make([]map[string]any, 0, len(list))
	for _, eds := range list {
		items = append(items, edsToMap(eds))
	}

	return c.JSON(http.StatusOK, map[string]any{"EventDataStores": items})
}

// --- RestoreEventDataStore ---

type restoreEventDataStoreBody struct {
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleRestoreEventDataStore(c *echo.Context, body []byte) error {
	var in restoreEventDataStoreBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	eds, err := h.Backend.RestoreEventDataStore(in.EventDataStore)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, edsToMap(eds))
}

// --- StartEventDataStoreIngestion ---

type startEventDataStoreIngestionBody struct {
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleStartEventDataStoreIngestion(c *echo.Context, body []byte) error {
	var in startEventDataStoreIngestionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.StartEventDataStoreIngestion(in.EventDataStore); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- StopEventDataStoreIngestion ---

type stopEventDataStoreIngestionBody struct {
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleStopEventDataStoreIngestion(c *echo.Context, body []byte) error {
	var in stopEventDataStoreIngestionBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.StopEventDataStoreIngestion(in.EventDataStore); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- GetChannel ---

type getChannelBody struct {
	Channel string `json:"Channel"`
}

func (h *Handler) handleGetChannel(c *echo.Context, body []byte) error {
	var in getChannelBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	ch, err := h.Backend.GetChannel(in.Channel)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyChannelArn:   ch.ChannelARN,
		keyName:         ch.Name,
		keySource:       ch.Source,
		keyDestinations: ch.Destinations,
	})
}

// --- UpdateChannel ---

type updateChannelBody struct {
	Channel      string        `json:"Channel"`
	Destinations []Destination `json:"Destinations"`
}

func (h *Handler) handleUpdateChannel(c *echo.Context, body []byte) error {
	var in updateChannelBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	ch, err := h.Backend.UpdateChannel(in.Channel, in.Destinations)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyChannelArn:   ch.ChannelARN,
		keyName:         ch.Name,
		keySource:       ch.Source,
		keyDestinations: ch.Destinations,
	})
}

// --- ListChannels ---

func (h *Handler) handleListChannels(c *echo.Context, _ []byte) error {
	list := h.Backend.ListChannels()
	items := make([]map[string]any, 0, len(list))
	for _, ch := range list {
		items = append(items, map[string]any{
			keyChannelArn: ch.ChannelARN,
			keyName:       ch.Name,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"Channels": items})
}

// --- GetDashboard ---

type getDashboardBody struct {
	DashboardID string `json:"DashboardId"`
}

func (h *Handler) handleGetDashboard(c *echo.Context, body []byte) error {
	var in getDashboardBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	d, err := h.Backend.GetDashboard(in.DashboardID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, dashToMap(d))
}

// --- UpdateDashboard ---

type updateDashboardBody struct {
	DashboardID string `json:"DashboardId"`
	Name        string `json:"Name"`
}

func (h *Handler) handleUpdateDashboard(c *echo.Context, body []byte) error {
	var in updateDashboardBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	d, err := h.Backend.UpdateDashboard(in.DashboardID, in.Name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, dashToMap(d))
}

// --- ListDashboards ---

func (h *Handler) handleListDashboards(c *echo.Context, _ []byte) error {
	list := h.Backend.ListDashboards()
	items := make([]map[string]any, 0, len(list))
	for _, d := range list {
		items = append(items, dashToMap(d))
	}

	return c.JSON(http.StatusOK, map[string]any{"Dashboards": items})
}

// --- StartDashboardRefresh ---

type startDashboardRefreshBody struct {
	DashboardID string `json:"DashboardId"`
}

func (h *Handler) handleStartDashboardRefresh(c *echo.Context, body []byte) error {
	var in startDashboardRefreshBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	d, err := h.Backend.StartDashboardRefresh(in.DashboardID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyDashboardArn: d.DashboardARN,
		keyStatus:       d.Status,
	})
}

// --- GetQueryResults ---

type getQueryResultsBody struct {
	QueryID        string `json:"QueryId"`
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleGetQueryResults(c *echo.Context, body []byte) error {
	var in getQueryResultsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.QueryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "QueryId is required"))
	}

	q, err := h.Backend.GetQueryResults(in.QueryID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyQueryID:        q.QueryID,
		keyQueryStatus:    q.QueryStatus,
		"QueryResultRows": []any{},
	})
}

// --- ListQueries ---

func (h *Handler) handleListQueries(c *echo.Context, _ []byte) error {
	list := h.Backend.ListQueries()
	items := make([]map[string]any, 0, len(list))
	for _, q := range list {
		items = append(items, map[string]any{
			keyQueryID:     q.QueryID,
			keyQueryStatus: q.QueryStatus,
			"CreationTime": q.CreationTime,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"Queries": items})
}

// --- StartImport ---

type startImportBody struct {
	ImportSource struct {
		S3 *struct {
			S3LocationURI string `json:"S3LocationUri"`
		} `json:"S3"`
	} `json:"ImportSource"`
	Destinations []string `json:"Destinations"`
}

func (h *Handler) handleStartImport(c *echo.Context, body []byte) error {
	var in startImportBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	var src string
	if in.ImportSource.S3 != nil {
		src = in.ImportSource.S3.S3LocationURI
	}

	imp, err := h.Backend.StartImport(in.Destinations, src)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyImportID:     imp.ImportID,
		keyImportStatus: imp.ImportStatus,
		keyDestinations: imp.Destinations,
	})
}

// --- GetImport ---

type getImportBody struct {
	ImportID string `json:"ImportId"`
}

func (h *Handler) handleGetImport(c *echo.Context, body []byte) error {
	var in getImportBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	imp, err := h.Backend.GetImport(in.ImportID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyImportID:     imp.ImportID,
		keyImportStatus: imp.ImportStatus,
		keyDestinations: imp.Destinations,
	})
}

// --- ListImports ---

func (h *Handler) handleListImports(c *echo.Context, _ []byte) error {
	list := h.Backend.ListImports()
	items := make([]map[string]any, 0, len(list))
	for _, imp := range list {
		items = append(items, map[string]any{
			keyImportID:     imp.ImportID,
			keyImportStatus: imp.ImportStatus,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"Imports": items})
}

// --- StopImport ---

type stopImportBody struct {
	ImportID string `json:"ImportId"`
}

func (h *Handler) handleStopImport(c *echo.Context, body []byte) error {
	var in stopImportBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	imp, err := h.Backend.StopImport(in.ImportID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyImportID:     imp.ImportID,
		keyImportStatus: imp.ImportStatus,
	})
}

// --- ListImportFailures ---

type listImportFailuresBody struct {
	ImportID string `json:"ImportId"`
}

func (h *Handler) handleListImportFailures(c *echo.Context, body []byte) error {
	var in listImportFailuresBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	failures := h.Backend.ListImportFailures(in.ImportID)

	return c.JSON(http.StatusOK, map[string]any{"Failures": failures})
}

// --- PutInsightSelectors ---

type putInsightSelectorsBody struct {
	TrailName        string            `json:"TrailName"`
	InsightSelectors []InsightSelector `json:"InsightSelectors"`
}

func (h *Handler) handlePutInsightSelectors(c *echo.Context, body []byte) error {
	var in putInsightSelectorsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.TrailName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "TrailName is required"))
	}

	t, err := h.Backend.PutInsightSelectors(in.TrailName, in.InsightSelectors)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyTrailARN:        t.TrailARN,
		"InsightSelectors": t.InsightSelectors,
	})
}

// --- GetInsightSelectors ---

type getInsightSelectorsBody struct {
	TrailName string `json:"TrailName"`
}

func (h *Handler) handleGetInsightSelectors(c *echo.Context, body []byte) error {
	var in getInsightSelectorsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.TrailName == "" {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "TrailName is required"))
	}

	trailARN, selectors, err := h.Backend.GetInsightSelectors(in.TrailName)
	if err != nil {
		return h.handleError(c, err)
	}

	if selectors == nil {
		selectors = []InsightSelector{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyTrailARN:        trailARN,
		"InsightSelectors": selectors,
	})
}

// --- GetResourcePolicy ---

type getResourcePolicyBody struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleGetResourcePolicy(c *echo.Context, body []byte) error {
	var in getResourcePolicyBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	rp, err := h.Backend.GetResourcePolicy(in.ResourceArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyResourceArn:   rp.ResourceARN,
		"ResourcePolicy": rp.ResourcePolicy,
	})
}

// --- PutResourcePolicy ---

type putResourcePolicyBody struct {
	ResourceArn    string `json:"ResourceArn"`
	ResourcePolicy string `json:"ResourcePolicy"`
}

func (h *Handler) handlePutResourcePolicy(c *echo.Context, body []byte) error {
	var in putResourcePolicyBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	rp := h.Backend.PutResourcePolicy(in.ResourceArn, in.ResourcePolicy)

	return c.JSON(http.StatusOK, map[string]any{
		keyResourceArn:   rp.ResourceARN,
		"ResourcePolicy": rp.ResourcePolicy,
	})
}

// --- RegisterOrganizationDelegatedAdmin ---

type registerOrgDelegatedAdminBody struct {
	MemberAccountID string `json:"MemberAccountId"`
}

func (h *Handler) handleRegisterOrganizationDelegatedAdmin(c *echo.Context, body []byte) error {
	var in registerOrgDelegatedAdminBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.RegisterOrganizationDelegatedAdmin(in.MemberAccountID); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- DisableFederation ---

type disableFederationBody struct {
	EventDataStore string `json:"EventDataStore"`
}

func (h *Handler) handleDisableFederation(c *echo.Context, body []byte) error {
	var in disableFederationBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.EventDataStore == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "EventDataStore is required"),
		)
	}

	eds, err := h.Backend.DisableFederation(in.EventDataStore)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyEDSArn:          eds.EventDataStoreARN,
		"FederationStatus": eds.FederationStatus,
	})
}

// --- EnableFederation ---

type enableFederationBody struct {
	EventDataStore    string `json:"EventDataStore"`
	FederationRoleArn string `json:"FederationRoleArn"`
}

func (h *Handler) handleEnableFederation(c *echo.Context, body []byte) error {
	var in enableFederationBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if in.EventDataStore == "" {
		return c.JSON(
			http.StatusBadRequest,
			errResp("InvalidParameterCombinationException", "EventDataStore is required"),
		)
	}

	eds, err := h.Backend.EnableFederation(in.EventDataStore, in.FederationRoleArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyEDSArn:           eds.EventDataStoreARN,
		"FederationStatus":  eds.FederationStatus,
		"FederationRoleArn": eds.FederationRoleArn,
	})
}

// --- GenerateQuery ---

type generateQueryBody struct {
	EventDataStores          []string `json:"EventDataStores"`
	RequestedQueryMaxResults int32    `json:"RequestedQueryMaxResults"`
}

func (h *Handler) handleGenerateQuery(c *echo.Context, body []byte) error {
	var in generateQueryBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	maxResults := in.RequestedQueryMaxResults
	if maxResults == 0 {
		maxResults = 1000
	}

	q, err := h.Backend.GenerateQuery(in.EventDataStores, maxResults)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyQueryID:    q.QueryID,
		"QueryString": q.QueryString,
	})
}

// --- GetEventConfiguration ---

type getEventConfigurationBody struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handleGetEventConfiguration(c *echo.Context, body []byte) error {
	var in getEventConfigurationBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	result := h.Backend.GetEventConfiguration(in.ResourceArn)

	return c.JSON(http.StatusOK, result)
}

// --- PutEventConfiguration ---

type putEventConfigurationBody struct {
	ResourceArn string `json:"ResourceArn"`
}

func (h *Handler) handlePutEventConfiguration(c *echo.Context, body []byte) error {
	var in putEventConfigurationBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	if err := h.Backend.PutEventConfiguration(in.ResourceArn); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- SearchSampleQueries ---

func (h *Handler) handleSearchSampleQueries(c *echo.Context, _ []byte) error {
	results := h.Backend.SearchSampleQueries()

	return c.JSON(http.StatusOK, map[string]any{"SampleQueries": results})
}

// --- ListPublicKeys ---

func (h *Handler) handleListPublicKeys(c *echo.Context, _ []byte) error {
	keys := h.Backend.ListPublicKeys()

	return c.JSON(http.StatusOK, map[string]any{"PublicKeyList": keys})
}

// --- ListInsightsData ---

func (h *Handler) handleListInsightsData(c *echo.Context, _ []byte) error {
	data := h.Backend.ListInsightsData()

	return c.JSON(http.StatusOK, map[string]any{"Insights": data})
}

// --- ListInsightsMetricData ---

func (h *Handler) handleListInsightsMetricData(c *echo.Context, _ []byte) error {
	data := h.Backend.ListInsightsMetricData()

	return c.JSON(http.StatusOK, map[string]any{"Values": data})
}

// edsToMap converts an EventDataStore to the JSON map used in API responses.
func edsToMap(eds *EventDataStore) map[string]any {
	m := map[string]any{
		keyEDSArn:                      eds.EventDataStoreARN,
		keyName:                        eds.Name,
		keyStatus:                      eds.Status,
		"MultiRegionEnabled":           eds.MultiRegionEnabled,
		"OrganizationEnabled":          eds.OrganizationEnabled,
		"TerminationProtectionEnabled": eds.TerminationProtected,
		"RetentionPeriod":              eds.RetentionPeriod,
		"CreatedTimestamp":             eds.CreatedTimestamp,
		"UpdatedTimestamp":             eds.UpdatedTimestamp,
	}
	if eds.BillingMode != "" {
		m["BillingMode"] = eds.BillingMode
	}
	if eds.FederationStatus != "" {
		m["FederationStatus"] = eds.FederationStatus
	}
	if eds.FederationRoleArn != "" {
		m["FederationRoleArn"] = eds.FederationRoleArn
	}
	if eds.KMSKeyID != "" {
		m["KmsKeyId"] = eds.KMSKeyID
	}
	if len(eds.AdvancedEventSelectors) > 0 {
		m["AdvancedEventSelectors"] = eds.AdvancedEventSelectors
	}
	if len(eds.InsightSelectors) > 0 {
		m["InsightSelectors"] = eds.InsightSelectors
	}

	return m
}

// dashToMap converts a Dashboard to the JSON map used in API responses.
func dashToMap(d *Dashboard) map[string]any {
	return map[string]any{
		keyDashboardArn: d.DashboardARN,
		keyName:         d.Name,
		"Type":          d.Type,
		keyStatus:       d.Status,
	}
}

// trailToMap converts a Trail to the JSON map used in API responses.
func trailToMap(t *Trail) map[string]any {
	m := map[string]any{
		keyName:                      t.Name,
		"S3BucketName":               t.S3BucketName,
		keyTrailARN:                  t.TrailARN,
		"HomeRegion":                 t.HomeRegion,
		"IncludeGlobalServiceEvents": t.IncludeGlobalServiceEvents,
		"IsMultiRegionTrail":         t.IsMultiRegionTrail,
		"LogFileValidationEnabled":   t.LogFileValidationEnabled,
		"HasCustomEventSelectors":    t.HasCustomEventSelectors,
		"HasInsightSelectors":        t.HasInsightSelectors,
		"IsOrganizationTrail":        t.IsOrganizationTrail,
	}
	if t.S3KeyPrefix != "" {
		m["S3KeyPrefix"] = t.S3KeyPrefix
	}
	if t.SnsTopicName != "" {
		m["SnsTopicName"] = t.SnsTopicName
		m["SnsTopicARN"] = t.SnsTopicARN
	}
	if t.CloudWatchLogsLogGroupARN != "" {
		m["CloudWatchLogsLogGroupArn"] = t.CloudWatchLogsLogGroupARN
	}
	if t.CloudWatchLogsRoleARN != "" {
		m["CloudWatchLogsRoleArn"] = t.CloudWatchLogsRoleARN
	}
	if t.KMSKeyID != "" {
		m["KMSKeyId"] = t.KMSKeyID
	}

	return m
}
