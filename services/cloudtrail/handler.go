package cloudtrail

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	cloudtrailMatchPriority = service.PriorityHeaderExact
	cloudtrailTargetPrefix  = "CloudTrail_20131101."
)

var errInvalidRequest = errors.New("invalid request")

// Handler is the Echo HTTP handler for AWS CloudTrail operations (JSON-1.1 protocol).
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new CloudTrail handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CloudTrail" }

// GetSupportedOperations returns the list of supported CloudTrail operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateTrail",
		"GetTrail",
		"DescribeTrails",
		"UpdateTrail",
		"DeleteTrail",
		"StartLogging",
		"StopLogging",
		"GetTrailStatus",
		"PutEventSelectors",
		"GetEventSelectors",
		"AddTags",
		"RemoveTags",
		"ListTags",
		"ListTrails",
	}
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
func (h *Handler) ExtractResource(c *echo.Context) string {
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

//nolint:cyclop // dispatch table for 14 operations is inherently wide
func (h *Handler) dispatch(c *echo.Context, operation string, body []byte) error {
	switch operation {
	case "CreateTrail":
		return h.handleCreateTrail(c, body)
	case "GetTrail":
		return h.handleGetTrail(c, body)
	case "DescribeTrails":
		return h.handleDescribeTrails(c, body)
	case "UpdateTrail":
		return h.handleUpdateTrail(c, body)
	case "DeleteTrail":
		return h.handleDeleteTrail(c, body)
	case "StartLogging":
		return h.handleStartLogging(c, body)
	case "StopLogging":
		return h.handleStopLogging(c, body)
	case "GetTrailStatus":
		return h.handleGetTrailStatus(c, body)
	case "PutEventSelectors":
		return h.handlePutEventSelectors(c, body)
	case "GetEventSelectors":
		return h.handleGetEventSelectors(c, body)
	case "AddTags":
		return h.handleAddTags(c, body)
	case "RemoveTags":
		return h.handleRemoveTags(c, body)
	case "ListTags":
		return h.handleListTags(c, body)
	case "ListTrails":
		return h.handleListTrails(c)
	default:
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "unknown operation: "+operation))
	}
}

func (h *Handler) handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errResp("TrailNotFoundException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errResp("TrailAlreadyExistsException", err.Error()))
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
	TagsList                   []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"TagsList"`
	Name                        string `json:"Name"`
	S3BucketName                string `json:"S3BucketName"`
	S3KeyPrefix                 string `json:"S3KeyPrefix"`
	SnsTopicName                string `json:"SnsTopicName"`
	CloudWatchLogsLogGroupArn   string `json:"CloudWatchLogsLogGroupArn"`
	CloudWatchLogsRoleArn       string `json:"CloudWatchLogsRoleArn"`
	KMSKeyID                    string `json:"KMSKeyId"`
	IncludeGlobalServiceEvents  bool   `json:"IncludeGlobalServiceEvents"`
	IsMultiRegionTrail          bool   `json:"IsMultiRegionTrail"`
	EnableLogFileValidation     bool   `json:"EnableLogFileValidation"`
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
	TrailNameList           []string `json:"trailNameList"`
	IncludeShadowTrails     bool     `json:"includeShadowTrails"`
}

func (h *Handler) handleDescribeTrails(c *echo.Context, body []byte) error {
	var in describeTrailsBody
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
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
	Name                        string `json:"Name"`
	S3BucketName                string `json:"S3BucketName"`
	S3KeyPrefix                 string `json:"S3KeyPrefix"`
	SnsTopicName                string `json:"SnsTopicName"`
	CloudWatchLogsLogGroupArn   string `json:"CloudWatchLogsLogGroupArn"`
	CloudWatchLogsRoleArn       string `json:"CloudWatchLogsRoleArn"`
	KMSKeyID                    string `json:"KMSKeyId"`
	IncludeGlobalServiceEvents  *bool  `json:"IncludeGlobalServiceEvents"`
	IsMultiRegionTrail          *bool  `json:"IsMultiRegionTrail"`
	EnableLogFileValidation     *bool  `json:"EnableLogFileValidation"`
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

	isLogging, err := h.Backend.GetTrailStatus(in.Name)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"IsLogging": isLogging,
	})
}

// --- PutEventSelectors ---

type putEventSelectorsBody struct {
	TrailName      string          `json:"TrailName"`
	EventSelectors []EventSelector `json:"EventSelectors"`
}

func (h *Handler) handlePutEventSelectors(c *echo.Context, body []byte) error {
	var in putEventSelectorsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	t, err := h.Backend.PutEventSelectors(in.TrailName, in.EventSelectors)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"TrailARN":       t.TrailARN,
		"EventSelectors": t.EventSelectors,
	})
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

	trailARN, selectors, err := h.Backend.GetEventSelectors(in.TrailName)
	if err != nil {
		return h.handleError(c, err)
	}

	if selectors == nil {
		selectors = []EventSelector{}
	}

	return c.JSON(http.StatusOK, map[string]any{
		"TrailARN":       trailARN,
		"EventSelectors": selectors,
	})
}

// --- AddTags ---

type addTagsBody struct {
	TagsList []struct {
		Key   string `json:"Key"`
		Value string `json:"Value"`
	} `json:"TagsList"`
	ResourceId string `json:"ResourceId"`
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

	if err := h.Backend.AddTags(in.ResourceId, kv); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- RemoveTags ---

type removeTagsBody struct {
	TagsList []struct {
		Key string `json:"Key"`
	} `json:"TagsList"`
	ResourceId string `json:"ResourceId"`
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

	if err := h.Backend.RemoveTags(in.ResourceId, keys); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// --- ListTags ---

type listTagsBody struct {
	ResourceIdList []string `json:"ResourceIdList"`
}

func (h *Handler) handleListTags(c *echo.Context, body []byte) error {
	var in listTagsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("InvalidParameterCombinationException", "invalid request body"))
	}

	tagsByResource := h.Backend.ListTags(in.ResourceIdList)
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

func (h *Handler) handleListTrails(c *echo.Context) error {
	trails := h.Backend.ListTrails()
	items := make([]map[string]any, 0, len(trails))

	for _, t := range trails {
		items = append(items, map[string]any{
			"TrailARN":   t.TrailARN,
			"Name":       t.Name,
			"HomeRegion": t.HomeRegion,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{"Trails": items})
}

// trailToMap converts a Trail to the JSON map used in API responses.
func trailToMap(t *Trail) map[string]any {
	m := map[string]any{
		"Name":                        t.Name,
		"S3BucketName":                t.S3BucketName,
		"TrailARN":                    t.TrailARN,
		"HomeRegion":                  t.HomeRegion,
		"IncludeGlobalServiceEvents":  t.IncludeGlobalServiceEvents,
		"IsMultiRegionTrail":          t.IsMultiRegionTrail,
		"LogFileValidationEnabled":    t.LogFileValidationEnabled,
		"HasCustomEventSelectors":     t.HasCustomEventSelectors,
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
