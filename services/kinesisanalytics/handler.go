package kinesisanalytics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	kinesisanalyticsTargetPrefix = "KinesisAnalytics_20150814."
	kinesisanalyticsService      = "kinesisanalytics"
)

var (
	errUnknownAction   = errors.New("unknown action")
	errApplicationName = errors.New("ApplicationName is required")
	errResourceARN     = errors.New("ResourceARN is required")
)

// Handler is the HTTP handler for the Kinesis Analytics v1 API.
type Handler struct {
	Backend       StorageBackend
	AccountID     string
	DefaultRegion string
}

// NewHandler creates a new Kinesis Analytics handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "KinesisAnalytics" }

// GetSupportedOperations returns the list of supported Kinesis Analytics operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApplication",
		"DeleteApplication",
		"DescribeApplication",
		"ListApplications",
		"StartApplication",
		"StopApplication",
		"UpdateApplication",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return kinesisanalyticsService }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher returns a function that matches Kinesis Analytics requests by X-Amz-Target header.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), kinesisanalyticsTargetPrefix)
	}
}

// MatchPriority returns the routing priority for header-based matching.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the Kinesis Analytics action from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, kinesisanalyticsTargetPrefix)

	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

type applicationNameInput struct {
	ApplicationName string `json:"ApplicationName"`
}

// ExtractResource extracts the application name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req applicationNameInput
	_ = json.Unmarshal(body, &req)

	return req.ApplicationName
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"KinesisAnalytics", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateApplication":   service.WrapOp(h.handleCreateApplication),
		"DeleteApplication":   service.WrapOp(h.handleDeleteApplication),
		"DescribeApplication": service.WrapOp(h.handleDescribeApplication),
		"ListApplications":    service.WrapOp(h.handleListApplications),
		"StartApplication":    service.WrapOp(h.handleStartApplication),
		"StopApplication":     service.WrapOp(h.handleStopApplication),
		"UpdateApplication":   service.WrapOp(h.handleUpdateApplication),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownAction, action)
	}

	result, err := fn(ctx, body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(result)
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusNotFound,
			errorResponse{Message: err.Error()})
	case errors.Is(err, awserr.ErrAlreadyExists),
		errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, errorResponse{Message: err.Error()})
	case errors.Is(err, ErrConcurrentUpdate):
		return c.JSON(http.StatusBadRequest, errorResponse{Message: err.Error()})
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse{Message: err.Error()})
	}
}

func (h *Handler) handleCreateApplication(
	_ context.Context,
	in *createApplicationInput,
) (*createApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	tags := make(map[string]string, len(in.Tags))

	for _, t := range in.Tags {
		tags[t.Key] = t.Value
	}

	app, err := h.Backend.CreateApplication(
		h.DefaultRegion,
		h.AccountID,
		in.ApplicationName,
		in.ApplicationDescription,
		in.ApplicationCode,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createApplicationOutput{
		ApplicationSummary: applicationSummary{
			ApplicationARN:    app.ApplicationARN,
			ApplicationName:   app.ApplicationName,
			ApplicationStatus: app.ApplicationStatus,
		},
	}, nil
}

func (h *Handler) handleDeleteApplication(
	_ context.Context,
	in *deleteApplicationInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if err := h.Backend.DeleteApplication(in.ApplicationName, nil); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleDescribeApplication(
	_ context.Context,
	in *describeApplicationInput,
) (*describeApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	app, err := h.Backend.DescribeApplication(in.ApplicationName)
	if err != nil {
		return nil, err
	}

	return &describeApplicationOutput{
		ApplicationDetail: toApplicationDetail(app),
	}, nil
}

func (h *Handler) handleListApplications(
	_ context.Context,
	in *listApplicationsInput,
) (*listApplicationsOutput, error) {
	apps, hasMore := h.Backend.ListApplications(in.ExclusiveStartApplicationName, in.Limit)
	summaries := make([]applicationSummary, 0, len(apps))

	for _, app := range apps {
		summaries = append(summaries, applicationSummary{
			ApplicationARN:    app.ApplicationARN,
			ApplicationName:   app.ApplicationName,
			ApplicationStatus: app.ApplicationStatus,
		})
	}

	return &listApplicationsOutput{
		ApplicationSummaries: summaries,
		HasMoreApplications:  hasMore,
	}, nil
}

func (h *Handler) handleStartApplication(
	_ context.Context,
	in *startApplicationInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if err := h.Backend.StartApplication(in.ApplicationName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleStopApplication(
	_ context.Context,
	in *stopApplicationInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	if err := h.Backend.StopApplication(in.ApplicationName); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleUpdateApplication(
	_ context.Context,
	in *updateApplicationInput,
) (*struct{}, error) {
	if in.ApplicationName == "" {
		return nil, errApplicationName
	}

	var codeUpdate string
	if in.ApplicationUpdate != nil {
		codeUpdate = in.ApplicationUpdate.ApplicationCodeUpdate
	}

	if _, err := h.Backend.UpdateApplication(
		in.ApplicationName,
		in.CurrentApplicationVersionID,
		codeUpdate,
	); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceARN == "" {
		return nil, errResourceARN
	}

	tagMap, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return nil, err
	}

	entries := make([]tagEntry, 0, len(tagMap))

	for k, v := range tagMap {
		entries = append(entries, tagEntry{Key: k, Value: v})
	}

	return &listTagsForResourceOutput{Tags: entries}, nil
}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*struct{}, error) {
	if in.ResourceARN == "" {
		return nil, errResourceARN
	}

	tagMap := make(map[string]string, len(in.Tags))

	for _, t := range in.Tags {
		tagMap[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(in.ResourceARN, tagMap); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*struct{}, error) {
	if in.ResourceARN == "" {
		return nil, errResourceARN
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

// toApplicationDetail converts an Application to the API detail struct.
func toApplicationDetail(app *Application) applicationDetail {
	detail := applicationDetail{
		ApplicationARN:         app.ApplicationARN,
		ApplicationName:        app.ApplicationName,
		ApplicationStatus:      app.ApplicationStatus,
		ApplicationVersionID:   app.ApplicationVersionID,
		ApplicationCode:        app.ApplicationCode,
		ApplicationDescription: app.ApplicationDescription,
	}

	if app.CreateTimestamp != nil {
		detail.CreateTimestamp = float64(app.CreateTimestamp.Unix())
	}

	if app.LastUpdateTimestamp != nil {
		detail.LastUpdateTimestamp = float64(app.LastUpdateTimestamp.Unix())
	}

	return detail
}
