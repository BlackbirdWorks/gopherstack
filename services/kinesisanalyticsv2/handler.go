package kinesisanalyticsv2

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// targetPrefix is the X-Amz-Target prefix for the Kinesis Analytics v2 JSON protocol.
	targetPrefix = "KinesisAnalytics_20180523."
)

// Handler is the HTTP handler for the Kinesis Data Analytics v2 JSON API.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Kinesis Data Analytics v2 handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "KinesisAnalyticsV2" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApplication",
		"DescribeApplication",
		"ListApplications",
		"UpdateApplication",
		"DeleteApplication",
		"StartApplication",
		"StopApplication",
		"CreateApplicationSnapshot",
		"DescribeApplicationSnapshot",
		"ListApplicationSnapshots",
		"DeleteApplicationSnapshot",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "kinesisanalyticsv2" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Kinesis Data Analytics v2 requests.
// The SDK uses X-Amz-Target: KinesisAnalytics_20180523.{Operation} with POST to /.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), targetPrefix)
}

// ExtractResource extracts the application name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		ApplicationName string `json:"ApplicationName"`
	}

	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.ApplicationName
}

// Handler returns the Echo handler function for Kinesis Data Analytics v2 requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		op := h.ExtractOperation(c)
		if op == "" {
			return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "missing X-Amz-Target header")
		}

		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "kinesisanalyticsv2: failed to read request body", "error", err)

			return h.writeError(c, http.StatusInternalServerError, "InternalFailure", "failed to read request body")
		}

		log.DebugContext(ctx, "kinesisanalyticsv2 request", "op", op)

		return h.dispatch(c, op, body)
	}
}

// dispatch routes a parsed operation to the appropriate handler.
//

func (h *Handler) dispatch(c *echo.Context, op string, body []byte) error {
	switch op {
	case "CreateApplication":
		return h.handleCreateApplication(c, body)
	case "DescribeApplication":
		return h.handleDescribeApplication(c, body)
	case "ListApplications":
		return h.handleListApplications(c)
	case "UpdateApplication":
		return h.handleUpdateApplication(c, body)
	case "DeleteApplication":
		return h.handleDeleteApplication(c, body)
	case "StartApplication":
		return h.handleStartApplication(c, body)
	case "StopApplication":
		return h.handleStopApplication(c, body)
	case "CreateApplicationSnapshot":
		return h.handleCreateApplicationSnapshot(c, body)
	case "DescribeApplicationSnapshot":
		return h.handleDescribeApplicationSnapshot(c, body)
	case "ListApplicationSnapshots":
		return h.handleListApplicationSnapshots(c, body)
	case "DeleteApplicationSnapshot":
		return h.handleDeleteApplicationSnapshot(c, body)
	case "TagResource":
		return h.handleTagResource(c, body)
	case "UntagResource":
		return h.handleUntagResource(c, body)
	case "ListTagsForResource":
		return h.handleListTagsForResource(c, body)
	}

	return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "unknown operation: "+op)
}

// ----------------------------------------
// Request / response types
// ----------------------------------------

type createApplicationInput struct {
	ApplicationName        string `json:"ApplicationName"`
	RuntimeEnvironment     string `json:"RuntimeEnvironment"`
	ServiceExecutionRole   string `json:"ServiceExecutionRole,omitempty"`
	ApplicationDescription string `json:"ApplicationDescription,omitempty"`
	ApplicationMode        string `json:"ApplicationMode,omitempty"`
	Tags                   []Tag  `json:"Tags,omitempty"`
}

type applicationDetailOutput struct {
	ApplicationARN         string  `json:"ApplicationARN"`
	ApplicationName        string  `json:"ApplicationName"`
	ApplicationStatus      string  `json:"ApplicationStatus"`
	RuntimeEnvironment     string  `json:"RuntimeEnvironment"`
	ServiceExecutionRole   string  `json:"ServiceExecutionRole,omitempty"`
	ApplicationDescription string  `json:"ApplicationDescription,omitempty"`
	ApplicationMode        string  `json:"ApplicationMode,omitempty"`
	Tags                   []Tag   `json:"Tags,omitempty"`
	ApplicationVersionID   int64   `json:"ApplicationVersionId"`
	CreateTimestamp        float64 `json:"CreateTimestamp"`
}

type createApplicationOutput struct {
	ApplicationDetail applicationDetailOutput `json:"ApplicationDetail"`
}

type describeApplicationInput struct {
	ApplicationName string `json:"ApplicationName"`
}

type describeApplicationOutput struct {
	ApplicationDetail applicationDetailOutput `json:"ApplicationDetail"`
}

type listApplicationsOutput struct {
	ApplicationSummaries []applicationSummary `json:"ApplicationSummaries"`
}

type updateApplicationInput struct {
	ApplicationName             string `json:"ApplicationName"`
	ServiceExecutionRoleUpdate  string `json:"ServiceExecutionRoleUpdate,omitempty"`
	ApplicationDescription      string `json:"ApplicationDescription,omitempty"`
	CurrentApplicationVersionID int64  `json:"CurrentApplicationVersionId"`
}

type updateApplicationOutput struct {
	ApplicationDetail applicationDetailOutput `json:"ApplicationDetail"`
}

type deleteApplicationInput struct {
	ApplicationName string      `json:"ApplicationName"`
	CreateTimestamp json.Number `json:"CreateTimestamp,omitempty"`
}

type startStopApplicationInput struct {
	ApplicationName string `json:"ApplicationName"`
}

type createSnapshotInput struct {
	ApplicationName string `json:"ApplicationName"`
	SnapshotName    string `json:"SnapshotName"`
}

type describeSnapshotInput struct {
	ApplicationName string `json:"ApplicationName"`
	SnapshotName    string `json:"SnapshotName"`
}

type describeSnapshotOutput struct {
	SnapshotDetails snapshotDetail `json:"SnapshotDetails"`
}

type listSnapshotsInput struct {
	ApplicationName string `json:"ApplicationName"`
}

type listSnapshotsOutput struct {
	SnapshotSummaries []snapshotDetail `json:"SnapshotSummaries"`
}

type deleteSnapshotInput struct {
	ApplicationName           string      `json:"ApplicationName"`
	SnapshotName              string      `json:"SnapshotName"`
	SnapshotCreationTimestamp json.Number `json:"SnapshotCreationTimestamp,omitempty"`
}

type tagResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
	Tags        []Tag  `json:"Tags"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type listTagsInput struct {
	ResourceARN string `json:"ResourceARN"`
}

type listTagsOutput struct {
	Tags []Tag `json:"Tags"`
}

type errorResponse struct {
	Message string `json:"message"`
	Code    string `json:"__type"`
}

// ----------------------------------------
// Application handlers
// ----------------------------------------

func (h *Handler) handleCreateApplication(c *echo.Context, body []byte) error {
	var in createApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	app, err := h.Backend.CreateApplication(
		in.ApplicationName,
		in.RuntimeEnvironment,
		in.ServiceExecutionRole,
		in.ApplicationDescription,
		in.ApplicationMode,
		in.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createApplicationOutput{
		ApplicationDetail: toDetailOutput(app),
	})
}

func (h *Handler) handleDescribeApplication(c *echo.Context, body []byte) error {
	var in describeApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	app, err := h.Backend.DescribeApplication(in.ApplicationName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeApplicationOutput{
		ApplicationDetail: toDetailOutput(app),
	})
}

func (h *Handler) handleListApplications(c *echo.Context) error {
	apps := h.Backend.ListApplications()
	summaries := make([]applicationSummary, 0, len(apps))

	for _, app := range apps {
		summaries = append(summaries, toSummary(app))
	}

	return c.JSON(http.StatusOK, listApplicationsOutput{ApplicationSummaries: summaries})
}

func (h *Handler) handleUpdateApplication(c *echo.Context, body []byte) error {
	var in updateApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	app, err := h.Backend.UpdateApplication(
		in.ApplicationName,
		in.ServiceExecutionRoleUpdate,
		in.ApplicationDescription,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateApplicationOutput{
		ApplicationDetail: toDetailOutput(app),
	})
}

func (h *Handler) handleDeleteApplication(c *echo.Context, body []byte) error {
	var in deleteApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplication(in.ApplicationName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleStartApplication(c *echo.Context, body []byte) error {
	var in startStopApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.StartApplication(in.ApplicationName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleStopApplication(c *echo.Context, body []byte) error {
	var in startStopApplicationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.StopApplication(in.ApplicationName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

// ----------------------------------------
// Snapshot handlers
// ----------------------------------------

func (h *Handler) handleCreateApplicationSnapshot(c *echo.Context, body []byte) error {
	var in createSnapshotInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	snap, err := h.Backend.CreateApplicationSnapshot(in.ApplicationName, in.SnapshotName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct {
		SnapshotDetails snapshotDetail `json:"SnapshotDetails"`
	}{SnapshotDetails: toSnapshotDetail(snap)})
}

func (h *Handler) handleDescribeApplicationSnapshot(c *echo.Context, body []byte) error {
	var in describeSnapshotInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	snaps, err := h.Backend.ListApplicationSnapshots(in.ApplicationName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	for _, s := range snaps {
		if s.SnapshotName == in.SnapshotName {
			return c.JSON(http.StatusOK, describeSnapshotOutput{SnapshotDetails: toSnapshotDetail(s)})
		}
	}

	return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "snapshot not found: "+in.SnapshotName)
}

func (h *Handler) handleListApplicationSnapshots(c *echo.Context, body []byte) error {
	var in listSnapshotsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	snaps, err := h.Backend.ListApplicationSnapshots(in.ApplicationName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	details := make([]snapshotDetail, 0, len(snaps))
	for _, s := range snaps {
		details = append(details, toSnapshotDetail(s))
	}

	return c.JSON(http.StatusOK, listSnapshotsOutput{SnapshotSummaries: details})
}

func (h *Handler) handleDeleteApplicationSnapshot(c *echo.Context, body []byte) error {
	var in deleteSnapshotInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.DeleteApplicationSnapshot(in.ApplicationName, in.SnapshotName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

// ----------------------------------------
// Tag handlers
// ----------------------------------------

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var in tagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.TagResource(in.ResourceARN, in.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	var in untagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	var in listTagsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	tags, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsOutput{Tags: tags})
}

// ----------------------------------------
// Helper functions
// ----------------------------------------

// toDetailOutput converts an Application to an API detail output.
func toDetailOutput(app *Application) applicationDetailOutput {
	return applicationDetailOutput{
		ApplicationARN:         app.ApplicationARN,
		ApplicationName:        app.ApplicationName,
		ApplicationStatus:      app.ApplicationStatus,
		RuntimeEnvironment:     app.RuntimeEnvironment,
		ServiceExecutionRole:   app.ServiceExecutionRole,
		ApplicationDescription: app.ApplicationDescription,
		ApplicationMode:        app.ApplicationMode,
		ApplicationVersionID:   app.ApplicationVersionID,
		Tags:                   app.Tags,
		CreateTimestamp:        float64(app.CreatedAt.Unix()),
	}
}

func (h *Handler) writeError(c *echo.Context, status int, code, message string) error {
	return c.JSON(status, errorResponse{
		Message: message,
		Code:    code,
	})
}

func (h *Handler) writeBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return h.writeError(c, http.StatusConflict, "ResourceInUseException", err.Error())
	}

	return h.writeError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
}
