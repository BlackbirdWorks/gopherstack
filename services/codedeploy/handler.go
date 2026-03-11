package codedeploy

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const codedeployTargetPrefix = "CodeDeploy_20141006."

// Handler is the Echo HTTP handler for AWS CodeDeploy operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new CodeDeploy handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "CodeDeploy" }

// GetSupportedOperations returns the list of supported CodeDeploy operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateApplication",
		"GetApplication",
		"ListApplications",
		"DeleteApplication",
		"CreateDeploymentGroup",
		"GetDeploymentGroup",
		"ListDeploymentGroups",
		"DeleteDeploymentGroup",
		"CreateDeployment",
		"GetDeployment",
		"ListDeployments",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "codedeploy" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this CodeDeploy instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches AWS CodeDeploy requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), codedeployTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the CodeDeploy operation from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, codedeployTargetPrefix)
	if action == "" || action == target {
		return "Unknown"
	}

	return action
}

// ExtractResource extracts the application name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, readErr := httputils.ReadBody(c.Request())
	if readErr != nil {
		return ""
	}

	var input struct {
		ApplicationName string `json:"applicationName"`
	}
	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return ""
	}

	return input.ApplicationName
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		log := logger.Load(r.Context())
		action := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), codedeployTargetPrefix)

		body, err := httputils.ReadBody(r)
		if err != nil {
			log.Error("failed to read body", "error", err)

			return c.JSON(
				http.StatusBadRequest,
				errorResponse("InvalidRequestException", "failed to read request body"),
			)
		}

		if handled, result := h.handleApplicationAction(c, action, body); handled {
			return result
		}

		if handled, result := h.handleDeploymentAction(c, action, body); handled {
			return result
		}

		if handled, result := h.handleTagAction(c, action, body); handled {
			return result
		}

		log.Warn("unknown CodeDeploy action", "action", action)

		return c.JSON(http.StatusBadRequest, errorResponse("UnknownOperationException", "unknown action: "+action))
	}
}

// handleApplicationAction routes application and deployment group operations.
func (h *Handler) handleApplicationAction(c *echo.Context, action string, body []byte) (bool, error) {
	switch action {
	case "CreateApplication":
		return true, h.handleCreateApplication(c, body)
	case "GetApplication":
		return true, h.handleGetApplication(c, body)
	case "ListApplications":
		return true, h.handleListApplications(c, body)
	case "DeleteApplication":
		return true, h.handleDeleteApplication(c, body)
	case "CreateDeploymentGroup":
		return true, h.handleCreateDeploymentGroup(c, body)
	case "GetDeploymentGroup":
		return true, h.handleGetDeploymentGroup(c, body)
	case "ListDeploymentGroups":
		return true, h.handleListDeploymentGroups(c, body)
	case "DeleteDeploymentGroup":
		return true, h.handleDeleteDeploymentGroup(c, body)
	}

	return false, nil
}

// handleDeploymentAction routes deployment operations.
func (h *Handler) handleDeploymentAction(c *echo.Context, action string, body []byte) (bool, error) {
	switch action {
	case "CreateDeployment":
		return true, h.handleCreateDeployment(c, body)
	case "GetDeployment":
		return true, h.handleGetDeployment(c, body)
	case "ListDeployments":
		return true, h.handleListDeployments(c, body)
	}

	return false, nil
}

// handleTagAction routes tagging operations.
func (h *Handler) handleTagAction(c *echo.Context, action string, body []byte) (bool, error) {
	switch action {
	case "TagResource":
		return true, h.handleTagResource(c, body)
	case "UntagResource":
		return true, h.handleUntagResource(c, body)
	case "ListTagsForResource":
		return true, h.handleListTagsForResource(c, body)
	}

	return false, nil
}

func (h *Handler) handleCreateApplication(c *echo.Context, body []byte) error {
	var input struct {
		ApplicationName string     `json:"applicationName"`
		ComputePlatform string     `json:"computePlatform"`
		Tags            []tagEntry `json:"tags"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.ApplicationName == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", "applicationName is required"))
	}

	if input.ComputePlatform == "" {
		input.ComputePlatform = "Server"
	}

	app, err := h.Backend.CreateApplication(input.ApplicationName, input.ComputePlatform, tagEntriesToMap(input.Tags))
	if err != nil {
		return handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"applicationId": app.ApplicationID,
	})
}

func (h *Handler) handleGetApplication(c *echo.Context, body []byte) error {
	var input struct {
		ApplicationName string `json:"applicationName"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.ApplicationName == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", "applicationName is required"))
	}

	app, err := h.Backend.GetApplication(input.ApplicationName)
	if err != nil {
		return handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"application": map[string]any{
			"applicationId":   app.ApplicationID,
			"applicationName": app.ApplicationName,
			"computePlatform": app.ComputePlatform,
			"createTime":      app.CreationTime.UnixMilli(),
		},
	})
}

func (h *Handler) handleListApplications(c *echo.Context, _ []byte) error {
	names := h.Backend.ListApplications()

	return c.JSON(http.StatusOK, map[string]any{
		"applications": names,
	})
}

func (h *Handler) handleDeleteApplication(c *echo.Context, body []byte) error {
	var input struct {
		ApplicationName string `json:"applicationName"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.ApplicationName == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", "applicationName is required"))
	}

	if err := h.Backend.DeleteApplication(input.ApplicationName); err != nil {
		return handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleCreateDeploymentGroup(c *echo.Context, body []byte) error {
	var input struct {
		ApplicationName      string     `json:"applicationName"`
		DeploymentGroupName  string     `json:"deploymentGroupName"`
		ServiceRoleArn       string     `json:"serviceRoleArn"`
		DeploymentConfigName string     `json:"deploymentConfigName"`
		Tags                 []tagEntry `json:"tags"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", "invalid request"))
	}

	if input.ApplicationName == "" || input.DeploymentGroupName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("InvalidRequestException", "applicationName and deploymentGroupName are required"),
		)
	}

	dg, err := h.Backend.CreateDeploymentGroup(
		input.ApplicationName,
		input.DeploymentGroupName,
		input.ServiceRoleArn,
		input.DeploymentConfigName,
		tagEntriesToMap(input.Tags),
	)
	if err != nil {
		return handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"deploymentGroupId": dg.DeploymentGroupID,
	})
}

func (h *Handler) handleGetDeploymentGroup(c *echo.Context, body []byte) error {
	var input struct {
		ApplicationName     string `json:"applicationName"`
		DeploymentGroupName string `json:"deploymentGroupName"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", "invalid request"))
	}

	dg, err := h.Backend.GetDeploymentGroup(input.ApplicationName, input.DeploymentGroupName)
	if err != nil {
		return handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"deploymentGroupInfo": map[string]any{
			"applicationName":      dg.ApplicationName,
			"deploymentGroupId":    dg.DeploymentGroupID,
			"deploymentGroupName":  dg.DeploymentGroupName,
			"serviceRoleArn":       dg.ServiceRoleArn,
			"deploymentConfigName": dg.DeploymentConfigName,
		},
	})
}

func (h *Handler) handleListDeploymentGroups(c *echo.Context, body []byte) error {
	var input struct {
		ApplicationName string `json:"applicationName"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.ApplicationName == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", "applicationName is required"))
	}

	names, err := h.Backend.ListDeploymentGroups(input.ApplicationName)
	if err != nil {
		return handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"applicationName":  input.ApplicationName,
		"deploymentGroups": names,
	})
}

func (h *Handler) handleDeleteDeploymentGroup(c *echo.Context, body []byte) error {
	var input struct {
		ApplicationName     string `json:"applicationName"`
		DeploymentGroupName string `json:"deploymentGroupName"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", "invalid request"))
	}

	if err := h.Backend.DeleteDeploymentGroup(input.ApplicationName, input.DeploymentGroupName); err != nil {
		return handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleCreateDeployment(c *echo.Context, body []byte) error {
	var input struct {
		ApplicationName     string `json:"applicationName"`
		DeploymentGroupName string `json:"deploymentGroupName"`
		Description         string `json:"description"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", "invalid request"))
	}

	if input.ApplicationName == "" || input.DeploymentGroupName == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("InvalidRequestException", "applicationName and deploymentGroupName are required"),
		)
	}

	d, err := h.Backend.CreateDeployment(input.ApplicationName, input.DeploymentGroupName, input.Description, "user")
	if err != nil {
		return handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]string{
		"deploymentId": d.DeploymentID,
	})
}

func (h *Handler) handleGetDeployment(c *echo.Context, body []byte) error {
	var input struct {
		DeploymentID string `json:"deploymentId"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.DeploymentID == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", "deploymentId is required"))
	}

	d, err := h.Backend.GetDeployment(input.DeploymentID)
	if err != nil {
		return handleBackendError(c, err)
	}

	deployInfo := map[string]any{
		"deploymentId":        d.DeploymentID,
		"applicationName":     d.ApplicationName,
		"deploymentGroupName": d.DeploymentGroupName,
		"status":              d.Status,
		"creator":             d.Creator,
		"createTime":          d.CreateTime.UnixMilli(),
	}
	if d.Description != "" {
		deployInfo["description"] = d.Description
	}
	if d.CompleteTime != nil {
		deployInfo["completeTime"] = d.CompleteTime.UnixMilli()
	}

	return c.JSON(http.StatusOK, map[string]any{
		"deploymentInfo": deployInfo,
	})
}

func (h *Handler) handleListDeployments(c *echo.Context, body []byte) error {
	var input struct {
		ApplicationName     string `json:"applicationName"`
		DeploymentGroupName string `json:"deploymentGroupName"`
	}
	if err := json.Unmarshal(body, &input); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", "invalid request"))
	}

	ids := h.Backend.ListDeployments(input.ApplicationName, input.DeploymentGroupName)

	return c.JSON(http.StatusOK, map[string]any{
		"deployments": ids,
	})
}

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var input struct {
		ResourceArn string     `json:"resourceArn"`
		Tags        []tagEntry `json:"tags"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.ResourceArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", "resourceArn is required"))
	}

	if err := h.Backend.TagResource(input.ResourceArn, tagEntriesToMap(input.Tags)); err != nil {
		return handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	var input struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.ResourceArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", "resourceArn is required"))
	}

	if err := h.Backend.UntagResource(input.ResourceArn, input.TagKeys); err != nil {
		return handleBackendError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	var input struct {
		ResourceArn string `json:"resourceArn"`
	}
	if err := json.Unmarshal(body, &input); err != nil || input.ResourceArn == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", "resourceArn is required"))
	}

	kv, err := h.Backend.ListTagsForResource(input.ResourceArn)
	if err != nil {
		return handleBackendError(c, err)
	}

	entries := make([]tagEntry, 0, len(kv))
	for k, v := range kv {
		entries = append(entries, tagEntry{Key: k, Value: v})
	}

	return c.JSON(http.StatusOK, map[string]any{
		"tags": entries,
	})
}

// tagEntry is a key-value tag pair for JSON (de)serialization.
type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// tagEntriesToMap converts a slice of tag entries to a map.
func tagEntriesToMap(entries []tagEntry) map[string]string {
	if len(entries) == 0 {
		return nil
	}

	m := make(map[string]string, len(entries))
	for _, e := range entries {
		m[e.Key] = e.Value
	}

	return m
}

// errorResponse builds a CodeDeploy JSON error body.
func errorResponse(code, message string) map[string]string {
	return map[string]string{
		"__type":  code,
		"message": message,
	}
}

// handleBackendError maps a backend error to an HTTP response.
func handleBackendError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSON(http.StatusNotFound, errorResponse("ApplicationDoesNotExistException", err.Error()))
	case errors.Is(err, ErrDeploymentGroupNotFound):
		return c.JSON(http.StatusNotFound, errorResponse("DeploymentGroupDoesNotExistException", err.Error()))
	case errors.Is(err, ErrDeploymentNotFound):
		return c.JSON(http.StatusNotFound, errorResponse("DeploymentDoesNotExistException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSON(http.StatusConflict, errorResponse("ApplicationAlreadyExistsException", err.Error()))
	case errors.Is(err, ErrDeploymentGroupAlreadyExists):
		return c.JSON(http.StatusConflict, errorResponse("DeploymentGroupAlreadyExistsException", err.Error()))
	default:
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", err.Error()))
	}
}
