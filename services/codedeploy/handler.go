package codedeploy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const codedeployTargetPrefix = "CodeDeploy_20141006."

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

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
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"CodeDeploy", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateApplication":     service.WrapOp(h.handleCreateApplication),
		"GetApplication":        service.WrapOp(h.handleGetApplication),
		"ListApplications":      service.WrapOp(h.handleListApplications),
		"DeleteApplication":     service.WrapOp(h.handleDeleteApplication),
		"CreateDeploymentGroup": service.WrapOp(h.handleCreateDeploymentGroup),
		"GetDeploymentGroup":    service.WrapOp(h.handleGetDeploymentGroup),
		"ListDeploymentGroups":  service.WrapOp(h.handleListDeploymentGroups),
		"DeleteDeploymentGroup": service.WrapOp(h.handleDeleteDeploymentGroup),
		"CreateDeployment":      service.WrapOp(h.handleCreateDeployment),
		"GetDeployment":         service.WrapOp(h.handleGetDeployment),
		"ListDeployments":       service.WrapOp(h.handleListDeployments),
		"TagResource":           service.WrapOp(h.handleTagResource),
		"UntagResource":         service.WrapOp(h.handleUntagResource),
		"ListTagsForResource":   service.WrapOp(h.handleListTagsForResource),
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

	makePayload := func(code, msg string) []byte {
		b, _ := json.Marshal(service.JSONErrorResponse{Type: code, Message: msg})

		return b
	}

	switch {
	case errors.Is(err, ErrNotFound):
		return c.JSONBlob(http.StatusNotFound,
			makePayload("ApplicationDoesNotExistException", err.Error()))
	case errors.Is(err, ErrDeploymentGroupNotFound):
		return c.JSONBlob(http.StatusNotFound,
			makePayload("DeploymentGroupDoesNotExistException", err.Error()))
	case errors.Is(err, ErrDeploymentNotFound):
		return c.JSONBlob(http.StatusNotFound,
			makePayload("DeploymentDoesNotExistException", err.Error()))
	case errors.Is(err, ErrAlreadyExists):
		return c.JSONBlob(http.StatusConflict,
			makePayload("ApplicationAlreadyExistsException", err.Error()))
	case errors.Is(err, ErrDeploymentGroupAlreadyExists):
		return c.JSONBlob(http.StatusConflict,
			makePayload("DeploymentGroupAlreadyExistsException", err.Error()))
	case errors.Is(err, errInvalidRequest), errors.Is(err, errUnknownAction),
		errors.As(err, &syntaxErr), errors.As(err, &typeErr):
		return c.JSONBlob(http.StatusBadRequest,
			makePayload("InvalidRequestException", err.Error()))
	default:
		return c.JSONBlob(http.StatusInternalServerError,
			makePayload("ServiceException", err.Error()))
	}
}

// --- Input/Output types and handlers ---

type createApplicationInput struct {
	ApplicationName string     `json:"applicationName"`
	ComputePlatform string     `json:"computePlatform"`
	Tags            []tagEntry `json:"tags"`
}

type createApplicationOutput struct {
	ApplicationID string `json:"applicationId"`
}

func (h *Handler) handleCreateApplication(
	_ context.Context,
	in *createApplicationInput,
) (*createApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	if in.ComputePlatform == "" {
		in.ComputePlatform = "Server"
	}

	app, err := h.Backend.CreateApplication(in.ApplicationName, in.ComputePlatform, tagEntriesToMap(in.Tags))
	if err != nil {
		return nil, err
	}

	return &createApplicationOutput{ApplicationID: app.ApplicationID}, nil
}

type getApplicationInput struct {
	ApplicationName string `json:"applicationName"`
}

type applicationInfo struct {
	ApplicationID   string `json:"applicationId"`
	ApplicationName string `json:"applicationName"`
	ComputePlatform string `json:"computePlatform"`
	CreateTime      int64  `json:"createTime"`
}

type getApplicationOutput struct {
	Application applicationInfo `json:"application"`
}

func (h *Handler) handleGetApplication(
	_ context.Context,
	in *getApplicationInput,
) (*getApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	app, err := h.Backend.GetApplication(in.ApplicationName)
	if err != nil {
		return nil, err
	}

	return &getApplicationOutput{
		Application: applicationInfo{
			ApplicationID:   app.ApplicationID,
			ApplicationName: app.ApplicationName,
			ComputePlatform: app.ComputePlatform,
			CreateTime:      app.CreationTime.UnixMilli(),
		},
	}, nil
}

type listApplicationsInput struct{}

type listApplicationsOutput struct {
	Applications []string `json:"applications"`
}

func (h *Handler) handleListApplications(
	_ context.Context,
	_ *listApplicationsInput,
) (*listApplicationsOutput, error) {
	return &listApplicationsOutput{Applications: h.Backend.ListApplications()}, nil
}

type deleteApplicationInput struct {
	ApplicationName string `json:"applicationName"`
}

type deleteApplicationOutput struct{}

func (h *Handler) handleDeleteApplication(
	_ context.Context,
	in *deleteApplicationInput,
) (*deleteApplicationOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	if err := h.Backend.DeleteApplication(in.ApplicationName); err != nil {
		return nil, err
	}

	return &deleteApplicationOutput{}, nil
}

type createDeploymentGroupInput struct {
	ApplicationName      string     `json:"applicationName"`
	DeploymentGroupName  string     `json:"deploymentGroupName"`
	ServiceRoleArn       string     `json:"serviceRoleArn"`
	DeploymentConfigName string     `json:"deploymentConfigName"`
	Tags                 []tagEntry `json:"tags"`
}

type createDeploymentGroupOutput struct {
	DeploymentGroupID string `json:"deploymentGroupId"`
}

func (h *Handler) handleCreateDeploymentGroup(
	_ context.Context,
	in *createDeploymentGroupInput,
) (*createDeploymentGroupOutput, error) {
	if in.ApplicationName == "" || in.DeploymentGroupName == "" {
		return nil, fmt.Errorf("%w: applicationName and deploymentGroupName are required", errInvalidRequest)
	}

	dg, err := h.Backend.CreateDeploymentGroup(
		in.ApplicationName, in.DeploymentGroupName,
		in.ServiceRoleArn, in.DeploymentConfigName,
		tagEntriesToMap(in.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createDeploymentGroupOutput{DeploymentGroupID: dg.DeploymentGroupID}, nil
}

type getDeploymentGroupInput struct {
	ApplicationName     string `json:"applicationName"`
	DeploymentGroupName string `json:"deploymentGroupName"`
}

type deploymentGroupInfo struct {
	ApplicationName      string `json:"applicationName"`
	DeploymentGroupID    string `json:"deploymentGroupId"`
	DeploymentGroupName  string `json:"deploymentGroupName"`
	ServiceRoleArn       string `json:"serviceRoleArn"`
	DeploymentConfigName string `json:"deploymentConfigName"`
}

type getDeploymentGroupOutput struct {
	DeploymentGroupInfo deploymentGroupInfo `json:"deploymentGroupInfo"`
}

func (h *Handler) handleGetDeploymentGroup(
	_ context.Context,
	in *getDeploymentGroupInput,
) (*getDeploymentGroupOutput, error) {
	dg, err := h.Backend.GetDeploymentGroup(in.ApplicationName, in.DeploymentGroupName)
	if err != nil {
		return nil, err
	}

	return &getDeploymentGroupOutput{
		DeploymentGroupInfo: deploymentGroupInfo{
			ApplicationName:      dg.ApplicationName,
			DeploymentGroupID:    dg.DeploymentGroupID,
			DeploymentGroupName:  dg.DeploymentGroupName,
			ServiceRoleArn:       dg.ServiceRoleArn,
			DeploymentConfigName: dg.DeploymentConfigName,
		},
	}, nil
}

type listDeploymentGroupsInput struct {
	ApplicationName string `json:"applicationName"`
}

type listDeploymentGroupsOutput struct {
	ApplicationName  string   `json:"applicationName"`
	DeploymentGroups []string `json:"deploymentGroups"`
}

func (h *Handler) handleListDeploymentGroups(
	_ context.Context,
	in *listDeploymentGroupsInput,
) (*listDeploymentGroupsOutput, error) {
	if in.ApplicationName == "" {
		return nil, fmt.Errorf("%w: applicationName is required", errInvalidRequest)
	}

	names, err := h.Backend.ListDeploymentGroups(in.ApplicationName)
	if err != nil {
		return nil, err
	}

	return &listDeploymentGroupsOutput{
		ApplicationName:  in.ApplicationName,
		DeploymentGroups: names,
	}, nil
}

type deleteDeploymentGroupInput struct {
	ApplicationName     string `json:"applicationName"`
	DeploymentGroupName string `json:"deploymentGroupName"`
}

type deleteDeploymentGroupOutput struct{}

func (h *Handler) handleDeleteDeploymentGroup(
	_ context.Context,
	in *deleteDeploymentGroupInput,
) (*deleteDeploymentGroupOutput, error) {
	if err := h.Backend.DeleteDeploymentGroup(in.ApplicationName, in.DeploymentGroupName); err != nil {
		return nil, err
	}

	return &deleteDeploymentGroupOutput{}, nil
}

type createDeploymentInput struct {
	ApplicationName     string `json:"applicationName"`
	DeploymentGroupName string `json:"deploymentGroupName"`
	Description         string `json:"description"`
}

type createDeploymentOutput struct {
	DeploymentID string `json:"deploymentId"`
}

func (h *Handler) handleCreateDeployment(
	_ context.Context,
	in *createDeploymentInput,
) (*createDeploymentOutput, error) {
	if in.ApplicationName == "" || in.DeploymentGroupName == "" {
		return nil, fmt.Errorf("%w: applicationName and deploymentGroupName are required", errInvalidRequest)
	}

	d, err := h.Backend.CreateDeployment(in.ApplicationName, in.DeploymentGroupName, in.Description, "user")
	if err != nil {
		return nil, err
	}

	return &createDeploymentOutput{DeploymentID: d.DeploymentID}, nil
}

type getDeploymentInput struct {
	DeploymentID string `json:"deploymentId"`
}

type deploymentInfo struct {
	CompleteTime        *int64 `json:"completeTime,omitempty"`
	DeploymentID        string `json:"deploymentId"`
	ApplicationName     string `json:"applicationName"`
	DeploymentGroupName string `json:"deploymentGroupName"`
	Status              string `json:"status"`
	Creator             string `json:"creator"`
	Description         string `json:"description,omitempty"`
	CreateTime          int64  `json:"createTime"`
}

type getDeploymentOutput struct {
	DeploymentInfo deploymentInfo `json:"deploymentInfo"`
}

func (h *Handler) handleGetDeployment(
	_ context.Context,
	in *getDeploymentInput,
) (*getDeploymentOutput, error) {
	if in.DeploymentID == "" {
		return nil, fmt.Errorf("%w: deploymentId is required", errInvalidRequest)
	}

	d, err := h.Backend.GetDeployment(in.DeploymentID)
	if err != nil {
		return nil, err
	}

	info := deploymentInfo{
		DeploymentID:        d.DeploymentID,
		ApplicationName:     d.ApplicationName,
		DeploymentGroupName: d.DeploymentGroupName,
		Status:              d.Status,
		Creator:             d.Creator,
		CreateTime:          d.CreateTime.UnixMilli(),
		Description:         d.Description,
	}

	if d.CompleteTime != nil {
		ms := d.CompleteTime.UnixMilli()
		info.CompleteTime = &ms
	}

	return &getDeploymentOutput{DeploymentInfo: info}, nil
}

type listDeploymentsInput struct {
	ApplicationName     string `json:"applicationName"`
	DeploymentGroupName string `json:"deploymentGroupName"`
}

type listDeploymentsOutput struct {
	Deployments []string `json:"deployments"`
}

func (h *Handler) handleListDeployments(
	_ context.Context,
	in *listDeploymentsInput,
) (*listDeploymentsOutput, error) {
	return &listDeploymentsOutput{
		Deployments: h.Backend.ListDeployments(in.ApplicationName, in.DeploymentGroupName),
	}, nil
}

type tagResourceInput struct {
	ResourceArn string     `json:"resourceArn"`
	Tags        []tagEntry `json:"tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceArn, tagEntriesToMap(in.Tags)); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []tagEntry `json:"tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	kv, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	entries := make([]tagEntry, 0, len(kv))
	for k, v := range kv {
		entries = append(entries, tagEntry{Key: k, Value: v})
	}

	return &listTagsForResourceOutput{Tags: entries}, nil
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
