package opsworks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	opsworksTargetPrefix = "OpsWorks_20130218."
	matchPriority        = service.PriorityHeaderExact
	contentType          = "application/x-amz-json-1.1"

	keyStackID      = "StackId"
	keyLayerID      = "LayerId"
	keyInstanceID   = "InstanceId"
	keyAppID        = "AppId"
	keyDeploymentID = "DeploymentId"
	keyArn          = "Arn"
	keyName         = "Name"
	keyStatus       = "Status"
	keyCreatedAt    = "CreatedAt"
	keyType         = "Type"
)

var (
	errUnknownAction  = errors.New("unknown action")
	errInvalidRequest = errors.New("invalid request")
)

// Handler handles OpsWorks HTTP requests.
type Handler struct {
	Backend StorageBackend
	ops     map[string]service.JSONOpFunc
}

// NewHandler constructs a new Handler.
func NewHandler(b StorageBackend) *Handler {
	h := &Handler{Backend: b}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "OpsWorks" }

// Reset resets the backend and rebuilds the dispatch table.
func (h *Handler) Reset() {
	h.Backend.Reset()
	h.ops = h.buildOps()
}

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateStack",
		"DescribeStacks",
		"UpdateStack",
		"DeleteStack",
		"CreateLayer",
		"DescribeLayers",
		"UpdateLayer",
		"DeleteLayer",
		"CreateInstance",
		"DescribeInstances",
		"UpdateInstance",
		"DeleteInstance",
		"StartInstance",
		"StopInstance",
		"RebootInstance",
		"CreateApp",
		"DescribeApps",
		"UpdateApp",
		"DeleteApp",
		"CreateDeployment",
		"DescribeDeployments",
		"DescribeCommands",
		"TagResource",
		"UntagResource",
		"ListTags",
	}
}

// RouteMatcher returns a function that matches OpsWorks API requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().Header.Get("X-Amz-Target"), opsworksTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")

	return strings.TrimPrefix(target, opsworksTargetPrefix)
}

// ExtractResource extracts the resource identifier from the request.
func (h *Handler) ExtractResource(_ *echo.Context) string { return "" }

// Handler returns the Echo handler function for OpsWorks requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"OpsWorks", contentType,
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateStack":         h.handleCreateStack,
		"DescribeStacks":      h.handleDescribeStacks,
		"UpdateStack":         h.handleUpdateStack,
		"DeleteStack":         h.handleDeleteStack,
		"CreateLayer":         h.handleCreateLayer,
		"DescribeLayers":      h.handleDescribeLayers,
		"UpdateLayer":         h.handleUpdateLayer,
		"DeleteLayer":         h.handleDeleteLayer,
		"CreateInstance":      h.handleCreateInstance,
		"DescribeInstances":   h.handleDescribeInstances,
		"UpdateInstance":      h.handleUpdateInstance,
		"DeleteInstance":      h.handleDeleteInstance,
		"StartInstance":       h.handleStartInstance,
		"StopInstance":        h.handleStopInstance,
		"RebootInstance":      h.handleRebootInstance,
		"CreateApp":           h.handleCreateApp,
		"DescribeApps":        h.handleDescribeApps,
		"UpdateApp":           h.handleUpdateApp,
		"DeleteApp":           h.handleDeleteApp,
		"CreateDeployment":    h.handleCreateDeployment,
		"DescribeDeployments": h.handleDescribeDeployments,
		"DescribeCommands":    h.handleDescribeCommands,
		"TagResource":         h.handleTagResource,
		"UntagResource":       h.handleUntagResource,
		"ListTags":            h.handleListTags,
	}
}

func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ops[action]
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
		return c.JSON(http.StatusNotFound, errResp("ResourceNotFoundException", err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", err.Error()))
	case errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusNotImplemented, errResp("UnsupportedOperationException", err.Error()))
	case errors.Is(err, errInvalidRequest),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", err.Error()))
	default:
		logger.Load(c.Request().Context()).ErrorContext(_, "opsworks error", "error", err)

		return c.JSON(http.StatusInternalServerError, errResp("ServiceException", err.Error()))
	}
}

func errResp(code, message string) map[string]string {
	return map[string]string{
		"__type":  code,
		"message": message,
	}
}

// handleCreateStack handles CreateStack requests.
func (h *Handler) handleCreateStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		Name                      string `json:"Name"`
		Region                    string `json:"Region"`
		DefaultInstanceProfileArn string `json:"DefaultInstanceProfileArn"`
		ServiceRoleArn            string `json:"ServiceRoleArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	stack, err := h.Backend.CreateStack(
		req.Name, req.Region,
		req.DefaultInstanceProfileArn,
		req.ServiceRoleArn,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyStackID: stack.StackID}, nil
}

// handleDescribeStacks handles DescribeStacks requests.
func (h *Handler) handleDescribeStacks(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackIds []string `json:"StackIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	stacks, err := h.Backend.DescribeStacks(req.StackIds)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Stacks": stacksToJSON(stacks)}, nil
}

// handleUpdateStack handles UpdateStack requests.
func (h *Handler) handleUpdateStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackId string `json:"StackId"`
		Name    string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateStack(req.StackId, req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeleteStack handles DeleteStack requests.
func (h *Handler) handleDeleteStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackId string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteStack(req.StackId); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleCreateLayer handles CreateLayer requests.
func (h *Handler) handleCreateLayer(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackId   string `json:"StackId"`
		Type      string `json:"Type"`
		Name      string `json:"Name"`
		Shortname string `json:"Shortname"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	layer, err := h.Backend.CreateLayer(req.StackId, req.Type, req.Name, req.Shortname)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyLayerID: layer.LayerID}, nil
}

// handleDescribeLayers handles DescribeLayers requests.
func (h *Handler) handleDescribeLayers(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackId  string   `json:"StackId"`
		LayerIds []string `json:"LayerIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	layers, err := h.Backend.DescribeLayers(req.StackId, req.LayerIds)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Layers": layersToJSON(layers)}, nil
}

// handleUpdateLayer handles UpdateLayer requests.
func (h *Handler) handleUpdateLayer(_ context.Context, body []byte) (any, error) {
	var req struct {
		LayerId string `json:"LayerId"`
		Name    string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateLayer(req.LayerId, req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeleteLayer handles DeleteLayer requests.
func (h *Handler) handleDeleteLayer(_ context.Context, body []byte) (any, error) {
	var req struct {
		LayerId string `json:"LayerId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteLayer(req.LayerId); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleCreateInstance handles CreateInstance requests.
func (h *Handler) handleCreateInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackId      string   `json:"StackId"`
		InstanceType string   `json:"InstanceType"`
		LayerIds     []string `json:"LayerIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	layerID := ""
	if len(req.LayerIds) > 0 {
		layerID = req.LayerIds[0]
	}

	instance, err := h.Backend.CreateInstance(req.StackId, layerID, req.InstanceType)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyInstanceID: instance.InstanceID}, nil
}

// handleDescribeInstances handles DescribeInstances requests.
func (h *Handler) handleDescribeInstances(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackId     string   `json:"StackId"`
		LayerId     string   `json:"LayerId"`
		InstanceIds []string `json:"InstanceIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	instances, err := h.Backend.DescribeInstances(req.StackId, req.LayerId, req.InstanceIds)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Instances": instancesToJSON(instances)}, nil
}

// handleUpdateInstance handles UpdateInstance requests.
func (h *Handler) handleUpdateInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceId string `json:"InstanceId"`
		Hostname   string `json:"Hostname"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateInstance(req.InstanceId, req.Hostname); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeleteInstance handles DeleteInstance requests.
func (h *Handler) handleDeleteInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceId string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteInstance(req.InstanceId); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleStartInstance handles StartInstance requests.
func (h *Handler) handleStartInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceId string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.StartInstance(req.InstanceId); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleStopInstance handles StopInstance requests.
func (h *Handler) handleStopInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceId string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.StopInstance(req.InstanceId); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleRebootInstance handles RebootInstance requests.
func (h *Handler) handleRebootInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceId string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.RebootInstance(req.InstanceId); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleCreateApp handles CreateApp requests.
func (h *Handler) handleCreateApp(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackId string `json:"StackId"`
		Name    string `json:"Name"`
		Type    string `json:"Type"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	app, err := h.Backend.CreateApp(req.StackId, req.Name, req.Type)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyAppID: app.AppID}, nil
}

// handleDescribeApps handles DescribeApps requests.
func (h *Handler) handleDescribeApps(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackId string   `json:"StackId"`
		AppIds  []string `json:"AppIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	apps, err := h.Backend.DescribeApps(req.StackId, req.AppIds)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Apps": appsToJSON(apps)}, nil
}

// handleUpdateApp handles UpdateApp requests.
func (h *Handler) handleUpdateApp(_ context.Context, body []byte) (any, error) {
	var req struct {
		AppId string `json:"AppId"`
		Name  string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateApp(req.AppId, req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeleteApp handles DeleteApp requests.
func (h *Handler) handleDeleteApp(_ context.Context, body []byte) (any, error) {
	var req struct {
		AppId string `json:"AppId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteApp(req.AppId); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleCreateDeployment handles CreateDeployment requests.
func (h *Handler) handleCreateDeployment(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackId string `json:"StackId"`
		AppId   string `json:"AppId"`
		Command struct {
			Name string `json:"Name"`
		} `json:"Command"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	deployment, err := h.Backend.CreateDeployment(req.StackId, req.AppId, req.Command.Name)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyDeploymentID: deployment.DeploymentID}, nil
}

// handleDescribeDeployments handles DescribeDeployments requests.
func (h *Handler) handleDescribeDeployments(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackId       string   `json:"StackId"`
		AppId         string   `json:"AppId"`
		DeploymentIds []string `json:"DeploymentIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	deployments, err := h.Backend.DescribeDeployments(req.StackId, req.AppId, req.DeploymentIds)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Deployments": deploymentsToJSON(deployments)}, nil
}

// handleDescribeCommands handles DescribeCommands requests.
func (h *Handler) handleDescribeCommands(_ context.Context, body []byte) (any, error) {
	var req struct {
		DeploymentId string   `json:"DeploymentId"`
		InstanceId   string   `json:"InstanceId"`
		CommandIds   []string `json:"CommandIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	commands, err := h.Backend.DescribeCommands(req.DeploymentId, req.InstanceId, req.CommandIds)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Commands": commandsToJSON(commands)}, nil
}

// handleTagResource handles TagResource requests.
func (h *Handler) handleTagResource(_ context.Context, body []byte) (any, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		ResourceArn string            `json:"ResourceArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.TagResource(req.ResourceArn, req.Tags); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleUntagResource handles UntagResource requests.
func (h *Handler) handleUntagResource(_ context.Context, body []byte) (any, error) {
	var req struct {
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UntagResource(req.ResourceArn, req.TagKeys); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleListTags handles ListTags requests.
func (h *Handler) handleListTags(_ context.Context, body []byte) (any, error) {
	var req struct {
		ResourceArn string `json:"ResourceArn"`
		NextToken   string `json:"NextToken"`
		MaxResults  int32  `json:"MaxResults"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	tags, nextToken, err := h.Backend.ListTags(req.ResourceArn, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, err
	}

	resp := map[string]any{"Tags": tags}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return resp, nil
}

// JSON conversion helpers.

func stacksToJSON(stacks []*Stack) []map[string]any {
	result := make([]map[string]any, 0, len(stacks))
	for _, s := range stacks {
		result = append(result, map[string]any{
			keyStackID:                  s.StackID,
			keyArn:                      s.Arn,
			keyName:                     s.Name,
			"Region":                    s.Region,
			"DefaultInstanceProfileArn": s.DefaultInstanceProfileArn,
			"ServiceRoleArn":            s.ServiceRoleArn,
			keyStatus:                   s.Status,
			keyCreatedAt:                s.CreatedAt.Format("2006-01-02T15:04:05+00:00"),
		})
	}

	return result
}

func layersToJSON(layers []*Layer) []map[string]any {
	result := make([]map[string]any, 0, len(layers))
	for _, l := range layers {
		result = append(result, map[string]any{
			keyLayerID:   l.LayerID,
			keyStackID:   l.StackID,
			keyArn:       l.Arn,
			keyType:      l.Type,
			keyName:      l.Name,
			"Shortname":  l.Shortname,
			keyCreatedAt: l.CreatedAt.Format("2006-01-02T15:04:05+00:00"),
		})
	}

	return result
}

func instancesToJSON(instances []*Instance) []map[string]any {
	result := make([]map[string]any, 0, len(instances))
	for _, i := range instances {
		result = append(result, map[string]any{
			keyInstanceID:  i.InstanceID,
			keyStackID:     i.StackID,
			keyLayerID:     i.LayerID,
			keyArn:         i.Arn,
			"Hostname":     i.Hostname,
			"InstanceType": i.InstanceType,
			keyStatus:      i.Status,
			keyCreatedAt:   i.CreatedAt.Format("2006-01-02T15:04:05+00:00"),
		})
	}

	return result
}

func appsToJSON(apps []*App) []map[string]any {
	result := make([]map[string]any, 0, len(apps))
	for _, a := range apps {
		result = append(result, map[string]any{
			keyAppID:     a.AppID,
			keyStackID:   a.StackID,
			keyArn:       a.Arn,
			keyName:      a.Name,
			keyType:      a.Type,
			keyCreatedAt: a.CreatedAt.Format("2006-01-02T15:04:05+00:00"),
		})
	}

	return result
}

func deploymentsToJSON(deployments []*Deployment) []map[string]any {
	result := make([]map[string]any, 0, len(deployments))
	for _, d := range deployments {
		result = append(result, map[string]any{
			keyDeploymentID: d.DeploymentID,
			keyStackID:      d.StackID,
			keyAppID:        d.AppID,
			"Command":       map[string]any{keyName: d.Command},
			keyStatus:       d.Status,
			"Duration":      d.Duration,
			keyCreatedAt:    d.CreatedAt.Format("2006-01-02T15:04:05+00:00"),
			"CompletedAt":   d.CompletedAt.Format("2006-01-02T15:04:05+00:00"),
		})
	}

	return result
}

func commandsToJSON(commands []*Command) []map[string]any {
	result := make([]map[string]any, 0, len(commands))
	for _, c := range commands {
		result = append(result, map[string]any{
			"CommandId":      c.CommandID,
			keyDeploymentID:  c.DeploymentID,
			keyInstanceID:    c.InstanceID,
			keyType:          c.Type,
			keyStatus:        c.Status,
			"ExitCode":       c.ExitCode,
			"LogUrl":         c.LogURL,
			keyCreatedAt:     c.CreatedAt.Format("2006-01-02T15:04:05+00:00"),
			"AcknowledgedAt": c.AcknowledgedAt.Format("2006-01-02T15:04:05+00:00"),
			"CompletedAt":    c.CompletedAt.Format("2006-01-02T15:04:05+00:00"),
		})
	}

	return result
}
