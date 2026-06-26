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

	fieldIamUserArn = "IamUserArn"
	fieldRegion     = "Region"
	fieldVersion    = "Version"
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
		"AssignInstance",
		"AssignVolume",
		"AssociateElasticIp",
		"AttachElasticLoadBalancer",
		"CloneStack",
		"CreateApp",
		"CreateDeployment",
		"CreateInstance",
		"CreateLayer",
		"CreateStack",
		"CreateUserProfile",
		"DeleteApp",
		"DeleteInstance",
		"DeleteLayer",
		"DeleteStack",
		"DeleteUserProfile",
		"DeregisterEcsCluster",
		"DeregisterElasticIp",
		"DeregisterInstance",
		"DeregisterRdsDbInstance",
		"DeregisterVolume",
		"DescribeAgentVersions",
		"DescribeApps",
		"DescribeCommands",
		"DescribeDeployments",
		"DescribeEcsClusters",
		"DescribeElasticIps",
		"DescribeElasticLoadBalancers",
		"DescribeInstances",
		"DescribeLayers",
		"DescribeLoadBasedAutoScaling",
		"DescribeMyUserProfile",
		"DescribeOperatingSystems",
		"DescribePermissions",
		"DescribeRaidArrays",
		"DescribeRdsDbInstances",
		"DescribeServiceErrors",
		"DescribeStackProvisioningParameters",
		"DescribeStackSummary",
		"DescribeStacks",
		"DescribeTimeBasedAutoScaling",
		"DescribeUserProfiles",
		"DescribeVolumes",
		"DetachElasticLoadBalancer",
		"DisassociateElasticIp",
		"GetHostnameSuggestion",
		"GrantAccess",
		"ListTags",
		"RebootInstance",
		"RegisterEcsCluster",
		"RegisterElasticIp",
		"RegisterInstance",
		"RegisterRdsDbInstance",
		"RegisterVolume",
		"SetLoadBasedAutoScaling",
		"SetPermission",
		"SetTimeBasedAutoScaling",
		"StartInstance",
		"StartStack",
		"StopInstance",
		"StopStack",
		"TagResource",
		"UnassignInstance",
		"UnassignVolume",
		"UntagResource",
		"UpdateApp",
		"UpdateElasticIp",
		"UpdateInstance",
		"UpdateLayer",
		"UpdateMyUserProfile",
		"UpdateRdsDbInstance",
		"UpdateStack",
		"UpdateUserProfile",
		"UpdateVolume",
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
		"AssignInstance":                      h.handleAssignInstance,
		"AssignVolume":                        h.handleAssignVolume,
		"AssociateElasticIp":                  h.handleAssociateElasticIP,
		"AttachElasticLoadBalancer":           h.handleAttachElasticLoadBalancer,
		"CloneStack":                          h.handleCloneStack,
		"CreateApp":                           h.handleCreateApp,
		"CreateDeployment":                    h.handleCreateDeployment,
		"CreateInstance":                      h.handleCreateInstance,
		"CreateLayer":                         h.handleCreateLayer,
		"CreateStack":                         h.handleCreateStack,
		"CreateUserProfile":                   h.handleCreateUserProfile,
		"DeleteApp":                           h.handleDeleteApp,
		"DeleteInstance":                      h.handleDeleteInstance,
		"DeleteLayer":                         h.handleDeleteLayer,
		"DeleteStack":                         h.handleDeleteStack,
		"DeleteUserProfile":                   h.handleDeleteUserProfile,
		"DeregisterEcsCluster":                h.handleDeregisterEcsCluster,
		"DeregisterElasticIp":                 h.handleDeregisterElasticIP,
		"DeregisterInstance":                  h.handleDeregisterInstance,
		"DeregisterRdsDbInstance":             h.handleDeregisterRdsDBInstance,
		"DeregisterVolume":                    h.handleDeregisterVolume,
		"DescribeAgentVersions":               h.handleDescribeAgentVersions,
		"DescribeApps":                        h.handleDescribeApps,
		"DescribeCommands":                    h.handleDescribeCommands,
		"DescribeDeployments":                 h.handleDescribeDeployments,
		"DescribeEcsClusters":                 h.handleDescribeEcsClusters,
		"DescribeElasticIps":                  h.handleDescribeElasticIps,
		"DescribeElasticLoadBalancers":        h.handleDescribeElasticLoadBalancers,
		"DescribeInstances":                   h.handleDescribeInstances,
		"DescribeLayers":                      h.handleDescribeLayers,
		"DescribeLoadBasedAutoScaling":        h.handleDescribeLoadBasedAutoScaling,
		"DescribeMyUserProfile":               h.handleDescribeMyUserProfile,
		"DescribeOperatingSystems":            h.handleDescribeOperatingSystems,
		"DescribePermissions":                 h.handleDescribePermissions,
		"DescribeRaidArrays":                  h.handleDescribeRaidArrays,
		"DescribeRdsDbInstances":              h.handleDescribeRdsDBInstances,
		"DescribeServiceErrors":               h.handleDescribeServiceErrors,
		"DescribeStackProvisioningParameters": h.handleDescribeStackProvisioningParameters,
		"DescribeStackSummary":                h.handleDescribeStackSummary,
		"DescribeStacks":                      h.handleDescribeStacks,
		"DescribeTimeBasedAutoScaling":        h.handleDescribeTimeBasedAutoScaling,
		"DescribeUserProfiles":                h.handleDescribeUserProfiles,
		"DescribeVolumes":                     h.handleDescribeVolumes,
		"DetachElasticLoadBalancer":           h.handleDetachElasticLoadBalancer,
		"DisassociateElasticIp":               h.handleDisassociateElasticIP,
		"GetHostnameSuggestion":               h.handleGetHostnameSuggestion,
		"GrantAccess":                         h.handleGrantAccess,
		"ListTags":                            h.handleListTags,
		"RebootInstance":                      h.handleRebootInstance,
		"RegisterEcsCluster":                  h.handleRegisterEcsCluster,
		"RegisterElasticIp":                   h.handleRegisterElasticIP,
		"RegisterInstance":                    h.handleRegisterInstance,
		"RegisterRdsDbInstance":               h.handleRegisterRdsDBInstance,
		"RegisterVolume":                      h.handleRegisterVolume,
		"SetLoadBasedAutoScaling":             h.handleSetLoadBasedAutoScaling,
		"SetPermission":                       h.handleSetPermission,
		"SetTimeBasedAutoScaling":             h.handleSetTimeBasedAutoScaling,
		"StartInstance":                       h.handleStartInstance,
		"StartStack":                          h.handleStartStack,
		"StopInstance":                        h.handleStopInstance,
		"StopStack":                           h.handleStopStack,
		"TagResource":                         h.handleTagResource,
		"UnassignInstance":                    h.handleUnassignInstance,
		"UnassignVolume":                      h.handleUnassignVolume,
		"UntagResource":                       h.handleUntagResource,
		"UpdateApp":                           h.handleUpdateApp,
		"UpdateElasticIp":                     h.handleUpdateElasticIP,
		"UpdateInstance":                      h.handleUpdateInstance,
		"UpdateLayer":                         h.handleUpdateLayer,
		"UpdateMyUserProfile":                 h.handleUpdateMyUserProfile,
		"UpdateRdsDbInstance":                 h.handleUpdateRdsDBInstance,
		"UpdateStack":                         h.handleUpdateStack,
		"UpdateUserProfile":                   h.handleUpdateUserProfile,
		"UpdateVolume":                        h.handleUpdateVolume,
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
		// AWS OpsWorks rejects an unrecognized action with HTTP 400
		// ValidationException, not 501.
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", err.Error()))
	case errors.Is(err, errInvalidRequest),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", err.Error()))
	default:
		logger.Load(c.Request().Context()).ErrorContext(c.Request().Context(), "opsworks error", "error", err)

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

// handleCloneStack handles CloneStack requests.
func (h *Handler) handleCloneStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		SourceStackID string `json:"SourceStackId"`
		Name          string `json:"Name"`
		Region        string `json:"Region"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	stack, err := h.Backend.CloneStack(req.SourceStackID, req.Name, req.Region)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyStackID: stack.StackID}, nil
}

// handleDescribeStacks handles DescribeStacks requests.
func (h *Handler) handleDescribeStacks(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackIDs []string `json:"StackIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	stacks, err := h.Backend.DescribeStacks(req.StackIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Stacks": stacksToJSON(stacks)}, nil
}

// handleUpdateStack handles UpdateStack requests.
func (h *Handler) handleUpdateStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
		Name    string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateStack(req.StackID, req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeleteStack handles DeleteStack requests.
func (h *Handler) handleDeleteStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteStack(req.StackID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleStartStack handles StartStack requests.
func (h *Handler) handleStartStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.StartStack(req.StackID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleStopStack handles StopStack requests.
func (h *Handler) handleStopStack(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.StopStack(req.StackID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleGetHostnameSuggestion handles GetHostnameSuggestion requests.
func (h *Handler) handleGetHostnameSuggestion(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
		LayerID string `json:"LayerId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	hostname, err := h.Backend.GetHostnameSuggestion(req.StackID, req.LayerID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Hostname": hostname}, nil
}

// handleDescribeStackSummary handles DescribeStackSummary requests.
func (h *Handler) handleDescribeStackSummary(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	summary, err := h.Backend.DescribeStackSummary(req.StackID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"StackSummary": stackSummaryToJSON(summary)}, nil
}

// handleDescribeStackProvisioningParameters handles DescribeStackProvisioningParameters requests.
func (h *Handler) handleDescribeStackProvisioningParameters(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	params, stackArn, err := h.Backend.DescribeStackProvisioningParameters(req.StackID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"Parameters":        params,
		"AgentInstallerUrl": params["AgentInstallerUrl"],
		"StackArn":          stackArn,
	}, nil
}

// handleCreateLayer handles CreateLayer requests.
func (h *Handler) handleCreateLayer(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID   string `json:"StackId"`
		Type      string `json:"Type"`
		Name      string `json:"Name"`
		Shortname string `json:"Shortname"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	layer, err := h.Backend.CreateLayer(req.StackID, req.Type, req.Name, req.Shortname)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyLayerID: layer.LayerID}, nil
}

// handleDescribeLayers handles DescribeLayers requests.
func (h *Handler) handleDescribeLayers(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID  string   `json:"StackId"`
		LayerIDs []string `json:"LayerIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	layers, err := h.Backend.DescribeLayers(req.StackID, req.LayerIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Layers": layersToJSON(layers)}, nil
}

// handleUpdateLayer handles UpdateLayer requests.
func (h *Handler) handleUpdateLayer(_ context.Context, body []byte) (any, error) {
	var req struct {
		LayerID string `json:"LayerId"`
		Name    string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateLayer(req.LayerID, req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeleteLayer handles DeleteLayer requests.
func (h *Handler) handleDeleteLayer(_ context.Context, body []byte) (any, error) {
	var req struct {
		LayerID string `json:"LayerId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteLayer(req.LayerID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleCreateInstance handles CreateInstance requests.
func (h *Handler) handleCreateInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID      string   `json:"StackId"`
		InstanceType string   `json:"InstanceType"`
		LayerIDs     []string `json:"LayerIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	layerID := ""
	if len(req.LayerIDs) > 0 {
		layerID = req.LayerIDs[0]
	}

	instance, err := h.Backend.CreateInstance(req.StackID, layerID, req.InstanceType)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyInstanceID: instance.InstanceID}, nil
}

// handleRegisterInstance handles RegisterInstance requests.
func (h *Handler) handleRegisterInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID  string `json:"StackId"`
		Hostname string `json:"Hostname"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	instanceID, err := h.Backend.RegisterInstance(req.StackID, req.Hostname)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyInstanceID: instanceID}, nil
}

// handleDeregisterInstance handles DeregisterInstance requests.
func (h *Handler) handleDeregisterInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeregisterInstance(req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleAssignInstance handles AssignInstance requests.
func (h *Handler) handleAssignInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string   `json:"InstanceId"`
		LayerIDs   []string `json:"LayerIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.AssignInstance(req.InstanceID, req.LayerIDs); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleUnassignInstance handles UnassignInstance requests.
func (h *Handler) handleUnassignInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UnassignInstance(req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeInstances handles DescribeInstances requests.
func (h *Handler) handleDescribeInstances(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID     string   `json:"StackId"`
		LayerID     string   `json:"LayerId"`
		InstanceIDs []string `json:"InstanceIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	instances, err := h.Backend.DescribeInstances(req.StackID, req.LayerID, req.InstanceIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Instances": instancesToJSON(instances)}, nil
}

// handleUpdateInstance handles UpdateInstance requests.
func (h *Handler) handleUpdateInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
		Hostname   string `json:"Hostname"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateInstance(req.InstanceID, req.Hostname); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeleteInstance handles DeleteInstance requests.
func (h *Handler) handleDeleteInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteInstance(req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleStartInstance handles StartInstance requests.
func (h *Handler) handleStartInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.StartInstance(req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleStopInstance handles StopInstance requests.
func (h *Handler) handleStopInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.StopInstance(req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleRebootInstance handles RebootInstance requests.
func (h *Handler) handleRebootInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.RebootInstance(req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleCreateApp handles CreateApp requests.
func (h *Handler) handleCreateApp(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
		Name    string `json:"Name"`
		Type    string `json:"Type"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	app, err := h.Backend.CreateApp(req.StackID, req.Name, req.Type)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyAppID: app.AppID}, nil
}

// handleDescribeApps handles DescribeApps requests.
func (h *Handler) handleDescribeApps(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string   `json:"StackId"`
		AppIDs  []string `json:"AppIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	apps, err := h.Backend.DescribeApps(req.StackID, req.AppIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Apps": appsToJSON(apps)}, nil
}

// handleUpdateApp handles UpdateApp requests.
func (h *Handler) handleUpdateApp(_ context.Context, body []byte) (any, error) {
	var req struct {
		AppID string `json:"AppId"`
		Name  string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateApp(req.AppID, req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeleteApp handles DeleteApp requests.
func (h *Handler) handleDeleteApp(_ context.Context, body []byte) (any, error) {
	var req struct {
		AppID string `json:"AppId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteApp(req.AppID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleCreateDeployment handles CreateDeployment requests.
func (h *Handler) handleCreateDeployment(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
		AppID   string `json:"AppId"`
		Command struct {
			Name string `json:"Name"`
		} `json:"Command"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	deployment, err := h.Backend.CreateDeployment(req.StackID, req.AppID, req.Command.Name)
	if err != nil {
		return nil, err
	}

	return map[string]any{keyDeploymentID: deployment.DeploymentID}, nil
}

// handleDescribeDeployments handles DescribeDeployments requests.
func (h *Handler) handleDescribeDeployments(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID       string   `json:"StackId"`
		AppID         string   `json:"AppId"`
		DeploymentIDs []string `json:"DeploymentIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	deployments, err := h.Backend.DescribeDeployments(req.StackID, req.AppID, req.DeploymentIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Deployments": deploymentsToJSON(deployments)}, nil
}

// handleDescribeCommands handles DescribeCommands requests.
// Note: AWS uses "CommandIds" (lowercase 'd') not "CommandIDs".
func (h *Handler) handleDescribeCommands(_ context.Context, body []byte) (any, error) {
	var req struct {
		DeploymentID string   `json:"DeploymentId"`
		InstanceID   string   `json:"InstanceId"`
		CommandIDs   []string `json:"CommandIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	commands, err := h.Backend.DescribeCommands(req.DeploymentID, req.InstanceID, req.CommandIDs)
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

// handleCreateUserProfile handles CreateUserProfile requests.
func (h *Handler) handleCreateUserProfile(_ context.Context, body []byte) (any, error) {
	var req struct {
		IamUserArn          string `json:"IamUserArn"`
		SSHUsername         string `json:"SshUsername"`
		SSHPublicKey        string `json:"SshPublicKey"`
		AllowSelfManagement bool   `json:"AllowSelfManagement"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	profile, err := h.Backend.CreateUserProfile(
		req.IamUserArn,
		req.SSHUsername,
		req.SSHPublicKey,
		req.AllowSelfManagement,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{fieldIamUserArn: profile.IamUserArn}, nil
}

// handleDeleteUserProfile handles DeleteUserProfile requests.
func (h *Handler) handleDeleteUserProfile(_ context.Context, body []byte) (any, error) {
	var req struct {
		IamUserArn string `json:"IamUserArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeleteUserProfile(req.IamUserArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeUserProfiles handles DescribeUserProfiles requests.
func (h *Handler) handleDescribeUserProfiles(_ context.Context, body []byte) (any, error) {
	var req struct {
		IamUserArns []string `json:"IamUserArns"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	profiles, err := h.Backend.DescribeUserProfiles(req.IamUserArns)
	if err != nil {
		return nil, err
	}

	return map[string]any{"UserProfiles": userProfilesToJSON(profiles)}, nil
}

// handleUpdateUserProfile handles UpdateUserProfile requests.
func (h *Handler) handleUpdateUserProfile(_ context.Context, body []byte) (any, error) {
	var req struct {
		IamUserArn   string `json:"IamUserArn"`
		SSHUsername  string `json:"SshUsername"`
		SSHPublicKey string `json:"SshPublicKey"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateUserProfile(req.IamUserArn, req.SSHUsername, req.SSHPublicKey); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeMyUserProfile handles DescribeMyUserProfile requests.
func (h *Handler) handleDescribeMyUserProfile(_ context.Context, _ []byte) (any, error) {
	profile, err := h.Backend.DescribeMyUserProfile()
	if err != nil {
		return nil, err
	}

	return map[string]any{"UserProfile": userProfileToJSON(profile)}, nil
}

// handleUpdateMyUserProfile handles UpdateMyUserProfile requests.
func (h *Handler) handleUpdateMyUserProfile(_ context.Context, body []byte) (any, error) {
	var req struct {
		SSHPublicKey string `json:"SshPublicKey"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateMyUserProfile(req.SSHPublicKey); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleAttachElasticLoadBalancer handles AttachElasticLoadBalancer requests.
func (h *Handler) handleAttachElasticLoadBalancer(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticLoadBalancerName string `json:"ElasticLoadBalancerName"`
		LayerID                 string `json:"LayerId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.AttachElasticLoadBalancer(req.ElasticLoadBalancerName, req.LayerID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDetachElasticLoadBalancer handles DetachElasticLoadBalancer requests.
func (h *Handler) handleDetachElasticLoadBalancer(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticLoadBalancerName string `json:"ElasticLoadBalancerName"`
		LayerID                 string `json:"LayerId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DetachElasticLoadBalancer(req.ElasticLoadBalancerName, req.LayerID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeElasticLoadBalancers handles DescribeElasticLoadBalancers requests.
func (h *Handler) handleDescribeElasticLoadBalancers(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID  string   `json:"StackId"`
		LayerIDs []string `json:"LayerIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	layerID := ""
	if len(req.LayerIDs) > 0 {
		layerID = req.LayerIDs[0]
	}

	elbs, err := h.Backend.DescribeElasticLoadBalancers(req.StackID, layerID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ElasticLoadBalancers": elasticLBsToJSON(elbs)}, nil
}

// handleRegisterElasticIP handles RegisterElasticIp requests.
func (h *Handler) handleRegisterElasticIP(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticIP string `json:"ElasticIp"`
		Region    string `json:"Region"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	eip, err := h.Backend.RegisterElasticIP(req.ElasticIP, req.Region)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ElasticIp": eip.IP}, nil
}

// handleDeregisterElasticIP handles DeregisterElasticIp requests.
func (h *Handler) handleDeregisterElasticIP(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticIP string `json:"ElasticIp"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeregisterElasticIP(req.ElasticIP); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleAssociateElasticIP handles AssociateElasticIp requests.
func (h *Handler) handleAssociateElasticIP(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticIP  string `json:"ElasticIp"`
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.AssociateElasticIP(req.ElasticIP, req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDisassociateElasticIP handles DisassociateElasticIp requests.
func (h *Handler) handleDisassociateElasticIP(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticIP string `json:"ElasticIp"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DisassociateElasticIP(req.ElasticIP); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeElasticIps handles DescribeElasticIps requests.
func (h *Handler) handleDescribeElasticIps(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID string   `json:"InstanceId"`
		Ips        []string `json:"Ips"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	eips, err := h.Backend.DescribeElasticIps(req.InstanceID, req.Ips)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ElasticIps": elasticIpsToJSON(eips)}, nil
}

// handleUpdateElasticIP handles UpdateElasticIp requests.
func (h *Handler) handleUpdateElasticIP(_ context.Context, body []byte) (any, error) {
	var req struct {
		ElasticIP string `json:"ElasticIp"`
		Name      string `json:"Name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateElasticIP(req.ElasticIP, req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleRegisterVolume handles RegisterVolume requests.
func (h *Handler) handleRegisterVolume(_ context.Context, body []byte) (any, error) {
	var req struct {
		Ec2VolumeID string `json:"Ec2VolumeId"`
		StackID     string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	volumeID, err := h.Backend.RegisterVolume(req.Ec2VolumeID, req.StackID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"VolumeId": volumeID}, nil
}

// handleDeregisterVolume handles DeregisterVolume requests.
func (h *Handler) handleDeregisterVolume(_ context.Context, body []byte) (any, error) {
	var req struct {
		VolumeID string `json:"VolumeId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeregisterVolume(req.VolumeID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleAssignVolume handles AssignVolume requests.
func (h *Handler) handleAssignVolume(_ context.Context, body []byte) (any, error) {
	var req struct {
		VolumeID   string `json:"VolumeId"`
		InstanceID string `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.AssignVolume(req.VolumeID, req.InstanceID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleUnassignVolume handles UnassignVolume requests.
func (h *Handler) handleUnassignVolume(_ context.Context, body []byte) (any, error) {
	var req struct {
		VolumeID string `json:"VolumeId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UnassignVolume(req.VolumeID); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeVolumes handles DescribeVolumes requests.
func (h *Handler) handleDescribeVolumes(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID  string   `json:"InstanceId"`
		RaidArrayID string   `json:"RaidArrayId"`
		VolumeIDs   []string `json:"VolumeIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	volumes, err := h.Backend.DescribeVolumes(req.InstanceID, req.RaidArrayID, req.VolumeIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Volumes": volumesToJSON(volumes)}, nil
}

// handleUpdateVolume handles UpdateVolume requests.
func (h *Handler) handleUpdateVolume(_ context.Context, body []byte) (any, error) {
	var req struct {
		VolumeID   string `json:"VolumeId"`
		Name       string `json:"Name"`
		MountPoint string `json:"MountPoint"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateVolume(req.VolumeID, req.Name, req.MountPoint); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleRegisterRdsDBInstance handles RegisterRdsDbInstance requests.
func (h *Handler) handleRegisterRdsDBInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID          string `json:"StackId"`
		RdsDBInstanceArn string `json:"RdsDbInstanceArn"`
		DBUser           string `json:"DbUser"`
		DBPassword       string `json:"DbPassword"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.RegisterRdsDBInstance(
		req.StackID, req.RdsDBInstanceArn, req.DBUser, req.DBPassword,
	); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDeregisterRdsDBInstance handles DeregisterRdsDbInstance requests.
func (h *Handler) handleDeregisterRdsDBInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		RdsDBInstanceArn string `json:"RdsDbInstanceArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeregisterRdsDBInstance(req.RdsDBInstanceArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeRdsDBInstances handles DescribeRdsDbInstances requests.
func (h *Handler) handleDescribeRdsDBInstances(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID           string   `json:"StackId"`
		RdsDBInstanceArns []string `json:"RdsDbInstanceArns"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	instances, err := h.Backend.DescribeRdsDBInstances(req.StackID, req.RdsDBInstanceArns)
	if err != nil {
		return nil, err
	}

	return map[string]any{"RdsDbInstances": rdsDBInstancesToJSON(instances)}, nil
}

// handleUpdateRdsDBInstance handles UpdateRdsDbInstance requests.
func (h *Handler) handleUpdateRdsDBInstance(_ context.Context, body []byte) (any, error) {
	var req struct {
		RdsDBInstanceArn string `json:"RdsDbInstanceArn"`
		DBUser           string `json:"DbUser"`
		DBPassword       string `json:"DbPassword"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.UpdateRdsDBInstance(req.RdsDBInstanceArn, req.DBUser, req.DBPassword); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleRegisterEcsCluster handles RegisterEcsCluster requests.
func (h *Handler) handleRegisterEcsCluster(_ context.Context, body []byte) (any, error) {
	var req struct {
		EcsClusterArn string `json:"EcsClusterArn"`
		StackID       string `json:"StackId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	ecsClusterArn, err := h.Backend.RegisterEcsCluster(req.EcsClusterArn, req.StackID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"EcsClusterArn": ecsClusterArn}, nil
}

// handleDeregisterEcsCluster handles DeregisterEcsCluster requests.
func (h *Handler) handleDeregisterEcsCluster(_ context.Context, body []byte) (any, error) {
	var req struct {
		EcsClusterArn string `json:"EcsClusterArn"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.DeregisterEcsCluster(req.EcsClusterArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeEcsClusters handles DescribeEcsClusters requests.
func (h *Handler) handleDescribeEcsClusters(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID        string   `json:"StackId"`
		EcsClusterArns []string `json:"EcsClusterArns"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	clusters, err := h.Backend.DescribeEcsClusters(req.StackID, req.EcsClusterArns)
	if err != nil {
		return nil, err
	}

	return map[string]any{"EcsClusters": ecsClustersToJSON(clusters)}, nil
}

// handleSetPermission handles SetPermission requests.
func (h *Handler) handleSetPermission(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID    string `json:"StackId"`
		IamUserArn string `json:"IamUserArn"`
		Level      string `json:"Level"`
		AllowSSH   bool   `json:"AllowSsh"`
		AllowSudo  bool   `json:"AllowSudo"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.SetPermission(req.StackID, req.IamUserArn, req.Level, req.AllowSSH, req.AllowSudo); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribePermissions handles DescribePermissions requests.
func (h *Handler) handleDescribePermissions(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID    string `json:"StackId"`
		IamUserArn string `json:"IamUserArn"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	perms, err := h.Backend.DescribePermissions(req.StackID, req.IamUserArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Permissions": permissionsToJSON(perms)}, nil
}

// handleSetTimeBasedAutoScaling handles SetTimeBasedAutoScaling requests.
func (h *Handler) handleSetTimeBasedAutoScaling(_ context.Context, body []byte) (any, error) {
	var req struct {
		AutoScalingSchedule *AutoScalingSchedule `json:"AutoScalingSchedule"`
		InstanceID          string               `json:"InstanceId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.SetTimeBasedAutoScaling(req.InstanceID, req.AutoScalingSchedule); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeTimeBasedAutoScaling handles DescribeTimeBasedAutoScaling requests.
func (h *Handler) handleDescribeTimeBasedAutoScaling(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceIDs []string `json:"InstanceIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	configs, err := h.Backend.DescribeTimeBasedAutoScaling(req.InstanceIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"TimeBasedAutoScalingConfigurations": timeBasedAutoScalingToJSON(configs)}, nil
}

// handleSetLoadBasedAutoScaling handles SetLoadBasedAutoScaling requests.
func (h *Handler) handleSetLoadBasedAutoScaling(_ context.Context, body []byte) (any, error) {
	var req struct {
		UpScaling   *ScalingParameters `json:"UpScaling"`
		DownScaling *ScalingParameters `json:"DownScaling"`
		LayerID     string             `json:"LayerId"`
		Enable      bool               `json:"Enable"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if err := h.Backend.SetLoadBasedAutoScaling(req.LayerID, req.Enable, req.UpScaling, req.DownScaling); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// handleDescribeLoadBasedAutoScaling handles DescribeLoadBasedAutoScaling requests.
func (h *Handler) handleDescribeLoadBasedAutoScaling(_ context.Context, body []byte) (any, error) {
	var req struct {
		LayerIDs []string `json:"LayerIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	configs, err := h.Backend.DescribeLoadBasedAutoScaling(req.LayerIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"LoadBasedAutoScalingConfigurations": loadBasedAutoScalingToJSON(configs)}, nil
}

// handleGrantAccess handles GrantAccess requests.
func (h *Handler) handleGrantAccess(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID        string `json:"InstanceId"`
		ValidForInMinutes int32  `json:"ValidForInMinutes"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	creds, err := h.Backend.GrantAccess(req.InstanceID, req.ValidForInMinutes)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"TemporaryCredential": map[string]any{
			"InstanceId":        creds.InstanceID,
			"Username":          creds.Username,
			"Password":          creds.Password,
			"ValidForInMinutes": creds.ValidForInMinutes,
		},
	}, nil
}

// handleDescribeServiceErrors handles DescribeServiceErrors requests.
func (h *Handler) handleDescribeServiceErrors(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID         string   `json:"StackId"`
		InstanceID      string   `json:"InstanceId"`
		ServiceErrorIDs []string `json:"ServiceErrorIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	errors, err := h.Backend.DescribeServiceErrors(req.StackID, req.InstanceID, req.ServiceErrorIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ServiceErrors": errors}, nil
}

// handleDescribeRaidArrays handles DescribeRaidArrays requests.
func (h *Handler) handleDescribeRaidArrays(_ context.Context, body []byte) (any, error) {
	var req struct {
		InstanceID   string   `json:"InstanceId"`
		StackID      string   `json:"StackId"`
		RaidArrayIDs []string `json:"RaidArrayIds"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	arrays, err := h.Backend.DescribeRaidArrays(req.InstanceID, req.StackID, req.RaidArrayIDs)
	if err != nil {
		return nil, err
	}

	return map[string]any{"RaidArrays": arrays}, nil
}

// handleDescribeAgentVersions handles DescribeAgentVersions requests.
func (h *Handler) handleDescribeAgentVersions(_ context.Context, body []byte) (any, error) {
	var req struct {
		StackID string `json:"StackId"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
		}
	}

	versions, err := h.Backend.DescribeAgentVersions(req.StackID)
	if err != nil {
		return nil, err
	}

	return map[string]any{"AgentVersions": agentVersionsToJSON(versions)}, nil
}

// handleDescribeOperatingSystems handles DescribeOperatingSystems requests.
func (h *Handler) handleDescribeOperatingSystems(_ context.Context, _ []byte) (any, error) {
	oses, err := h.Backend.DescribeOperatingSystems()
	if err != nil {
		return nil, err
	}

	return map[string]any{"OperatingSystems": operatingSystemsToJSON(oses)}, nil
}

// JSON conversion helpers.

func stacksToJSON(stacks []*Stack) []map[string]any {
	result := make([]map[string]any, 0, len(stacks))
	for _, s := range stacks {
		result = append(result, map[string]any{
			keyStackID:                  s.StackID,
			keyArn:                      s.Arn,
			keyName:                     s.Name,
			fieldRegion:                 s.Region,
			"DefaultInstanceProfileArn": s.DefaultInstanceProfileArn,
			"ServiceRoleArn":            s.ServiceRoleArn,
			keyStatus:                   s.Status,
			keyCreatedAt:                s.CreatedAt.Format("2006-01-02T15:04:05+00:00"),
		})
	}

	return result
}

func stackSummaryToJSON(s *StackSummary) map[string]any {
	ic := map[string]any{}
	if s.InstancesCount != nil {
		ic = map[string]any{
			"Online":   s.InstancesCount.Online,
			"Stopped":  s.InstancesCount.Stopped,
			"Starting": s.InstancesCount.Starting,
			"Stopping": s.InstancesCount.Stopping,
			"Total":    s.InstancesCount.Total,
		}
	}

	return map[string]any{
		keyStackID:       s.StackID,
		keyArn:           s.Arn,
		keyName:          s.Name,
		"InstancesCount": ic,
		"LayersCount":    s.LayersCount,
		"AppsCount":      s.AppsCount,
	}
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
		completedAt := ""
		if !d.CompletedAt.IsZero() && d.CompletedAt != d.CreatedAt {
			completedAt = d.CompletedAt.Format("2006-01-02T15:04:05+00:00")
		}

		result = append(result, map[string]any{
			keyDeploymentID: d.DeploymentID,
			keyStackID:      d.StackID,
			keyAppID:        d.AppID,
			"Command":       map[string]any{keyName: d.Command},
			keyStatus:       d.Status,
			"Duration":      d.Duration,
			keyCreatedAt:    d.CreatedAt.Format("2006-01-02T15:04:05+00:00"),
			"CompletedAt":   completedAt,
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

func userProfileToJSON(u *UserProfile) map[string]any {
	return map[string]any{
		fieldIamUserArn:       u.IamUserArn,
		keyName:               u.Name,
		"SshUsername":         u.SSHUsername,
		"SshPublicKey":        u.SSHPublicKey,
		"AllowSelfManagement": u.AllowSelfManagement,
	}
}

func userProfilesToJSON(profiles []*UserProfile) []map[string]any {
	result := make([]map[string]any, 0, len(profiles))
	for _, u := range profiles {
		result = append(result, userProfileToJSON(u))
	}

	return result
}

func elasticLBsToJSON(elbs []*ElasticLoadBalancer) []map[string]any {
	result := make([]map[string]any, 0, len(elbs))
	for _, e := range elbs {
		result = append(result, map[string]any{
			"ElasticLoadBalancerName": e.ElasticLoadBalancerName,
			fieldRegion:               e.Region,
			"DnsName":                 e.DNSName,
			keyStackID:                e.StackID,
			keyLayerID:                e.LayerID,
		})
	}

	return result
}

func elasticIpsToJSON(eips []*ElasticIP) []map[string]any {
	result := make([]map[string]any, 0, len(eips))
	for _, e := range eips {
		result = append(result, map[string]any{
			"Ip":          e.IP,
			"Domain":      e.Domain,
			keyName:       e.Name,
			fieldRegion:   e.Region,
			keyInstanceID: e.InstanceID,
		})
	}

	return result
}

func volumesToJSON(vols []*Volume) []map[string]any {
	result := make([]map[string]any, 0, len(vols))
	for _, v := range vols {
		result = append(result, map[string]any{
			"VolumeId":    v.VolumeID,
			"Ec2VolumeId": v.Ec2VolumeID,
			keyStackID:    v.StackID,
			keyInstanceID: v.InstanceID,
			keyName:       v.Name,
			"MountPoint":  v.MountPoint,
			fieldRegion:   v.Region,
			keyStatus:     v.Status,
			"Size":        v.Size,
		})
	}

	return result
}

func rdsDBInstancesToJSON(rdbs []*RdsDBInstance) []map[string]any {
	result := make([]map[string]any, 0, len(rdbs))
	for _, r := range rdbs {
		result = append(result, map[string]any{
			"RdsDbInstanceArn":     r.RdsDBInstanceArn,
			"DbInstanceIdentifier": r.DBInstanceIdentifier,
			"DbUser":               r.DBUser,
			keyStackID:             r.StackID,
			fieldRegion:            r.Region,
			"Address":              r.Address,
		})
	}

	return result
}

func ecsClustersToJSON(clusters []*EcsCluster) []map[string]any {
	result := make([]map[string]any, 0, len(clusters))
	for _, e := range clusters {
		result = append(result, map[string]any{
			"EcsClusterArn":  e.EcsClusterArn,
			"EcsClusterName": e.EcsClusterName,
			keyStackID:       e.StackID,
			keyStatus:        e.Status,
			"RegisteredAt":   e.RegisteredAt.Format("2006-01-02T15:04:05+00:00"),
		})
	}

	return result
}

func permissionsToJSON(perms []*Permission) []map[string]any {
	result := make([]map[string]any, 0, len(perms))
	for _, p := range perms {
		result = append(result, map[string]any{
			keyStackID:      p.StackID,
			fieldIamUserArn: p.IamUserArn,
			"Level":         p.Level,
			"AllowSsh":      p.AllowSSH,
			"AllowSudo":     p.AllowSudo,
		})
	}

	return result
}

func timeBasedAutoScalingToJSON(configs []*TimeBasedAutoScaling) []map[string]any {
	result := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		schedule := map[string]any{}
		if c.AutoScalingSchedule != nil {
			s := c.AutoScalingSchedule
			schedule = map[string]any{
				"Monday":    s.Monday,
				"Tuesday":   s.Tuesday,
				"Wednesday": s.Wednesday,
				"Thursday":  s.Thursday,
				"Friday":    s.Friday,
				"Saturday":  s.Saturday,
				"Sunday":    s.Sunday,
			}
		}
		result = append(result, map[string]any{
			keyInstanceID:         c.InstanceID,
			"AutoScalingSchedule": schedule,
		})
	}

	return result
}

func loadBasedAutoScalingToJSON(configs []*LoadBasedAutoScaling) []map[string]any {
	result := make([]map[string]any, 0, len(configs))
	for _, c := range configs {
		m := map[string]any{
			keyLayerID: c.LayerID,
			"Enable":   c.Enable,
		}
		if c.UpScaling != nil {
			m["UpScaling"] = scalingParamsToJSON(c.UpScaling)
		}
		if c.DownScaling != nil {
			m["DownScaling"] = scalingParamsToJSON(c.DownScaling)
		}
		result = append(result, m)
	}

	return result
}

func scalingParamsToJSON(p *ScalingParameters) map[string]any {
	return map[string]any{
		"CpuThreshold":       p.CPUThreshold,
		"IgnoreMetricsTime":  p.IgnoreMetricsTime,
		"InstanceCount":      p.InstanceCount,
		"LoadThreshold":      p.LoadThreshold,
		"MemoryThreshold":    p.MemoryThreshold,
		"ThresholdsWaitTime": p.ThresholdsWaitTime,
	}
}

func agentVersionsToJSON(versions []*AgentVersion) []map[string]any {
	result := make([]map[string]any, 0, len(versions))
	for _, v := range versions {
		m := map[string]any{fieldVersion: v.Version}
		if v.ConfigurationManager != nil {
			m["ConfigurationManager"] = map[string]any{
				keyName:      v.ConfigurationManager.Name,
				fieldVersion: v.ConfigurationManager.Version,
			}
		}
		result = append(result, m)
	}

	return result
}

func operatingSystemsToJSON(oses []*OperatingSystem) []map[string]any {
	result := make([]map[string]any, 0, len(oses))
	for _, os := range oses {
		managers := make([]map[string]any, 0, len(os.ConfigurationManagers))
		for _, cm := range os.ConfigurationManagers {
			managers = append(managers, map[string]any{
				keyName:      cm.Name,
				fieldVersion: cm.Version,
			})
		}
		result = append(result, map[string]any{
			"Id":                    os.ID,
			keyName:                 os.Name,
			keyType:                 os.Type,
			"ConfigurationManagers": managers,
			"ReportedVersion":       os.ReportedVersion,
			"Supported":             os.Supported,
		})
	}

	return result
}
