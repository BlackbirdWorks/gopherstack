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
