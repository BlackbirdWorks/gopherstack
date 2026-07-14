package ecs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	keyMessageField = "message"
)

const (
	ecsTargetPrefix   = "AmazonEC2ContainerServiceV20141113."
	unknownActionName = "Unknown"
	statusMissing     = "MISSING"
	transportTCP      = "tcp"
	keyTypeField      = "__type"
)

var errUnknownAction = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for ECS operations.
type Handler struct {
	Backend Backend
	ops     map[string]service.JSONOpFunc
	region  string
}

// NewHandler creates a new ECS handler.
func NewHandler(backend Backend) *Handler {
	h := &Handler{Backend: backend, region: backend.GetRegion()}
	h.ops = h.buildOps()

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "ECS" }

// GetSupportedOperations returns the list of supported ECS operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateCluster",
		"ListClusters",
		"DescribeClusters",
		"DeleteCluster",
		"RegisterTaskDefinition",
		"DescribeTaskDefinition",
		"DeregisterTaskDefinition",
		"ListTaskDefinitions",
		"DeleteTaskDefinitions",
		"CreateService",
		"DescribeServices",
		"UpdateService",
		"DeleteService",
		"ListServices",
		"RunTask",
		"DescribeTasks",
		"StopTask",
		"ListTasks",
		"RegisterContainerInstance",
		"DeregisterContainerInstance",
		"DescribeContainerInstances",
		"ListContainerInstances",
		"UpdateContainerInstancesState",
		"UpdateContainerAgent",
		"CreateTaskSet",
		"DeleteTaskSet",
		"DescribeTaskSets",
		"UpdateTaskSet",
		"UpdateServicePrimaryTaskSet",
		"ExecuteCommand",
		"CreateCapacityProvider",
		"DeleteCapacityProvider",
		"DescribeCapacityProviders",
		"PutClusterCapacityProviders",
		"DeleteAccountSetting",
		"ListAccountSettings",
		"PutAccountSetting",
		"PutAccountSettingDefault",
		"DeleteAttributes",
		"ListAttributes",
		"PutAttributes",
		"UpdateClusterSettings",
		"DescribeServiceDeployments",
		"CreateExpressGatewayService",
		"DeleteExpressGatewayService",
		"DescribeExpressGatewayService",
		"UpdateExpressGatewayService",
		"GetTaskProtection",
		"UpdateTaskProtection",
		"UpdateCluster",
		"UpdateCapacityProvider",
		"ListTaskDefinitionFamilies",
		"StartTask",
		"ListServicesByNamespace",
		"TagResource",
		"UntagResource",
		"ListTagsForResource",
		"ListServiceDeployments",
		"StopServiceDeployment",
		"ContinueServiceDeployment",
		// Daemon operations (stub implementations).
		"CreateDaemon",
		"DeleteDaemon",
		"DeleteDaemonTaskDefinition",
		"DescribeDaemon",
		"DescribeDaemonDeployments",
		"DescribeDaemonRevisions",
		"DescribeDaemonTaskDefinition",
		"ListDaemonDeployments",
		"ListDaemonTaskDefinitions",
		"ListDaemons",
		"RegisterDaemonTaskDefinition",
		"UpdateDaemon",
		// Service revisions.
		"DescribeServiceRevisions",
		// Internal agent endpoint.
		"DiscoverPollEndpoint",
		// State change submissions (internal container agent protocol).
		"SubmitAttachmentStateChanges",
		"SubmitContainerStateChange",
		"SubmitTaskStateChange",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "ecs" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this ECS instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches ECS requests via X-Amz-Target.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, ecsTargetPrefix)
	}
}

// MatchPriority returns the routing priority for ECS.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the ECS action from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, ecsTargetPrefix)

	if action == "" || action == target {
		return unknownActionName
	}

	return action
}

// ExtractResource extracts the primary resource identifier from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		Cluster           string `json:"cluster"`
		Service           string `json:"service"`
		ServiceName       string `json:"serviceName"`
		ClusterName       string `json:"clusterName"`
		Family            string `json:"family"`
		TaskArn           string `json:"task"`
		ContainerInstance string `json:"containerInstance"`
		TaskSet           string `json:"taskSet"`
	}

	_ = json.Unmarshal(body, &req)

	switch {
	case req.ClusterName != "":
		return req.ClusterName
	case req.Cluster != "":
		return req.Cluster
	case req.ServiceName != "":
		return req.ServiceName
	case req.Service != "":
		return req.Service
	case req.Family != "":
		return req.Family
	case req.TaskArn != "":
		return req.TaskArn
	case req.ContainerInstance != "":
		return req.ContainerInstance
	case req.TaskSet != "":
		return req.TaskSet
	}

	return ""
}

// Handler returns the Echo handler function for ECS requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"ECS", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateCluster":            service.WrapOp(h.handleCreateCluster),
		"ListClusters":             service.WrapOp(h.handleListClusters),
		"DescribeClusters":         service.WrapOp(h.handleDescribeClusters),
		"DeleteCluster":            service.WrapOp(h.handleDeleteCluster),
		"RegisterTaskDefinition":   service.WrapOp(h.handleRegisterTaskDefinition),
		"DescribeTaskDefinition":   service.WrapOp(h.handleDescribeTaskDefinition),
		"DeregisterTaskDefinition": service.WrapOp(h.handleDeregisterTaskDefinition),
		"ListTaskDefinitions":      service.WrapOp(h.handleListTaskDefinitions),
		"CreateService":            service.WrapOp(h.handleCreateService),
		"DescribeServices":         service.WrapOp(h.handleDescribeServices),
		"UpdateService":            service.WrapOp(h.handleUpdateService),
		"DeleteService":            service.WrapOp(h.handleDeleteService),
		"ListServices":             service.WrapOp(h.handleListServices),
		"RunTask":                  service.WrapOp(h.handleRunTask),
		"DescribeTasks":            service.WrapOp(h.handleDescribeTasks),
		"StopTask":                 service.WrapOp(h.handleStopTask),
		"ListTasks":                service.WrapOp(h.handleListTasks),
		// Container instances
		"RegisterContainerInstance":     service.WrapOp(h.handleRegisterContainerInstance),
		"DeregisterContainerInstance":   service.WrapOp(h.handleDeregisterContainerInstance),
		"DescribeContainerInstances":    service.WrapOp(h.handleDescribeContainerInstances),
		"ListContainerInstances":        service.WrapOp(h.handleListContainerInstances),
		"UpdateContainerInstancesState": service.WrapOp(h.handleUpdateContainerInstancesState),
		"UpdateContainerAgent":          service.WrapOp(h.handleUpdateContainerAgent),
		// Task sets
		"CreateTaskSet":               service.WrapOp(h.handleCreateTaskSet),
		"DeleteTaskSet":               service.WrapOp(h.handleDeleteTaskSet),
		"DescribeTaskSets":            service.WrapOp(h.handleDescribeTaskSets),
		"UpdateTaskSet":               service.WrapOp(h.handleUpdateTaskSet),
		"UpdateServicePrimaryTaskSet": service.WrapOp(h.handleUpdateServicePrimaryTaskSet),
		// ECS Exec
		"ExecuteCommand": service.WrapOp(h.handleExecuteCommand),
		// Capacity providers
		"CreateCapacityProvider":      service.WrapOp(h.handleCreateCapacityProvider),
		"DeleteCapacityProvider":      service.WrapOp(h.handleDeleteCapacityProvider),
		"DescribeCapacityProviders":   service.WrapOp(h.handleDescribeCapacityProviders),
		"PutClusterCapacityProviders": service.WrapOp(h.handlePutClusterCapacityProviders),
		// Account settings
		"DeleteAccountSetting":     service.WrapOp(h.handleDeleteAccountSetting),
		"ListAccountSettings":      service.WrapOp(h.handleListAccountSettings),
		"PutAccountSetting":        service.WrapOp(h.handlePutAccountSetting),
		"PutAccountSettingDefault": service.WrapOp(h.handlePutAccountSettingDefault),
		// Attributes
		"DeleteAttributes": service.WrapOp(h.handleDeleteAttributes),
		"ListAttributes":   service.WrapOp(h.handleListAttributes),
		"PutAttributes":    service.WrapOp(h.handlePutAttributes),
		// Cluster settings
		"UpdateClusterSettings": service.WrapOp(h.handleUpdateClusterSettings),
		// Task definitions (batch delete)
		"DeleteTaskDefinitions": service.WrapOp(h.handleDeleteTaskDefinitions),
		// Service deployments
		"DescribeServiceDeployments": service.WrapOp(h.handleDescribeServiceDeployments),
		// Express gateway services
		"CreateExpressGatewayService":   service.WrapOp(h.handleCreateExpressGatewayService),
		"DeleteExpressGatewayService":   service.WrapOp(h.handleDeleteExpressGatewayService),
		"DescribeExpressGatewayService": service.WrapOp(h.handleDescribeExpressGatewayService),
		"UpdateExpressGatewayService":   service.WrapOp(h.handleUpdateExpressGatewayService),
		// Task protection
		"GetTaskProtection":    service.WrapOp(h.handleGetTaskProtection),
		"UpdateTaskProtection": service.WrapOp(h.handleUpdateTaskProtection),
		// Cluster management
		"UpdateCluster": service.WrapOp(h.handleUpdateCluster),
		// Capacity provider management
		"UpdateCapacityProvider": service.WrapOp(h.handleUpdateCapacityProvider),
		// Task definition families
		"ListTaskDefinitionFamilies": service.WrapOp(h.handleListTaskDefinitionFamilies),
		// Task placement
		"StartTask": service.WrapOp(h.handleStartTask),
		// Namespace-scoped service listing
		"ListServicesByNamespace": service.WrapOp(h.handleListServicesByNamespace),
		// Tagging
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
		// Service deployments
		"ListServiceDeployments":    service.WrapOp(h.handleListServiceDeployments),
		"StopServiceDeployment":     service.WrapOp(h.handleStopServiceDeployment),
		"ContinueServiceDeployment": service.WrapOp(h.handleContinueServiceDeployment),
		// Daemon stubs
		"CreateDaemon":                 service.WrapOp(h.handleCreateDaemon),
		"DeleteDaemon":                 service.WrapOp(h.handleDeleteDaemon),
		"DeleteDaemonTaskDefinition":   service.WrapOp(h.handleDeleteDaemonTaskDefinition),
		"DescribeDaemon":               service.WrapOp(h.handleDescribeDaemon),
		"DescribeDaemonDeployments":    service.WrapOp(h.handleDescribeDaemonDeployments),
		"DescribeDaemonRevisions":      service.WrapOp(h.handleDescribeDaemonRevisions),
		"DescribeDaemonTaskDefinition": service.WrapOp(h.handleDescribeDaemonTaskDefinition),
		"ListDaemonDeployments":        service.WrapOp(h.handleListDaemonDeployments),
		"ListDaemonTaskDefinitions":    service.WrapOp(h.handleListDaemonTaskDefinitions),
		"ListDaemons":                  service.WrapOp(h.handleListDaemons),
		"RegisterDaemonTaskDefinition": service.WrapOp(h.handleRegisterDaemonTaskDefinition),
		"UpdateDaemon":                 service.WrapOp(h.handleUpdateDaemon),
		// Service revisions
		"DescribeServiceRevisions": service.WrapOp(h.handleDescribeServiceRevisions),
		// Internal agent endpoints
		"DiscoverPollEndpoint":         service.WrapOp(h.handleDiscoverPollEndpoint),
		"SubmitAttachmentStateChanges": service.WrapOp(h.handleSubmitAttachmentStateChanges),
		"SubmitContainerStateChange":   service.WrapOp(h.handleSubmitContainerStateChange),
		"SubmitTaskStateChange":        service.WrapOp(h.handleSubmitTaskStateChange),
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

// Reset clears the backend state.
func (h *Handler) Reset() {
	if r, ok := h.Backend.(interface{ Reset() }); ok {
		r.Reset()
	}
}

func (h *Handler) handleError(_ context.Context, c *echo.Context, _ string, err error) error {
	var syntaxErr *json.SyntaxError
	var typeErr *json.UnmarshalTypeError

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		code := errorCode(err)

		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyTypeField: code, keyMessageField: err.Error()},
		)
	case errors.Is(err, awserr.ErrAlreadyExists):
		code := errorCode(err)

		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyTypeField: code, keyMessageField: err.Error()},
		)
	case errors.Is(err, errUnknownAction):
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{
				keyTypeField:    "UnknownOperationException",
				keyMessageField: err.Error(),
			},
		)
	case errors.Is(err, awserr.ErrInvalidParameter),
		errors.As(err, &syntaxErr),
		errors.As(err, &typeErr):
		code := errorCode(err)

		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyTypeField: code, keyMessageField: err.Error()},
		)
	default:
		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyTypeField: "ServerException", keyMessageField: err.Error()},
		)
	}
}

// errorCode extracts the AWS-style error code from a wrapped error.
//
// Errors are typically built as fmt.Errorf("%w: detail", ErrXxx), where ErrXxx
// is an awserr wrapper whose own message is the bare exception code (for
// example "ClientException"). The chain therefore looks like:
//
//	fmt.wrapError("ClientException: detail") -> awserr("ClientException") -> sentinel
//
// AWS surfaces only the bare code in the response __type / x-amzn-errortype, so
// errorCode walks to the deepest non-sentinel message in the chain, which is the
// bare code rather than the human-readable detail.
func errorCode(err error) string {
	// isSentinel returns true for AWS error sentinel messages that should not be used as error codes.
	isSentinel := func(msg string) bool {
		switch msg {
		case "resource not found", "resource already exists", "invalid parameter", "conflict":
			return true
		}

		return false
	}

	code := "ServerException"

	for currentErr := err; currentErr != nil; currentErr = errors.Unwrap(currentErr) {
		msg := currentErr.Error()
		if !isSentinel(msg) {
			code = msg
		}
	}

	return code
}

// Purge implements service.Purgeable by removing all ECS resources older than cutoff.
func (h *Handler) Purge(ctx context.Context, cutoff time.Time) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Purge(ctx, cutoff)
	}
}

const defaultECSMaxResults = 100

// applyNextTokenSlice applies nextToken-based pagination to a string slice
// using the shared pkgs/page opaque token format.
func applyNextTokenSlice(items []string, nextToken string, maxResults int) ([]string, string) {
	p := page.New(items, nextToken, maxResults, defaultECSMaxResults)

	return p.Data, p.Next
}
