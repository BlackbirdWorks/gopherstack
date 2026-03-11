package emr

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
	emrTargetPrefix = "ElasticMapReduce."
	unknownAction   = "Unknown"
)

var errUnknownAction = errors.New("UnknownOperationException")

// Handler is the Echo HTTP handler for AWS EMR operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new EMR handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "EMR" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"RunJobFlow",
		"DescribeCluster",
		"ListClusters",
		"TerminateJobFlows",
		"AddTags",
		"RemoveTags",
		"ListTagsForResource",
		"ListSteps",
		"AddJobFlowSteps",
		"ListInstanceGroups",
		"ListInstanceFleets",
		"ListBootstrapActions",
		"GetAutoTerminationPolicy",
		"GetManagedScalingPolicy",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "emr" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches EMR requests via X-Amz-Target.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, emrTargetPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation returns the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	action := strings.TrimPrefix(target, emrTargetPrefix)

	if action == "" || action == target {
		return unknownAction
	}

	return action
}

// ExtractResource extracts a resource identifier from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var req struct {
		ClusterID  string `json:"ClusterId"`
		JobFlowID  string `json:"JobFlowId"`
		ResourceID string `json:"ResourceId"`
	}

	_ = json.Unmarshal(body, &req)

	switch {
	case req.ClusterID != "":
		return req.ClusterID
	case req.JobFlowID != "":
		return req.JobFlowID
	case req.ResourceID != "":
		return req.ResourceID
	}

	return ""
}

// Handler returns the Echo handler function for EMR requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"EMR", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

func (h *Handler) dispatchTable() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"RunJobFlow":           service.WrapOp(h.handleRunJobFlow),
		"DescribeCluster":      service.WrapOp(h.handleDescribeCluster),
		"ListClusters":         service.WrapOp(h.handleListClusters),
		"TerminateJobFlows":    service.WrapOp(h.handleTerminateJobFlows),
		"AddTags":              service.WrapOp(h.handleAddTags),
		"RemoveTags":           service.WrapOp(h.handleRemoveTags),
		"ListTagsForResource":  service.WrapOp(h.handleListTagsForResource),
		"ListSteps":            service.WrapOp(h.handleListSteps),
		"AddJobFlowSteps":      service.WrapOp(h.handleAddJobFlowSteps),
		"ListInstanceGroups":   service.WrapOp(h.handleListInstanceGroups),
		"ListInstanceFleets":   service.WrapOp(h.handleListInstanceFleets),
		"ListBootstrapActions": service.WrapOp(h.handleListBootstrapActions),
		"GetAutoTerminationPolicy": service.WrapOp(h.handleGetAutoTerminationPolicy),
		"GetManagedScalingPolicy":  service.WrapOp(h.handleGetManagedScalingPolicy),
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
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", err.Error()))
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", err.Error()))
	case errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, errorResponse("UnknownOperationException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

// --- Input / Output types ---

type runJobFlowInstances struct {
	InstanceGroups []InstanceGroupSpec `json:"InstanceGroups"`
}

type runJobFlowInput struct {
	Name         string              `json:"Name"`
	ReleaseLabel string              `json:"ReleaseLabel"`
	Tags         []Tag               `json:"Tags"`
	Instances    runJobFlowInstances `json:"Instances"`
}

type runJobFlowOutput struct {
	JobFlowID  string `json:"JobFlowId"`
	ClusterArn string `json:"ClusterArn"`
}

func (h *Handler) handleRunJobFlow(_ context.Context, in *runJobFlowInput) (*runJobFlowOutput, error) {
	if in.ReleaseLabel == "" {
		in.ReleaseLabel = "emr-6.0.0"
	}

	cluster, err := h.Backend.RunJobFlow(in.Name, in.ReleaseLabel, in.Tags, in.Instances.InstanceGroups)
	if err != nil {
		return nil, err
	}

	return &runJobFlowOutput{
		JobFlowID:  cluster.ID,
		ClusterArn: cluster.ARN,
	}, nil
}

type describeClusterInput struct {
	ClusterID string `json:"ClusterId"`
}

type describeClusterOutput struct {
	Cluster *Cluster `json:"Cluster"`
}

func (h *Handler) handleDescribeCluster(_ context.Context, in *describeClusterInput) (*describeClusterOutput, error) {
	cluster, err := h.Backend.DescribeCluster(in.ClusterID)
	if err != nil {
		return nil, err
	}

	return &describeClusterOutput{Cluster: cluster}, nil
}

type listClustersInput struct{}

type listClustersOutput struct {
	Clusters []ClusterSummary `json:"Clusters"`
}

func (h *Handler) handleListClusters(_ context.Context, _ *listClustersInput) (*listClustersOutput, error) {
	clusters := h.Backend.ListClusters()

	return &listClustersOutput{Clusters: clusters}, nil
}

type terminateJobFlowsInput struct {
	JobFlowIDs []string `json:"JobFlowIds"`
}

type emptyOutput struct{}

func (h *Handler) handleTerminateJobFlows(
	_ context.Context,
	in *terminateJobFlowsInput,
) (*emptyOutput, error) {
	if err := h.Backend.TerminateJobFlows(in.JobFlowIDs); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type addTagsInput struct {
	ResourceID string `json:"ResourceId"`
	Tags       []Tag  `json:"Tags"`
}

func (h *Handler) handleAddTags(_ context.Context, in *addTagsInput) (*emptyOutput, error) {
	if err := h.Backend.AddTags(in.ResourceID, in.Tags); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type removeTagsInput struct {
	ResourceID string   `json:"ResourceId"`
	TagKeys    []string `json:"TagKeys"`
}

func (h *Handler) handleRemoveTags(_ context.Context, in *removeTagsInput) (*emptyOutput, error) {
	if err := h.Backend.RemoveTags(in.ResourceID, in.TagKeys); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type listTagsForResourceInput struct {
	ResourceID string `json:"ResourceId"`
}

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(in.ResourceID)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

type listStepsInput struct {
	ClusterID string `json:"ClusterId"`
}

type listStepsOutput struct {
	Steps []any `json:"Steps"`
}

func (h *Handler) handleListSteps(_ context.Context, _ *listStepsInput) (*listStepsOutput, error) {
	return &listStepsOutput{Steps: []any{}}, nil
}

type addJobFlowStepsInput struct {
	JobFlowID string `json:"JobFlowId"`
	Steps     []any  `json:"Steps"`
}

type addJobFlowStepsOutput struct {
	StepIDs []string `json:"StepIds"`
}

func (h *Handler) handleAddJobFlowSteps(
	_ context.Context,
	_ *addJobFlowStepsInput,
) (*addJobFlowStepsOutput, error) {
	return &addJobFlowStepsOutput{StepIDs: []string{}}, nil
}

type listInstanceGroupsInput struct {
	ClusterID string `json:"ClusterId"`
}

type listInstanceGroupsOutput struct {
	InstanceGroups []InstanceGroup `json:"InstanceGroups"`
}

func (h *Handler) handleListInstanceGroups(
	_ context.Context,
	in *listInstanceGroupsInput,
) (*listInstanceGroupsOutput, error) {
	groups, err := h.Backend.ListInstanceGroups(in.ClusterID)
	if err != nil {
		return nil, err
	}

	return &listInstanceGroupsOutput{InstanceGroups: groups}, nil
}

type listInstanceFleetsInput struct {
	ClusterID string `json:"ClusterId"`
}

type listInstanceFleetsOutput struct {
	InstanceFleets []any `json:"InstanceFleets"`
}

func (h *Handler) handleListInstanceFleets(
	_ context.Context,
	_ *listInstanceFleetsInput,
) (*listInstanceFleetsOutput, error) {
	return &listInstanceFleetsOutput{InstanceFleets: []any{}}, nil
}

type listBootstrapActionsInput struct {
	ClusterID string `json:"ClusterId"`
}

type listBootstrapActionsOutput struct {
	BootstrapActions []any `json:"BootstrapActions"`
}

func (h *Handler) handleListBootstrapActions(
	_ context.Context,
	_ *listBootstrapActionsInput,
) (*listBootstrapActionsOutput, error) {
	return &listBootstrapActionsOutput{BootstrapActions: []any{}}, nil
}

type getAutoTerminationPolicyInput struct {
	ClusterID string `json:"ClusterId"`
}

type autoTerminationPolicy struct {
	IdleTimeout int64 `json:"IdleTimeout"`
}

type getAutoTerminationPolicyOutput struct {
	AutoTerminationPolicy autoTerminationPolicy `json:"AutoTerminationPolicy"`
}

func (h *Handler) handleGetAutoTerminationPolicy(
	_ context.Context,
	_ *getAutoTerminationPolicyInput,
) (*getAutoTerminationPolicyOutput, error) {
	return &getAutoTerminationPolicyOutput{AutoTerminationPolicy: autoTerminationPolicy{}}, nil
}

type getManagedScalingPolicyInput struct {
	ClusterID string `json:"ClusterId"`
}

type managedScalingPolicy struct{}

type getManagedScalingPolicyOutput struct {
	ManagedScalingPolicy managedScalingPolicy `json:"ManagedScalingPolicy"`
}

func (h *Handler) handleGetManagedScalingPolicy(
	_ context.Context,
	_ *getManagedScalingPolicyInput,
) (*getManagedScalingPolicyOutput, error) {
	return &getManagedScalingPolicyOutput{ManagedScalingPolicy: managedScalingPolicy{}}, nil
}
