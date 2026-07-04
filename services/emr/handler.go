package emr

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
	ops     map[string]service.JSONOpFunc
	janitor *Janitor
}

// NewHandler creates a new EMR handler backed by backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.ops = h.buildOps()

	return h
}

// WithJanitor attaches a background janitor to the handler.
func (h *Handler) WithJanitor(interval, terminatedTTL time.Duration, taskTimeout ...time.Duration) *Handler {
	j := NewJanitor(h.Backend, interval, terminatedTTL)
	if len(taskTimeout) > 0 {
		j.TaskTimeout = taskTimeout[0]
	}

	h.janitor = j

	return h
}

// StartWorker starts the background janitor if configured.
func (h *Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

// Reset clears all in-memory state from the backend.
func (h *Handler) Reset() {
	h.Backend.Reset()
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
		"AddInstanceFleet",
		"AddInstanceGroups",
		"CancelSteps",
		"CreatePersistentAppUI",
		"CreateSecurityConfiguration",
		"CreateStudio",
		"CreateStudioSessionMapping",
		"DeleteSecurityConfiguration",
		"DeleteStudio",
		"DeleteStudioSessionMapping",
		"DescribeSecurityConfiguration",
		"DescribeJobFlows",
		"DescribeNotebookExecution",
		"DescribePersistentAppUI",
		"DescribeReleaseLabel",
		"DescribeStep",
		"DescribeStudio",
		"GetBlockPublicAccessConfiguration",
		"GetClusterSessionCredentials",
		"GetOnClusterAppUIPresignedURL",
		"GetPersistentAppUIPresignedURL",
		"GetStudioSessionMapping",
		"ListInstances",
		"ListNotebookExecutions",
		"ListReleaseLabels",
		"ListSecurityConfigurations",
		"ListStudioSessionMappings",
		"ListStudios",
		"ListSupportedInstanceTypes",
		"ModifyCluster",
		"ModifyInstanceFleet",
		"ModifyInstanceGroups",
		"PutAutoScalingPolicy",
		"PutAutoTerminationPolicy",
		"PutBlockPublicAccessConfiguration",
		"PutManagedScalingPolicy",
		"RemoveAutoScalingPolicy",
		"RemoveAutoTerminationPolicy",
		"RemoveManagedScalingPolicy",
		"SetKeepJobFlowAliveWhenNoSteps",
		"SetTerminationProtection",
		"SetUnhealthyNodeReplacement",
		"SetVisibleToAllUsers",
		"StartNotebookExecution",
		"StopNotebookExecution",
		"UpdateStudio",
		"UpdateStudioSessionMapping",
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
		ClusterID         string `json:"ClusterId"`
		JobFlowID         string `json:"JobFlowId"`
		ResourceID        string `json:"ResourceId"`
		StudioID          string `json:"StudioId"`
		TargetResourceArn string `json:"TargetResourceArn"`
	}

	_ = json.Unmarshal(body, &req)

	switch {
	case req.ClusterID != "":
		return req.ClusterID
	case req.JobFlowID != "":
		return req.JobFlowID
	case req.ResourceID != "":
		return req.ResourceID
	case req.StudioID != "":
		return req.StudioID
	case req.TargetResourceArn != "":
		return req.TargetResourceArn
	}

	return ""
}

// Handler returns the Echo handler function for EMR requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		// Resolve the per-request region (from SigV4 / X-Amz-Region) and attach
		// it to the context so backend operations are region-scoped.
		region := httputils.ExtractRegionFromRequest(c.Request(), h.Backend.Region())

		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"EMR", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(ctx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(context.WithValue(ctx, regionContextKey{}, region), action, body)
			},
			h.handleError,
		)
	}
}

func (h *Handler) buildOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"RunJobFlow":                        service.WrapOp(h.handleRunJobFlow),
		"DescribeCluster":                   service.WrapOp(h.handleDescribeCluster),
		"ListClusters":                      service.WrapOp(h.handleListClusters),
		"TerminateJobFlows":                 service.WrapOp(h.handleTerminateJobFlows),
		"AddTags":                           service.WrapOp(h.handleAddTags),
		"RemoveTags":                        service.WrapOp(h.handleRemoveTags),
		"ListTagsForResource":               service.WrapOp(h.handleListTagsForResource),
		"ListSteps":                         service.WrapOp(h.handleListSteps),
		"AddJobFlowSteps":                   service.WrapOp(h.handleAddJobFlowSteps),
		"ListInstanceGroups":                service.WrapOp(h.handleListInstanceGroups),
		"ListInstanceFleets":                service.WrapOp(h.handleListInstanceFleets),
		"ListBootstrapActions":              service.WrapOp(h.handleListBootstrapActions),
		"GetAutoTerminationPolicy":          service.WrapOp(h.handleGetAutoTerminationPolicy),
		"GetManagedScalingPolicy":           service.WrapOp(h.handleGetManagedScalingPolicy),
		"AddInstanceFleet":                  service.WrapOp(h.handleAddInstanceFleet),
		"AddInstanceGroups":                 service.WrapOp(h.handleAddInstanceGroups),
		"CancelSteps":                       service.WrapOp(h.handleCancelSteps),
		"CreatePersistentAppUI":             service.WrapOp(h.handleCreatePersistentAppUI),
		"CreateSecurityConfiguration":       service.WrapOp(h.handleCreateSecurityConfiguration),
		"CreateStudio":                      service.WrapOp(h.handleCreateStudio),
		"CreateStudioSessionMapping":        service.WrapOp(h.handleCreateStudioSessionMapping),
		"DeleteSecurityConfiguration":       service.WrapOp(h.handleDeleteSecurityConfiguration),
		"DeleteStudio":                      service.WrapOp(h.handleDeleteStudio),
		"DeleteStudioSessionMapping":        service.WrapOp(h.handleDeleteStudioSessionMapping),
		"DescribeSecurityConfiguration":     service.WrapOp(h.handleDescribeSecurityConfiguration),
		"DescribeJobFlows":                  service.WrapOp(h.handleDescribeJobFlows),
		"DescribeNotebookExecution":         service.WrapOp(h.handleDescribeNotebookExecution),
		"DescribePersistentAppUI":           service.WrapOp(h.handleDescribePersistentAppUI),
		"DescribeReleaseLabel":              service.WrapOp(h.handleDescribeReleaseLabel),
		"DescribeStep":                      service.WrapOp(h.handleDescribeStep),
		"DescribeStudio":                    service.WrapOp(h.handleDescribeStudio),
		"GetBlockPublicAccessConfiguration": service.WrapOp(h.handleGetBlockPublicAccessConfiguration),
		"GetClusterSessionCredentials":      service.WrapOp(h.handleGetClusterSessionCredentials),
		"GetOnClusterAppUIPresignedURL":     service.WrapOp(h.handleGetOnClusterAppUIPresignedURL),
		"GetPersistentAppUIPresignedURL":    service.WrapOp(h.handleGetPersistentAppUIPresignedURL),
		"GetStudioSessionMapping":           service.WrapOp(h.handleGetStudioSessionMapping),
		"ListInstances":                     service.WrapOp(h.handleListInstances),
		"ListNotebookExecutions":            service.WrapOp(h.handleListNotebookExecutions),
		"ListReleaseLabels":                 service.WrapOp(h.handleListReleaseLabels),
		"ListSecurityConfigurations":        service.WrapOp(h.handleListSecurityConfigurations),
		"ListStudioSessionMappings":         service.WrapOp(h.handleListStudioSessionMappings),
		"ListStudios":                       service.WrapOp(h.handleListStudios),
		"ListSupportedInstanceTypes":        service.WrapOp(h.handleListSupportedInstanceTypes),
		"ModifyCluster":                     service.WrapOp(h.handleModifyCluster),
		"ModifyInstanceFleet":               service.WrapOp(h.handleModifyInstanceFleet),
		"ModifyInstanceGroups":              service.WrapOp(h.handleModifyInstanceGroups),
		"PutAutoScalingPolicy":              service.WrapOp(h.handlePutAutoScalingPolicy),
		"PutAutoTerminationPolicy":          service.WrapOp(h.handlePutAutoTerminationPolicy),
		"PutBlockPublicAccessConfiguration": service.WrapOp(h.handlePutBlockPublicAccessConfiguration),
		"PutManagedScalingPolicy":           service.WrapOp(h.handlePutManagedScalingPolicy),
		"RemoveAutoScalingPolicy":           service.WrapOp(h.handleRemoveAutoScalingPolicy),
		"RemoveAutoTerminationPolicy":       service.WrapOp(h.handleRemoveAutoTerminationPolicy),
		"RemoveManagedScalingPolicy":        service.WrapOp(h.handleRemoveManagedScalingPolicy),
		"SetKeepJobFlowAliveWhenNoSteps":    service.WrapOp(h.handleSetKeepJobFlowAliveWhenNoSteps),
		"SetTerminationProtection":          service.WrapOp(h.handleSetTerminationProtection),
		"SetUnhealthyNodeReplacement":       service.WrapOp(h.handleSetUnhealthyNodeReplacement),
		"SetVisibleToAllUsers":              service.WrapOp(h.handleSetVisibleToAllUsers),
		"StartNotebookExecution":            service.WrapOp(h.handleStartNotebookExecution),
		"StopNotebookExecution":             service.WrapOp(h.handleStopNotebookExecution),
		"UpdateStudio":                      service.WrapOp(h.handleUpdateStudio),
		"UpdateStudioSessionMapping":        service.WrapOp(h.handleUpdateStudioSessionMapping),
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
	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", err.Error()))
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.JSON(http.StatusBadRequest, errorResponse("InvalidRequestException", err.Error()))
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", err.Error()))
	case errors.Is(err, errUnknownAction):
		return c.JSON(http.StatusBadRequest, errorResponse("UnknownOperationException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

// --- RunJobFlow ---

type runJobFlowInput struct {
	SecurityConfiguration   string                  `json:"SecurityConfiguration"`
	ReleaseLabel            string                  `json:"ReleaseLabel"`
	OSReleaseLabel          string                  `json:"OSReleaseLabel"`
	LogURI                  string                  `json:"LogUri"`
	ServiceRole             string                  `json:"ServiceRole"`
	AutoScalingRole         string                  `json:"AutoScalingRole"`
	Name                    string                  `json:"Name"`
	ScaleDownBehavior       string                  `json:"ScaleDownBehavior"`
	CustomAmiID             string                  `json:"CustomAmiId"`
	Tags                    []Tag                   `json:"Tags"`
	Applications            []Application           `json:"Applications"`
	Configurations          []Configuration         `json:"Configurations"`
	Steps                   []StepSpec              `json:"Steps"`
	BootstrapActions        []BootstrapActionConfig `json:"BootstrapActions"`
	Instances               RunJobFlowInstances     `json:"Instances"`
	StepConcurrencyLevel    int                     `json:"StepConcurrencyLevel"`
	EbsRootVolumeSize       int                     `json:"EbsRootVolumeSize"`
	EbsRootVolumeIops       int                     `json:"EbsRootVolumeIops"`
	EbsRootVolumeThroughput int                     `json:"EbsRootVolumeThroughput"`
	VisibleToAllUsers       bool                    `json:"VisibleToAllUsers"`
}

type runJobFlowOutput struct {
	JobFlowID  string `json:"JobFlowId"`
	ClusterArn string `json:"ClusterArn"`
}

func (h *Handler) handleRunJobFlow(ctx context.Context, in *runJobFlowInput) (*runJobFlowOutput, error) {
	cluster, err := h.Backend.RunJobFlow(ctx, RunJobFlowParams{
		Name:                    in.Name,
		ReleaseLabel:            in.ReleaseLabel,
		OSReleaseLabel:          in.OSReleaseLabel,
		Tags:                    in.Tags,
		Applications:            in.Applications,
		Configurations:          in.Configurations,
		Steps:                   in.Steps,
		BootstrapActions:        in.BootstrapActions,
		Instances:               in.Instances,
		LogURI:                  in.LogURI,
		ServiceRole:             in.ServiceRole,
		AutoScalingRole:         in.AutoScalingRole,
		ScaleDownBehavior:       in.ScaleDownBehavior,
		SecurityConfiguration:   in.SecurityConfiguration,
		CustomAmiID:             in.CustomAmiID,
		StepConcurrencyLevel:    in.StepConcurrencyLevel,
		EbsRootVolumeSize:       in.EbsRootVolumeSize,
		EbsRootVolumeIops:       in.EbsRootVolumeIops,
		EbsRootVolumeThroughput: in.EbsRootVolumeThroughput,
		VisibleToAllUsers:       in.VisibleToAllUsers,
	})
	if err != nil {
		return nil, err
	}

	return &runJobFlowOutput{
		JobFlowID:  cluster.ID,
		ClusterArn: cluster.ARN,
	}, nil
}

// --- DescribeCluster ---

type describeClusterInput struct {
	ClusterID string `json:"ClusterId"`
}

type describeClusterOutput struct {
	Cluster *Cluster `json:"Cluster"`
}

func (h *Handler) handleDescribeCluster(ctx context.Context, in *describeClusterInput) (*describeClusterOutput, error) {
	cluster, err := h.Backend.DescribeCluster(ctx, in.ClusterID)
	if err != nil {
		return nil, err
	}

	return &describeClusterOutput{Cluster: cluster}, nil
}

// --- ListClusters ---

type listClustersInput struct {
	CreatedAfter  *float64 `json:"CreatedAfter"`
	CreatedBefore *float64 `json:"CreatedBefore"`
	Marker        string   `json:"Marker"`
	ClusterStates []string `json:"ClusterStates"`
}

type listClustersOutput struct {
	Marker   string           `json:"Marker,omitempty"`
	Clusters []ClusterSummary `json:"Clusters"`
}

func (h *Handler) handleListClusters(ctx context.Context, in *listClustersInput) (*listClustersOutput, error) {
	params := ListClustersParams{
		ClusterStates: in.ClusterStates,
		Marker:        in.Marker,
	}

	if in.CreatedAfter != nil {
		t := time.UnixMilli(int64(*in.CreatedAfter))
		params.CreatedAfter = &t
	}

	if in.CreatedBefore != nil {
		t := time.UnixMilli(int64(*in.CreatedBefore))
		params.CreatedBefore = &t
	}

	clusters, nextMarker := h.Backend.ListClusters(ctx, params)

	return &listClustersOutput{Clusters: clusters, Marker: nextMarker}, nil
}

// --- TerminateJobFlows ---

type terminateJobFlowsInput struct {
	JobFlowIDs []string `json:"JobFlowIds"`
}

type emptyOutput struct{}

func (h *Handler) handleTerminateJobFlows(
	ctx context.Context,
	in *terminateJobFlowsInput,
) (*emptyOutput, error) {
	if err := h.Backend.TerminateJobFlows(ctx, in.JobFlowIDs); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- AddTags ---

type addTagsInput struct {
	ResourceID string `json:"ResourceId"`
	Tags       []Tag  `json:"Tags"`
}

func (h *Handler) handleAddTags(ctx context.Context, in *addTagsInput) (*emptyOutput, error) {
	if err := h.Backend.AddTags(ctx, in.ResourceID, in.Tags); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- RemoveTags ---

type removeTagsInput struct {
	ResourceID string   `json:"ResourceId"`
	TagKeys    []string `json:"TagKeys"`
}

func (h *Handler) handleRemoveTags(ctx context.Context, in *removeTagsInput) (*emptyOutput, error) {
	if err := h.Backend.RemoveTags(ctx, in.ResourceID, in.TagKeys); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- ListTagsForResource ---

type listTagsForResourceInput struct {
	ResourceID string `json:"ResourceId"`
}

type listTagsForResourceOutput struct {
	Tags []Tag `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceID)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tags}, nil
}

// --- ListSteps ---

type listStepsInput struct {
	ClusterID  string   `json:"ClusterId"`
	Marker     string   `json:"Marker"`
	StepStates []string `json:"StepStates"`
	StepIDs    []string `json:"StepIds"`
}

type listStepsOutput struct {
	Marker string `json:"Marker,omitempty"`
	Steps  []Step `json:"Steps"`
}

func (h *Handler) handleListSteps(ctx context.Context, in *listStepsInput) (*listStepsOutput, error) {
	steps, nextMarker := h.Backend.ListSteps(ctx, in.ClusterID, in.StepStates, in.StepIDs, in.Marker)

	return &listStepsOutput{Steps: steps, Marker: nextMarker}, nil
}

// --- AddJobFlowSteps ---

type addJobFlowStepsInput struct {
	JobFlowID string     `json:"JobFlowId"`
	Steps     []StepSpec `json:"Steps"`
}

type addJobFlowStepsOutput struct {
	StepIDs []string `json:"StepIds"`
}

func (h *Handler) handleAddJobFlowSteps(
	ctx context.Context,
	in *addJobFlowStepsInput,
) (*addJobFlowStepsOutput, error) {
	ids, err := h.Backend.AddJobFlowSteps(ctx, in.JobFlowID, in.Steps)
	if err != nil {
		return nil, err
	}

	return &addJobFlowStepsOutput{StepIDs: ids}, nil
}

// --- ListInstanceGroups ---

type listInstanceGroupsInput struct {
	ClusterID string `json:"ClusterId"`
}

type listInstanceGroupsOutput struct {
	InstanceGroups []InstanceGroup `json:"InstanceGroups"`
}

func (h *Handler) handleListInstanceGroups(
	ctx context.Context,
	in *listInstanceGroupsInput,
) (*listInstanceGroupsOutput, error) {
	groups, err := h.Backend.ListInstanceGroups(ctx, in.ClusterID)
	if err != nil {
		return nil, err
	}

	return &listInstanceGroupsOutput{InstanceGroups: groups}, nil
}

// --- ListInstanceFleets ---

type listInstanceFleetsInput struct {
	ClusterID string `json:"ClusterId"`
}

type listInstanceFleetsOutput struct {
	InstanceFleets []InstanceFleet `json:"InstanceFleets"`
}

func (h *Handler) handleListInstanceFleets(
	ctx context.Context,
	in *listInstanceFleetsInput,
) (*listInstanceFleetsOutput, error) {
	fleets, err := h.Backend.ListInstanceFleets(ctx, in.ClusterID)
	if err != nil {
		return nil, err
	}

	return &listInstanceFleetsOutput{InstanceFleets: fleets}, nil
}

// --- ListBootstrapActions ---

type listBootstrapActionsInput struct {
	ClusterID string `json:"ClusterId"`
	Marker    string `json:"Marker"`
}

type listBootstrapActionsOutput struct {
	Marker           string    `json:"Marker,omitempty"`
	BootstrapActions []Command `json:"BootstrapActions"`
}

func (h *Handler) handleListBootstrapActions(
	ctx context.Context,
	in *listBootstrapActionsInput,
) (*listBootstrapActionsOutput, error) {
	commands, nextMarker, err := h.Backend.ListBootstrapActions(ctx, in.ClusterID, in.Marker)
	if err != nil {
		return nil, err
	}

	if commands == nil {
		commands = []Command{}
	}

	return &listBootstrapActionsOutput{BootstrapActions: commands, Marker: nextMarker}, nil
}

// --- GetAutoTerminationPolicy ---

type getAutoTerminationPolicyInput struct {
	ClusterID string `json:"ClusterId"`
}

type getAutoTerminationPolicyOutput struct {
	AutoTerminationPolicy AutoTerminationPolicy `json:"AutoTerminationPolicy"`
}

func (h *Handler) handleGetAutoTerminationPolicy(
	ctx context.Context,
	in *getAutoTerminationPolicyInput,
) (*getAutoTerminationPolicyOutput, error) {
	policy, err := h.Backend.GetAutoTerminationPolicy(ctx, in.ClusterID)
	if err != nil {
		return nil, err
	}

	if policy == nil {
		return &getAutoTerminationPolicyOutput{AutoTerminationPolicy: AutoTerminationPolicy{}}, nil
	}

	return &getAutoTerminationPolicyOutput{AutoTerminationPolicy: *policy}, nil
}

// --- GetManagedScalingPolicy ---

type getManagedScalingPolicyInput struct {
	ClusterID string `json:"ClusterId"`
}

type getManagedScalingPolicyOutput struct {
	ManagedScalingPolicy ManagedScalingPolicy `json:"ManagedScalingPolicy"`
}

func (h *Handler) handleGetManagedScalingPolicy(
	ctx context.Context,
	in *getManagedScalingPolicyInput,
) (*getManagedScalingPolicyOutput, error) {
	policy, err := h.Backend.GetManagedScalingPolicy(ctx, in.ClusterID)
	if err != nil {
		return nil, err
	}

	if policy == nil {
		return &getManagedScalingPolicyOutput{ManagedScalingPolicy: ManagedScalingPolicy{}}, nil
	}

	return &getManagedScalingPolicyOutput{ManagedScalingPolicy: *policy}, nil
}

// --- AddInstanceFleet ---

type addInstanceFleetInput struct {
	ClusterID     string            `json:"ClusterId"`
	InstanceFleet InstanceFleetSpec `json:"InstanceFleet"`
}

type addInstanceFleetOutput struct {
	ClusterArn      string `json:"ClusterArn"`
	ClusterID       string `json:"ClusterId"`
	InstanceFleetID string `json:"InstanceFleetId"`
}

func (h *Handler) handleAddInstanceFleet(
	ctx context.Context,
	in *addInstanceFleetInput,
) (*addInstanceFleetOutput, error) {
	fleet, clusterARN, err := h.Backend.AddInstanceFleet(ctx, in.ClusterID, in.InstanceFleet)
	if err != nil {
		return nil, err
	}

	return &addInstanceFleetOutput{
		ClusterArn:      clusterARN,
		ClusterID:       in.ClusterID,
		InstanceFleetID: fleet.ID,
	}, nil
}

// --- AddInstanceGroups ---

type addInstanceGroupsInput struct {
	JobFlowID      string              `json:"JobFlowId"`
	InstanceGroups []InstanceGroupSpec `json:"InstanceGroups"`
}

type addInstanceGroupsOutput struct {
	ClusterArn       string   `json:"ClusterArn"`
	JobFlowID        string   `json:"JobFlowId"`
	InstanceGroupIDs []string `json:"InstanceGroupIds"`
}

func (h *Handler) handleAddInstanceGroups(
	ctx context.Context,
	in *addInstanceGroupsInput,
) (*addInstanceGroupsOutput, error) {
	groupIDs, clusterARN, err := h.Backend.AddInstanceGroups(ctx, in.JobFlowID, in.InstanceGroups)
	if err != nil {
		return nil, err
	}

	return &addInstanceGroupsOutput{
		ClusterArn:       clusterARN,
		InstanceGroupIDs: groupIDs,
		JobFlowID:        in.JobFlowID,
	}, nil
}

// --- CancelSteps ---

type cancelStepsInput struct {
	ClusterID string   `json:"ClusterId"`
	StepIDs   []string `json:"StepIds"`
}

type cancelStepsOutput struct {
	CancelStepsInfoList []any `json:"CancelStepsInfoList"`
}

func (h *Handler) handleCancelSteps(
	ctx context.Context,
	in *cancelStepsInput,
) (*cancelStepsOutput, error) {
	results, err := h.Backend.CancelSteps(ctx, in.ClusterID, in.StepIDs)
	if err != nil {
		return nil, err
	}

	list := make([]any, 0, len(results))
	for _, r := range results {
		list = append(list, r)
	}

	return &cancelStepsOutput{CancelStepsInfoList: list}, nil
}

// --- CreatePersistentAppUI ---

type createPersistentAppUIInput struct {
	TargetResourceArn string `json:"TargetResourceArn"`
}

type createPersistentAppUIOutput struct {
	PersistentAppUIID         string `json:"PersistentAppUIId"`
	RuntimeRoleEnabledCluster bool   `json:"RuntimeRoleEnabledCluster"`
}

func (h *Handler) handleCreatePersistentAppUI(
	ctx context.Context,
	in *createPersistentAppUIInput,
) (*createPersistentAppUIOutput, error) {
	ui, err := h.Backend.CreatePersistentAppUI(ctx, in.TargetResourceArn)
	if err != nil {
		return nil, err
	}

	return &createPersistentAppUIOutput{
		PersistentAppUIID:         ui.ID,
		RuntimeRoleEnabledCluster: ui.RuntimeRoleEnabledCluster,
	}, nil
}

// --- CreateSecurityConfiguration ---

type createSecurityConfigurationInput struct {
	Name                  string `json:"Name"`
	SecurityConfiguration string `json:"SecurityConfiguration"`
}

type createSecurityConfigurationOutput struct {
	CreationDateTime string `json:"CreationDateTime"`
	Name             string `json:"Name"`
}

func (h *Handler) handleCreateSecurityConfiguration(
	ctx context.Context,
	in *createSecurityConfigurationInput,
) (*createSecurityConfigurationOutput, error) {
	sc, err := h.Backend.CreateSecurityConfiguration(ctx, in.Name, in.SecurityConfiguration)
	if err != nil {
		return nil, err
	}

	return &createSecurityConfigurationOutput{
		Name:             sc.Name,
		CreationDateTime: sc.CreationDateTime.UTC().Format("2006-01-02T15:04:05Z"),
	}, nil
}

// --- DeleteSecurityConfiguration ---

type deleteSecurityConfigurationInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteSecurityConfiguration(
	ctx context.Context,
	in *deleteSecurityConfigurationInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteSecurityConfiguration(ctx, in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- CreateStudio ---

type createStudioInput struct {
	Name                     string   `json:"Name"`
	AuthMode                 string   `json:"AuthMode"`
	DefaultS3Location        string   `json:"DefaultS3Location"`
	EngineSecurityGroupID    string   `json:"EngineSecurityGroupId"`
	ServiceRole              string   `json:"ServiceRole"`
	VpcID                    string   `json:"VpcId"`
	WorkspaceSecurityGroupID string   `json:"WorkspaceSecurityGroupId"`
	SubnetIDs                []string `json:"SubnetIds"`
	Tags                     []Tag    `json:"Tags"`
}

type createStudioOutput struct {
	StudioID string `json:"StudioId"`
	URL      string `json:"Url"`
}

func (h *Handler) handleCreateStudio(
	ctx context.Context,
	in *createStudioInput,
) (*createStudioOutput, error) {
	studio, err := h.Backend.CreateStudio(ctx, in.Name,
		in.AuthMode,
		in.DefaultS3Location,
		in.EngineSecurityGroupID,
		in.ServiceRole,
		in.VpcID,
		in.WorkspaceSecurityGroupID,
		in.SubnetIDs,
		in.Tags,
	)
	if err != nil {
		return nil, err
	}

	return &createStudioOutput{
		StudioID: studio.StudioID,
		URL:      studio.URL,
	}, nil
}

// --- DeleteStudio ---

type deleteStudioInput struct {
	StudioID string `json:"StudioId"`
}

func (h *Handler) handleDeleteStudio(
	ctx context.Context,
	in *deleteStudioInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteStudio(ctx, in.StudioID); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- CreateStudioSessionMapping ---

type createStudioSessionMappingInput struct {
	StudioID         string `json:"StudioId"`
	IdentityType     string `json:"IdentityType"`
	IdentityID       string `json:"IdentityId,omitempty"`
	IdentityName     string `json:"IdentityName,omitempty"`
	SessionPolicyArn string `json:"SessionPolicyArn"`
}

func (h *Handler) handleCreateStudioSessionMapping(
	ctx context.Context,
	in *createStudioSessionMappingInput,
) (*emptyOutput, error) {
	if err := h.Backend.CreateStudioSessionMapping(ctx, in.StudioID,
		in.IdentityType,
		in.IdentityID,
		in.IdentityName,
		in.SessionPolicyArn,
	); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- DeleteStudioSessionMapping ---

type deleteStudioSessionMappingInput struct {
	StudioID     string `json:"StudioId"`
	IdentityType string `json:"IdentityType"`
	IdentityID   string `json:"IdentityId,omitempty"`
	IdentityName string `json:"IdentityName,omitempty"`
}

func (h *Handler) handleDeleteStudioSessionMapping(
	ctx context.Context,
	in *deleteStudioSessionMappingInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteStudioSessionMapping(ctx, in.StudioID,
		in.IdentityType,
		in.IdentityID,
		in.IdentityName,
	); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// --- DescribeSecurityConfiguration ---

type describeSecurityConfigurationInput struct {
	Name string `json:"Name"`
}

type describeSecurityConfigurationOutput struct {
	CreationDateTime      string `json:"CreationDateTime"`
	Name                  string `json:"Name"`
	SecurityConfiguration string `json:"SecurityConfiguration"`
}

func (h *Handler) handleDescribeSecurityConfiguration(
	ctx context.Context,
	in *describeSecurityConfigurationInput,
) (*describeSecurityConfigurationOutput, error) {
	sc, err := h.Backend.DescribeSecurityConfiguration(ctx, in.Name)
	if err != nil {
		return nil, err
	}

	return &describeSecurityConfigurationOutput{
		Name:                  sc.Name,
		SecurityConfiguration: sc.SecurityConfig,
		CreationDateTime:      sc.CreationDateTime.UTC().Format("2006-01-02T15:04:05Z"),
	}, nil
}
