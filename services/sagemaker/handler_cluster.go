package sagemaker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// instanceGroupStatusInService is the (only) status this emulator reports for
// an instance group, since instance groups transition to InService immediately.
const instanceGroupStatusInService = "InService"

// clusterOpsSupported returns the real SageMaker HyperPod Cluster operations.
func clusterOpsSupported() []string {
	return []string{
		"CreateCluster",
		"DescribeCluster",
		"ListClusters",
		"DeleteCluster",
		"UpdateCluster",
		"UpdateClusterSoftware",
		"DescribeClusterNode",
		"ListClusterNodes",
		"DescribeClusterEvent",
		"ListClusterEvents",
		"DetachClusterNodeVolume",
		"StartClusterHealthCheck",
	}
}

// dispatchClusterOps handles the SageMaker HyperPod Cluster operation family.
func (h *Handler) dispatchClusterOps(ctx context.Context, op string, body []byte) ([]byte, bool, error) {
	switch op {
	case "CreateCluster":
		r, err := h.handleCreateCluster(ctx, body)

		return r, true, err
	case "DescribeCluster":
		r, err := h.handleDescribeCluster(ctx, body)

		return r, true, err
	case "ListClusters":
		r, err := h.handleListClusters(ctx, body)

		return r, true, err
	case "DeleteCluster":
		r, err := h.handleDeleteCluster(ctx, body)

		return r, true, err
	case "UpdateCluster":
		r, err := h.handleUpdateCluster(ctx, body)

		return r, true, err
	case "UpdateClusterSoftware":
		r, err := h.handleUpdateClusterSoftware(ctx, body)

		return r, true, err
	case "DescribeClusterNode":
		r, err := h.handleDescribeClusterNode(ctx, body)

		return r, true, err
	case "ListClusterNodes":
		r, err := h.handleListClusterNodes(ctx, body)

		return r, true, err
	case "DescribeClusterEvent":
		r, err := h.handleDescribeClusterEvent(ctx, body)

		return r, true, err
	case "ListClusterEvents":
		r, err := h.handleListClusterEvents(ctx, body)

		return r, true, err
	case "DetachClusterNodeVolume":
		r, err := h.handleDetachClusterNodeVolume(ctx, body)

		return r, true, err
	case "StartClusterHealthCheck":
		r, err := h.handleStartClusterHealthCheck(ctx, body)

		return r, true, err
	}

	return nil, false, nil
}

// instanceGroupHealthCheckConfigRequest is the wire shape for one entry of
// StartClusterHealthCheckInput.DeepHealthCheckConfigurations
// ([]types.InstanceGroupHealthCheckConfiguration). Its contents are only
// validated for presence (required, non-empty request array) — this
// emulator does not synthesize per-node deep-health-check results, so the
// per-entry fields are accepted but not otherwise interpreted.
type instanceGroupHealthCheckConfigRequest struct {
	InstanceGroupName string   `json:"InstanceGroupName"`
	DeepHealthChecks  []string `json:"DeepHealthChecks"`
	InstanceIDs       []string `json:"InstanceIds"`
}

// startClusterHealthCheckInput is StartClusterHealthCheck's request shape
// (api_op_StartClusterHealthCheck.go:28-40).
type startClusterHealthCheckInput struct {
	ClusterName                   string                                  `json:"ClusterName"`
	DeepHealthCheckConfigurations []instanceGroupHealthCheckConfigRequest `json:"DeepHealthCheckConfigurations"`
}

func (h *Handler) handleStartClusterHealthCheck(ctx context.Context, body []byte) ([]byte, error) {
	var req startClusterHealthCheckInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	clusterARN, err := h.Backend.StartClusterHealthCheck(
		ctx,
		req.ClusterName,
		len(req.DeepHealthCheckConfigurations) > 0,
	)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]string{keyClusterArn: clusterARN})
}

// clusterInstanceGroupRequest is the wire shape for a
// ClusterInstanceGroupSpecification (Create/UpdateCluster requests).
type clusterInstanceGroupRequest struct {
	InstanceGroupName string `json:"InstanceGroupName"`
	InstanceType      string `json:"InstanceType,omitempty"`
	ExecutionRole     string `json:"ExecutionRole,omitempty"`
	InstanceCount     int32  `json:"InstanceCount,omitempty"`
}

func toClusterInstanceGroups(reqs []clusterInstanceGroupRequest) []ClusterInstanceGroup {
	groups := make([]ClusterInstanceGroup, 0, len(reqs))

	for _, r := range reqs {
		groups = append(groups, ClusterInstanceGroup(r))
	}

	return groups
}

// clusterInstanceGroupDetails is the wire shape for a
// ClusterInstanceGroupDetails (Describe/UpdateCluster responses).
type clusterInstanceGroupDetails struct {
	InstanceGroupName string `json:"InstanceGroupName"`
	InstanceType      string `json:"InstanceType,omitempty"`
	ExecutionRole     string `json:"ExecutionRole,omitempty"`
	Status            string `json:"Status"`
	CurrentCount      int32  `json:"CurrentCount"`
	TargetCount       int32  `json:"TargetCount"`
}

func fromClusterInstanceGroups(groups []ClusterInstanceGroup) []clusterInstanceGroupDetails {
	details := make([]clusterInstanceGroupDetails, 0, len(groups))

	for _, g := range groups {
		details = append(details, clusterInstanceGroupDetails{
			InstanceGroupName: g.InstanceGroupName,
			InstanceType:      g.InstanceType,
			ExecutionRole:     g.ExecutionRole,
			Status:            instanceGroupStatusInService,
			CurrentCount:      g.InstanceCount,
			TargetCount:       g.InstanceCount,
		})
	}

	return details
}

// createClusterRequest is the request body for CreateCluster.
type createClusterRequest struct {
	VpcConfig            *VpcConfig                    `json:"VpcConfig,omitempty"`
	AutoScaling          *ClusterAutoScalingConfig     `json:"AutoScaling,omitempty"`
	Orchestrator         *ClusterOrchestrator          `json:"Orchestrator,omitempty"`
	TieredStorageConfig  *ClusterTieredStorageConfig   `json:"TieredStorageConfig,omitempty"`
	ClusterName          string                        `json:"ClusterName"`
	NodeRecovery         string                        `json:"NodeRecovery,omitempty"`
	ClusterRole          string                        `json:"ClusterRole,omitempty"`
	NodeProvisioningMode string                        `json:"NodeProvisioningMode,omitempty"`
	InstanceGroups       []clusterInstanceGroupRequest `json:"InstanceGroups"`
	Tags                 []tagObject                   `json:"Tags"`
}

func (h *Handler) handleCreateCluster(ctx context.Context, body []byte) ([]byte, error) {
	var req createClusterRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	c, err := h.Backend.CreateCluster(ctx, CreateClusterOptions{
		ClusterName:          req.ClusterName,
		InstanceGroups:       toClusterInstanceGroups(req.InstanceGroups),
		NodeRecovery:         req.NodeRecovery,
		ClusterRole:          req.ClusterRole,
		NodeProvisioningMode: req.NodeProvisioningMode,
		VpcConfig:            req.VpcConfig,
		AutoScaling:          req.AutoScaling,
		Orchestrator:         req.Orchestrator,
		TieredStorageConfig:  req.TieredStorageConfig,
		Tags:                 fromTagObjects(req.Tags),
	})
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: created cluster", "name", c.ClusterName, "arn", c.ClusterArn)

	return json.Marshal(map[string]string{keyClusterArn: c.ClusterArn})
}

// clusterAutoScalingStatusInService is the (only) status this emulator
// reports for a configured autoscaler, mirroring instanceGroupStatusInService
// since asynchronous autoscaler provisioning is not modeled.
const clusterAutoScalingStatusInService = "InService"

// clusterAutoScalingConfigResponse is the wire shape for DescribeClusterOutput.AutoScaling
// (types.ClusterAutoScalingConfigOutput, types/types.go:4507, sagemaker@v1.263.2) — adds
// the required Status field over the input-only ClusterAutoScalingConfig.
type clusterAutoScalingConfigResponse struct {
	Mode           string `json:"Mode"`
	Status         string `json:"Status"`
	AutoScalerType string `json:"AutoScalerType,omitempty"`
}

func (h *Handler) describeClusterResponse(c *Cluster) []byte {
	resp := map[string]any{
		keyClusterArn:    c.ClusterArn,
		"ClusterName":    c.ClusterName,
		"ClusterStatus":  c.ClusterStatus,
		"InstanceGroups": fromClusterInstanceGroups(c.InstanceGroups),
		keyCreationTime:  epochSeconds(c.CreationTime),
	}

	if c.NodeRecovery != "" {
		resp["NodeRecovery"] = c.NodeRecovery
	}
	if c.ClusterRole != "" {
		resp["ClusterRole"] = c.ClusterRole
	}
	if c.NodeProvisioningMode != "" {
		resp["NodeProvisioningMode"] = c.NodeProvisioningMode
	}
	if c.VpcConfig != nil {
		resp["VpcConfig"] = c.VpcConfig
	}
	if c.AutoScaling != nil {
		resp["AutoScaling"] = clusterAutoScalingConfigResponse{
			Mode:           c.AutoScaling.Mode,
			Status:         clusterAutoScalingStatusInService,
			AutoScalerType: c.AutoScaling.AutoScalerType,
		}
	}
	if c.Orchestrator != nil {
		resp["Orchestrator"] = c.Orchestrator
	}
	if c.TieredStorageConfig != nil {
		resp["TieredStorageConfig"] = c.TieredStorageConfig
	}

	b, _ := json.Marshal(resp)

	return b
}

// describeClusterInput is DescribeCluster's request shape
// (api_op_DescribeCluster.go:28-36).
type describeClusterInput struct {
	ClusterName string `json:"ClusterName"`
}

func (h *Handler) handleDescribeCluster(ctx context.Context, body []byte) ([]byte, error) {
	var req describeClusterInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	c, err := h.Backend.DescribeCluster(ctx, req.ClusterName)
	if err != nil {
		return nil, err
	}

	return h.describeClusterResponse(c), nil
}

// clusterSummary is a summary of a SageMaker HyperPod cluster for list responses.
type clusterSummary struct {
	ClusterArn    string  `json:"ClusterArn"`
	ClusterName   string  `json:"ClusterName"`
	ClusterStatus string  `json:"ClusterStatus"`
	CreationTime  float64 `json:"CreationTime"`
}

// listClustersInput is ListClusters' request shape (api_op_ListClusters.go:
// 30-71). TrainingPlanArn is decoded for wire-shape fidelity but is a
// disclosed no-op -- see ListClustersParams' doc comment in cluster.go.
type listClustersInput struct {
	CreationTimeAfter  *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore *float64 `json:"CreationTimeBefore"`
	NameContains       string   `json:"NameContains"`
	NextToken          string   `json:"NextToken"`
	SortBy             string   `json:"SortBy"`
	SortOrder          string   `json:"SortOrder"`
	TrainingPlanArn    string   `json:"TrainingPlanArn"`
	MaxResults         int32    `json:"MaxResults"`
}

func (h *Handler) handleListClusters(ctx context.Context, body []byte) ([]byte, error) {
	var req listClustersInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	clusters, nextToken := h.Backend.ListClusters(ctx, ListClustersParams{
		CreationTimeAfter:  timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore: timeFromEpochSecondsPtr(req.CreationTimeBefore),
		NameContains:       req.NameContains,
		NextToken:          req.NextToken,
		SortBy:             req.SortBy,
		SortOrder:          req.SortOrder,
		MaxResults:         req.MaxResults,
	})
	summaries := make([]clusterSummary, 0, len(clusters))

	for _, c := range clusters {
		summaries = append(summaries, clusterSummary{
			ClusterArn:    c.ClusterArn,
			ClusterName:   c.ClusterName,
			ClusterStatus: c.ClusterStatus,
			CreationTime:  epochSeconds(c.CreationTime),
		})
	}

	resp := map[string]any{"ClusterSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// deleteClusterInput is DeleteCluster's request shape
// (api_op_DeleteCluster.go:28-36).
type deleteClusterInput struct {
	ClusterName string `json:"ClusterName"`
}

func (h *Handler) handleDeleteCluster(ctx context.Context, body []byte) ([]byte, error) {
	var req deleteClusterInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	clusterArn, err := h.Backend.DeleteCluster(ctx, req.ClusterName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: deleted cluster", "name", req.ClusterName)

	return json.Marshal(map[string]string{keyClusterArn: clusterArn})
}

// updateClusterRequest is the request body for UpdateCluster.
type updateClusterRequest struct {
	AutoScaling            *ClusterAutoScalingConfig     `json:"AutoScaling,omitempty"`
	Orchestrator           *ClusterOrchestrator          `json:"Orchestrator,omitempty"`
	TieredStorageConfig    *ClusterTieredStorageConfig   `json:"TieredStorageConfig,omitempty"`
	ClusterName            string                        `json:"ClusterName"`
	NodeRecovery           string                        `json:"NodeRecovery,omitempty"`
	NodeProvisioningMode   string                        `json:"NodeProvisioningMode,omitempty"`
	InstanceGroups         []clusterInstanceGroupRequest `json:"InstanceGroups"`
	InstanceGroupsToDelete []string                      `json:"InstanceGroupsToDelete"`
}

func (h *Handler) handleUpdateCluster(ctx context.Context, body []byte) ([]byte, error) {
	var req updateClusterRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	c, err := h.Backend.UpdateCluster(ctx, UpdateClusterOptions{
		NameOrArn:              req.ClusterName,
		InstanceGroups:         toClusterInstanceGroups(req.InstanceGroups),
		InstanceGroupsToDelete: req.InstanceGroupsToDelete,
		NodeRecovery:           req.NodeRecovery,
		NodeProvisioningMode:   req.NodeProvisioningMode,
		AutoScaling:            req.AutoScaling,
		Orchestrator:           req.Orchestrator,
		TieredStorageConfig:    req.TieredStorageConfig,
	})
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated cluster", "name", c.ClusterName)

	return json.Marshal(map[string]string{keyClusterArn: c.ClusterArn})
}

// updateClusterSoftwareInstanceGroupRequest is the wire shape for one entry
// of UpdateClusterSoftwareInput.InstanceGroups
// (types.UpdateClusterSoftwareInstanceGroupSpecification, types/types.go:
// 24134-24146).
type updateClusterSoftwareInstanceGroupRequest struct {
	InstanceGroupName   string `json:"InstanceGroupName"`
	ImageReleaseVersion string `json:"ImageReleaseVersion"`
}

// updateClusterSoftwareInput is UpdateClusterSoftware's request shape
// (api_op_UpdateClusterSoftware.go:28-63). DeploymentConfig/ImageId/
// InstanceGroups are decoded for wire-shape fidelity but are disclosed
// no-ops: this backend applies an UpdateClusterSoftware request immediately
// with no observable AMI/software-version state to update per instance group
// (see UpdateClusterSoftware's doc comment in cluster.go), so there is
// nothing for a per-group image ID/version or rollout policy to act on.
type updateClusterSoftwareInput struct {
	DeploymentConfig json.RawMessage                             `json:"DeploymentConfig"`
	ClusterName      string                                      `json:"ClusterName"`
	ImageID          string                                      `json:"ImageId"`
	InstanceGroups   []updateClusterSoftwareInstanceGroupRequest `json:"InstanceGroups"`
}

func (h *Handler) handleUpdateClusterSoftware(ctx context.Context, body []byte) ([]byte, error) {
	var req updateClusterSoftwareInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	clusterArn, err := h.Backend.UpdateClusterSoftware(ctx, req.ClusterName)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: updated cluster software", "name", req.ClusterName)

	return json.Marshal(map[string]string{keyClusterArn: clusterArn})
}

// clusterInstanceStatusDetails mirrors the AWS ClusterInstanceStatusDetails shape.
type clusterInstanceStatusDetails struct {
	Status string `json:"Status"`
}

// clusterNodeDetails is the wire shape for DescribeClusterNode's NodeDetails
// (types.ClusterNodeDetails, types/types.go:5306-5389). LaunchTime is the
// only field of this (large, mostly infrastructure-simulation) response type
// this backend populates; CapacityType, the Current/DesiredImage* AMI-patch
// fields, KubernetesConfig, NetworkInterface, Placement,
// PrivateDnsHostname/PrivatePrimaryIp, ThreadsPerCore, and UltraServerInfo
// all describe EC2/network/Kubernetes state this emulator does not simulate
// and are disclosed, not modeled.
type clusterNodeDetails struct {
	InstanceGroupName string                       `json:"InstanceGroupName,omitempty"`
	InstanceID        string                       `json:"InstanceId,omitempty"`
	InstanceType      string                       `json:"InstanceType,omitempty"`
	InstanceStatus    clusterInstanceStatusDetails `json:"InstanceStatus"`
	LaunchTime        float64                      `json:"LaunchTime"`
}

func toClusterNodeDetails(n *ClusterNode) clusterNodeDetails {
	return clusterNodeDetails{
		InstanceGroupName: n.InstanceGroupName,
		InstanceID:        n.NodeID,
		InstanceType:      n.InstanceType,
		InstanceStatus:    clusterInstanceStatusDetails{Status: n.NodeStatus},
		LaunchTime:        epochSeconds(n.CreationTime),
	}
}

// describeClusterNodeInput is DescribeClusterNode's request shape
// (api_op_DescribeClusterNode.go:28-42). NodeLogicalId is decoded for
// wire-shape fidelity but is a disclosed no-op: every node in this backend
// gets its NodeId assigned synchronously at creation (see newClusterNode),
// so there is never a still-provisioning node reachable only by a separate
// logical ID for it to resolve.
type describeClusterNodeInput struct {
	ClusterName   string `json:"ClusterName"`
	NodeID        string `json:"NodeId"`
	NodeLogicalID string `json:"NodeLogicalId"`
}

func (h *Handler) handleDescribeClusterNode(ctx context.Context, body []byte) ([]byte, error) {
	var req describeClusterNodeInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	if req.NodeID == "" {
		return nil, fmt.Errorf("%w: NodeId is required", errInvalidRequest)
	}

	n, err := h.Backend.DescribeClusterNode(ctx, req.ClusterName, req.NodeID)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"NodeDetails": toClusterNodeDetails(n)})
}

// clusterNodeSummary is the wire shape for a ListClusterNodes entry
// (types.ClusterNodeSummary, types/types.go:5398-5423). LaunchTime is a
// required member -- previously omitted entirely, so every real client's
// ListClusterNodes call saw every summary missing it, even though
// DescribeClusterNode always populated ClusterNodeDetails.LaunchTime's
// equivalent for the same node.
type clusterNodeSummary struct {
	InstanceGroupName string                       `json:"InstanceGroupName,omitempty"`
	InstanceID        string                       `json:"InstanceId,omitempty"`
	InstanceType      string                       `json:"InstanceType,omitempty"`
	InstanceStatus    clusterInstanceStatusDetails `json:"InstanceStatus"`
	LaunchTime        float64                      `json:"LaunchTime"`
}

// listClusterNodesInput is ListClusterNodes' request shape
// (api_op_ListClusterNodes.go:30-70). IncludeNodeLogicalIds is decoded for
// wire-shape fidelity but is a disclosed no-op -- see
// ListClusterNodesParams' doc comment in cluster.go.
type listClusterNodesInput struct {
	CreationTimeAfter         *float64 `json:"CreationTimeAfter"`
	CreationTimeBefore        *float64 `json:"CreationTimeBefore"`
	ClusterName               string   `json:"ClusterName"`
	InstanceGroupNameContains string   `json:"InstanceGroupNameContains"`
	NextToken                 string   `json:"NextToken"`
	SortBy                    string   `json:"SortBy"`
	SortOrder                 string   `json:"SortOrder"`
	MaxResults                int32    `json:"MaxResults"`
	IncludeNodeLogicalIDs     bool     `json:"IncludeNodeLogicalIds"`
}

func (h *Handler) handleListClusterNodes(ctx context.Context, body []byte) ([]byte, error) {
	var req listClusterNodesInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	nodes, nextToken, err := h.Backend.ListClusterNodes(ctx, req.ClusterName, ListClusterNodesParams{
		CreationTimeAfter:         timeFromEpochSecondsPtr(req.CreationTimeAfter),
		CreationTimeBefore:        timeFromEpochSecondsPtr(req.CreationTimeBefore),
		InstanceGroupNameContains: req.InstanceGroupNameContains,
		NextToken:                 req.NextToken,
		SortBy:                    req.SortBy,
		SortOrder:                 req.SortOrder,
		MaxResults:                req.MaxResults,
	})
	if err != nil {
		return nil, err
	}

	summaries := make([]clusterNodeSummary, 0, len(nodes))
	for _, n := range nodes {
		summaries = append(summaries, clusterNodeSummary{
			InstanceGroupName: n.InstanceGroupName,
			InstanceID:        n.NodeID,
			InstanceType:      n.InstanceType,
			InstanceStatus:    clusterInstanceStatusDetails{Status: n.NodeStatus},
			LaunchTime:        epochSeconds(n.CreationTime),
		})
	}

	resp := map[string]any{"ClusterNodeSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

// describeClusterEventInput is DescribeClusterEvent's request shape
// (api_op_DescribeClusterEvent.go:28-40).
type describeClusterEventInput struct {
	ClusterName string `json:"ClusterName"`
	EventID     string `json:"EventId"`
}

func (h *Handler) handleDescribeClusterEvent(ctx context.Context, body []byte) ([]byte, error) {
	var req describeClusterEventInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	if req.EventID == "" {
		return nil, fmt.Errorf("%w: EventId is required", errInvalidRequest)
	}

	if err := h.Backend.DescribeClusterEvent(ctx, req.ClusterName, req.EventID); err != nil {
		return nil, err
	}

	// Unreachable in practice: DescribeClusterEvent always errors above because
	// this emulator never generates cluster events.
	return json.Marshal(map[string]any{})
}

// listClusterEventsInput is ListClusterEvents' request shape
// (api_op_ListClusterEvents.go:29-72). Every field besides ClusterName is
// decoded for wire-shape fidelity but is a disclosed no-op: this backend
// never generates a single cluster event (see DescribeClusterEvent/
// ListClusterEvents' doc comments in cluster.go), so every filter is
// trivially satisfied by the always-empty result set.
type listClusterEventsInput struct {
	EventTimeAfter    *float64 `json:"EventTimeAfter"`
	EventTimeBefore   *float64 `json:"EventTimeBefore"`
	ClusterName       string   `json:"ClusterName"`
	InstanceGroupName string   `json:"InstanceGroupName"`
	NextToken         string   `json:"NextToken"`
	NodeID            string   `json:"NodeId"`
	ResourceType      string   `json:"ResourceType"`
	SortBy            string   `json:"SortBy"`
	SortOrder         string   `json:"SortOrder"`
	MaxResults        int32    `json:"MaxResults"`
}

func (h *Handler) handleListClusterEvents(ctx context.Context, body []byte) ([]byte, error) {
	var req listClusterEventsInput

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	if err := h.Backend.ListClusterEvents(ctx, req.ClusterName); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Events": []any{}})
}

// detachClusterNodeVolumeRequest is the request body for DetachClusterNodeVolume.
type detachClusterNodeVolumeRequest struct {
	ClusterArn string `json:"ClusterArn"`
	NodeID     string `json:"NodeId"`
	VolumeID   string `json:"VolumeId"`
}

func (h *Handler) handleDetachClusterNodeVolume(ctx context.Context, body []byte) ([]byte, error) {
	var req detachClusterNodeVolumeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	dv, err := h.Backend.DetachClusterNodeVolume(ctx, req.ClusterArn, req.NodeID, req.VolumeID)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(
		ctx, "sagemaker: detached cluster node volume",
		"cluster", dv.ClusterArn, "node", dv.NodeID, "volume", dv.VolumeID,
	)

	return json.Marshal(map[string]any{
		keyClusterArn: dv.ClusterArn,
		"NodeId":      dv.NodeID,
		"VolumeId":    dv.VolumeID,
		"DeviceName":  dv.DeviceName,
		"Status":      dv.Status,
		"AttachTime":  epochSeconds(dv.AttachTime),
	})
}

// clusterNodeVolumeRequest is the volume config in the handler request.
type clusterNodeVolumeRequest struct {
	VolumeName string `json:"VolumeName"`
	SizeInGB   int32  `json:"SizeInGB,omitempty"`
}

// attachClusterNodeVolumeRequest is the request body for AttachClusterNodeVolume.
type attachClusterNodeVolumeRequest struct {
	ClusterName  string                   `json:"ClusterName"`
	NodeID       string                   `json:"NodeId"`
	VolumeConfig clusterNodeVolumeRequest `json:"VolumeConfig"`
}

func (h *Handler) handleAttachClusterNodeVolume(ctx context.Context, body []byte) ([]byte, error) {
	var req attachClusterNodeVolumeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	if req.NodeID == "" {
		return nil, fmt.Errorf("%w: NodeId is required", errInvalidRequest)
	}

	vol := ClusterNodeVolume{
		VolumeName: req.VolumeConfig.VolumeName,
		SizeInGB:   req.VolumeConfig.SizeInGB,
	}

	clusterArn, nodeID, err := h.Backend.AttachClusterNodeVolume(ctx, req.ClusterName, req.NodeID, vol)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(
		ctx,
		"sagemaker: attached cluster node volume",
		"cluster",
		clusterArn,
		"node",
		nodeID,
	)

	return json.Marshal(map[string]string{
		keyClusterArn: clusterArn,
		"NodeId":      nodeID,
	})
}

// clusterNodeRequest represents a node config in batch cluster operations.
type clusterNodeRequest struct {
	NodeID       string `json:"NodeId"`
	InstanceType string `json:"InstanceType,omitempty"`
}

// batchClusterNodesWithFailures is a shared helper for cluster batch operations that return Failures.
func (h *Handler) batchClusterNodesWithFailures(
	ctx context.Context,
	clusterName, logMsg string,
	nodes []ClusterNode,
	fn func(context.Context, string, []ClusterNode) (string, []string, error),
) ([]byte, error) {
	clusterArn, failures, err := fn(ctx, clusterName, nodes)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, logMsg, "cluster", clusterArn)

	if failures == nil {
		failures = []string{}
	}

	return json.Marshal(map[string]any{
		keyClusterArn: clusterArn,
		"Failures":    failures,
	})
}

// toClusterNodes converts a slice of clusterNodeRequest to ClusterNode.
func toClusterNodes(reqs []clusterNodeRequest) []ClusterNode {
	nodes := make([]ClusterNode, 0, len(reqs))

	for _, r := range reqs {
		nodes = append(nodes, ClusterNode{
			NodeID:       r.NodeID,
			InstanceType: r.InstanceType,
		})
	}

	return nodes
}

// batchAddClusterNodesRequest is the request body for BatchAddClusterNodes.
type batchAddClusterNodesRequest struct {
	ClusterName string               `json:"ClusterName"`
	NodeConfigs []clusterNodeRequest `json:"NodeConfigs"`
}

func (h *Handler) handleBatchAddClusterNodes(ctx context.Context, body []byte) ([]byte, error) {
	var req batchAddClusterNodesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	return h.batchClusterNodesWithFailures(
		ctx,
		req.ClusterName,
		"sagemaker: batch added cluster nodes",
		toClusterNodes(req.NodeConfigs),
		h.Backend.BatchAddClusterNodes,
	)
}

// batchDeleteClusterNodesRequest is the request body for BatchDeleteClusterNodes.
type batchDeleteClusterNodesRequest struct {
	ClusterName string   `json:"ClusterName"`
	NodeIDs     []string `json:"NodeIds"`
}

func (h *Handler) handleBatchDeleteClusterNodes(ctx context.Context, body []byte) ([]byte, error) {
	var req batchDeleteClusterNodesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	clusterArn, errored, successful, err := h.Backend.BatchDeleteClusterNodes(
		ctx,
		req.ClusterName,
		req.NodeIDs,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: batch deleted cluster nodes", "cluster", clusterArn)

	if errored == nil {
		errored = []string{}
	}

	if successful == nil {
		successful = []string{}
	}

	return json.Marshal(map[string]any{
		keyClusterArn: clusterArn,
		"Errors":      errored,
		"Successful":  successful,
	})
}

// batchRebootClusterNodesRequest is the request body for BatchRebootClusterNodes.
type batchRebootClusterNodesRequest struct {
	ClusterName string   `json:"ClusterName"`
	NodeIDs     []string `json:"NodeIds"`
}

func (h *Handler) handleBatchRebootClusterNodes(ctx context.Context, body []byte) ([]byte, error) {
	var req batchRebootClusterNodesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	clusterArn, failures, successful, err := h.Backend.BatchRebootClusterNodes(
		ctx,
		req.ClusterName,
		req.NodeIDs,
	)
	if err != nil {
		return nil, err
	}

	log := logger.Load(ctx)
	log.InfoContext(ctx, "sagemaker: batch rebooted cluster nodes", "cluster", clusterArn)

	if failures == nil {
		failures = []string{}
	}

	if successful == nil {
		successful = []string{}
	}

	return json.Marshal(map[string]any{
		keyClusterArn: clusterArn,
		"Failures":    failures,
		"Successful":  successful,
	})
}

// batchReplaceClusterNodesRequest is the request body for BatchReplaceClusterNodes.
type batchReplaceClusterNodesRequest struct {
	ClusterName string               `json:"ClusterName"`
	Nodes       []clusterNodeRequest `json:"Nodes"`
}

func (h *Handler) handleBatchReplaceClusterNodes(ctx context.Context, body []byte) ([]byte, error) {
	var req batchReplaceClusterNodesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	return h.batchClusterNodesWithFailures(
		ctx,
		req.ClusterName,
		"sagemaker: batch replaced cluster nodes",
		toClusterNodes(req.Nodes),
		h.Backend.BatchReplaceClusterNodes,
	)
}
