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
	}

	return nil, false, nil
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
	ClusterName    string                        `json:"ClusterName"`
	NodeRecovery   string                        `json:"NodeRecovery,omitempty"`
	InstanceGroups []clusterInstanceGroupRequest `json:"InstanceGroups"`
	Tags           []tagObject                   `json:"Tags"`
}

func (h *Handler) handleCreateCluster(ctx context.Context, body []byte) ([]byte, error) {
	var req createClusterRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	c, err := h.Backend.CreateCluster(
		ctx,
		req.ClusterName,
		toClusterInstanceGroups(req.InstanceGroups),
		req.NodeRecovery,
		fromTagObjects(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).
		InfoContext(ctx, "sagemaker: created cluster", "name", c.ClusterName, "arn", c.ClusterArn)

	return json.Marshal(map[string]string{keyClusterArn: c.ClusterArn})
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

	b, _ := json.Marshal(resp)

	return b
}

func (h *Handler) handleDescribeCluster(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterName string `json:"ClusterName"`
	}

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

func (h *Handler) handleListClusters(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		NameContains string `json:"NameContains"`
		NextToken    string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	clusters, nextToken := h.Backend.ListClusters(ctx, req.NextToken, req.NameContains)
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

func (h *Handler) handleDeleteCluster(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterName string `json:"ClusterName"`
	}

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
	ClusterName            string                        `json:"ClusterName"`
	NodeRecovery           string                        `json:"NodeRecovery,omitempty"`
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

	c, err := h.Backend.UpdateCluster(
		ctx,
		req.ClusterName,
		toClusterInstanceGroups(req.InstanceGroups),
		req.InstanceGroupsToDelete,
		req.NodeRecovery,
	)
	if err != nil {
		return nil, err
	}

	logger.Load(ctx).InfoContext(ctx, "sagemaker: updated cluster", "name", c.ClusterName)

	return json.Marshal(map[string]string{keyClusterArn: c.ClusterArn})
}

func (h *Handler) handleUpdateClusterSoftware(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterName string `json:"ClusterName"`
	}

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

// clusterNodeDetails is the wire shape for DescribeClusterNode's NodeDetails.
type clusterNodeDetails struct {
	InstanceGroupName string                       `json:"InstanceGroupName,omitempty"`
	InstanceID        string                       `json:"InstanceId,omitempty"`
	InstanceType      string                       `json:"InstanceType,omitempty"`
	InstanceStatus    clusterInstanceStatusDetails `json:"InstanceStatus"`
}

func toClusterNodeDetails(n *ClusterNode) clusterNodeDetails {
	return clusterNodeDetails{
		InstanceGroupName: n.InstanceGroupName,
		InstanceID:        n.NodeID,
		InstanceType:      n.InstanceType,
		InstanceStatus:    clusterInstanceStatusDetails{Status: n.NodeStatus},
	}
}

func (h *Handler) handleDescribeClusterNode(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterName string `json:"ClusterName"`
		NodeID      string `json:"NodeId"`
	}

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

// clusterNodeSummary is the wire shape for a ListClusterNodes entry.
type clusterNodeSummary struct {
	InstanceGroupName string                       `json:"InstanceGroupName,omitempty"`
	InstanceID        string                       `json:"InstanceId,omitempty"`
	InstanceType      string                       `json:"InstanceType,omitempty"`
	InstanceStatus    clusterInstanceStatusDetails `json:"InstanceStatus"`
}

func (h *Handler) handleListClusterNodes(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterName string `json:"ClusterName"`
		NextToken   string `json:"NextToken"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ClusterName == "" {
		return nil, fmt.Errorf("%w: ClusterName is required", errInvalidRequest)
	}

	nodes, nextToken, err := h.Backend.ListClusterNodes(ctx, req.ClusterName, req.NextToken)
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
		})
	}

	resp := map[string]any{"ClusterNodeSummaries": summaries}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return json.Marshal(resp)
}

func (h *Handler) handleDescribeClusterEvent(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterName string `json:"ClusterName"`
		EventID     string `json:"EventId"`
	}

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

func (h *Handler) handleListClusterEvents(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ClusterName string `json:"ClusterName"`
	}

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
