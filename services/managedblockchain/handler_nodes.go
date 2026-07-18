package managedblockchain

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// handleCreateNode handles POST /networks/{networkId}/nodes. Unlike member
// and node paths elsewhere, the owning member is not part of the URI -- the
// real API carries it as MemberId in the JSON body (see parsePath's doc
// comment) -- so it is read from the decoded request, not from resource.
func (h *Handler) handleCreateNode(c *echo.Context, networkID string, body []byte) error {
	if networkID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNetworkID.Error())
	}

	var req createNodeRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	if req.MemberID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNodeMemberID.Error())
	}

	node, err := h.Backend.CreateNode(
		h.DefaultRegion,
		h.AccountID,
		networkID,
		req.MemberID,
		req.NodeConfiguration.InstanceType,
		req.NodeConfiguration.AvailabilityZone,
		req.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createNodeResponse{NodeID: node.ID})
}

// handleGetNode handles GET /networks/{networkId}/nodes/{nodeId}. The owning
// member is carried as the "memberId" query parameter, not the URI.
func (h *Handler) handleGetNode(c *echo.Context, resource string) error {
	networkID, nodeID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	memberID := c.Request().URL.Query().Get("memberId")
	if memberID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNodeMemberID.Error())
	}

	node, err := h.Backend.GetNode(networkID, memberID, nodeID)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, getNodeResponse{Node: toNodeObject(node)})
}

// handleListNodes handles GET /networks/{networkId}/nodes. The owning member
// is carried as the "memberId" query parameter, not the URI.
func (h *Handler) handleListNodes(c *echo.Context, networkID string) error {
	if networkID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNetworkID.Error())
	}

	q := c.Request().URL.Query()

	memberID := q.Get("memberId")
	if memberID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNodeMemberID.Error())
	}

	filter := ListNodesFilter{
		Status: q.Get("status"),
	}

	nodes, err := h.Backend.ListNodes(networkID, memberID, filter)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	summaries := make([]nodeSummaryObject, 0, len(nodes))
	for _, n := range nodes {
		summaries = append(summaries, toNodeSummaryObject(n))
	}

	return c.JSON(http.StatusOK, listNodesResponse{Nodes: summaries})
}

// handleDeleteNode handles DELETE /networks/{networkId}/nodes/{nodeId}. The
// owning member is carried as the "memberId" query parameter, not the URI.
func (h *Handler) handleDeleteNode(c *echo.Context, resource string) error {
	networkID, nodeID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	memberID := c.Request().URL.Query().Get("memberId")
	if memberID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNodeMemberID.Error())
	}

	if err := h.Backend.DeleteNode(networkID, memberID, nodeID); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// handleUpdateNode handles PATCH /networks/{networkId}/nodes/{nodeId}. The
// owning member is carried as the "memberId" query parameter, not the URI.
func (h *Handler) handleUpdateNode(c *echo.Context, resource string, body []byte) error {
	networkID, nodeID, ok := splitResource(resource)
	if !ok {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid resource path")
	}

	memberID := c.Request().URL.Query().Get("memberId")
	if memberID == "" {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", ErrMissingNodeMemberID.Error())
	}

	var req updateNodeRequest

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
		}
	}

	_, err := h.Backend.UpdateNode(networkID, memberID, nodeID, buildNodeLogConfig(req.LogPublishingConfiguration))
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func buildNodeLogConfig(req *nodeLogPublishingConfigReq) *NodeLogPublishingConfigState {
	if req == nil {
		return nil
	}

	logConfig := &NodeLogPublishingConfigState{}

	if req.Fabric == nil {
		return logConfig
	}

	fabric := &NodeFabricLogState{}

	if req.Fabric.ChaincodeLogs != nil {
		cl := &LogConfigState{}
		if req.Fabric.ChaincodeLogs.CloudWatch != nil {
			cl.CloudWatch = &CloudWatchLogState{Enabled: req.Fabric.ChaincodeLogs.CloudWatch.Enabled}
		}
		fabric.ChaincodeLogs = cl
	}

	if req.Fabric.PeerLogs != nil {
		pl := &LogConfigState{}
		if req.Fabric.PeerLogs.CloudWatch != nil {
			pl.CloudWatch = &CloudWatchLogState{Enabled: req.Fabric.PeerLogs.CloudWatch.Enabled}
		}
		fabric.PeerLogs = pl
	}

	logConfig.Fabric = fabric

	return logConfig
}

// toNodeObject converts a Node to its JSON representation.
func toNodeObject(n *Node) nodeObject {
	obj := nodeObject{
		ID:               n.ID,
		Arn:              n.Arn,
		InstanceType:     n.InstanceType,
		AvailabilityZone: n.AvailabilityZone,
		MemberID:         n.MemberID,
		NetworkID:        n.NetworkID,
		Status:           n.Status,
		CreationDate:     n.CreationDate,
		Tags:             n.Tags,
	}

	if n.LogPublishingConfiguration != nil {
		obj.LogPublishingConfiguration = toNodeLogConfigRespObj(n.LogPublishingConfiguration)
	}

	return obj
}

// toNodeLogConfigRespObj converts NodeLogPublishingConfigState to its response JSON.
func toNodeLogConfigRespObj(c *NodeLogPublishingConfigState) *nodeLogPublishingConfigRespObj {
	if c == nil {
		return nil
	}

	obj := &nodeLogPublishingConfigRespObj{}

	if c.Fabric != nil {
		fabric := &nodeFabricLogRespObj{}

		if c.Fabric.ChaincodeLogs != nil {
			fabric.ChaincodeLogs = toLogConfigRespObj(c.Fabric.ChaincodeLogs)
		}

		if c.Fabric.PeerLogs != nil {
			fabric.PeerLogs = toLogConfigRespObj(c.Fabric.PeerLogs)
		}

		obj.Fabric = fabric
	}

	return obj
}

// toNodeSummaryObject converts a Node to its summary JSON representation.
func toNodeSummaryObject(n *Node) nodeSummaryObject {
	return nodeSummaryObject{
		ID:               n.ID,
		Arn:              n.Arn,
		InstanceType:     n.InstanceType,
		AvailabilityZone: n.AvailabilityZone,
		Status:           n.Status,
		CreationDate:     n.CreationDate,
	}
}
