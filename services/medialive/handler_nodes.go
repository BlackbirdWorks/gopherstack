package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Node handlers ---

// nodeOutput mirrors DescribeNodeOutput/CreateNodeOutput/UpdateNodeOutput/
// UpdateNodeStateOutput exactly -- like Cluster, the real API has NO "tags"
// field here (only ListTagsForResource echoes Node tags). "channelPlacementGroups"
// is derived live from ChannelPlacementGroup.Nodes (see
// channelPlacementGroupIDsForNode).
type nodeOutput struct {
	Arn                    string                 `json:"arn"`
	ID                     string                 `json:"id"`
	Name                   string                 `json:"name"`
	ClusterID              string                 `json:"clusterId"`
	Role                   string                 `json:"role"`
	State                  string                 `json:"state"`
	ConnectionState        string                 `json:"connectionState"`
	ChannelPlacementGroups []string               `json:"channelPlacementGroups"`
	NodeInterfaceMappings  []NodeInterfaceMapping `json:"nodeInterfaceMappings"`
	SdiSourceMappings      []SdiSourceMapping     `json:"sdiSourceMappings"`
}

func extractNodeInterfaceMappings(body map[string]any) []NodeInterfaceMapping {
	raw, ok := body["nodeInterfaceMappings"].([]any)
	if !ok {
		return nil
	}

	out := make([]NodeInterfaceMapping, 0, len(raw))

	for _, item := range raw {
		obj, _ := item.(map[string]any)

		var m NodeInterfaceMapping

		m.LogicalInterfaceName, _ = obj["logicalInterfaceName"].(string)
		m.NetworkInterfaceMode, _ = obj["networkInterfaceMode"].(string)
		m.PhysicalInterfaceName, _ = obj["physicalInterfaceName"].(string)

		ips, _ := obj["physicalInterfaceIpAddresses"].([]any)
		for _, ip := range ips {
			if s, isStr := ip.(string); isStr {
				m.PhysicalInterfaceIPAddresses = append(m.PhysicalInterfaceIPAddresses, s)
			}
		}

		out = append(out, m)
	}

	return out
}

func toNodeOutput(n *Node) nodeOutput {
	cpgIDs := n.ChannelPlacementGroups
	if cpgIDs == nil {
		cpgIDs = []string{}
	}

	return nodeOutput{
		Arn:                    n.ARN,
		ID:                     n.ID,
		Name:                   n.Name,
		ClusterID:              n.ClusterID,
		Role:                   n.Role,
		State:                  n.State,
		ConnectionState:        n.ConnectionState,
		ChannelPlacementGroups: cpgIDs,
		NodeInterfaceMappings:  nonNilMappings(n.NodeInterfaceMappings),
		SdiSourceMappings:      nonNilSdi(n.SdiSourceMappings),
	}
}

func nonNilSdi(m []SdiSourceMapping) []SdiSourceMapping {
	if m == nil {
		return []SdiSourceMapping{}
	}

	return m
}

func extractSdiSourceMappings(body map[string]any) []SdiSourceMapping {
	raw, ok := body["sdiSourceMappings"].([]any)
	if !ok {
		return nil
	}

	out := make([]SdiSourceMapping, 0, len(raw))

	for _, item := range raw {
		obj, _ := item.(map[string]any)
		card, _ := obj["cardNumber"].(float64)
		ch, _ := obj["channelNumber"].(float64)
		src, _ := obj["sdiSource"].(string)
		out = append(out, SdiSourceMapping{CardNumber: int32(card), ChannelNumber: int32(ch), SdiSource: src})
	}

	return out
}

func nonNilMappings(m []NodeInterfaceMapping) []NodeInterfaceMapping {
	if m == nil {
		return []NodeInterfaceMapping{}
	}

	return m
}

func (h *Handler) handleCreateNode(c *echo.Context, clusterID string, body map[string]any) error {
	name, _ := body["name"].(string)
	role, _ := body["role"].(string)
	tags := extractTags(body)

	n, err := h.Backend.CreateNode(clusterID, name, role, extractNodeInterfaceMappings(body), tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toNodeOutput(n))
}

func (h *Handler) handleDescribeNode(c *echo.Context, resource string) error {
	clusterID, nodeID := splitClusterNode(resource)

	n, err := h.Backend.DescribeNode(clusterID, nodeID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNodeOutput(n))
}

func (h *Handler) handleUpdateNode(c *echo.Context, resource string, body map[string]any) error {
	clusterID, nodeID := splitClusterNode(resource)

	name, _ := body["name"].(string)
	role, _ := body["role"].(string)

	n, err := h.Backend.UpdateNode(clusterID, nodeID, name, role, extractSdiSourceMappings(body))
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNodeOutput(n))
}

func (h *Handler) handleUpdateNodeState(
	c *echo.Context,
	resource string,
	body map[string]any,
) error {
	clusterID, nodeID := splitClusterNode(resource)

	state, _ := body["state"].(string)

	n, err := h.Backend.UpdateNodeState(clusterID, nodeID, state)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNodeOutput(n))
}

func (h *Handler) handleDeleteNode(c *echo.Context, resource string) error {
	clusterID, nodeID := splitClusterNode(resource)

	n, err := h.Backend.DeleteNode(clusterID, nodeID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNodeOutput(n))
}

func (h *Handler) handleListNodes(c *echo.Context, clusterID string) error {
	if err := validPaging(c); err != nil {
		return respondErr(c, err)
	}

	maxResults, nextTokenParam := paginationParams(c)
	summaries, nextToken, err := h.Backend.ListNodes(clusterID, maxResults, nextTokenParam)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(summaries))
	for _, s := range summaries {
		cpgIDs := s.ChannelPlacementGroups
		if cpgIDs == nil {
			cpgIDs = []string{}
		}

		out = append(out, map[string]any{
			keyArn:                   s.ARN,
			keyID:                    s.ID,
			keyName:                  s.Name,
			keyState:                 s.State,
			"clusterId":              s.ClusterID,
			"role":                   s.Role,
			"connectionState":        s.ConnectionState,
			"channelPlacementGroups": cpgIDs,
			"nodeInterfaceMappings":  nonNilMappings(s.NodeInterfaceMappings),
			"sdiSourceMappings":      nonNilSdi(s.SdiSourceMappings),
		})
	}

	resp := map[string]any{"nodes": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
