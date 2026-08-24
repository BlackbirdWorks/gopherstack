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
	Arn                    string   `json:"arn"`
	ID                     string   `json:"id"`
	Name                   string   `json:"name"`
	ClusterID              string   `json:"clusterId"`
	Role                   string   `json:"role"`
	State                  string   `json:"state"`
	ConnectionState        string   `json:"connectionState"`
	ChannelPlacementGroups []string `json:"channelPlacementGroups"`
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
	}
}

func (h *Handler) handleCreateNode(c *echo.Context, clusterID string, body map[string]any) error {
	name, _ := body["name"].(string)
	role, _ := body["role"].(string)
	tags := extractTags(body)

	n, err := h.Backend.CreateNode(clusterID, name, role, tags)
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

	n, err := h.Backend.UpdateNode(clusterID, nodeID, name, role)
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
		})
	}

	resp := map[string]any{"nodes": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
