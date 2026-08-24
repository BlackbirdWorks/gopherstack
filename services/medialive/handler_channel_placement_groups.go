package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- ChannelPlacementGroup handlers ---

func toChannelPlacementGroupOutput(g *ChannelPlacementGroup) map[string]any {
	channels := g.Channels
	if channels == nil {
		channels = []string{}
	}
	nodes := g.Nodes
	if nodes == nil {
		nodes = []string{}
	}

	return map[string]any{
		keyArn: g.ARN, keyID: g.ID, keyName: g.Name, "clusterId": g.ClusterID,
		keyState: g.State, "channels": channels, "nodes": nodes,
	}
}

func (h *Handler) handleCreateChannelPlacementGroup(
	c *echo.Context,
	clusterID string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)
	nodes := extractStringSlice(body, "nodes")
	tags := extractTags(body)

	g, err := h.Backend.CreateChannelPlacementGroup(clusterID, name, nodes, tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toChannelPlacementGroupOutput(g))
}

func (h *Handler) handleDescribeChannelPlacementGroup(c *echo.Context, resource string) error {
	clusterID, groupID := splitClusterNode(resource)

	g, err := h.Backend.DescribeChannelPlacementGroup(clusterID, groupID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelPlacementGroupOutput(g))
}

func (h *Handler) handleUpdateChannelPlacementGroup(
	c *echo.Context,
	resource string,
	body map[string]any,
) error {
	clusterID, groupID := splitClusterNode(resource)
	name, _ := body["name"].(string)
	nodes := extractStringSlice(body, "nodes")

	g, err := h.Backend.UpdateChannelPlacementGroup(clusterID, groupID, name, nodes)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelPlacementGroupOutput(g))
}

func (h *Handler) handleDeleteChannelPlacementGroup(c *echo.Context, resource string) error {
	clusterID, groupID := splitClusterNode(resource)

	g, err := h.Backend.DeleteChannelPlacementGroup(clusterID, groupID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toChannelPlacementGroupOutput(g))
}

func (h *Handler) handleListChannelPlacementGroups(c *echo.Context, clusterID string) error {
	maxResults, nextTokenParam := paginationParams(c)
	groups, nextToken, err := h.Backend.ListChannelPlacementGroups(clusterID, maxResults, nextTokenParam)
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(groups))
	for _, g := range groups {
		out = append(out, toChannelPlacementGroupOutput(g))
	}

	resp := map[string]any{"channelPlacementGroups": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
