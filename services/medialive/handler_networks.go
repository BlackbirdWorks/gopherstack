package medialive

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Network handlers ---

func toNetworkOutput(n *Network) map[string]any {
	tags := n.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	clusters := n.AssociatedClusterIDs
	if clusters == nil {
		clusters = []string{}
	}
	pools := n.IPPools
	if pools == nil {
		pools = []IPPool{}
	}
	routes := n.Routes
	if routes == nil {
		routes = []Route{}
	}

	return map[string]any{
		keyArn: n.ARN, keyID: n.ID, keyName: n.Name, keyState: n.State,
		"associatedClusterIds": clusters, "ipPools": pools, "routes": routes,
		keyTags: tags,
	}
}

func extractIPPools(body map[string]any) []IPPool {
	raw, _ := body["ipPools"].([]any)
	if raw == nil {
		return nil
	}

	pools := make([]IPPool, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		cidr, _ := m["cidr"].(string)
		pools = append(pools, IPPool{Cidr: cidr})
	}

	return pools
}

func extractRoutes(body map[string]any) []Route {
	raw, _ := body["routes"].([]any)
	if raw == nil {
		return nil
	}

	routes := make([]Route, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		cidr, _ := m["cidr"].(string)
		gateway, _ := m["gateway"].(string)
		routes = append(routes, Route{Cidr: cidr, Gateway: gateway})
	}

	return routes
}

func (h *Handler) handleCreateNetwork(c *echo.Context, body map[string]any) error {
	name, _ := body["name"].(string)
	tags := extractTags(body)

	n, err := h.Backend.CreateNetwork(name, extractIPPools(body), extractRoutes(body), tags)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusCreated, toNetworkOutput(n))
}

func (h *Handler) handleDescribeNetwork(c *echo.Context, networkID string) error {
	n, err := h.Backend.DescribeNetwork(networkID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNetworkOutput(n))
}

func (h *Handler) handleUpdateNetwork(
	c *echo.Context,
	networkID string,
	body map[string]any,
) error {
	name, _ := body["name"].(string)

	n, err := h.Backend.UpdateNetwork(networkID, name, extractIPPools(body), extractRoutes(body))
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNetworkOutput(n))
}

func (h *Handler) handleDeleteNetwork(c *echo.Context, networkID string) error {
	n, err := h.Backend.DeleteNetwork(networkID)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, toNetworkOutput(n))
}

func (h *Handler) handleListNetworks(c *echo.Context) error {
	nets, nextToken, err := h.Backend.ListNetworks(0, "")
	if err != nil {
		return respondErr(c, err)
	}

	out := make([]map[string]any, 0, len(nets))
	for _, n := range nets {
		out = append(out, toNetworkOutput(n))
	}

	resp := map[string]any{"networks": out}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
