package directoryservice

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleAddIpRoutes(c *echo.Context) error { //nolint:revive,staticcheck // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string     `json:"DirectoryId"`
		IpRoutes    []struct { //nolint:revive,staticcheck // existing issue.
			CidrIp      string `json:"CidrIp"` //nolint:revive,staticcheck // existing issue.
			Description string `json:"Description"`
		} `json:"IpRoutes"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	routes := make([]IpRoute, 0, len(req.IpRoutes))
	for _, r := range req.IpRoutes {
		routes = append(routes, IpRoute{CidrIP: r.CidrIp, Description: r.Description})
	}

	if addErr := h.Backend.AddIpRoutes(h.contextWithRegion(c), req.DirectoryID, routes); addErr != nil {
		return h.mapError(c, addErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleRemoveIpRoutes(c *echo.Context) error { //nolint:revive,staticcheck // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string   `json:"DirectoryId"`
		CidrIPs     []string `json:"CidrIps"`
	}

	if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	if removeErr := h.Backend.RemoveIpRoutes(h.contextWithRegion(c), req.DirectoryID, req.CidrIPs); removeErr != nil {
		return h.mapError(c, removeErr)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListIpRoutes(c *echo.Context) error { //nolint:revive,staticcheck // existing issue.
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid body"))
	}

	var req struct {
		DirectoryID string `json:"DirectoryId"`
		NextToken   string `json:"NextToken"`
		Limit       int32  `json:"Limit"`
	}

	if len(body) > 0 {
		if jsonErr := json.Unmarshal(body, &req); jsonErr != nil {
			return c.JSON(http.StatusBadRequest, errResp("ClientException", "invalid JSON"))
		}
	}

	if req.DirectoryID == "" {
		return c.JSON(http.StatusBadRequest, errResp("ClientException", "DirectoryId is required"))
	}

	routes, nextToken, listErr := h.Backend.ListIpRoutes(
		h.contextWithRegion(c),
		req.DirectoryID,
		req.Limit,
		req.NextToken,
	)
	if listErr != nil {
		return h.mapError(c, listErr)
	}

	routeList := make([]map[string]any, 0, len(routes))
	for _, r := range routes {
		routeList = append(routeList, map[string]any{
			keyDirectoryID:     r.DirectoryID,
			"CidrIp":           r.CidrIP,
			"Description":      r.Description, //nolint:goconst // existing issue.
			"AddedDateTime":    awstime.Epoch(r.AddedTime),
			"IpRouteStatusMsg": r.Status,
		})
	}

	resp := map[string]any{"IpRoutesInfo": routeList}
	if nextToken != "" {
		resp["NextToken"] = nextToken
	}

	return c.JSON(http.StatusOK, resp)
}
