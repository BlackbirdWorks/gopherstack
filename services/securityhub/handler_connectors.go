package securityhub

import (
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// msgConnectorNotFound is shared by Get/Update/Delete's 404 responses.
const msgConnectorNotFound = "Connector not found"

// classifyConnectorsPath classifies requests under /connectors (the CSPM
// third-party cloud provider connector family). It is only reached once
// classifyPath's dispatch table has already ruled out /connectorsv2 (see
// handler.go's pathClassifiers ordering -- the V2 prefix is checked first),
// so every path this function sees is genuinely the non-V2 family.
func classifyConnectorsPath(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == pathConnectors:
		return opCreateConnector, ""
	case method == http.MethodGet && path == pathConnectors:
		return opListConnectors, ""
	case method == http.MethodGet && strings.HasPrefix(path, pathConnectors+"/"):
		return opGetConnector, strings.TrimPrefix(path, pathConnectors+"/")
	case method == http.MethodPatch && strings.HasPrefix(path, pathConnectors+"/"):
		return opUpdateConnector, strings.TrimPrefix(path, pathConnectors+"/")
	case method == http.MethodDelete && strings.HasPrefix(path, pathConnectors+"/"):
		return opDeleteConnector, strings.TrimPrefix(path, pathConnectors+"/")
	}

	return opUnknown, ""
}

func (h *Handler) handleCreateConnector(c *echo.Context, body map[string]any) error {
	name, _ := body[keyName].(string)
	description, _ := body[keyDescription].(string)

	var provider map[string]any

	if p, ok := body["Provider"].(map[string]any); ok {
		provider = p
	}

	var tags map[string]string

	if t, ok := body["Tags"].(map[string]any); ok {
		tags = make(map[string]string, len(t))

		for k, v := range t {
			tags[k], _ = v.(string)
		}
	}

	if name == "" {
		return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: msgNameRequired})
	}

	if provider == nil {
		return c.JSON(http.StatusBadRequest, map[string]any{keyMessage: "Provider is required"})
	}

	conn, err := h.Backend.CreateConnector(name, description, provider, tags)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyConnectorArn:     conn.ConnectorArn,
		keyConnectorID:      conn.ConnectorId,
		keyConnectorStatus:  conn.ConnectorStatus,
		keyEnablementStatus: conn.EnablementStatus,
	})
}

func (h *Handler) handleGetConnector(c *echo.Context, connectorID string) error {
	conn, err := h.Backend.GetConnector(connectorID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: msgConnectorNotFound})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, connectorToGetResponse(conn))
}

func (h *Handler) handleListConnectors(c *echo.Context) error {
	connectorStatus := c.QueryParam(keyConnectorStatus)
	enablementStatus := c.QueryParam(keyEnablementStatus)
	providerName := c.QueryParam("ProviderName")
	nextToken := c.QueryParam("NextToken")
	maxResults := queryInt(c)

	connectors, next := h.Backend.ListConnectors(connectorStatus, enablementStatus, providerName, nextToken, maxResults)

	out := make([]map[string]any, 0, len(connectors))

	for _, conn := range connectors {
		out = append(out, connectorToSummaryResponse(conn))
	}

	resp := map[string]any{"Connectors": out}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateConnector(c *echo.Context, connectorID string, body map[string]any) error {
	description, _ := body[keyDescription].(string)

	var provider map[string]any

	if p, ok := body["Provider"].(map[string]any); ok {
		provider = p
	}

	conn, err := h.Backend.UpdateConnector(connectorID, description, provider)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: msgConnectorNotFound})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyConnectorStatus:  conn.ConnectorStatus,
		keyEnablementStatus: conn.EnablementStatus,
	})
}

func (h *Handler) handleDeleteConnector(c *echo.Context, connectorID string) error {
	status, err := h.Backend.DeleteConnector(connectorID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.JSON(http.StatusNotFound, map[string]any{keyMessage: msgConnectorNotFound})
		}

		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{keyEnablementStatus: status})
}

// connectorToGetResponse builds the GetConnector wire shape: ConnectorId,
// CreatedAt, Health, LastUpdatedAt, Name, ProviderDetail are all required
// per the real GetConnectorOutput; ConnectorArn/CreatedBy/Description/
// EnablementStatus are optional but always populated here since this mock
// always has them.
func connectorToGetResponse(conn *CspmConnector) map[string]any {
	return map[string]any{
		keyConnectorID:      conn.ConnectorId,
		keyConnectorArn:     conn.ConnectorArn,
		keyName:             conn.Name,
		keyDescription:      conn.Description,
		keyCreatedAt:        conn.CreatedAt,
		"LastUpdatedAt":     conn.LastUpdatedAt,
		keyCreatedBy:        conn.CreatedBy,
		keyEnablementStatus: conn.EnablementStatus,
		"Health": map[string]any{
			keyConnectorStatus: conn.ConnectorStatus,
			"LastCheckedAt":    conn.HealthCheckedAt,
			"Message":          conn.HealthMessage,
			"Issues":           []any{},
		},
		"ProviderDetail": conn.Provider,
	}
}

// connectorToSummaryResponse builds one CspmConnectorSummary entry for
// ListConnectors, nesting the health/provider fields under ProviderSummary
// exactly as types.CspmConnectorSummary does.
func connectorToSummaryResponse(conn *CspmConnector) map[string]any {
	return map[string]any{
		keyConnectorArn:     conn.ConnectorArn,
		keyConnectorID:      conn.ConnectorId,
		keyCreatedAt:        conn.CreatedAt,
		keyCreatedBy:        conn.CreatedBy,
		keyDescription:      conn.Description,
		keyEnablementStatus: conn.EnablementStatus,
		keyName:             conn.Name,
		"ProviderSummary": map[string]any{
			keyConnectorStatus:      conn.ConnectorStatus,
			"ProviderConfiguration": conn.Provider,
			"ProviderName":          conn.ProviderName,
		},
	}
}

// connectorsOpHandlers returns the CSPM Connectors operation dispatch table
// for handleREST.
func (h *Handler) connectorsOpHandlers(
	c *echo.Context,
	resource string,
	body map[string]any,
) map[string]func() error {
	return map[string]func() error{
		opCreateConnector: func() error { return h.handleCreateConnector(c, body) },
		opGetConnector:    func() error { return h.handleGetConnector(c, resource) },
		opUpdateConnector: func() error { return h.handleUpdateConnector(c, resource, body) },
		opDeleteConnector: func() error { return h.handleDeleteConnector(c, resource) },
		opListConnectors:  func() error { return h.handleListConnectors(c) },
	}
}
