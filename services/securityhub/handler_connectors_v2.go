package securityhub

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

func classifyConnectorsV2Path(method, path string) (string, string) {
	switch {
	case method == http.MethodPost && path == pathConnectorsV2:
		return opCreateConnectorV2, ""
	case method == http.MethodGet && path == pathConnectorsV2:
		return opListConnectorsV2, ""
	case method == http.MethodPost && path == "/connectorsv2/register": //nolint:goconst // existing issue.
		return opRegisterConnectorV2, ""
	case method == http.MethodGet && strings.HasPrefix(path, "/connectorsv2/") &&
		path != "/connectorsv2/register":
		return opGetConnectorV2, strings.TrimPrefix(path, "/connectorsv2/")
	case method == http.MethodPatch && strings.HasPrefix(path, "/connectorsv2/"):
		return opUpdateConnectorV2, strings.TrimPrefix(path, "/connectorsv2/")
	case method == http.MethodDelete && strings.HasPrefix(path, "/connectorsv2/") &&
		path != "/connectorsv2/register":
		return opDeleteConnectorV2, strings.TrimPrefix(path, "/connectorsv2/")
	}

	return opUnknown, ""
}

func classifyTicketsV2Path(method, path string) (string, string) {
	if method == http.MethodPost && path == "/ticketsv2" {
		return opCreateTicketV2, ""
	}

	return opUnknown, ""
}

func (h *Handler) handleCreateConnectorV2(c *echo.Context, body map[string]any) error {
	name, _ := body["Name"].(string)
	description, _ := body["Description"].(string)

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

	conn, err := h.Backend.CreateConnectorV2(name, description, provider, tags)
	if err != nil {
		return typedErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	return c.JSON(http.StatusOK, connectorV2ToResponse(conn))
}

func (h *Handler) handleGetConnectorV2(c *echo.Context, connectorID string) error {
	conn, err := h.Backend.GetConnectorV2(connectorID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return typedErrorResponse(
				c, http.StatusNotFound, "ResourceNotFoundException",
				"Connector V2 not found",
			)
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	return c.JSON(http.StatusOK, connectorV2ToGetResponse(conn))
}

func (h *Handler) handleListConnectorsV2(c *echo.Context) error {
	nextToken := c.QueryParam("NextToken")
	maxResults := 0

	if v := c.QueryParam("MaxResults"); v != "" {
		maxResults, _ = strconv.Atoi(v)
	}

	connectors, next := h.Backend.ListConnectorsV2(nextToken, maxResults)

	var out []map[string]any //nolint:prealloc // existing issue.

	for _, conn := range connectors {
		out = append(out, connectorV2ToResponse(conn))
	}

	if out == nil {
		out = []map[string]any{}
	}

	resp := map[string]any{"Connectors": out}

	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateConnectorV2(c *echo.Context, connectorID string, body map[string]any) error {
	name, _ := body["Name"].(string)
	description, _ := body["Description"].(string)

	var provider map[string]any

	if p, ok := body["Provider"].(map[string]any); ok {
		provider = p
	}

	conn, err := h.Backend.UpdateConnectorV2(connectorID, name, description, provider)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return typedErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", "Connector V2 not found")
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	return c.JSON(http.StatusOK, connectorV2ToResponse(conn))
}

func (h *Handler) handleDeleteConnectorV2(c *echo.Context, connectorID string) error {
	if err := h.Backend.DeleteConnectorV2(connectorID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return typedErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", "Connector V2 not found")
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleRegisterConnectorV2(c *echo.Context, body map[string]any) error {
	authCode, _ := body["AuthCode"].(string)
	authState, _ := body["AuthState"].(string)

	if authCode == "" || authState == "" {
		return typedErrorResponse(
			c, http.StatusBadRequest, "ValidationException",
			"AuthCode and AuthState are required",
		)
	}

	conn, err := h.Backend.RegisterConnectorV2(authCode, authState)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return typedErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", "Connector V2 not found")
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	return c.JSON(http.StatusOK, connectorV2ToResponse(conn))
}

func connectorV2ToResponse(conn *ConnectorV2) map[string]any {
	return map[string]any{
		keyConnectorID:     conn.ConnectorId,
		keyConnectorArn:    conn.ConnectorArn,
		keyName:            conn.Name,
		keyDescription:     conn.Description,
		keyCreatedAt:       conn.CreatedAt,
		keyUpdatedAt:       conn.UpdatedAt,
		keyConnectorStatus: conn.ConnectorStatus,
		"Provider":         conn.Provider,
	}
}

// connectorV2ToGetResponse builds the GetConnectorV2 wire shape: ConnectorId,
// CreatedAt, Health, LastUpdatedAt, Name, ProviderDetail are all required per
// the real GetConnectorV2Output (securityhub@v1.75.4
// api_op_GetConnectorV2.go:39-79); ConnectorArn/Description are optional but
// always populated here. Mirrors connectorToGetResponse's shape for the V1
// CSPM Connector family (handler_connectors.go). Unlike CspmConnector,
// ConnectorV2 tracks a single UpdatedAt timestamp rather than separate
// LastUpdatedAt/HealthCheckedAt fields, so both LastUpdatedAt and
// Health.LastCheckedAt reuse it: the two events coincide in this backend,
// since ConnectorStatus only changes on Update/Register. ProviderDetail
// echoes Provider verbatim -- ProviderConfiguration (the create-time input
// union) and ProviderDetail (this get-time output union) share the same
// member tags (Azure/JiraCloud/ServiceNow -- types.go:17161-17220), so the
// stored value is already wire-correct for this key.
func connectorV2ToGetResponse(conn *ConnectorV2) map[string]any {
	return map[string]any{
		keyConnectorID:  conn.ConnectorId,
		keyConnectorArn: conn.ConnectorArn,
		keyName:         conn.Name,
		keyDescription:  conn.Description,
		keyCreatedAt:    conn.CreatedAt,
		"LastUpdatedAt": conn.UpdatedAt,
		"Health": map[string]any{
			keyConnectorStatus: conn.ConnectorStatus,
			"LastCheckedAt":    conn.UpdatedAt,
		},
		"ProviderDetail": conn.Provider,
	}
}

func (h *Handler) handleCreateTicketV2(c *echo.Context, body map[string]any) error {
	connectorID, _ := body["ConnectorId"].(string)
	if connectorID == "" {
		return typedErrorResponse(c, http.StatusBadRequest, "ValidationException", "ConnectorId is required")
	}

	findingMetadataUID, _ := body["FindingMetadataUid"].(string)
	if findingMetadataUID == "" {
		return typedErrorResponse(c, http.StatusBadRequest, "ValidationException", "FindingMetadataUid is required")
	}

	mode, _ := body["Mode"].(string)
	if mode != "" && mode != "DRYRUN" {
		return typedErrorResponse(c, http.StatusBadRequest, "ValidationException", "Mode must be DRYRUN")
	}

	ticket, err := h.Backend.CreateTicketV2(connectorID, findingMetadataUID, mode)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return typedErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", "Connector V2 not found")
		}

		return typedErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	resp := map[string]any{"TicketId": ticket.TicketId}
	if ticket.TicketSrcUrl != "" {
		resp["TicketSrcUrl"] = ticket.TicketSrcUrl
	}

	return c.JSON(http.StatusOK, resp)
}

// connectorsV2OpHandlers returns the Connectors V2 + Tickets V2 operation
// dispatch table for handleREST.
func (h *Handler) connectorsV2OpHandlers(
	c *echo.Context,
	resource string,
	body map[string]any,
) map[string]func() error {
	return map[string]func() error{
		opCreateConnectorV2:   func() error { return h.handleCreateConnectorV2(c, body) },
		opGetConnectorV2:      func() error { return h.handleGetConnectorV2(c, resource) },
		opListConnectorsV2:    func() error { return h.handleListConnectorsV2(c) },
		opUpdateConnectorV2:   func() error { return h.handleUpdateConnectorV2(c, resource, body) },
		opDeleteConnectorV2:   func() error { return h.handleDeleteConnectorV2(c, resource) },
		opRegisterConnectorV2: func() error { return h.handleRegisterConnectorV2(c, body) },
		opCreateTicketV2:      func() error { return h.handleCreateTicketV2(c, body) },
	}
}
