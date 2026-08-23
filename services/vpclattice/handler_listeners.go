package vpclattice

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ------- Listener handlers -------

func (h *Handler) handleCreateListener(
	c *echo.Context,
	serviceID string,
	body map[string]any,
) error {
	name, _ := body[keyName].(string)
	protocol, _ := body[keyProtocol].(string)

	if name == "" || protocol == "" {
		return validationError(c, "name and protocol are required")
	}

	port := bodyInt32(body, keyPort)
	defaultAction := extractRuleAction(body, "defaultAction")
	tags := extractTags(body)

	l, err := h.Backend.CreateListener(serviceID, name, protocol, port, defaultAction, tags)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusCreated, listenerToJSON(l))
}

func (h *Handler) handleGetListener(c *echo.Context, serviceID, listenerID string) error {
	l, err := h.Backend.GetListener(serviceID, listenerID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, listenerToJSON(l))
}

func (h *Handler) handleUpdateListener(
	c *echo.Context,
	serviceID, listenerID string,
	body map[string]any,
) error {
	defaultAction := extractRuleAction(body, "defaultAction")

	l, err := h.Backend.UpdateListener(serviceID, listenerID, defaultAction)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, listenerToJSON(l))
}

func (h *Handler) handleDeleteListener(c *echo.Context, serviceID, listenerID string) error {
	if err := h.Backend.DeleteListener(serviceID, listenerID); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListListeners(c *echo.Context, serviceID string) error {
	maxResults := queryInt32(c)
	nextToken := c.QueryParam("nextToken")

	items, next, err := h.Backend.ListListeners(serviceID, maxResults, nextToken)
	if err != nil {
		return h.handleError(c, err)
	}

	summaries := make([]any, 0, len(items))
	for _, l := range items {
		summaries = append(summaries, listenerSummaryToJSON(l))
	}

	resp := map[string]any{keyItems: summaries}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

// ------- Listener JSON serialization -------

func listenerToJSON(l *Listener) map[string]any {
	m := map[string]any{
		keyARN:           l.ARN,
		"id":             l.ID,
		keyServiceARN:    l.ServiceARN,
		keyServiceID:     l.ServiceID,
		keyName:          l.Name,
		keyProtocol:      l.Protocol,
		keyPort:          l.Port,
		keyCreatedAt:     l.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt: l.LastUpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}

	if l.DefaultAction != nil {
		m["defaultAction"] = ruleActionToJSON(l.DefaultAction)
	}

	return m
}

func listenerSummaryToJSON(l *ListenerSummary) map[string]any {
	return map[string]any{
		keyARN:           l.ARN,
		"id":             l.ID,
		keyName:          l.Name,
		keyProtocol:      l.Protocol,
		keyPort:          l.Port,
		keyCreatedAt:     l.CreatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		keyLastUpdatedAt: l.LastUpdatedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
}
