package azurearm

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	egActionRegenerate = "regenerateKey"
	egActionFullURL    = "getFullUrl"
	egActionDelivery   = "getDeliveryAttributes"
	egDomainTopicsSegs = 9
	egProviderSegIdx   = 5
	egMinActionSegs    = 8
	egOperationSegs    = 8
	egOperationsStatus = "operationsStatus"
)

func isEventGridScope(segs []string) bool {
	return len(segs) >= egMinActionSegs && isSubscriptionScoped(segs) &&
		strings.EqualFold(segs[egProviderSegIdx], namespaceMicrosoftEventGrid)
}

func isEventGridOperation(segs []string) bool {
	return len(segs) == egOperationSegs && isSubscriptionScoped(segs) &&
		strings.EqualFold(segs[3], namespaceMicrosoftEventGrid) && strings.EqualFold(segs[4], "locations") &&
		strings.EqualFold(segs[6], egOperationsStatus)
}

func isEventGridAction(segs []string, action string, r *http.Request) bool {
	return r.Method == http.MethodPost && len(segs) >= egMinActionSegs && isSubscriptionScoped(segs) &&
		strings.EqualFold(segs[egProviderSegIdx], namespaceMicrosoftEventGrid) &&
		strings.EqualFold(segs[len(segs)-1], action)
}

//nolint:gochecknoglobals // static route table, read-only after init
var eventGridRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodDelete && isEventGridScope(segs)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleEventGridDelete(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isEventGridOperation(segs)
		},
		handle: func(h *Handler, c *echo.Context, _ []string) error {
			return h.writeJSON(c, http.StatusOK, map[string]any{sbPropStatus: provisioningStateSucceeded})
		},
	},
	{
		match: func(segs []string, r *http.Request) bool { return isEventGridAction(segs, egActionRegenerate, r) },
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleEventGridRegenerate(c, segs)
		},
	},
	{
		match:  func(segs []string, r *http.Request) bool { return isEventGridAction(segs, egActionFullURL, r) },
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleEventGridFullURL(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool { return isEventGridAction(segs, egActionDelivery, r) },
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleEventGridDeliveryAttributes(c, segs)
		},
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && len(segs) == egDomainTopicsSegs && isEventGridScope(segs) &&
				strings.EqualFold(segs[6], egDomainsType) && strings.EqualFold(segs[8], "topics")
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleListDomainTopics(c, segs) },
	},
}

func (h *Handler) handleListDomainTopics(c *echo.Context, segs []string) error {
	p, ok := h.eventGridProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	values, err := p.ListDomainTopics(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: values})
}

func (h *Handler) handleEventGridDeliveryAttributes(c *echo.Context, segs []string) error {
	p, ok := h.eventGridProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := p.DeliveryAttributes(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleEventGridRegenerate(c *echo.Context, segs []string) error {
	p, ok := h.eventGridProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	keyName, _ := body[fieldKeyName].(string)

	out, err := p.RegenerateKey(c.Request().Context(), id, keyName)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleEventGridFullURL(c *echo.Context, segs []string) error {
	p, ok := h.eventGridProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := p.FullURL(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

// handleEventGridDelete deletes the resource and answers 202 with an Azure-AsyncOperation URL, as
// go-azure-sdk's long-running-operation poller expects; the operation is already complete when polled.
func (h *Handler) handleEventGridDelete(c *echo.Context, segs []string) error {
	return h.deleteAsync(c, segs, namespaceMicrosoftEventGrid, egOperationsStatus)
}
