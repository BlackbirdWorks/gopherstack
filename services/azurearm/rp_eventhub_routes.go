package azurearm

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	ehActionRegenerateKeys = "regenerateKeys"
	ehNSRuleSegs           = 11
	ehHubRuleSegs          = 13
	ehRuleTypeBack         = 3
)

func isEventHubRegenerate(segs []string, r *http.Request) bool {
	n := len(segs)

	return r.Method == http.MethodPost && (n == ehNSRuleSegs || n == ehHubRuleSegs) && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		segAt(segs, redisNsIdx, namespaceMicrosoftEventHub) && segAt(segs, redisTypeIdx, ehNamespacesType) &&
		strings.EqualFold(segs[n-ehRuleTypeBack], ehRulesType) && strings.EqualFold(segs[n-1], ehActionRegenerateKeys)
}

const (
	ehDRActionSegs = 11
	ehDRCheckSegs  = 10
	ehDRSegmentIdx = 8
	ehDRAliasIdx   = 9
)

func isEventHubDRBase(segs []string) bool {
	return isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		segAt(segs, redisNsIdx, namespaceMicrosoftEventHub) && segAt(segs, redisTypeIdx, ehNamespacesType) &&
		segAt(segs, ehDRSegmentIdx, ehDRType)
}

func isEventHubDRAction(action string) func([]string, *http.Request) bool {
	return func(segs []string, r *http.Request) bool {
		return r.Method == http.MethodPost && len(segs) == ehDRActionSegs && isEventHubDRBase(segs) &&
			strings.EqualFold(segs[ehDRActionSegs-1], action)
	}
}

func isEventHubDRCheck(segs []string, r *http.Request) bool {
	return r.Method == http.MethodPost && len(segs) == ehDRCheckSegs && isEventHubDRBase(segs) &&
		strings.EqualFold(segs[ehDRAliasIdx], ehDRActionCheck)
}

func isEventHubChildList(segs []string, r *http.Request) bool {
	const minSegs = 9

	if r.Method != http.MethodGet || len(segs) < minSegs || len(segs)%2 == 0 || !isSubscriptionScoped(segs) ||
		!segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) ||
		!segAt(segs, providerNamespaceActionIdx, providersSegment) ||
		!segAt(segs, redisNsIdx, namespaceMicrosoftEventHub) || !segAt(segs, redisTypeIdx, ehNamespacesType) {
		return false
	}

	switch strings.ToLower(segs[len(segs)-1]) {
	case strings.ToLower(ehHubsType), strings.ToLower(ehGroupsType), strings.ToLower(ehRulesType),
		strings.ToLower(ehDRType):
		return true
	}

	return false
}

func (h *Handler) handleEventHubChildList(c *echo.Context, segs []string) error {
	p, ok := h.eventHubProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	id.Types = append(id.Types, segs[len(segs)-1])

	values, err := p.List(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	if values == nil {
		values = []map[string]any{}
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: values})
}

//nolint:gochecknoglobals // static route table, read-only after init
var eventHubRoutes = []armRoute{
	{
		match:  isEventHubChildList,
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleEventHubChildList(c, segs) },
	},
	{
		match:  isEventHubDRAction(ehDRActionBreak),
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleEventHubDR(c, segs) },
	},
	{
		match:  isEventHubDRAction(ehDRActionFailover),
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleEventHubDR(c, segs) },
	},
	{
		match:  isEventHubDRCheck,
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleEventHubDRCheck(c, segs) },
	},
	{
		match:  isEventHubRegenerate,
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleEventHubRegenerate(c, segs) },
	},
}

func (h *Handler) handleEventHubRegenerate(c *echo.Context, segs []string) error {
	p, ok := h.eventHubProvider()
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

	out, err := p.RegenerateKeys(id, body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleEventHubDR(c *echo.Context, segs []string) error {
	p, ok := h.eventHubProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	if strings.EqualFold(segs[len(segs)-1], ehDRActionBreak) {
		err = p.BreakPairing(id)
	} else {
		err = p.FailOver(id)
	}

	if err != nil {
		return h.writeAPIError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleEventHubDRCheck(c *echo.Context, segs []string) error {
	p, ok := h.eventHubProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:ehDRSegmentIdx]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := p.CheckDRNameAvailability(id, body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}
