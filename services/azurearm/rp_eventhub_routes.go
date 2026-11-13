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

//nolint:gochecknoglobals // static route table, read-only after init
var eventHubRoutes = []armRoute{
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
