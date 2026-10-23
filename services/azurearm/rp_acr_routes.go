package azurearm

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	acrActionListCredentials  = "listCredentials"
	acrActionRegenerate       = "regenerateCredential"
	acrActionCheckName        = "checkNameAvailability"
	acrActionCallbackConfig   = "getCallbackConfig"
	acrSubCheckLen            = 5
	acrRegistryLen            = 8
	acrRegistryActionLen      = 9
	acrWebhookActionLen       = 11
	acrProviderNsIdx          = 5
	acrTypeIdx                = 6
	acrWebhookTypeIdx         = 8
	acrSubProviderNsIdx       = 3
	acrSubProviderSegIdx      = 2
	acrSubCheckActionIdx      = 4
	acrGroupsSegIdx           = 2
	acrProvidersInGroupSegIdx = 4
)

func isACRScope(segs []string, n int) bool {
	return len(segs) == n && isSubscriptionScoped(segs) && segAt(segs, acrGroupsSegIdx, resourceGroupsSegment) &&
		segAt(segs, acrProvidersInGroupSegIdx, providersSegment) &&
		segAt(segs, acrProviderNsIdx, namespaceMicrosoftContainerRegistry) && segAt(segs, acrTypeIdx, acrRegistriesType)
}

func isACRRegistryAction(segs []string, r *http.Request, method, action string) bool {
	return r.Method == method && isACRScope(segs, acrRegistryActionLen) && strings.EqualFold(segs[len(segs)-1], action)
}

//nolint:gochecknoglobals // static route table, read-only after init
var containerRegistryRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPost && len(segs) == acrSubCheckLen && isSubscriptionScoped(segs) &&
				segAt(segs, acrSubProviderSegIdx, providersSegment) &&
				segAt(segs, acrSubProviderNsIdx, namespaceMicrosoftContainerRegistry) &&
				segAt(segs, acrSubCheckActionIdx, acrActionCheckName)
		},
		handle: func(h *Handler, c *echo.Context, _ []string) error { return h.handleACRCheckName(c) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPatch && isACRScope(segs, acrRegistryLen)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACRPatch(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isACRRegistryAction(segs, r, http.MethodPost, acrActionListCredentials)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACRCredentials(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isACRRegistryAction(segs, r, http.MethodPost, acrActionRegenerate)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACRRegenerate(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isACRRegistryAction(segs, r, http.MethodGet, acrReplicationsType)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleACRListChildren(c, segs, acrKindReplication)
		},
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isACRRegistryAction(segs, r, http.MethodGet, acrWebhooksType)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleACRListChildren(c, segs, acrKindWebhook)
		},
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPost && isACRScope(segs, acrWebhookActionLen) &&
				segAt(segs, acrWebhookTypeIdx, acrWebhooksType) &&
				strings.EqualFold(segs[len(segs)-1], acrActionCallbackConfig)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACRCallbackConfig(c, segs) },
	},
}

func (h *Handler) acrProviderOrNotFound(c *echo.Context) (*ContainerRegistryProvider, error) {
	p, ok := h.containerRegistryProvider()
	if !ok {
		return nil, h.writeNotFound(c)
	}

	return p, nil
}

func (h *Handler) handleACRCheckName(c *echo.Context) error {
	p, err := h.acrProviderOrNotFound(c)
	if p == nil {
		return err
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	name, _ := body[fieldName].(string)

	return h.writeJSON(c, http.StatusOK, p.CheckName(name))
}

func (h *Handler) handleACRPatch(c *echo.Context, segs []string) error {
	p, err := h.acrProviderOrNotFound(c)
	if p == nil {
		return err
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := p.Patch(c.Request().Context(), id, body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleACRCredentials(c *echo.Context, segs []string) error {
	p, err := h.acrProviderOrNotFound(c)
	if p == nil {
		return err
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := p.ListCredentials(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleACRRegenerate(c *echo.Context, segs []string) error {
	p, err := h.acrProviderOrNotFound(c)
	if p == nil {
		return err
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	which, _ := body[fieldName].(string)

	out, err := p.RegenerateCredential(c.Request().Context(), id, which)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleACRListChildren(c *echo.Context, segs []string, kind acrKind) error {
	p, err := h.acrProviderOrNotFound(c)
	if p == nil {
		return err
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := p.ListChildren(id, kind)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: out})
}

func (h *Handler) handleACRCallbackConfig(c *echo.Context, segs []string) error {
	p, err := h.acrProviderOrNotFound(c)
	if p == nil {
		return err
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := p.CallbackConfig(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}
