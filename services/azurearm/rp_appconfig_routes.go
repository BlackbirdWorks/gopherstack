package azurearm

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	acActionRegenerate  = "regenerateKey"
	acActionKeyValue    = "listKeyValue"
	acActionCheckName   = "checkNameAvailability"
	acActionPurge       = "purge"
	acSubProviderIdx    = 2
	acSubNsIdx          = 3
	acSubLocationsIdx   = 4
	acSubLocationNameAt = 5
	acSubDeletedAt      = 6
	acSubCheckLen       = 5
	acSubDeletedLen     = 8
	acSubPurgeLen       = 9
	acStoreActionLen    = 9
	acStoreNsIdx        = 5
	acStoreTypeIdx      = 6
)

func isAppConfigSub(segs []string, n int) bool {
	return len(segs) == n && isSubscriptionScoped(segs) && segAt(segs, acSubProviderIdx, providersSegment) &&
		segAt(segs, acSubNsIdx, namespaceMicrosoftAppConfig)
}

func isAppConfigDeleted(segs []string, n int) bool {
	return isAppConfigSub(segs, n) && segAt(segs, acSubLocationsIdx, "locations") &&
		segAt(segs, acSubDeletedAt, acDeletedType)
}

func isAppConfigStoreAction(segs []string, r *http.Request, method, action string) bool {
	return r.Method == method && len(segs) == acStoreActionLen && isSubscriptionScoped(segs) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		segAt(segs, acStoreNsIdx, namespaceMicrosoftAppConfig) &&
		segAt(segs, acStoreTypeIdx, acStoresType) && strings.EqualFold(segs[len(segs)-1], action)
}

//nolint:gochecknoglobals // static route table, read-only after init
var appConfigRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPost && isAppConfigSub(segs, acSubCheckLen) &&
				segAt(segs, acSubLocationsIdx, acActionCheckName)
		},
		handle: func(h *Handler, c *echo.Context, _ []string) error { return h.handleAppConfigCheckName(c) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isAppConfigSub(segs, acSubCheckLen) &&
				segAt(segs, acSubLocationsIdx, acDeletedType)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleAppConfigListDeleted(c, segs[1])
		},
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isAppConfigDeleted(segs, acSubDeletedLen)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleAppConfigDeleted(c, segs, false)
		},
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPost && isAppConfigDeleted(segs, acSubPurgeLen) &&
				strings.EqualFold(segs[len(segs)-1], acActionPurge)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleAppConfigDeleted(c, segs, true)
		},
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isAppConfigStoreAction(segs, r, http.MethodPost, acActionRegenerate)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleAppConfigRegenerate(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isAppConfigStoreAction(segs, r, http.MethodPost, acActionKeyValue)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleAppConfigKeyValue(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isAppConfigStoreAction(segs, r, http.MethodGet, acReplicasType)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleAppConfigList(c, segs, true) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isAppConfigStoreAction(segs, r, http.MethodGet, acKeyValuesType)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleAppConfigList(c, segs, false) },
	},
}

func (h *Handler) appConfigProviderOrNotFound(c *echo.Context) (*AppConfigProvider, error) {
	p, ok := h.appConfigProvider()
	if !ok {
		return nil, h.writeNotFound(c)
	}

	return p, nil
}

func (h *Handler) handleAppConfigCheckName(c *echo.Context) error {
	p, err := h.appConfigProviderOrNotFound(c)
	if p == nil {
		return err
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	name, _ := body[fieldName].(string)

	return h.writeJSON(c, http.StatusOK, p.CheckName(c.Request().Context(), name))
}

func (h *Handler) handleAppConfigListDeleted(c *echo.Context, subscription string) error {
	p, err := h.appConfigProviderOrNotFound(c)
	if p == nil {
		return err
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{
		fieldValue: p.ListDeletedStores(c.Request().Context(), subscription),
	})
}

func (h *Handler) handleAppConfigDeleted(c *echo.Context, segs []string, purge bool) error {
	p, err := h.appConfigProviderOrNotFound(c)
	if p == nil {
		return err
	}

	ctx := c.Request().Context()
	location, name := segs[acSubLocationNameAt], segs[acSubDeletedAt+1]

	if purge {
		if err = p.PurgeDeletedStore(ctx, location, name); err != nil {
			return h.writeAPIError(c, err)
		}

		return c.NoContent(http.StatusOK)
	}

	body, err := p.DeletedStore(ctx, location, name)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, body)
}

func (h *Handler) handleAppConfigRegenerate(c *echo.Context, segs []string) error {
	p, err := h.appConfigProviderOrNotFound(c)
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

	keyID, _ := body["id"].(string)

	out, err := p.RegenerateKey(c.Request().Context(), id, keyID)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleAppConfigKeyValue(c *echo.Context, segs []string) error {
	p, err := h.appConfigProviderOrNotFound(c)
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

	key, _ := body[acFieldKey].(string)
	label, _ := body[acFieldLabel].(string)

	out, err := p.KeyValue(c.Request().Context(), id, key, label)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleAppConfigList(c *echo.Context, segs []string, replicas bool) error {
	p, err := h.appConfigProviderOrNotFound(c)
	if p == nil {
		return err
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	list := p.ListKeyValues
	if replicas {
		list = p.ListReplicas
	}

	values, err := list(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: values})
}
