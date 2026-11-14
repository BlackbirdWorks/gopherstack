package azurearm

import (
	"context"
	"maps"
	"net/http"

	"github.com/labstack/echo/v5"
)

const (
	acStoreLen          = 8
	acShortRetentionDay = 7
	acLongRetentionDay  = 30
)

// acRetentionDays is the revision history window per tier (Free and Developer 7 days, Standard and Premium 30).
func acRetentionDays(sku string) int {
	if sku == acSkuFree || sku == "developer" {
		return acShortRetentionDay
	}

	return acLongRetentionDay
}

// PatchStore applies a ConfigurationStoreUpdateParameters body (tags, sku, identity, properties).
func (p *AppConfigProvider) PatchStore(
	_ context.Context, id ResourceID, body map[string]any,
) (map[string]any, error) {
	if classifyAppConfigPath(id) != acKindStore {
		return nil, errUnsupportedAppConfigType(id)
	}

	p.mu.Lock("PatchStore")
	defer p.mu.Unlock()

	rec, ok := p.store(id.ResourceGroup, id.LeafName())
	if !ok {
		return nil, ErrAppConfigStoreNotFound
	}

	if _, has := body[ehFieldSKU]; has {
		rec.sku = acSkuOf(body)

		if err := p.dataPlane.SetRevisionRetention(rec.name, acRetentionDays(rec.sku)); err != nil {
			return nil, badAppConfigRequest(err)
		}
	}

	if v, has := body[fieldTags]; has {
		rec.tags = stringTags(v)
	}

	if _, has := body[acFieldIdentity]; has {
		rec.identity = acIdentityOf(body)
	}

	if props, _ := body[fieldProperties].(map[string]any); props != nil {
		maps.Copy(rec.extra, dropEmptyObjects(ehExtra(body, acPropEndpoint, acPropCreationDate, acPropCreateMode)))
	}

	return p.storeBody(id, rec), nil
}

func (h *Handler) handleAppConfigPatch(c *echo.Context, segs []string) error {
	p, err := h.appConfigProviderOrNotFound(c)
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

	out, err := p.PatchStore(c.Request().Context(), id, body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}
