package azurearm

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

const (
	cosmosAccountSegs       = 8
	cosmosOperationSegs     = 8
	cosmosLocationsIdx      = 4
	cosmosOperationsIdx     = 6
	cosmosOperationIDIdx    = 7
	cosmosOperationsStatus  = "operationsStatus"
	cosmosAccountActionSegs = 9
	cosmosContainerListSegs = 11
	cosmosActionIdx         = 8
	cosmosDatabasesIdx      = 8
	cosmosContainersIdx     = 10
	cosmosNamespaceIdx      = 5
	cosmosKeyKindField      = "keyKind"

	cosmosActionReadonlyKeys = "readonlykeys"
	cosmosActionConnStrings  = "listConnectionStrings"
	cosmosActionRegenerate   = "regenerateKey"
)

func isCosmosScope(segs []string) bool {
	return len(segs) >= cosmosAccountSegs && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		strings.EqualFold(segs[cosmosNamespaceIdx], namespaceMicrosoftDocumentDB) &&
		strings.EqualFold(segs[6], cosmosAccountsType)
}

func isCosmosChildList(segs []string) bool {
	if len(segs) <= cosmosDatabasesIdx || !isCosmosScope(segs) ||
		!strings.EqualFold(segs[cosmosDatabasesIdx], cosmosSQLDatabasesType) {
		return false
	}

	switch len(segs) {
	case cosmosAccountActionSegs:
		return true
	case cosmosContainerListSegs:
		return strings.EqualFold(segs[cosmosContainersIdx], cosmosContainersType)
	}

	return false
}

func isCosmosAction(segs []string, action string) bool {
	return len(segs) == cosmosAccountActionSegs && isCosmosScope(segs) &&
		strings.EqualFold(segs[cosmosActionIdx], action)
}

//nolint:gochecknoglobals // static route table, read-only after init
var cosmosRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodDelete && isCosmosScope(segs)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleCosmosDelete(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return len(segs) == cosmosOperationSegs && r.Method == http.MethodGet && isCosmosOperationPath(segs)
		},
		handle: func(h *Handler, c *echo.Context, _ []string) error {
			return h.writeJSON(c, http.StatusOK, map[string]any{sbPropStatus: "Succeeded"})
		},
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return len(segs) == cosmosAccountSegs && r.Method == http.MethodPatch && isCosmosScope(segs)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleCosmosPatch(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isCosmosChildList(segs)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleCosmosList(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isCosmosAction(segs, cosmosActionReadonlyKeys) &&
				(r.Method == http.MethodGet || r.Method == http.MethodPost)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleCosmosKeysAction(c, segs, (*CosmosProvider).ReadonlyKeys)
		},
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isCosmosAction(segs, cosmosActionConnStrings) && r.Method == http.MethodPost
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleCosmosKeysAction(c, segs, (*CosmosProvider).ConnectionStrings)
		},
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isCosmosAction(segs, cosmosActionRegenerate) && r.Method == http.MethodPost
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleCosmosRegenerate(c, segs) },
	},
}

func (h *Handler) cosmosProvider() (*CosmosProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftDocumentDB)
	if !ok {
		return nil, false
	}

	cp, ok := p.(*CosmosProvider)

	return cp, ok
}

func (h *Handler) handleCosmosPatch(c *echo.Context, segs []string) error {
	cp, ok := h.cosmosProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := cp.Patch(c.Request().Context(), id, body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleCosmosList(c *echo.Context, segs []string) error {
	cp, ok := h.cosmosProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	parent, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	parent.Types = append(parent.Types, segs[len(segs)-1])

	values, err := cp.List(c.Request().Context(), parent)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: values})
}

func (h *Handler) handleCosmosKeysAction(
	c *echo.Context, segs []string,
	action func(*CosmosProvider, context.Context, ResourceID) (map[string]any, error),
) error {
	cp, ok := h.cosmosProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := action(cp, c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleCosmosRegenerate(c *echo.Context, segs []string) error {
	cp, ok := h.cosmosProvider()
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

	kind, _ := body[cosmosKeyKindField].(string)
	if err = cp.RegenerateKey(c.Request().Context(), id, kind); err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{})
}

func isCosmosOperationPath(segs []string) bool {
	return isSubscriptionScoped(segs) && segAt(segs, providersOrResourceGroupsIdx, providersSegment) &&
		strings.EqualFold(
			segs[3],
			namespaceMicrosoftDocumentDB,
		) && strings.EqualFold(segs[cosmosLocationsIdx], "locations") &&
		strings.EqualFold(segs[cosmosOperationsIdx], cosmosOperationsStatus)
}

// handleCosmosDelete deletes the resource and answers 202 with an Azure-AsyncOperation URL, as azurerm's
// long-running-operation poller expects; the operation is already complete when polled.
func (h *Handler) handleCosmosDelete(c *echo.Context, segs []string) error {
	return h.deleteAsync(c, segs, namespaceMicrosoftDocumentDB, cosmosOperationsStatus)
}

// deleteAsync deletes the resource at segs and answers 202 with an operationsStatus URL under providerNS.
func (h *Handler) deleteAsync(c *echo.Context, segs []string, providerNS, opsSegment string) error {
	id, err := ParseGenericResourcePath("/" + joinSegs(segs))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	if err = h.Registry.Delete(c.Request().Context(), id); err != nil {
		return h.writeAPIError(c, err)
	}

	op := h.baseURLFor(c.Request()) + "/subscriptions/" + id.SubscriptionID + "/providers/" +
		providerNS + "/locations/" + DefaultLocation + "/" + opsSegment + "/" + uuid.NewString()
	c.Response().Header().Set("Azure-Asyncoperation", op)
	c.Response().Header().Set("Location", op)
	c.Response().Header().Set("Retry-After", "1")

	return c.NoContent(http.StatusAccepted)
}
