package dashboard

import (
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	idbackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

const ociIdentityPrefix = ociProxyPrefix + "/identity"

type ociAPIKeyJSON struct {
	TimeCreated time.Time `json:"timeCreated"`
	Fingerprint string    `json:"fingerprint"`
	KeyID       string    `json:"keyId"`
	State       string    `json:"lifecycleState"`
}

type ociIdentityAPI struct {
	b *idbackend.InMemoryBackend
}

// registerOCIIdentityRoutes serves the Identity page from the backend in-process. API keys are listed by
// fingerprint only; neither key material nor any credential secret is returned.
func (h *DashboardHandler) registerOCIIdentityRoutes() {
	api := &ociIdentityAPI{}
	if h.config.OCIIdentityOps != nil {
		api.b = h.config.OCIIdentityOps.Backend
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return ociGuard(api.b != nil, "OCI Identity", next)
	}

	r := h.SubRouter
	p := ociIdentityPrefix
	r.Add(http.MethodGet, p+"/tenancy", guard(api.tenancy))
	r.Add(http.MethodGet, p+"/compartments", guard(api.compartments))
	r.Add(http.MethodGet, p+"/users", guard(api.users))
	r.Add(http.MethodGet, p+"/users/:user/keys", guard(api.apiKeys))
	r.Add(http.MethodGet, p+"/groups", guard(api.groups))
	r.Add(http.MethodGet, p+"/memberships", guard(api.memberships))
	r.Add(http.MethodGet, p+"/policies", guard(api.policies))
}

func (a *ociIdentityAPI) tenancy(c *echo.Context) error {
	t, err := a.b.GetTenancy(idbackend.TenancyID)
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, t)
}

func (a *ociIdentityAPI) compartmentList() ([]any, error) {
	root, _, err := a.b.GetCompartment(idbackend.TenancyID)
	if err != nil {
		return nil, err //nolint:wrapcheck // backend error passes through to ociFail
	}

	rest, err := ociCollect(func(token string) (page.Page[any], error) {
		pg, perr := a.b.ListCompartments(
			idbackend.CompartmentFilter{CompartmentID: idbackend.TenancyID, Subtree: true, AccessLevel: "ANY"},
			idbackend.ListOpts{Page: token, Limit: ociListLimit},
		)

		return anyPage(pg.Data, pg.Next), perr
	})
	if err != nil {
		return nil, err
	}

	return append([]any{root}, rest...), nil
}

func (a *ociIdentityAPI) compartments(c *echo.Context) error {
	items, err := a.compartmentList()
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: items})
}

func (a *ociIdentityAPI) users(c *echo.Context) error {
	items, err := ociCollect(func(token string) (page.Page[any], error) {
		pg, perr := a.b.ListUsers(
			idbackend.NameFilter{CompartmentID: idbackend.TenancyID},
			idbackend.ListOpts{Page: token, Limit: ociListLimit},
		)

		return anyPage(pg.Data, pg.Next), perr
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"users": items})
}

func (a *ociIdentityAPI) apiKeys(c *echo.Context) error {
	keys, err := a.b.ListAPIKeys(c.Param("user"))
	if err != nil {
		return ociFail(c, err)
	}

	items := make([]ociAPIKeyJSON, 0, len(keys))
	for _, k := range keys {
		items = append(items, ociAPIKeyJSON{
			TimeCreated: k.TimeCreated, Fingerprint: k.Fingerprint, KeyID: k.KeyID, State: k.LifecycleState,
		})
	}

	return c.JSON(http.StatusOK, map[string]any{iamKeysKind: items})
}

func (a *ociIdentityAPI) groups(c *echo.Context) error {
	items, err := ociCollect(func(token string) (page.Page[any], error) {
		pg, perr := a.b.ListGroups(
			idbackend.NameFilter{CompartmentID: idbackend.TenancyID},
			idbackend.ListOpts{Page: token, Limit: ociListLimit},
		)

		return anyPage(pg.Data, pg.Next), perr
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{ociKeyGroups: items})
}

func (a *ociIdentityAPI) memberships(c *echo.Context) error {
	items, err := ociCollect(func(token string) (page.Page[any], error) {
		pg, perr := a.b.ListMemberships(
			idbackend.TenancyID,
			"",
			"",
			idbackend.ListOpts{Page: token, Limit: ociListLimit},
		)

		return anyPage(pg.Data, pg.Next), perr
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"memberships": items})
}

// policies lists the policies of every compartment, the root included.
func (a *ociIdentityAPI) policies(c *echo.Context) error {
	comps, err := a.b.ListCompartments(
		idbackend.CompartmentFilter{CompartmentID: idbackend.TenancyID, Subtree: true, AccessLevel: "ANY"},
		idbackend.ListOpts{Limit: ociListLimit},
	)
	if err != nil {
		return ociFail(c, err)
	}

	ids := []string{idbackend.TenancyID}
	for _, comp := range comps.Data {
		ids = append(ids, comp.ID)
	}

	items, err := ociAcross(ids, func(id string) ([]any, error) {
		return ociCollect(func(token string) (page.Page[any], error) {
			pg, perr := a.b.ListPolicies(
				idbackend.PolicyFilter{CompartmentID: id},
				idbackend.ListOpts{Page: token, Limit: ociListLimit},
			)

			return anyPage(pg.Data, pg.Next), perr
		})
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"policies": items})
}
