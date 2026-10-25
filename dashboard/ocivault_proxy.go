package dashboard

import (
	"encoding/base64"
	"net/http"
	"strconv"
	"unicode/utf8"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	vaultbackend "github.com/blackbirdworks/gopherstack/services/ocivault"
)

const ociVaultPrefix = ociProxyPrefix + "/vault"

type ociVaultAPI struct {
	b *vaultbackend.InMemoryBackend
}

type ociRevealJSON struct {
	Text          string `json:"text,omitempty"`
	Base64        string `json:"base64"`
	IsText        bool   `json:"isText"`
	VersionNumber int64  `json:"versionNumber"`
}

// registerOCIVaultRoutes serves the Vault page from the backend in-process. Secret content is never part of a
// listing or detail response; it leaves the server only through the explicit reveal route.
func (h *DashboardHandler) registerOCIVaultRoutes() {
	api := &ociVaultAPI{}
	if h.config.OCIVaultOps != nil {
		api.b = h.config.OCIVaultOps.Backend
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return ociGuard(api.b != nil, "OCI Vault", next)
	}

	r := h.SubRouter
	p := ociVaultPrefix
	r.Add(http.MethodGet, p+"/vaults", guard(api.vaults))
	r.Add(http.MethodGet, p+"/vaults/:vault/keys", guard(api.keys))
	r.Add(http.MethodGet, p+"/vaults/:vault/keys/:key/versions", guard(api.keyVersions))
	r.Add(http.MethodGet, p+"/secrets", guard(api.secrets))
	r.Add(http.MethodGet, p+"/secrets/:secret/versions", guard(api.secretVersions))
	r.Add(http.MethodPost, p+"/secrets/:secret/versions/:number/reveal", guard(api.reveal))
}

func (a *ociVaultAPI) vaults(c *echo.Context) error {
	items, err := ociAcross(a.b.Compartments(), func(comp string) ([]any, error) {
		vs, lerr := ociCollect(func(token string) (page.Page[any], error) {
			pg, perr := a.b.ListVaults(comp, "", vaultbackend.ListOpts{Page: token, Limit: ociListLimit})

			return anyPage(pg.Data, pg.Next), perr
		})

		return vs, lerr
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"vaults": items})
}

func (a *ociVaultAPI) keys(c *echo.Context) error {
	vault := c.Param("vault")

	items, err := ociAcross(a.b.Compartments(), func(comp string) ([]any, error) {
		return ociCollect(func(token string) (page.Page[any], error) {
			pg, perr := a.b.ListKeys(vault, comp, vaultbackend.KeyFilter{}, vaultbackend.ListOpts{Page: token, Limit: ociListLimit})

			return anyPage(pg.Data, pg.Next), perr
		})
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"keys": items})
}

func (a *ociVaultAPI) keyVersions(c *echo.Context) error {
	items, err := ociCollect(func(token string) (page.Page[any], error) {
		pg, perr := a.b.ListKeyVersions(c.Param("vault"), c.Param("key"), vaultbackend.ListOpts{Page: token, Limit: ociListLimit})

		return anyPage(pg.Data, pg.Next), perr
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"versions": items})
}

func (a *ociVaultAPI) secrets(c *echo.Context) error {
	vault := c.QueryParam("vaultId")

	items, err := ociAcross(a.b.Compartments(), func(comp string) ([]any, error) {
		return ociCollect(func(token string) (page.Page[any], error) {
			pg, perr := a.b.ListSecrets(
				vaultbackend.SecretFilter{Compartment: comp, VaultID: vault},
				vaultbackend.ListOpts{Page: token, Limit: ociListLimit},
			)

			return anyPage(pg.Data, pg.Next), perr
		})
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"secrets": items})
}

func (a *ociVaultAPI) secretVersions(c *echo.Context) error {
	items, err := ociCollect(func(token string) (page.Page[any], error) {
		pg, perr := a.b.ListSecretVersions(c.Param("secret"), vaultbackend.ListOpts{Page: token, Limit: ociListLimit})

		return anyPage(pg.Data, pg.Next), perr
	})
	if err != nil {
		return ociFail(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"versions": items})
}

func (a *ociVaultAPI) reveal(c *echo.Context) error {
	n, err := strconv.ParseInt(c.Param("number"), 10, 64)
	if err != nil || n < 1 {
		return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "invalid version number")
	}

	bundle, _, err := a.b.GetSecretBundle(c.Param("secret"), vaultbackend.BundleSelector{VersionNumber: &n})
	if err != nil {
		return ociFail(c, err)
	}

	out := ociRevealJSON{VersionNumber: bundle.VersionNumber}

	if bundle.Content != nil {
		out.Base64 = bundle.Content.Content

		if raw, derr := base64.StdEncoding.DecodeString(bundle.Content.Content); derr == nil && utf8.Valid(raw) {
			out.IsText, out.Text = true, string(raw)
		}
	}

	c.Response().Header().Set("Cache-Control", "no-store")

	return c.JSON(http.StatusOK, out)
}
