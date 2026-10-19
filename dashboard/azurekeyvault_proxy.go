package dashboard

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	azurekeyvaultbackend "github.com/blackbirdworks/gopherstack/services/azurekeyvault"
)

const (
	azureKeyVaultProxyPrefix  = "/dashboard/api/v1/azure/keyvault"
	azureKeyVaultMaxBodyBytes = 1 << 20
	azureKeyVaultMaxSegments  = 6
	azureKeyVaultAPIVersion   = "7.5"
	azureKeyVaultTokenTTL     = 5 * time.Minute
	azureKeyVaultVaultsSeg    = "vaults"
	azureKeyVaultMinSegments  = 3
)

var azureKeyVaultNameRe = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9-]{1,22}[A-Za-z0-9]$`)

func azureKeyVaultKinds() []string {
	return []string{"secrets", "keys", "certificates", "deletedsecrets", "deletedkeys", "deletedcertificates"}
}

// azureKeyVaultDenied lists sub-resources the page never needs.
func azureKeyVaultDenied() []string {
	return []string{"backup", "restore", "contacts", "issuers", "import", "pending", "policy", "rotationpolicy"}
}

// registerAzureKeyVaultRoutes exposes the vault list and the Key Vault data
// plane to the SPA by dispatching in-process to its handler.
func (h *DashboardHandler) registerAzureKeyVaultRoutes() {
	var inner echo.HandlerFunc

	if h.config.AzureKeyVaultOps != nil {
		kv := h.config.AzureKeyVaultOps.Handler()
		inner = func(c *echo.Context) error {
			r := c.Request()
			r.Header.Set("Authorization", "Bearer "+azureKeyVaultInternalToken())

			if r.URL.Query().Get("api-version") == "" {
				q := r.URL.Query()
				q.Set("api-version", azureKeyVaultAPIVersion)
				r.URL.RawQuery = q.Encode()
			}

			return kv(c)
		}

		h.SubRouter.Add(http.MethodGet, azureKeyVaultProxyPrefix+"/vaults", h.azureKeyVaultList)
		h.SubRouter.Add(http.MethodPost, azureKeyVaultProxyPrefix+"/vaults", h.azureKeyVaultCreateVault)
	}

	serve := newAzureProxyHandler(azureKeyVaultProxyPrefix, azureKeyVaultMaxBodyBytes, inner, azureKeyVaultProxyPath)

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete} {
		h.SubRouter.Add(method, azureKeyVaultProxyPrefix+"/*", serve)
	}
}

// azureKeyVaultInternalToken mints a short-lived unsigned JWT that satisfies
// the data plane's Bearer check, in-process only; the real listener is unchanged.
func azureKeyVaultInternalToken() string {
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]any{
		"iss": "gopherstack-dashboard",
		"exp": time.Now().Add(azureKeyVaultTokenTTL).Unix(),
	})

	return header + "." + enc.EncodeToString(claims) + "."
}

func (h *DashboardHandler) azureKeyVaultList(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"vaults": h.config.AzureKeyVaultOps.Backend.ListVaults()})
}

func (h *DashboardHandler) azureKeyVaultCreateVault(c *echo.Context) error {
	var in struct {
		Name string `json:"name"`
	}

	if err := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, azureKeyVaultMaxBodyBytes)).
		Decode(&in); err != nil || !azureKeyVaultNameRe.MatchString(in.Name) {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"error": "vault name must be 3-24 characters: letters, digits and hyphens, starting with a letter",
		})
	}

	h.config.AzureKeyVaultOps.Backend.CreateVault(in.Name, azurekeyvaultbackend.VaultOptions{})

	return c.JSON(http.StatusCreated, map[string]string{"name": in.Name})
}

// azureKeyVaultProxyPath admits only /vaults/<name>/<kind>[/<name>[/<segment>[/<segment>]]]
// for the secret, key and certificate surfaces the page uses.
func azureKeyVaultProxyPath(raw string) (string, bool) {
	if !strings.HasPrefix(raw, "/") || len(raw) < 2 {
		return "", false
	}

	segs := strings.Split(raw[1:], "/")
	if len(segs) > azureKeyVaultMaxSegments || len(segs) < azureKeyVaultMinSegments {
		return "", false
	}

	for _, seg := range segs {
		if seg == "" || seg == ".." || seg == "." {
			return "", false
		}
	}

	if segs[0] != azureKeyVaultVaultsSeg || !azureKeyVaultNameRe.MatchString(segs[1]) ||
		!slices.Contains(azureKeyVaultKinds(), segs[2]) {
		return "", false
	}

	for _, seg := range segs[3:] {
		if slices.Contains(azureKeyVaultDenied(), seg) {
			return "", false
		}
	}

	return raw, true
}
