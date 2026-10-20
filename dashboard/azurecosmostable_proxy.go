package dashboard

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	azureCosmosTableProxyPrefix = "/dashboard/api/v1/azure/cosmos-table"
	azureCosmosTableMaxBody     = 1 << 20
)

// registerAzureCosmosTableRoutes exposes the Cosmos DB Table API to the SPA. It reuses the Azure Table proxy's
// /<account>/<resource> shape and entity-method rules; the account segment is dropped because Cosmos has none.
// Master-key signing happens server-side when --cosmosdb-validate-auth is on.
func (h *DashboardHandler) registerAzureCosmosTableRoutes() {
	var handler echo.HandlerFunc

	if ops := h.config.AzureCosmosOps; ops != nil {
		inner := ops.Handler()

		handler = func(c *echo.Context) error {
			if ops.ValidateAuth {
				if err := ops.SignRequest(c.Request()); err != nil {
					return c.String(http.StatusInternalServerError, "failed to sign Cosmos DB request")
				}
			}

			return inner(c)
		}
	}

	serve := newAzureProxyHandler(azureCosmosTableProxyPrefix, azureCosmosTableMaxBody, handler, azureCosmosTablePath)

	guarded := func(c *echo.Context) error {
		raw := strings.TrimPrefix(c.Request().URL.Path, azureCosmosTableProxyPrefix)
		if !azureTableMethodOK(c.Request().Method, raw) {
			return c.String(http.StatusBadRequest, "unsupported proxy request")
		}

		c.Request().Header.Del("Authorization")

		return serve(c)
	}

	for _, method := range []string{
		http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete,
	} {
		h.SubRouter.Add(method, azureCosmosTableProxyPrefix+"/*", guarded)
	}
}

// azureCosmosTablePath maps /<account>/<resource> to the Cosmos Table API path /<resource>.
func azureCosmosTablePath(raw string) (string, bool) {
	p, ok := azureTableProxyPath(raw)
	if !ok {
		return "", false
	}

	_, resource, _ := strings.Cut(strings.TrimPrefix(p, "/"), "/")

	if resource == "dbs" || resource == "offers" {
		return "", false
	}

	return "/" + resource, true
}
