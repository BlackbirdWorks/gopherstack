package dashboard

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	azureCosmosProxyPrefix  = "/dashboard/api/v1/azure/cosmos"
	azureCosmosMaxBodyBytes = 2 << 20
	azureCosmosDBsSeg       = "dbs"
	azureCosmosCollsSeg     = "colls"
	azureCosmosDocsSeg      = "docs"
	azureCosmosMaxSegments  = 6
	azureCosmosDocsDepth    = 5
)

// registerAzureCosmosRoutes exposes the Cosmos DB emulator's SQL API to the SPA
// by dispatching in-process to its handler. Browser-supplied credentials are
// dropped; when the emulator enforces master-key auth (--cosmosdb-validate-auth)
// the proxy signs each forwarded request server-side, so no key reaches the browser.
func (h *DashboardHandler) registerAzureCosmosRoutes() {
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

	serve := newAzureProxyHandler(azureCosmosProxyPrefix, azureCosmosMaxBodyBytes, handler, azureCosmosProxyPath)

	guarded := func(c *echo.Context) error {
		c.Request().Header.Del("Authorization")

		return serve(c)
	}

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
		h.SubRouter.Add(method, azureCosmosProxyPrefix+"/*", guarded)
	}
}

// azureCosmosProxyPath admits only /dbs[/<db>[/colls[/<coll>[/docs[/<id>]]]]];
// the account root and Table API paths are not proxied.
func azureCosmosProxyPath(raw string) (string, bool) {
	if !strings.HasPrefix(raw, "/") || len(raw) < 2 {
		return "", false
	}

	segs := strings.Split(raw[1:], "/")
	if len(segs) > azureCosmosMaxSegments || segs[0] != azureCosmosDBsSeg {
		return "", false
	}

	for _, seg := range segs {
		if seg == "" || seg == ".." || seg == "." {
			return "", false
		}
	}

	if len(segs) > 2 && segs[2] != azureCosmosCollsSeg {
		return "", false
	}

	if len(segs) >= azureCosmosDocsDepth && segs[4] != azureCosmosDocsSeg {
		return "", false
	}

	return raw, true
}
