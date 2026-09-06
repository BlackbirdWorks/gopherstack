package cosmosdb

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// accountRegionName is the region name reported in every
// writableLocations/readableLocations entry. Its value is never validated
// against a real Azure region list by any SDK -- it only has to be a
// non-empty string, since azcosmos's location cache indexes by name and
// silently skips any entry whose Name is empty (see
// getEndpointsByLocation in azure-sdk-for-go/sdk/data/azcosmos). "South
// Central US" mirrors the real Cosmos DB Local Emulator's own documented
// default region, purely for verisimilitude.
const accountRegionName = "South Central US"

// handleAccountRoot serves the Cosmos DB database-account root resource
// ("" or "/", per parseResourcePath). This is not an optional nicety: every
// real Cosmos SDK -- azcosmos included -- issues a GET here before it will
// make a single data-plane call, to discover the account's readable/
// writable regional endpoints (see azure-sdk-for-go/sdk/data/azcosmos's
// globalEndpointManager.GetAccountProperties). Omitting this resource
// entirely makes the service unreachable by any unmodified SDK: every
// request fails during client-side endpoint discovery, long before it
// would reach any database/container/document operation this package
// otherwise fully implements.
func (h *Handler) handleAccountRoot(c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodGet, http.MethodHead:
		return h.writeJSON(c, http.StatusOK, h.accountPropertiesBody(c.Request()))
	default:
		return h.writeError(
			c,
			http.StatusMethodNotAllowed,
			"MethodNotAllowed",
			"The resource doesn't support the specified HTTP verb.",
		)
	}
}

// accountPropertiesBody builds the DatabaseAccount JSON body real Cosmos
// SDKs unmarshal from a GET against the account root.
//
// databaseAccountEndpoint is deliberately built from the REQUEST's own Host
// header, not from h.Port/DefaultPort: gopherstack's own configured port
// (8081 by default) is frequently NOT the port a client actually connects
// through -- e.g. under testcontainers, or any other port-mapping/proxy
// setup, the container's 8081 is published to an arbitrary host port. If
// this handler advertised its own configured port instead of echoing back
// exactly how the client reached it, an SDK would dutifully "discover" a
// writable/readable location pointing at a port nothing is listening on
// and redirect every subsequent request there -- a strictly worse failure
// mode than never implementing this resource at all, since it would look
// like a routing bug rather than a missing feature.
func (h *Handler) accountPropertiesBody(r *http.Request) map[string]any {
	endpoint := "http://" + r.Host + "/"

	location := map[string]any{"name": accountRegionName, "databaseAccountEndpoint": endpoint}

	return map[string]any{
		sysPropSelf:                    "",
		"id":                           "gopherstack",
		sysPropRID:                     fakeRID("accounts/gopherstack"),
		"media":                        "//media/",
		"addresses":                    "//addresses/",
		"_dbs":                         "//dbs/",
		"writableLocations":            []map[string]any{location},
		"readableLocations":            []map[string]any{location},
		"enableMultipleWriteLocations": false,
		"userConsistencyPolicy":        map[string]any{"defaultConsistencyLevel": "Session"},
	}
}
