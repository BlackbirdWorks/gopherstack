package apigatewayv2

import (
	"net"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/executeapi"
)

// customDomainOwner returns the handler (h or a peer region) holding the custom domain named by
// the request Host, or nil when the host is not a stored domain name.
func (h *Handler) customDomainOwner(r *http.Request) (*Handler, string) {
	host := executeapi.HostName(r)
	if !strings.Contains(host, ".") || net.ParseIP(host) != nil {
		return nil, ""
	}

	if _, err := h.Backend.GetDomainName(host); err == nil {
		return h, host
	}

	for _, p := range h.peers.All() {
		if _, err := p.Backend.GetDomainName(host); err == nil {
			return p, host
		}
	}

	return nil, ""
}

// handleCustomDomain routes a request addressed to a custom domain through its API mappings; the
// longest matching ApiMappingKey wins and the mapping's stage replaces any stage in the path.
func (h *Handler) handleCustomDomain(c *echo.Context, owner *Handler, host string) error {
	mappings, err := owner.Backend.GetAPIMappings(host)
	if err != nil {
		return writeForbidden(c)
	}

	keys := make([]string, 0, len(mappings))
	for i := range mappings {
		keys = append(keys, mappings[i].APIMappingKey)
	}

	key, rest, ok := executeapi.MatchBasePath(keys, c.Request().URL.Path)
	if !ok {
		return writeForbidden(c)
	}

	for i := range mappings {
		if mappings[i].APIMappingKey == key {
			return h.handleProxy(c, mappings[i].APIID, mappings[i].Stage, rest)
		}
	}

	return writeForbidden(c)
}

func writeForbidden(c *echo.Context) error {
	c.Response().Header().Set("X-Amzn-Errortype", "ForbiddenException")

	return c.JSON(http.StatusForbidden, map[string]string{jsonKeyMessage: "Forbidden"})
}
