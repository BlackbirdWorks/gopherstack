package apigateway

import (
	"net"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/executeapi"
)

const forbiddenBody = `{"message":"Forbidden"}`

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

// serveCustomDomain serves the request when its Host is a stored custom domain and the path is
// not a control-plane path.
func (h *Handler) serveCustomDomain(c *echo.Context) (bool, error) {
	if isAPIGWTopLevelRESTPath(c.Request().URL.Path) {
		return false, nil
	}

	owner, host := h.customDomainOwner(c.Request())
	if owner == nil {
		return false, nil
	}

	return true, h.handleCustomDomain(c, owner, host)
}

// handleCustomDomain routes a request addressed to a custom domain through its base path
// mappings. A path no mapping covers is answered 403 Forbidden, as API Gateway does.
func (h *Handler) handleCustomDomain(c *echo.Context, owner *Handler, host string) error {
	mappings, err := owner.Backend.GetBasePathMappings(host)
	if err != nil {
		return writeForbidden(c)
	}

	keys := make([]string, 0, len(mappings))
	for i := range mappings {
		keys = append(keys, mappings[i].BasePath)
	}

	key, rest, ok := executeapi.MatchBasePath(keys, c.Request().URL.Path)
	if !ok {
		return writeForbidden(c)
	}

	var m *BasePathMapping

	for i := range mappings {
		if mappings[i].BasePath == key {
			m = &mappings[i]

			break
		}
	}

	if m == nil || m.RestAPIID == "" {
		return writeForbidden(c)
	}

	stagePath := rest
	if m.Stage != "" {
		if rest == "/" {
			stagePath = "/" + m.Stage
		} else {
			stagePath = "/" + m.Stage + rest
		}
	} else if rest == "/" {
		return writeForbidden(c)
	}

	return h.handleExecuteAPIEcho(c, m.RestAPIID, stagePath)
}

func writeForbidden(c *echo.Context) error {
	c.Response().Header().Set(headerContentType, "application/json")
	c.Response().Header().Set("X-Amzn-Errortype", "ForbiddenException")

	return c.String(http.StatusForbidden, forbiddenBody)
}
