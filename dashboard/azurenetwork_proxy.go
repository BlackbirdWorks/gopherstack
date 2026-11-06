package dashboard

import (
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	azureNetworkPrefix = "/dashboard/api/v1/azure/network"
	netPxNamespace     = "microsoft.network"
	netPxAPIVersion    = "2024-05-01"
	netPxVNets         = "virtualnetworks"
	netPxSubnets       = "subnets"
	netPxNSGs          = "networksecuritygroups"
	netPxRules         = "securityrules"
	netPxLBs           = "loadbalancers"
	netPxAGWs          = "applicationgateways"
	netPxPolicies      = "firewallpolicies"
	netPxRCGs          = "rulecollectiongroups"
	netPxLocations     = "locations"
	netPxRGList        = 7
	netPxRGTypeIdx     = 6
	netPxOpSegs        = 8
	netPxOpKindIdx     = 6
	netPxOpLocIdx      = 4
	netPxActionOnly    = 1
	netPxChildItem     = 2
)

var (
	netPxReadTypes = []string{ //nolint:gochecknoglobals // immutable allow-list
		netPxVNets, netPxNSGs, "publicipaddresses", "networkinterfaces", "natgateways", netPxLBs,
		"privateendpoints", "privatednszones", netPxAGWs, "azurefirewalls", netPxPolicies, "ipgroups",
		"applicationsecuritygroups",
	}
	netPxAGWActions = []string{aciPxStart, aciPxStop, "backendhealth"} //nolint:gochecknoglobals // lookup
	netPxOpKinds    = []string{"operations", "operationresults"}       //nolint:gochecknoglobals // lookup
)

// registerAzureNetworkRoutes exposes Microsoft.Network resources by dispatching in-process to the ARM handler.
// Reads cover the whole network surface; writes are limited to virtual networks, subnets, security rules, load
// balancer deletion and application gateway start/stop/backend health. Every write forwards a body rebuilt from
// fixed fields, the api-version is pinned, and async operation headers are relayed as proxy-relative URLs.
func (h *DashboardHandler) registerAzureNetworkRoutes() {
	h.registerARMProxy(armProxy{
		name: "Azure Networking", prefix: azureNetworkPrefix,
		queryKeys: []string{}, version: netPxAPIVersion, relayOps: true,
		methods: []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete},
		allowed: netProxyAllowed,
		body:    netProxyBody,
	})
}

func netProxyAllowed(method string, p []string) bool {
	switch {
	case len(p) == netPxOpSegs && p[2] == armPxProvSeg:
		return netProxyOperation(method, p)
	case len(p) == netPxRGList:
		return netProxyRGList(method, p)
	}

	typ, name, rest, ok := armScope(p, netPxNamespace)
	if !ok || !slices.Contains(netPxReadTypes, typ) {
		return false
	}

	if name == "" {
		return method == http.MethodGet
	}

	switch len(rest) {
	case 0:
		return netProxyItemOp(method, typ)
	case netPxActionOnly:
		return (method == http.MethodGet && typ == netPxPolicies && rest[0] == netPxRCGs) ||
			(method == http.MethodPost && typ == netPxAGWs && slices.Contains(netPxAGWActions, rest[0]))
	case netPxChildItem:
		return netProxyChildOp(method, typ, rest[0])
	}

	return false
}

func netProxyOperation(method string, p []string) bool {
	return method == http.MethodGet && p[0] == armPxSubsSeg && p[2] == armPxProvSeg && p[3] == netPxNamespace &&
		p[netPxOpLocIdx] == netPxLocations && slices.Contains(netPxOpKinds, p[netPxOpKindIdx])
}

func netProxyRGList(method string, p []string) bool {
	return method == http.MethodGet && p[0] == armPxSubsSeg && p[2] == armPxRGsSeg && p[4] == armPxProvSeg &&
		p[5] == netPxNamespace && slices.Contains(netPxReadTypes, p[netPxRGTypeIdx])
}

func netProxyItemOp(method, typ string) bool {
	switch method {
	case http.MethodGet:
		return true
	case http.MethodPut:
		return typ == netPxVNets
	case http.MethodDelete:
		return typ == netPxVNets || typ == netPxLBs
	}

	return false
}

func netProxyChildOp(method, typ, kind string) bool {
	switch {
	case typ == netPxVNets && kind == netPxSubnets, typ == netPxNSGs && kind == netPxRules:
		return method == http.MethodGet || method == http.MethodPut || method == http.MethodDelete
	case typ == netPxPolicies && kind == netPxRCGs:
		return method == http.MethodGet
	}

	return false
}

// netRelayOps exposes the async operation URLs of a response as paths below the proxy prefix.
func netRelayOps(c *echo.Context, hdr http.Header, prefix string) {
	for _, k := range []string{"Azure-Asyncoperation", "Location"} {
		raw := hdr.Get(k)
		if raw == "" {
			continue
		}

		u, err := url.Parse(raw)
		if err != nil || !strings.HasPrefix(strings.ToLower(u.Path), "/subscriptions/") {
			continue
		}

		u.Scheme, u.Host = "", ""
		u.Path = prefix + u.Path
		c.Response().Header().Set(k, u.String())
	}
}
