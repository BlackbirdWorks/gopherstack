package ocicore

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opListServices   = "ListServices"
	opGetService     = "GetService"
	opAttachService  = "AttachServiceId"
	opDetachService  = "DetachServiceId"
	opConnectLpg     = "ConnectLocalPeeringGateways"
	opAddNsgRules    = "AddNetworkSecurityGroupSecurityRules"
	opUpdateNsgRules = "UpdateNetworkSecurityGroupSecurityRules"
	opRemoveNsgRules = "RemoveNetworkSecurityGroupSecurityRules"
	opListNsgRules   = "ListNetworkSecurityGroupSecurityRules"
	opListNsgVnics   = "ListNetworkSecurityGroupVnics"
	opPublicIPByAddr = "GetPublicIpByIpAddress"
	opAdminKeys      = "ManageAPIKeys"
	opUnknown        = "Unknown"
)

const (
	routesPerKind = 6
	adminPrefix   = "_gopherstack"
	apiBase       = "/20160918"
)

var resourceParams = []string{ //nolint:gochecknoglobals // immutable
	"vcnId", "subnetId", "igId", "natGatewayId", "serviceGatewayId", "rtId", "securityListId",
	"networkSecurityGroupId", "dhcpId", "localPeeringGatewayId", "publicIpId", "serviceId",
}

func rt(method, path, op string) restroute.Route {
	return restroute.Route{Method: method, Path: apiBase + path, Op: op}
}

func routeTable() []restroute.Route {
	nsg := "/networkSecurityGroups/{networkSecurityGroupId}"
	fixed := []restroute.Route{
		rt(http.MethodGet, "/services", opListServices),
		rt(http.MethodGet, "/services/{serviceId}", opGetService),
		rt(http.MethodPost, "/serviceGateways/{serviceGatewayId}/actions/attachService", opAttachService),
		rt(http.MethodPost, "/serviceGateways/{serviceGatewayId}/actions/detachService", opDetachService),
		rt(http.MethodPost, "/localPeeringGateways/{localPeeringGatewayId}/actions/connect", opConnectLpg),
		rt(http.MethodPost, nsg+"/actions/addSecurityRules", opAddNsgRules),
		rt(http.MethodPost, nsg+"/actions/updateSecurityRules", opUpdateNsgRules),
		rt(http.MethodPost, nsg+"/actions/removeSecurityRules", opRemoveNsgRules),
		rt(http.MethodGet, nsg+"/securityRules", opListNsgRules),
		rt(http.MethodGet, nsg+"/vnics", opListNsgVnics),
		rt(http.MethodPost, "/publicIps/actions/getByIpAddress", opPublicIPByAddr),
	}
	routes := append(make([]restroute.Route, 0, len(fixed)+routesPerKind*len(cruds())), fixed...)

	for _, c := range cruds() {
		item := c.path + "/{" + c.id + "}"
		routes = append(routes,
			rt(http.MethodPost, c.path, "Create"+c.noun), rt(http.MethodGet, c.path, c.list),
			rt(http.MethodGet, item, "Get"+c.noun), rt(http.MethodPut, item, "Update"+c.noun),
			rt(http.MethodDelete, item, "Delete"+c.noun),
			rt(http.MethodPost, item+"/actions/changeCompartment", "Change"+c.noun+"Compartment"),
		)
	}

	return routes
}

//nolint:gochecknoglobals // immutable route table
var router = restroute.New(routeTable())

type route struct {
	params map[string]string
	op     string
}

// parseRoute maps method and path to an operation.
func parseRoute(r *http.Request) route {
	path := r.URL.EscapedPath()
	if strings.HasPrefix(path, "/"+adminPrefix+"/") {
		return route{op: opAdminKeys}
	}

	op, params := router.Match(r.Method, path)
	if op == "" {
		op = opUnknown
	}

	return route{op: op, params: params}
}
