package ociloadbalancer

import (
	"net/http"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opUnknown   = "Unknown"
	opAdminKeys = "ManageAPIKeys"
	opAdminEnds = "ListDataPlaneEndpoints"
	adminPrefix = "_gopherstack"
	base        = "/20170115"
	lbPath      = base + "/loadBalancers/{loadBalancerId}"
	bsPath      = lbPath + "/backendSets/{backendSetName}"
	bePath      = bsPath + "/backends/{backendName}"
)

const (
	opCreateLB          = "CreateLoadBalancer"
	opGetLB             = "GetLoadBalancer"
	opListLB            = "ListLoadBalancers"
	opUpdateLB          = "UpdateLoadBalancer"
	opDeleteLB          = "DeleteLoadBalancer"
	opChangeComp        = "ChangeLoadBalancerCompartment"
	opUpdateShape       = "UpdateLoadBalancerShape"
	opUpdateNSG         = "UpdateNetworkSecurityGroups"
	opListShapes        = "ListShapes"
	opListPolicies      = "ListPolicies"
	opListProtocols     = "ListProtocols"
	opGetWR             = "GetWorkRequest"
	opListWR            = "ListWorkRequests"
	opListHealths       = "ListLoadBalancerHealths"
	opGetLBHealth       = "GetLoadBalancerHealth"
	opCreateBS          = "CreateBackendSet"
	opGetBS             = "GetBackendSet"
	opListBS            = "ListBackendSets"
	opUpdateBS          = "UpdateBackendSet"
	opDeleteBS          = "DeleteBackendSet"
	opGetBSHealth       = "GetBackendSetHealth"
	opGetHC             = "GetHealthChecker"
	opUpdateHC          = "UpdateHealthChecker"
	opCreateBE          = "CreateBackend"
	opGetBE             = "GetBackend"
	opListBE            = "ListBackends"
	opUpdateBE          = "UpdateBackend"
	opDeleteBE          = "DeleteBackend"
	opGetBEHealth       = "GetBackendHealth"
	opCreateListener    = "CreateListener"
	opUpdateListener    = "UpdateListener"
	opDeleteListener    = "DeleteListener"
	opListListenerRules = "ListListenerRules"
	opCreateHostname    = "CreateHostname"
	opGetHostname       = "GetHostname"
	opListHostnames     = "ListHostnames"
	opUpdateHostname    = "UpdateHostname"
	opDeleteHostname    = "DeleteHostname"
	opCreatePRS         = "CreatePathRouteSet"
	opGetPRS            = "GetPathRouteSet"
	opListPRS           = "ListPathRouteSets"
	opUpdatePRS         = "UpdatePathRouteSet"
	opDeletePRS         = "DeletePathRouteSet"
	opCreateRP          = "CreateRoutingPolicy"
	opGetRP             = "GetRoutingPolicy"
	opListRP            = "ListRoutingPolicies"
	opUpdateRP          = "UpdateRoutingPolicy"
	opDeleteRP          = "DeleteRoutingPolicy"
	opCreateRS          = "CreateRuleSet"
	opGetRS             = "GetRuleSet"
	opListRS            = "ListRuleSets"
	opUpdateRS          = "UpdateRuleSet"
	opDeleteRS          = "DeleteRuleSet"
	opCreateSuite       = "CreateSSLCipherSuite"
	opGetSuite          = "GetSSLCipherSuite"
	opListSuites        = "ListSSLCipherSuites"
	opUpdateSuite       = "UpdateSSLCipherSuite"
	opDeleteSuite       = "DeleteSSLCipherSuite"
	opCreateCert        = "CreateCertificate"
	opListCerts         = "ListCertificates"
	opDeleteCert        = "DeleteCertificate"
)

func sub(coll, param string) string { return lbPath + "/" + coll + "/{" + param + "}" }

//nolint:gochecknoglobals // immutable route table
var router = restroute.New(buildRoutes())

func buildRoutes() []restroute.Route {
	return slices.Concat(lbRoutes(), subRoutes())
}

func lbRoutes() []restroute.Route {
	g, p, u, d := http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete

	return []restroute.Route{
		{Method: p, Path: base + "/loadBalancers", Op: opCreateLB},
		{Method: g, Path: base + "/loadBalancers", Op: opListLB},
		{Method: g, Path: lbPath, Op: opGetLB},
		{Method: u, Path: lbPath, Op: opUpdateLB},
		{Method: d, Path: lbPath, Op: opDeleteLB},
		{Method: p, Path: lbPath + "/changeCompartment", Op: opChangeComp},
		{Method: u, Path: lbPath + "/updateShape", Op: opUpdateShape},
		{Method: u, Path: lbPath + "/networkSecurityGroups", Op: opUpdateNSG},
		{Method: g, Path: base + "/loadBalancerShapes", Op: opListShapes},
		{Method: g, Path: base + "/loadBalancerPolicies", Op: opListPolicies},
		{Method: g, Path: base + "/loadBalancerProtocols", Op: opListProtocols},
		{Method: g, Path: base + "/loadBalancerHealths", Op: opListHealths},
		{Method: g, Path: lbPath + "/health", Op: opGetLBHealth},
		{Method: g, Path: base + "/loadBalancerWorkRequests/{workRequestId}", Op: opGetWR},
		{Method: g, Path: lbPath + "/workRequests", Op: opListWR},
		{
			Method: p,
			Path:   lbPath + "/backendSets",
			Op:     opCreateBS,
		},
		{Method: g, Path: lbPath + "/backendSets", Op: opListBS},
		{Method: g, Path: bsPath, Op: opGetBS},
		{Method: u, Path: bsPath, Op: opUpdateBS},
		{Method: d, Path: bsPath, Op: opDeleteBS},
		{Method: g, Path: bsPath + "/health", Op: opGetBSHealth},
		{
			Method: g,
			Path:   bsPath + "/healthChecker",
			Op:     opGetHC,
		},
		{Method: u, Path: bsPath + "/healthChecker", Op: opUpdateHC},
		{Method: p, Path: bsPath + "/backends", Op: opCreateBE},
		{Method: g, Path: bsPath + "/backends", Op: opListBE},
		{Method: g, Path: bePath, Op: opGetBE},
		{Method: u, Path: bePath, Op: opUpdateBE},
		{Method: d, Path: bePath, Op: opDeleteBE},
		{Method: g, Path: bePath + "/health", Op: opGetBEHealth},
	}
}

func subRoutes() []restroute.Route {
	g, p, u, d := http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete

	return []restroute.Route{
		{Method: p, Path: lbPath + "/listeners", Op: opCreateListener},
		{Method: u, Path: sub("listeners", "listenerName"), Op: opUpdateListener},
		{Method: d, Path: sub("listeners", "listenerName"), Op: opDeleteListener},
		{Method: g, Path: sub("listeners", "listenerName") + "/rules", Op: opListListenerRules},
		{
			Method: p,
			Path:   lbPath + "/hostnames",
			Op:     opCreateHostname,
		},
		{Method: g, Path: lbPath + "/hostnames", Op: opListHostnames},
		{
			Method: g,
			Path:   sub("hostnames", "name"),
			Op:     opGetHostname,
		},
		{Method: u, Path: sub("hostnames", "name"), Op: opUpdateHostname},
		{Method: d, Path: sub("hostnames", "name"), Op: opDeleteHostname},
		{
			Method: p,
			Path:   lbPath + "/pathRouteSets",
			Op:     opCreatePRS,
		},
		{Method: g, Path: lbPath + "/pathRouteSets", Op: opListPRS},
		{Method: g, Path: sub("pathRouteSets", "pathRouteSetName"), Op: opGetPRS},
		{Method: u, Path: sub("pathRouteSets", "pathRouteSetName"), Op: opUpdatePRS},
		{Method: d, Path: sub("pathRouteSets", "pathRouteSetName"), Op: opDeletePRS},
		{
			Method: p,
			Path:   lbPath + "/routingPolicies",
			Op:     opCreateRP,
		},
		{Method: g, Path: lbPath + "/routingPolicies", Op: opListRP},
		{Method: g, Path: sub("routingPolicies", "routingPolicyName"), Op: opGetRP},
		{Method: u, Path: sub("routingPolicies", "routingPolicyName"), Op: opUpdateRP},
		{Method: d, Path: sub("routingPolicies", "routingPolicyName"), Op: opDeleteRP},
		{Method: p, Path: lbPath + "/ruleSets", Op: opCreateRS},
		{Method: g, Path: lbPath + "/ruleSets", Op: opListRS},
		{
			Method: g,
			Path:   sub("ruleSets", "ruleSetName"),
			Op:     opGetRS,
		},
		{Method: u, Path: sub("ruleSets", "ruleSetName"), Op: opUpdateRS},
		{Method: d, Path: sub("ruleSets", "ruleSetName"), Op: opDeleteRS},
		{
			Method: p,
			Path:   lbPath + "/sslCipherSuites",
			Op:     opCreateSuite,
		},
		{Method: g, Path: lbPath + "/sslCipherSuites", Op: opListSuites},
		{
			Method: g,
			Path:   sub("sslCipherSuites", "name"),
			Op:     opGetSuite,
		},
		{Method: u, Path: sub("sslCipherSuites", "name"), Op: opUpdateSuite},
		{Method: d, Path: sub("sslCipherSuites", "name"), Op: opDeleteSuite},
		{
			Method: p,
			Path:   lbPath + "/certificates",
			Op:     opCreateCert,
		},
		{Method: g, Path: lbPath + "/certificates", Op: opListCerts},
		{Method: d, Path: sub("certificates", "certificateName"), Op: opDeleteCert},
	}
}

type route struct {
	params map[string]string
	op     string
}

// parseRoute maps method and path to an operation.
func parseRoute(r *http.Request) route {
	path := r.URL.EscapedPath()
	if strings.HasPrefix(path, "/"+adminPrefix+"/") {
		if strings.HasSuffix(path, "/endpoints") {
			return route{op: opAdminEnds}
		}

		return route{op: opAdminKeys}
	}

	op, params := router.Match(r.Method, path)
	if op == "" {
		op = opUnknown
	}

	return route{op: op, params: params}
}
