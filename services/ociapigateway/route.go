package ociapigateway

import (
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

// Operation names, matching the OCI SDK operation names.
const (
	opCreateGateway     = "CreateGateway"
	opGetGateway        = "GetGateway"
	opListGateways      = "ListGateways"
	opUpdateGateway     = "UpdateGateway"
	opDeleteGateway     = "DeleteGateway"
	opChangeGatewayComp = "ChangeGatewayCompartment"
	opCreateDeployment  = "CreateDeployment"
	opGetDeployment     = "GetDeployment"
	opListDeployments   = "ListDeployments"
	opUpdateDeployment  = "UpdateDeployment"
	opDeleteDeployment  = "DeleteDeployment"
	opChangeDeployComp  = "ChangeDeploymentCompartment"
	opCreateCert        = "CreateCertificate"
	opGetCert           = "GetCertificate"
	opListCerts         = "ListCertificates"
	opUpdateCert        = "UpdateCertificate"
	opDeleteCert        = "DeleteCertificate"
	opChangeCertComp    = "ChangeCertificateCompartment"
	opGetWorkRequest    = "GetWorkRequest"
	opListWorkRequests  = "ListWorkRequests"
	opListWorkReqErrors = "ListWorkRequestErrors"
	opListWorkReqLogs   = "ListWorkRequestLogs"
	opAdminKeys         = "ManageAPIKeys"
	opDataPlane         = "Invoke"
	opUnknown           = "Unknown"
	adminPrefix         = "_gopherstack"
	dataPathPrefix      = "/_apigw/"
	base                = "/20190501"
	gatewayPath         = base + "/gateways/{gatewayId}"
	deploymentPath      = base + "/deployments/{deploymentId}"
	certificatePath     = base + "/certificates/{certificateId}"
	workRequestPath     = base + "/workRequests/{workRequestId}"
)

//nolint:gochecknoglobals // immutable route table
var router = restroute.New([]restroute.Route{
	{Method: http.MethodPost, Path: base + "/gateways", Op: opCreateGateway},
	{Method: http.MethodGet, Path: base + "/gateways", Op: opListGateways},
	{Method: http.MethodGet, Path: gatewayPath, Op: opGetGateway},
	{Method: http.MethodPut, Path: gatewayPath, Op: opUpdateGateway},
	{Method: http.MethodDelete, Path: gatewayPath, Op: opDeleteGateway},
	{Method: http.MethodPost, Path: gatewayPath + "/actions/changeCompartment", Op: opChangeGatewayComp},
	{Method: http.MethodPost, Path: base + "/deployments", Op: opCreateDeployment},
	{Method: http.MethodGet, Path: base + "/deployments", Op: opListDeployments},
	{Method: http.MethodGet, Path: deploymentPath, Op: opGetDeployment},
	{Method: http.MethodPut, Path: deploymentPath, Op: opUpdateDeployment},
	{Method: http.MethodDelete, Path: deploymentPath, Op: opDeleteDeployment},
	{Method: http.MethodPost, Path: deploymentPath + "/actions/changeCompartment", Op: opChangeDeployComp},
	{Method: http.MethodPost, Path: base + "/certificates", Op: opCreateCert},
	{Method: http.MethodGet, Path: base + "/certificates", Op: opListCerts},
	{Method: http.MethodGet, Path: certificatePath, Op: opGetCert},
	{Method: http.MethodPut, Path: certificatePath, Op: opUpdateCert},
	{Method: http.MethodDelete, Path: certificatePath, Op: opDeleteCert},
	{Method: http.MethodPost, Path: certificatePath + "/actions/changeCompartment", Op: opChangeCertComp},
	{Method: http.MethodGet, Path: base + "/workRequests", Op: opListWorkRequests},
	{Method: http.MethodGet, Path: workRequestPath, Op: opGetWorkRequest},
	{Method: http.MethodGet, Path: workRequestPath + "/errors", Op: opListWorkReqErrors},
	{Method: http.MethodGet, Path: workRequestPath + "/logs", Op: opListWorkReqLogs},
})

type route struct {
	params map[string]string
	op     string
}

// isDataPlane reports whether the request addresses a deployed API rather than the control plane:
// a gateway hostname in Host, or the /_apigw/<gatewayId>/ path scheme.
func isDataPlane(r *http.Request) bool {
	return strings.HasPrefix(r.URL.Path, dataPathPrefix) || hostGateway(r.Host)
}

// hostGateway reports whether host is a gateway hostname (optionally with a port).
func hostGateway(host string) bool {
	host = stripPort(host)

	return strings.HasSuffix(host, ".apigateway."+Region+".oci.customer-oci.com")
}

func stripPort(host string) string {
	if i := strings.LastIndexByte(host, ':'); i >= 0 && !strings.Contains(host[i:], "]") {
		return host[:i]
	}

	return host
}

// parseRoute maps method and path to an operation.
func parseRoute(r *http.Request) route {
	path := r.URL.EscapedPath()
	if strings.HasPrefix(path, "/"+adminPrefix+"/") {
		return route{op: opAdminKeys}
	}

	if isDataPlane(r) {
		return route{op: opDataPlane}
	}

	op, params := router.Match(r.Method, path)
	if op == "" {
		op = opUnknown
	}

	return route{op: op, params: params}
}
