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
	opCancelWorkRequest = "CancelWorkRequest"
	opCreateAPI         = "CreateApi"
	opGetAPI            = "GetApi"
	opListAPIs          = "ListApis"
	opUpdateAPI         = "UpdateApi"
	opDeleteAPI         = "DeleteApi"
	opChangeAPIComp     = "ChangeApiCompartment"
	opGetAPIContent     = "GetApiContent"
	opGetAPIDeploySpec  = "GetApiDeploymentSpecification"
	opGetAPIValidations = "GetApiValidations"
	opCreateSdk         = "CreateSdk"
	opGetSdk            = "GetSdk"
	opListSdks          = "ListSdks"
	opUpdateSdk         = "UpdateSdk"
	opDeleteSdk         = "DeleteSdk"
	opListSdkLangs      = "ListSdkLanguageTypes"
	opCreateUsagePlan   = "CreateUsagePlan"
	opGetUsagePlan      = "GetUsagePlan"
	opListUsagePlans    = "ListUsagePlans"
	opUpdateUsagePlan   = "UpdateUsagePlan"
	opDeleteUsagePlan   = "DeleteUsagePlan"
	opChangeUPComp      = "ChangeUsagePlanCompartment"
	opCreateSubscriber  = "CreateSubscriber"
	opGetSubscriber     = "GetSubscriber"
	opListSubscribers   = "ListSubscribers"
	opUpdateSubscriber  = "UpdateSubscriber"
	opDeleteSubscriber  = "DeleteSubscriber"
	opChangeSubComp     = "ChangeSubscriberCompartment"
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
	apiPath             = base + "/apis/{apiId}"
	sdkPath             = base + "/sdks/{sdkId}"
	usagePlanPath       = base + "/usagePlans/{usagePlanId}"
	subscriberPath      = base + "/subscribers/{subscriberId}"
)

// lockKinds maps each lockable resource to its path template and path parameter.
//
//nolint:gochecknoglobals // immutable table
var lockKinds = []struct{ kind, path, param string }{
	{kindGateway, gatewayPath, "gatewayId"}, {kindDeployment, deploymentPath, "deploymentId"},
	{kindCertificate, certificatePath, "certificateId"}, {kindAPI, apiPath, "apiId"},
	{kindSdk, sdkPath, "sdkId"}, {kindSubscriber, subscriberPath, "subscriberId"},
	{kindUsagePlan, usagePlanPath, "usagePlanId"},
}

//nolint:gochecknoglobals // immutable route table
var router = restroute.New(append(lockRoutes(), []restroute.Route{
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
	{Method: http.MethodDelete, Path: workRequestPath, Op: opCancelWorkRequest},
	{Method: http.MethodPost, Path: base + "/apis", Op: opCreateAPI},
	{Method: http.MethodGet, Path: base + "/apis", Op: opListAPIs},
	{Method: http.MethodGet, Path: apiPath, Op: opGetAPI},
	{Method: http.MethodPut, Path: apiPath, Op: opUpdateAPI},
	{Method: http.MethodDelete, Path: apiPath, Op: opDeleteAPI},
	{Method: http.MethodPost, Path: apiPath + "/actions/changeCompartment", Op: opChangeAPIComp},
	{Method: http.MethodGet, Path: apiPath + "/content", Op: opGetAPIContent},
	{Method: http.MethodGet, Path: apiPath + "/deploymentSpecification", Op: opGetAPIDeploySpec},
	{Method: http.MethodGet, Path: apiPath + "/validations", Op: opGetAPIValidations},
	{Method: http.MethodPost, Path: base + "/sdks", Op: opCreateSdk},
	{Method: http.MethodGet, Path: base + "/sdks", Op: opListSdks},
	{Method: http.MethodGet, Path: sdkPath, Op: opGetSdk},
	{Method: http.MethodPut, Path: sdkPath, Op: opUpdateSdk},
	{Method: http.MethodDelete, Path: sdkPath, Op: opDeleteSdk},
	{Method: http.MethodGet, Path: base + "/sdkLanguageTypes", Op: opListSdkLangs},
	{Method: http.MethodPost, Path: base + "/usagePlans", Op: opCreateUsagePlan},
	{Method: http.MethodGet, Path: base + "/usagePlans", Op: opListUsagePlans},
	{Method: http.MethodGet, Path: usagePlanPath, Op: opGetUsagePlan},
	{Method: http.MethodPut, Path: usagePlanPath, Op: opUpdateUsagePlan},
	{Method: http.MethodDelete, Path: usagePlanPath, Op: opDeleteUsagePlan},
	{Method: http.MethodPost, Path: usagePlanPath + "/actions/changeCompartment", Op: opChangeUPComp},
	{Method: http.MethodPost, Path: base + "/subscribers", Op: opCreateSubscriber},
	{Method: http.MethodGet, Path: base + "/subscribers", Op: opListSubscribers},
	{Method: http.MethodGet, Path: subscriberPath, Op: opGetSubscriber},
	{Method: http.MethodPut, Path: subscriberPath, Op: opUpdateSubscriber},
	{Method: http.MethodDelete, Path: subscriberPath, Op: opDeleteSubscriber},
	{Method: http.MethodPost, Path: subscriberPath + "/actions/changeCompartment", Op: opChangeSubComp},
}...))

func lockRoutes() []restroute.Route {
	out := make([]restroute.Route, 0, lockOpsPerKind*len(lockKinds))

	for _, k := range lockKinds {
		out = append(
			out,
			restroute.Route{Method: http.MethodPost, Path: k.path + "/actions/addLock", Op: "Add" + k.kind + "Lock"},
			restroute.Route{
				Method: http.MethodPost,
				Path:   k.path + "/actions/removeLock",
				Op:     "Remove" + k.kind + "Lock",
			},
		)
	}

	return out
}

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
