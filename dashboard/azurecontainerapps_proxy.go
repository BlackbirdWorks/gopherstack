package dashboard

import (
	"net/http"
	"slices"
)

const (
	azureContainerAppsPrefix = "/dashboard/api/v1/azure/containerapps"
	acaPxNamespace           = "microsoft.app"
	acaPxApps                = "containerapps"
	acaPxEnvs                = "managedenvironments"
	acaPxJobs                = "jobs"
	acaPxRevisions           = "revisions"
	acaPxReplicas            = "replicas"
	acaPxExecutions          = "executions"
	acaPxStart               = "start"
	acaPxStop                = "stop"
	acaPxRevItem             = 2
	acaPxRevChild            = 3
	acaPxActionOnly          = 1
)

var (
	acaPxAppActions = []string{acaPxStart, acaPxStop}                  //nolint:gochecknoglobals // lookup table
	acaPxRevActions = []string{"activate", "deactivate", aciPxRestart} //nolint:gochecknoglobals // lookup table
)

// registerAzureContainerAppsRoutes exposes Microsoft.App environments, container apps with their revisions and
// replicas, and jobs by dispatching in-process to the ARM handler. Only reads, app start/stop, revision
// activate/deactivate/restart and job start are reachable; secrets are never listed.
func (h *DashboardHandler) registerAzureContainerAppsRoutes() {
	h.registerARMProxy(armProxy{
		name: "Azure Container Apps", prefix: azureContainerAppsPrefix,
		queryKeys: []string{armQueryVersion},
		methods:   []string{http.MethodGet, http.MethodPost},
		allowed:   acaProxyAllowed,
	})
}

func acaProxyAllowed(method string, p []string) bool {
	typ, name, rest, ok := armScope(p, acaPxNamespace)
	if !ok || !slices.Contains([]string{acaPxApps, acaPxEnvs, acaPxJobs}, typ) {
		return false
	}

	if name == "" || len(rest) == 0 {
		return method == http.MethodGet
	}

	switch typ {
	case acaPxApps:
		return acaProxyAppOp(method, rest)
	case acaPxJobs:
		return (method == http.MethodGet && len(rest) == acaPxActionOnly && rest[0] == acaPxExecutions) ||
			(method == http.MethodPost && len(rest) == acaPxActionOnly && rest[0] == acaPxStart)
	}

	return false
}

func acaProxyAppOp(method string, rest []string) bool {
	switch method {
	case http.MethodGet:
		switch len(rest) {
		case acaPxActionOnly, acaPxRevItem:
			return rest[0] == acaPxRevisions
		case acaPxRevChild:
			return rest[0] == acaPxRevisions && rest[2] == acaPxReplicas
		}
	case http.MethodPost:
		switch len(rest) {
		case acaPxActionOnly:
			return slices.Contains(acaPxAppActions, rest[0])
		case acaPxRevChild:
			return rest[0] == acaPxRevisions && slices.Contains(acaPxRevActions, rest[2])
		}
	}

	return false
}
