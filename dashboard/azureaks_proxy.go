package dashboard

import (
	"net/http"
	"slices"
)

const (
	azureAKSPrefix   = "/dashboard/api/v1/azure/aks"
	aksPxNamespace   = "microsoft.containerservice"
	aksPxClusters    = "managedclusters"
	aksPxPools       = "agentpools"
	aksPxUpgrades    = "upgradeprofiles"
	aksPxActionOnly  = 1
	aksPxChildItem   = 2
	aksPxUpgradeName = "default"
)

var aksPxPowerActions = []string{aciPxStart, aciPxStop} //nolint:gochecknoglobals // lookup table

// registerAzureAKSRoutes exposes managed clusters and their agent pools by dispatching in-process to the ARM
// handler. Only reads, start/stop, agent pool PUT (scaling) and cluster deletion are reachable; credential,
// run-command and maintenance actions are not, and profile secrets are scrubbed from every response.
func (h *DashboardHandler) registerAzureAKSRoutes() {
	h.registerARMProxy(armProxy{
		name: "Azure Kubernetes Service", prefix: azureAKSPrefix,
		queryKeys: []string{armQueryVersion},
		methods:   []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete},
		allowed:   aksProxyAllowed,
	})
}

func aksProxyAllowed(method string, p []string) bool {
	typ, name, rest, ok := armScope(p, aksPxNamespace)
	if !ok || typ != aksPxClusters {
		return false
	}

	if name == "" {
		return method == http.MethodGet
	}

	switch len(rest) {
	case 0:
		return method == http.MethodGet || method == http.MethodDelete
	case aksPxActionOnly:
		return (method == http.MethodGet && rest[0] == aksPxPools) ||
			(method == http.MethodPost && slices.Contains(aksPxPowerActions, rest[0]))
	case aksPxChildItem:
		switch method {
		case http.MethodGet:
			return rest[0] == aksPxPools || (rest[0] == aksPxUpgrades && rest[1] == aksPxUpgradeName)
		case http.MethodPut:
			return rest[0] == aksPxPools
		}
	}

	return false
}
