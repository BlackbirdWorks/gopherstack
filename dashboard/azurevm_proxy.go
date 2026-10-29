package dashboard

import (
	"net/http"
	"slices"
)

const (
	azureVMPrefix = "/dashboard/api/v1/azure/vm"
	vmPxCompute   = "microsoft.compute"
	vmPxNetwork   = "microsoft.network"
	vmPxVMs       = "virtualmachines"
	vmPxInstance  = "instanceview"
	vmPxChildOnly = 1
)

var (
	vmPxPowerActions = []string{aciPxStart, "poweroff", "deallocate", aciPxRestart} //nolint:gochecknoglobals // lookup
	vmPxComputeTypes = []string{vmPxVMs, "disks"}                                   //nolint:gochecknoglobals // lookup
	vmPxNetworkTypes = []string{                                                    //nolint:gochecknoglobals // lookup
		"virtualnetworks", "networksecuritygroups", "networkinterfaces", "publicipaddresses",
	}
)

// registerAzureVMRoutes exposes virtual machines, disks and the network resources they attach to by dispatching
// in-process to the ARM handler. Only reads, a VM's instance view and the power actions are reachable.
func (h *DashboardHandler) registerAzureVMRoutes() {
	h.registerARMProxy(armProxy{
		name: "Azure Virtual Machines", prefix: azureVMPrefix,
		queryKeys: []string{armQueryVersion, "statusOnly"},
		methods:   []string{http.MethodGet, http.MethodPost},
		allowed:   vmProxyAllowed,
	})
}

func vmProxyAllowed(method string, p []string) bool {
	if typ, name, rest, ok := armScope(p, vmPxCompute); ok && slices.Contains(vmPxComputeTypes, typ) {
		return vmProxyComputeOp(method, typ, name, rest)
	}

	typ, name, rest, ok := armScope(p, vmPxNetwork)

	return ok && slices.Contains(vmPxNetworkTypes, typ) && method == http.MethodGet && (name == "" || len(rest) == 0)
}

func vmProxyComputeOp(method, typ, name string, rest []string) bool {
	if name == "" || len(rest) == 0 {
		return method == http.MethodGet
	}

	if typ != vmPxVMs || len(rest) != vmPxChildOnly {
		return false
	}

	if method == http.MethodGet {
		return rest[0] == vmPxInstance
	}

	return method == http.MethodPost && slices.Contains(vmPxPowerActions, rest[0])
}
