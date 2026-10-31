package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpcomputebackend "github.com/blackbirdworks/gopherstack/services/gcpcompute"
	gcpcontainerbackend "github.com/blackbirdworks/gopherstack/services/gcpcontainer"
)

// wireGCPContainer validates cluster networks, subnetworks, secondary ranges and machine types against the
// Compute Engine emulator.
func wireGCPContainer(byName map[string]service.Registerable) {
	gke, ok := byName["GCPContainer"].(*gcpcontainerbackend.Handler)
	if !ok {
		return
	}

	if ce, isCE := byName["GCPCompute"].(*gcpcomputebackend.Handler); isCE {
		gke.Backend.SetCompute(ce.Backend)
	}
}
