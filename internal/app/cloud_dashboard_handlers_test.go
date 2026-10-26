package app

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurefile"
	"github.com/blackbirdworks/gopherstack/services/azuremonitor"
	"github.com/blackbirdworks/gopherstack/services/gcpdns"
	"github.com/blackbirdworks/gopherstack/services/gcpworkflows"
	"github.com/blackbirdworks/gopherstack/services/ocifunctions"
	"github.com/blackbirdworks/gopherstack/services/ociidentity"
	"github.com/blackbirdworks/gopherstack/services/ociobjectstorage"
	"github.com/blackbirdworks/gopherstack/services/ocistreaming"
	"github.com/blackbirdworks/gopherstack/services/ocivault"
)

func TestConsolePageHandlersReachDashboard(t *testing.T) {
	t.Parallel()

	wf := gcpworkflows.NewHandler(gcpworkflows.NewInMemoryBackend())
	t.Cleanup(wf.Backend.Close)

	byName := map[string]service.Registerable{
		"GCPWorkflows":     wf,
		"GCPDNS":           gcpdns.NewHandler(gcpdns.NewInMemoryBackend()),
		"AzureFile":        azurefile.NewHandler(azurefile.NewInMemoryBackend()),
		"OCIObjectStorage": ociobjectstorage.NewHandler(ociobjectstorage.NewInMemoryBackend()),
		"OCIVault":         ocivault.NewHandler(ocivault.NewInMemoryBackend()),
		"OCIStreaming":     ocistreaming.NewHandler(ocistreaming.NewInMemoryBackend()),
		"OCIIdentity":      ociidentity.NewHandler(ociidentity.NewInMemoryBackend()),
		"OCIFunctions":     ocifunctions.NewHandler(ocifunctions.NewInMemoryBackend()),
		"AzureMonitor": azuremonitor.NewHandler(
			azuremonitor.NewBackend(azuremonitor.DefaultSettings()), azuremonitor.DefaultSettings()),
	}

	cli := &CLI{}
	storeCLIExtendedHandlers(cli, byName)

	got := map[string]service.Registerable{
		"GCPWorkflows":     cli.GetGCPWorkflowsHandler(),
		"GCPDNS":           cli.GetGCPDNSHandler(),
		"AzureFile":        cli.GetAzureFileHandler(),
		"OCIObjectStorage": cli.GetOCIObjectStorageHandler(),
		"OCIVault":         cli.GetOCIVaultHandler(),
		"OCIStreaming":     cli.GetOCIStreamingHandler(),
		"OCIIdentity":      cli.GetOCIIdentityHandler(),
		"OCIFunctions":     cli.GetOCIFunctionsHandler(),
		"AzureMonitor":     cli.GetAzureMonitorHandler(),
	}

	for name, want := range byName {
		assert.Same(t, want, got[name], name)
	}
}
