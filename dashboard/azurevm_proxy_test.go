package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
)

const (
	vmComputeAPI = "?api-version=2024-07-01"
	vmNetworkAPI = "?api-version=2024-05-01"
	vmNet        = armBase + "Microsoft.Network"
	vmVNet       = vmNet + "/virtualNetworks/vnet"
	vmSubnet     = vmVNet + "/subnets/default"
	vmPIP        = vmNet + "/publicIPAddresses/pip"
	vmNIC        = vmNet + "/networkInterfaces/web-nic"
	vmVM         = armBase + "Microsoft.Compute/virtualMachines/web"
	vmProxy      = "/dashboard/api/v1/azure/vm"
	vmPassword   = "P@ssw0rd-Correct-1"
)

func newVMDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	rule := `{"name":"ssh","properties":{"priority":100,"direction":"Inbound","access":"Allow","protocol":"Tcp",` +
		`"sourcePortRange":"*","destinationPortRange":"22","sourceAddressPrefix":"*","destinationAddressPrefix":"*"}}`

	return newARMDashboard(t, []struct{ path, body string }{
		{"/subscriptions/" + armSub + "/resourceGroups/demo?api-version=2021-04-01", `{"location":"eastus"}`},
		{vmNet + "/networkSecurityGroups/nsg" + vmNetworkAPI,
			`{"location":"eastus","properties":{"securityRules":[` + rule + `]}}`},
		{
			vmVNet + vmNetworkAPI,
			`{"location":"eastus","properties":{"addressSpace":{"addressPrefixes":["10.0.0.0/16"]},` +
				`"subnets":[{"name":"default","properties":{"addressPrefix":"10.0.0.0/24"}}]}}`,
		},
		{vmPIP + vmNetworkAPI, `{"location":"eastus","sku":{"name":"Standard"},` +
			`"properties":{"publicIPAllocationMethod":"Static"}}`},
		{vmNIC + vmNetworkAPI, `{"location":"eastus","properties":{"ipConfigurations":[{"name":"ipc","properties":{` +
			`"subnet":{"id":"` + vmSubnet + `"},"privateIPAllocationMethod":"Dynamic",` +
			`"publicIPAddress":{"id":"` + vmPIP + `"}}}]}}`},
		{vmVM + vmComputeAPI, `{"location":"eastus","properties":{"hardwareProfile":{"vmSize":"Standard_B2s"},` +
			`"storageProfile":{"imageReference":{"publisher":"Canonical","offer":"0001-com-ubuntu-server-jammy",` +
			`"sku":"22_04-lts","version":"latest"},"osDisk":{"createOption":"FromImage","name":"web-os"}},` +
			`"osProfile":{"computerName":"web","adminUsername":"azureuser","adminPassword":"` + vmPassword + `",` +
			`"customData":"c2VjcmV0LWluaXQ=","linuxConfiguration":{"disablePasswordAuthentication":false}},` +
			`"networkProfile":{"networkInterfaces":[{"id":"` + vmNIC + `"}]}}}`},
	})
}

func TestAzureVMProxy(t *testing.T) {
	t.Parallel()

	px := vmProxy + vmVM
	subs := vmProxy + "/subscriptions/" + armSub + "/providers/Microsoft."

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"read", []gcpStep{
			{
				http.MethodGet,
				subs + "Compute/virtualMachines" + vmComputeAPI + "&statusOnly=true",
				"",
				"PowerState/running",
				200,
			},
			{http.MethodGet, px + vmComputeAPI, "", `"vmSize":"Standard_B2s"`, 200},
			{http.MethodGet, px + "/instanceView" + vmComputeAPI, "", "PowerState/running", 200},
			{http.MethodGet, subs + "Compute/disks" + vmComputeAPI, "", `"name":"web-os"`, 200},
			{http.MethodGet, subs + "Network/virtualNetworks" + vmNetworkAPI, "", `"10.0.0.0/16"`, 200},
			{http.MethodGet, subs + "Network/networkSecurityGroups" + vmNetworkAPI, "", `"ssh"`, 200},
			{http.MethodGet, subs + "Network/networkInterfaces" + vmNetworkAPI, "", `"web-nic"`, 200},
			{http.MethodGet, subs + "Network/publicIPAddresses" + vmNetworkAPI, "", `"ipAddress"`, 200},
			{http.MethodGet, vmProxy + vmVNet + vmNetworkAPI, "", `"name":"default"`, 200},
		}},
		{"power", []gcpStep{
			{http.MethodPost, px + "/powerOff" + vmComputeAPI, "", "", http.StatusAccepted},
			{http.MethodGet, px + "/instanceView" + vmComputeAPI, "", "PowerState/stopped", 200},
			{http.MethodPost, px + "/start" + vmComputeAPI, "", "", http.StatusAccepted},
			{http.MethodGet, px + "/instanceView" + vmComputeAPI, "", "PowerState/running", 200},
			{http.MethodPost, px + "/restart" + vmComputeAPI, "", "", http.StatusAccepted},
			{http.MethodPost, px + "/deallocate" + vmComputeAPI, "", "", http.StatusAccepted},
			{http.MethodGet, px + "/instanceView" + vmComputeAPI, "", "PowerState/deallocated", 200},
		}},
		{"rejects", []gcpStep{
			{http.MethodDelete, px + vmComputeAPI, "", "", http.StatusMethodNotAllowed},
			{http.MethodPut, px + vmComputeAPI, `{}`, "", http.StatusMethodNotAllowed},
			{http.MethodPost, px + "/runCommand" + vmComputeAPI, `{}`, "unsupported", 400},
			{http.MethodPost, px + "/generalize" + vmComputeAPI, "", "unsupported", 400},
			{http.MethodPost, px + "/capture" + vmComputeAPI, "", "unsupported", 400},
			{
				http.MethodPost,
				subs + "Network/virtualNetworks/vnet/subnets/default" + vmNetworkAPI,
				"",
				"unsupported",
				400,
			},
			{http.MethodPost, vmProxy + vmNIC + vmNetworkAPI, "", "unsupported", 400},
			{http.MethodGet, subs + "Storage/storageAccounts" + vmNetworkAPI, "", "unsupported", 400},
			{http.MethodGet, subs + "Compute/snapshots" + vmComputeAPI, "", "unsupported", 400},
			{http.MethodGet, px + "/../x" + vmComputeAPI, "", "unsupported", 400},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runGCPSteps(t, newVMDashboard(t), tt.steps)
		})
	}

	t.Run("secrets stay server side", func(t *testing.T) {
		t.Parallel()

		h := newVMDashboard(t)
		for _, p := range []string{
			px + vmComputeAPI, subs + "Compute/virtualMachines" + vmComputeAPI + "&statusOnly=true",
			px + "/instanceView" + vmComputeAPI,
		} {
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(echo.NewContext(httptest.NewRequest(http.MethodGet, p, nil), rec)))
			assert.Equal(t, http.StatusOK, rec.Code, p)
			assert.NotContains(t, rec.Body.String(), vmPassword, p)
			assert.NotContains(t, rec.Body.String(), "c2VjcmV0LWluaXQ=", p)
			assert.NotContains(t, rec.Body.String(), "adminPassword", p)
		}
	})

	t.Run("unserved", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, subs+"Compute/virtualMachines"+vmComputeAPI, nil)
		require.NoError(t, dashboard.NewHandler(dashboard.Config{}).Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}
