package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
)

const (
	netAPI    = "?api-version=2024-05-01"
	netRoot   = "/dashboard/api/v1/azure/network"
	netNS     = armBase + "Microsoft.Network"
	netVNetID = netNS + "/virtualNetworks/hub"
	netNSGID  = netNS + "/networkSecurityGroups/web"
	netPolID  = netNS + "/firewallPolicies/pol"
	netRGList = "/subscriptions/" + armSub + "/resourceGroups/demo/providers/Microsoft.Network/"
	netOps    = netRoot + "/subscriptions/" + armSub + "/providers/Microsoft.Network/locations/eastus/"
	netAGWs   = netRoot + netNS + "/applicationGateways/g"

	netBadVNetPrefix = `{"location":"eastus","properties":{"addressSpace":{"addressPrefixes":["nope"]}}}`
	netBadVNetRegion = `{"location":"East US","properties":{"addressSpace":{"addressPrefixes":["10.0.0.0/8"]}}}`
	netBadNAT        = `{"properties":{"addressPrefix":"10.0.5.0/24","natGateway":{"id":"/subscriptions/x/evil"}}}`
	netBadDelegation = `{"properties":{"addressPrefix":"10.0.5.0/24",` +
		`"delegations":[{"name":"d","properties":{"serviceName":"nope"}}]}}`
)

func newNetworkDashboard(t *testing.T) *dashboard.DashboardHandler {
	t.Helper()

	rule := `{"name":"ssh","properties":{"priority":100,"direction":"Inbound","access":"Allow","protocol":"Tcp",` +
		`"sourcePortRange":"*","destinationPortRange":"22","sourceAddressPrefix":"*","destinationAddressPrefix":"*"}}`
	rcg := `{"properties":{"priority":200,"ruleCollections":[{"ruleCollectionType":"FirewallPolicyFilterRuleCollection",` +
		`"name":"allow","priority":100,"action":{"type":"Allow"},"rules":[{"ruleType":"NetworkRule","name":"r1",` +
		`"ipProtocols":["TCP"],"sourceAddresses":["10.0.0.0/24"],"destinationAddresses":["*"],` +
		`"destinationPorts":["443"]}]}]}}`

	return newARMDashboard(t, []struct{ path, body string }{
		{"/subscriptions/" + armSub + "/resourceGroups/demo?api-version=2021-04-01", `{"location":"eastus"}`},
		{netNSGID + netAPI, `{"location":"eastus","properties":{"securityRules":[` + rule + `]}}`},
		{netVNetID + netAPI, `{"location":"eastus","properties":{"addressSpace":{"addressPrefixes":["10.0.0.0/16"]},` +
			`"subnets":[{"name":"default","properties":{"addressPrefix":"10.0.0.0/24"}}]}}`},
		{netPolID + netAPI, `{"location":"eastus","properties":{"sku":{"tier":"Standard"}}}`},
		{netPolID + "/ruleCollectionGroups/grp" + netAPI, rcg},
	})
}

func TestAzureNetworkProxy(t *testing.T) {
	t.Parallel()

	px := netRoot + netVNetID
	nsg := netRoot + netNSGID
	rules := nsg + "/securityRules/"
	rule := func(extra string) string {
		return `{"properties":{"priority":300,"direction":"Inbound","access":"Deny","protocol":"Udp",` +
			`"sourcePortRange":"*","destinationPortRanges":["53","1000-2000"],"sourceAddressPrefix":"Internet",` +
			`"destinationAddressPrefix":"10.0.0.0/24"` + extra + `}}`
	}
	badRule := func(old, repl string) string { return strings.Replace(rule(""), old, repl, 1) }
	sub := netRoot + "/subscriptions/" + armSub + "/providers/Microsoft.Network/"

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"read", []gcpStep{
			{http.MethodGet, netRoot + netRGList + "virtualNetworks" + netAPI, "", `"10.0.0.0/16"`, 200},
			{http.MethodGet, sub + "networkSecurityGroups" + netAPI, "", `"ssh"`, 200},
			{http.MethodGet, px + netAPI, "", `"name":"default"`, 200},
			{http.MethodGet, px + "/subnets/default" + netAPI, "", `"10.0.0.0/24"`, 200},
			{http.MethodGet, nsg + "/securityRules/ssh" + netAPI, "", `"destinationPortRange":"22"`, 200},
			{http.MethodGet, netRoot + netPolID + "/ruleCollectionGroups" + netAPI, "", `"grp"`, 200},
			{http.MethodGet, netRoot + netPolID + "/ruleCollectionGroups/grp" + netAPI, "", `"r1"`, 200},
			{http.MethodGet, netRoot + netRGList + "applicationGateways" + netAPI, "", `"value":[]`, 200},
			{http.MethodGet, netRoot + netRGList + "azureFirewalls" + netAPI, "", `"value":[]`, 200},
			{http.MethodGet, netRoot + netRGList + "ipGroups" + netAPI, "", `"value":[]`, 200},
			{http.MethodGet, netRoot + netRGList + "loadBalancers" + netAPI, "", `"value":[]`, 200},
		}},
		{"pinned version", []gcpStep{
			{http.MethodGet, px + "?api-version=1999-01-01", "", `"name":"default"`, 200},
			{http.MethodGet, px, "", `"name":"default"`, 200},
		}},
		{"writes", []gcpStep{
			{
				http.MethodPut, netRoot + netNS + "/virtualNetworks/spoke" + netAPI,
				`{"location":"westus","tags":{"x":"y"},"properties":{"dhcpOptions":{"dnsServers":["8.8.8.8"]},` +
					`"addressSpace":{"addressPrefixes":["10.1.0.0/16"],"evil":1},` +
					`"subnets":[{"name":"app","properties":{"addressPrefix":"10.1.0.0/24","serviceEndpoints":[{}]}}]}}`,
				`"10.1.0.0/16"`, 201,
			},
			{http.MethodGet, netRoot + netNS + "/virtualNetworks/spoke" + netAPI, "", `"name":"app"`, 200},
			{
				http.MethodPut, netRoot + netNS + "/virtualNetworks/spoke/subnets/db" + netAPI,
				`{"properties":{"addressPrefix":"10.1.1.0/24","networkSecurityGroup":{"id":"` + netNSGID + `"},` +
					`"delegations":[{"name":"d","properties":{"serviceName":"Microsoft.Web/serverFarms"}}]}}`,
				`Microsoft.Web/serverFarms`, 201,
			},
			{
				http.MethodDelete,
				netRoot + netNS + "/virtualNetworks/spoke/subnets/db" + netAPI,
				"",
				"",
				http.StatusAccepted,
			},
			{http.MethodPut, rules + "https" + netAPI, rule(`,"description":"dns"`), `"priority":300`, 201},
			{http.MethodGet, nsg + netAPI, "", `"https"`, 200},
			{http.MethodDelete, rules + "https" + netAPI, "", "", http.StatusAccepted},
			{http.MethodDelete, netRoot + netNS + "/virtualNetworks/spoke" + netAPI, "", "", http.StatusAccepted},
			{http.MethodGet, netRoot + netNS + "/virtualNetworks/spoke" + netAPI, "", "NotFound", http.StatusNotFound},
		}},
		{"body validation", []gcpStep{
			{http.MethodPut, rules + "r" + netAPI, badRule(`"priority":300`, `"priority":99`), "invalid", 400},
			{http.MethodPut, rules + "r" + netAPI, badRule(`"priority":300`, `"priority":4097`), "invalid", 400},
			{http.MethodPut, rules + "r" + netAPI, badRule(`"priority":300`, `"priority":300.5`), "invalid", 400},
			{http.MethodPut, rules + "r" + netAPI, badRule(`"Inbound"`, `"Sideways"`), "invalid", 400},
			{http.MethodPut, rules + "r" + netAPI, badRule(`"Deny"`, `"Maybe"`), "invalid", 400},
			{http.MethodPut, rules + "r" + netAPI, badRule(`"Udp"`, `"Gre"`), "invalid", 400},
			{http.MethodPut, rules + "r" + netAPI, badRule(`"53"`, `"http"`), "invalid", 400},
			{http.MethodPut, rules + "r" + netAPI, badRule(`"Internet"`, `"bad prefix"`), "invalid", 400},
			{http.MethodPut, rules + "r" + netAPI, `{"properties":{"priority":300}}`, "invalid", 400},
			{http.MethodPut, rules + "r" + netAPI, `[]`, "invalid", 400},
			{http.MethodPut, px + netAPI, netBadVNetPrefix, "invalid", 400},
			{http.MethodPut, px + netAPI, netBadVNetRegion, "invalid", 400},
			{http.MethodPut, px + "/subnets/x" + netAPI, netBadNAT, "invalid", 400},
			{http.MethodPut, px + "/subnets/x" + netAPI, netBadDelegation, "invalid", 400},
		}},
		{"rejects", []gcpStep{
			{http.MethodDelete, nsg + netAPI, "", "unsupported", 400},
			{http.MethodPut, nsg + netAPI, `{}`, "unsupported", 400},
			{http.MethodPost, px + netAPI, `{}`, "unsupported", 400},
			{http.MethodPut, netRoot + netNS + "/publicIPAddresses/p" + netAPI, `{}`, "unsupported", 400},
			{http.MethodDelete, netAGWs + netAPI, "", "unsupported", 400},
			{http.MethodPost, netAGWs + "/restart" + netAPI, "", "unsupported", 400},
			{http.MethodPut, netRoot + netPolID + "/ruleCollectionGroups/grp" + netAPI, `{}`, "unsupported", 400},
			{http.MethodDelete, netRoot + netPolID + netAPI, "", "unsupported", 400},
			{
				http.MethodGet,
				netRoot + armBase + "Microsoft.Compute/virtualMachines/x" + netAPI,
				"",
				"unsupported",
				400,
			},
			{http.MethodGet, netRoot + netNS + "/routeTables" + netAPI, "", "unsupported", 400},
			{http.MethodGet, px + "/../x" + netAPI, "", "unsupported", 400},
			{http.MethodGet, netOps + "foo/1", "", "unsupported", 400},
			{http.MethodPost, netOps + "operations/1", "", "unsupported", 400},
		}},
		{"appgw actions reach the RP", []gcpStep{
			{http.MethodPost, netAGWs + "/stop" + netAPI, "", "NotFound", http.StatusNotFound},
			{http.MethodPost, netAGWs + "/backendHealth" + netAPI, "", "NotFound", http.StatusNotFound},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			runGCPSteps(t, newNetworkDashboard(t), tt.steps)
		})
	}

	t.Run("async operations are relayed under the proxy", func(t *testing.T) {
		t.Parallel()

		h := newNetworkDashboard(t)
		do := func(method, path, body string) *httptest.ResponseRecorder {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(method, path, strings.NewReader(body))
			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

			return rec
		}

		del := do(http.MethodDelete, px+netAPI, "")
		require.Equal(t, http.StatusAccepted, del.Code)

		op := del.Header().Get("Azure-Asyncoperation")
		require.True(
			t,
			strings.HasPrefix(op, netRoot+"/subscriptions/"+armSub+"/providers/Microsoft.Network/locations/"),
			op,
		)
		assert.NotContains(t, op, "https://")

		status := do(http.MethodGet, op, "")
		assert.Equal(t, http.StatusOK, status.Code)
		assert.Contains(t, status.Body.String(), "Succeeded")

		loc := del.Header().Get("Location")
		require.True(t, strings.HasPrefix(loc, netRoot+"/subscriptions/"), loc)
		assert.Equal(t, http.StatusNoContent, do(http.MethodGet, loc, "").Code)
	})

	t.Run("extra fields are dropped", func(t *testing.T) {
		t.Parallel()

		h := newNetworkDashboard(t)
		do := func(method, path, body string) string {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(method, path, strings.NewReader(body))
			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
			require.Less(t, rec.Code, http.StatusBadRequest, rec.Body.String())

			return rec.Body.String()
		}

		vnet := netRoot + netNS + "/virtualNetworks/spoke" + netAPI
		do(
			http.MethodPut,
			vnet,
			`{"location":"westus","tags":{"marker":"y"},"properties":{"dhcpOptions":{"dnsServers":["8.8.8.8"]},`+
				`"addressSpace":{"addressPrefixes":["10.1.0.0/16"]}}}`,
		)

		got := do(http.MethodGet, vnet, "")
		assert.NotContains(t, got, "marker")
		assert.NotContains(t, got, "8.8.8.8")
		assert.Contains(t, got, "10.1.0.0/16")
	})

	t.Run("unserved", func(t *testing.T) {
		t.Parallel()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, sub+"virtualNetworks"+netAPI, nil)
		require.NoError(t, dashboard.NewHandler(dashboard.Config{}).Handler()(echo.NewContext(req, rec)))
		assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	})
}
