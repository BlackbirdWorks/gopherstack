package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlexPrivateAccess(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	vnet := "/subscriptions/" + aciSub + "/resourceGroups/" + flexRG + "/providers/Microsoft.Network/virtualNetworks/vn"

	status, body := s.raw(t, http.MethodPut, vnet, `{"location":"`+flexLoc+`","properties":{"addressSpace":{"addressPrefixes":["10.0.0.0/16"]},"subnets":[`+
		`{"name":"pgsub","properties":{"addressPrefix":"10.0.1.0/24","delegations":[{"name":"d","properties":{"serviceName":"Microsoft.DBforPostgreSQL/flexibleServers"}}]}},`+
		`{"name":"mysub","properties":{"addressPrefix":"10.0.2.0/24","delegations":[{"name":"d","properties":{"serviceName":"Microsoft.DBforMySQL/flexibleServers"}}]}},`+
		`{"name":"plain","properties":{"addressPrefix":"10.0.3.0/24"}}]}}`, "2023-09-01")
	require.Less(t, status, http.StatusMultipleChoices, body)

	zone := func(kind string) string {
		return "/subscriptions/" + aciSub + "/resourceGroups/" + flexRG +
			"/providers/Microsoft.Network/privateDnsZones/zone.private." + kind + ".database.azure.com"
	}

	pgBody := func(subnet, extra string) string {
		return `{"location":"x","sku":{"name":"Standard_B1ms","tier":"Burstable"},"properties":{"administratorLogin":"a1",` +
			`"administratorLoginPassword":"` + flexPassword + `","network":{"delegatedSubnetResourceId":"` + vnet + `/subnets/` +
			subnet + `","privateDnsZoneArmResourceId":"` + zone("postgres") + `"` + extra + `}}}`
	}

	tests := []struct {
		name   string
		ns     string
		server string
		body   string
		api    string
		status int
	}{
		{"pg delegated", pgNS, "pgpriv", pgBody("pgsub", ""), flexAPIPG, http.StatusCreated},
		{"pg wrong delegation", pgNS, "pgwrong", pgBody("mysub", ""), flexAPIPG, http.StatusBadRequest},
		{"pg undelegated", pgNS, "pgplain", pgBody("plain", ""), flexAPIPG, http.StatusBadRequest},
		{"pg missing subnet", pgNS, "pgmissing", pgBody("nope", ""), flexAPIPG, http.StatusBadRequest},
		{"pg public enabled", pgNS, "pgboth", pgBody("pgsub", `,"publicNetworkAccess":"Enabled"`), flexAPIPG, http.StatusBadRequest},
		{"mysql delegated", myNS, "mypriv", `{"location":"x","sku":{"name":"Standard_B1ms","tier":"Burstable"},"properties":{"administratorLogin":"a1",` +
			`"administratorLoginPassword":"` + flexPassword + `","network":{"delegatedSubnetResourceId":"` + vnet + `/subnets/mysub",` +
			`"privateDnsZoneResourceId":"` + zone("mysql") + `"}}}`, flexAPIMy, http.StatusCreated},
	}

	t.Run("cases", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				status, resp := s.raw(t, http.MethodPut, flexServerPath(tt.ns, tt.server), tt.body, tt.api)
				assert.Equal(t, tt.status, status, resp)
			})
		}
	})

	t.Run("effects", func(t *testing.T) {
		_, srv := s.raw(t, http.MethodGet, flexServerPath(pgNS, "pgpriv"), "", flexAPIPG)
		props, _ := srv["properties"].(map[string]any)
		net, _ := props["network"].(map[string]any)

		assert.Equal(t, "Disabled", net["publicNetworkAccess"])
		assert.Equal(t, "pgpriv.zone.private.postgres.database.azure.com", props["fullyQualifiedDomainName"])

		status, resp := s.raw(t, http.MethodPut, flexServerPath(pgNS, "pgpriv")+"/firewallRules/r",
			`{"properties":{"startIpAddress":"1.1.1.1","endIpAddress":"1.1.1.1"}}`, flexAPIPG)
		assert.Equal(t, http.StatusBadRequest, status, resp)

		status, resp = s.raw(t, http.MethodPatch, flexServerPath(pgNS, "pgpriv"),
			`{"properties":{"network":{"delegatedSubnetResourceId":"`+vnet+`/subnets/mysub","privateDnsZoneArmResourceId":"`+zone("postgres")+`"}}}`,
			flexAPIPG)
		assert.Equal(t, http.StatusBadRequest, status, resp)
	})
}
