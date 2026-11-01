package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func flexVNetFixture(t *testing.T, s *flexStack) string {
	t.Helper()

	vnet := "/subscriptions/" + aciSub + "/resourceGroups/" + flexRG + "/providers/Microsoft.Network/virtualNetworks/vn"
	subnet := func(name, prefix, service string) string {
		deleg := ""
		if service != "" {
			deleg = `,"delegations":[{"name":"d","properties":{"serviceName":"` + service + `"}}]`
		}

		return `{"name":"` + name + `","properties":{"addressPrefix":"` + prefix + `"` + deleg + `}}`
	}

	body := `{"location":"` + flexLoc + `","properties":{"addressSpace":{"addressPrefixes":["10.0.0.0/16"]},"subnets":[` +
		subnet(
			"pgsub",
			"10.0.1.0/24",
			"Microsoft.DBforPostgreSQL/flexibleServers",
		) + "," +
		subnet(
			"mysub",
			"10.0.2.0/24",
			"Microsoft.DBforMySQL/flexibleServers",
		) + "," +
		subnet(
			"plain",
			"10.0.3.0/24",
			"",
		) + "," +
		subnet(
			"pgsmall",
			"10.0.4.0/29",
			"Microsoft.DBforPostgreSQL/flexibleServers",
		) + "," +
		subnet(
			"pg28",
			"10.0.7.0/28",
			"Microsoft.DBforPostgreSQL/flexibleServers",
		) + "," +
		subnet(
			"mysmall",
			"10.0.5.0/29",
			"Microsoft.DBforMySQL/flexibleServers",
		) + `]}}`

	status, resp := s.raw(t, http.MethodPut, vnet, body, "2023-09-01")
	require.Less(t, status, http.StatusMultipleChoices, resp)

	return vnet
}

func flexZone(kind string) string {
	return "/subscriptions/" + aciSub + "/resourceGroups/" + flexRG +
		"/providers/Microsoft.Network/privateDnsZones/zone.private." + kind + ".database.azure.com"
}

func flexPrivateBody(vnet, subnet, dnsKey, zone, extra string) string {
	return `{"location":"x","sku":{"name":"Standard_B1ms","tier":"Burstable"},"properties":{` + pgLoginPW +
		`,"network":{"delegatedSubnetResourceId":"` + vnet + `/subnets/` + subnet + `","` + dnsKey + `":"` + zone +
		`"` + extra + `}}}`
}

func TestFlexPrivateAccessValidation(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	vnet := flexVNetFixture(t, s)
	pgZone, myZone := flexZone("postgres"), flexZone("mysql")

	tests := []struct {
		name   string
		ns     string
		server string
		body   string
		api    string
		status int
	}{
		{"pg delegated", pgNS, "pgpriv", flexPrivateBody(vnet, "pgsub", "privateDnsZoneArmResourceId", pgZone, ""),
			flexAPIPG, http.StatusCreated},
		{
			"pg wrong delegation",
			pgNS,
			"pgwrong",
			flexPrivateBody(vnet, "mysub", "privateDnsZoneArmResourceId", pgZone, ""),
			flexAPIPG,
			http.StatusBadRequest,
		},
		{"pg undelegated", pgNS, "pgplain", flexPrivateBody(vnet, "plain", "privateDnsZoneArmResourceId", pgZone, ""),
			flexAPIPG, http.StatusBadRequest},
		{
			"pg missing subnet",
			pgNS,
			"pgmissing",
			flexPrivateBody(vnet, "nope", "privateDnsZoneArmResourceId", pgZone, ""),
			flexAPIPG,
			http.StatusBadRequest,
		},
		{"pg public enabled", pgNS, "pgboth",
			flexPrivateBody(vnet, "pgsub", "privateDnsZoneArmResourceId", pgZone, `,"publicNetworkAccess":"Enabled"`),
			flexAPIPG, http.StatusBadRequest},
		{"mysql delegated", myNS, "mypriv", flexPrivateBody(vnet, "mysub", "privateDnsZoneResourceId", myZone, ""),
			flexAPIMy, http.StatusCreated},
		{
			"pg /29 too small",
			pgNS,
			"pgsmall",
			flexPrivateBody(vnet, "pgsmall", "privateDnsZoneArmResourceId", pgZone, ""),
			flexAPIPG,
			http.StatusBadRequest,
		},
		{"pg /28 ok", pgNS, "pg28", flexPrivateBody(vnet, "pg28", "privateDnsZoneArmResourceId", pgZone, ""),
			flexAPIPG, http.StatusCreated},
		{"mysql /29 ok", myNS, "mysmall", flexPrivateBody(vnet, "mysmall", "privateDnsZoneResourceId", myZone, ""),
			flexAPIMy, http.StatusCreated},
		{"pg wrong zone suffix", pgNS, "pgbadzone",
			flexPrivateBody(
				vnet,
				"pgsub",
				"privateDnsZoneArmResourceId",
				myZone,
				"",
			), flexAPIPG, http.StatusBadRequest},
		{"mysql wrong zone suffix", myNS, "mybadzone",
			flexPrivateBody(vnet, "mysub", "privateDnsZoneResourceId", pgZone, ""), flexAPIMy, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			status, resp := s.raw(t, http.MethodPut, flexServerPath(tt.ns, tt.server), tt.body, tt.api)
			assert.Equal(t, tt.status, status, resp)
		})
	}
}

func TestFlexPrivateAccessEffects(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	vnet := flexVNetFixture(t, s)
	zone := flexZone("postgres")
	path := flexServerPath(pgNS, "pgeff")

	status, resp := s.raw(t, http.MethodPut, path,
		flexPrivateBody(vnet, "pgsub", "privateDnsZoneArmResourceId", zone, ""), flexAPIPG)
	require.Equal(t, http.StatusCreated, status, resp)

	props, _ := resp["properties"].(map[string]any)
	net, _ := props["network"].(map[string]any)

	assert.Equal(t, "Disabled", net["publicNetworkAccess"])
	assert.Equal(t, "pgeff.zone.private.postgres.database.azure.com", props["fullyQualifiedDomainName"])

	status, resp = s.raw(t, http.MethodPut, path+"/firewallRules/r",
		`{"properties":{"startIpAddress":"1.1.1.1","endIpAddress":"1.1.1.1"}}`, flexAPIPG)
	assert.Equal(t, http.StatusBadRequest, status, resp)

	status, resp = s.raw(
		t,
		http.MethodPatch,
		path,
		`{"properties":{"network":{"delegatedSubnetResourceId":"`+vnet+`/subnets/mysub","privateDnsZoneArmResourceId":"`+
			zone+`"}}}`,
		flexAPIPG,
	)
	assert.Equal(t, http.StatusBadRequest, status, resp)
}
