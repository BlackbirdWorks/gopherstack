package azurearm_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlexPrivateZoneNotServerName(t *testing.T) {
	t.Parallel()

	zoneOf := func(name, kind string) string {
		return "/subscriptions/" + aciSub + "/resourceGroups/" + flexRG +
			"/providers/Microsoft.Network/privateDnsZones/" + name + "." + kind + ".database.azure.com"
	}

	tests := []struct {
		name     string
		preexist string
		server   string
		zone     string
		status   int
	}{
		{name: "zone is own name", server: "pgself", zone: zoneOf("pgself", "postgres"), status: http.StatusBadRequest},
		{
			name: "zone is other server", preexist: "pgother", server: "pgnew",
			zone: zoneOf("pgother", "postgres"), status: http.StatusBadRequest,
		},
		{name: "zone is unrelated", server: "pgfree", zone: zoneOf("shared", "postgres"), status: http.StatusAccepted},
		{
			name: "two label zone", server: "pgdots",
			zone: zoneOf("pgdots.private", "postgres"), status: http.StatusAccepted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newFlexStack(t, nil)
			vnet := flexVNetFixture(t, s)

			if tt.preexist != "" {
				status, resp := s.raw(t, http.MethodPut, flexServerPath(pgNS, tt.preexist),
					flexPrivateBody(vnet, "pgsub", "privateDnsZoneArmResourceId", flexZone("postgres"), ""), flexAPIPG)
				require.Equal(t, http.StatusAccepted, status, resp)
			}

			status, resp := s.raw(t, http.MethodPut, flexServerPath(pgNS, tt.server),
				flexPrivateBody(vnet, "pgsub", "privateDnsZoneArmResourceId", tt.zone, ""), flexAPIPG)
			assert.Equal(t, tt.status, status, resp)
		})
	}
}

func TestFlexMySQLZoneMayShareServerName(t *testing.T) {
	t.Parallel()

	s := newFlexStack(t, nil)
	vnet := flexVNetFixture(t, s)
	zone := "/subscriptions/" + aciSub + "/resourceGroups/" + flexRG +
		"/providers/Microsoft.Network/privateDnsZones/mysame.mysql.database.azure.com"

	status, resp := s.raw(t, http.MethodPut, flexServerPath(myNS, "mysame"),
		flexPrivateBody(vnet, "mysub", "privateDnsZoneResourceId", zone, ""), flexAPIMy)
	assert.Equal(t, http.StatusAccepted, status, resp)
}
