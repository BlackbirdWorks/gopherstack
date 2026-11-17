package azurearm_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/mysql/armmysqlflexibleservers/v2"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

type flexParamCase struct {
	create    func(t *testing.T, s *flexStack, name string) string
	ns        string
	api       string
	name      string
	dynamic   string
	dynValue  string
	dynDef    string
	static    string
	statValue string
	engineOff string
	offValue  string
}

func flexParamCases() []flexParamCase {
	return []flexParamCase{
		{
			name: "postgres", ns: pgNS, api: flexAPIPG,
			create: func(t *testing.T, s *flexStack, name string) string {
				t.Helper()

				return *s.pgCreate(t, name).ID
			},
			dynamic: "work_mem", dynValue: "8192", dynDef: "4096",
			static: "wal_level", statValue: "logical",
			engineOff: "azure.extensions", offValue: "PG_TRGM",
		},
		{
			name: "mysql", ns: myNS, api: flexAPIMy,
			create: func(t *testing.T, s *flexStack, name string) string {
				t.Helper()

				return *s.myCreate(t, name).ID
			},
			dynamic: "wait_timeout", dynValue: "100", dynDef: "28800",
			static: "character_set_server", statValue: "latin1",
			engineOff: "require_secure_transport", offValue: "OFF",
		},
	}
}

func (c flexParamCase) put(t *testing.T, s *flexStack, param, body string) (int, map[string]any) {
	t.Helper()

	return s.raw(t, http.MethodPut, flexServerPath(c.ns, "srv")+"/configurations/"+param, body, c.api)
}

func (c flexParamCase) get(t *testing.T, s *flexStack, param string) map[string]any {
	t.Helper()

	status, body := s.raw(t, http.MethodGet, flexServerPath(c.ns, "srv")+"/configurations/"+param, "", c.api)
	require.Equal(t, http.StatusOK, status)

	props, _ := body["properties"].(map[string]any)

	return props
}

func isFalse(v any) bool { return strings.EqualFold(fmt.Sprint(v), "false") }

func valueBody(v string) string {
	return `{"properties":{"value":"` + v + `","source":"user-override"}}`
}

func TestFlexParametersReachTheEngine(t *testing.T) {
	t.Parallel()

	for _, c := range flexParamCases() {
		t.Run(c.name+" dynamic", func(t *testing.T) {
			t.Parallel()

			eng := newFakeFlexEngine()
			s := newFlexStack(t, eng)
			c.create(t, s, "srv")

			status, _ := c.put(t, s, c.dynamic, valueBody(c.dynValue))
			require.Less(t, status, 300)

			got, ok := eng.param(flexKey(c.ns, "srv"), c.dynamic)
			require.True(t, ok, "the parameter must reach the running engine")
			assert.Equal(t, azurearm.FlexParam{Name: c.dynamic, Value: c.dynValue}, got)

			props := c.get(t, s, c.dynamic)
			assert.Equal(t, c.dynValue, props["value"])
			assert.Equal(t, "user-override", props["source"])
			assert.True(t, isFalse(props["isConfigPendingRestart"]))
		})

		t.Run(c.name+" static waits for restart", func(t *testing.T) {
			t.Parallel()

			eng := newFakeFlexEngine()
			s := newFlexStack(t, eng)
			c.create(t, s, "srv")

			status, _ := c.put(t, s, c.static, valueBody(c.statValue))
			require.Less(t, status, 300)

			got, ok := eng.param(flexKey(c.ns, "srv"), c.static)
			require.True(t, ok)
			assert.True(t, got.Static)
			assert.Equal(t, c.statValue, got.Value)
			assert.False(t, isFalse(c.get(t, s, c.static)["isConfigPendingRestart"]))

			status, _ = s.raw(t, http.MethodPost, flexServerPath(c.ns, "srv")+"/restart", "", c.api)
			require.Equal(t, http.StatusAccepted, status)

			props := c.get(t, s, c.static)
			assert.True(t, isFalse(props["isConfigPendingRestart"]), "a restart applies static parameters")
			assert.Equal(t, c.statValue, props["value"])
			assert.Contains(t, eng.log(), "restart")
		})

		t.Run(c.name+" reset applies the default", func(t *testing.T) {
			t.Parallel()

			eng := newFakeFlexEngine()
			s := newFlexStack(t, eng)
			c.create(t, s, "srv")

			status, _ := c.put(t, s, c.dynamic, valueBody(c.dynValue))
			require.Less(t, status, 300)

			status, _ = c.put(t, s, c.dynamic, `{"properties":{"source":"system-default"}}`)
			require.Less(t, status, 300)

			got, ok := eng.param(flexKey(c.ns, "srv"), c.dynamic)
			require.True(t, ok)
			assert.Equal(t, c.dynDef, got.Value)

			props := c.get(t, s, c.dynamic)
			assert.Equal(t, "system-default", props["source"])
		})

		t.Run(c.name+" platform only parameter stays out of the engine", func(t *testing.T) {
			t.Parallel()

			eng := newFakeFlexEngine()
			s := newFlexStack(t, eng)
			c.create(t, s, "srv")

			status, _ := c.put(t, s, c.engineOff, valueBody(c.offValue))
			require.Less(t, status, 300)

			_, ok := eng.param(flexKey(c.ns, "srv"), c.engineOff)
			assert.False(t, ok)
			assert.Equal(t, c.offValue, c.get(t, s, c.engineOff)["value"])
		})

		t.Run(c.name+" refused change is rolled back", func(t *testing.T) {
			t.Parallel()

			eng := newFakeFlexEngine()
			s := newFlexStack(t, eng)
			c.create(t, s, "srv")
			eng.failApply = true

			status, body := c.put(t, s, c.static, valueBody(c.statValue))
			assert.Equal(t, http.StatusServiceUnavailable, status, body)

			props := c.get(t, s, c.static)
			assert.Equal(t, "system-default", props["source"], "the stored value is rolled back")
			assert.True(t, isFalse(props["isConfigPendingRestart"]))
		})

		t.Run(c.name+" stopped server", func(t *testing.T) {
			t.Parallel()

			eng := newFakeFlexEngine()
			s := newFlexStack(t, eng)
			c.create(t, s, "srv")

			status, _ := s.raw(t, http.MethodPost, flexServerPath(c.ns, "srv")+"/stop", "", c.api)
			require.Equal(t, http.StatusAccepted, status)

			status, _ = c.put(t, s, c.dynamic, valueBody(c.dynValue))
			assert.Equal(t, http.StatusConflict, status)

			_, ok := eng.param(flexKey(c.ns, "srv"), c.dynamic)
			assert.False(t, ok, "a stopped engine is not configured")
		})

		t.Run(c.name+" invalid value", func(t *testing.T) {
			t.Parallel()

			eng := newFakeFlexEngine()
			s := newFlexStack(t, eng)
			c.create(t, s, "srv")

			status, _ := c.put(t, s, c.dynamic, valueBody("1'; DROP DATABASE x; --"))
			assert.Equal(t, http.StatusBadRequest, status)

			_, ok := eng.param(flexKey(c.ns, "srv"), c.dynamic)
			assert.False(t, ok)
		})
	}
}

func TestMySQLFlexBatchParametersReachTheEngine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want map[string]string
		name string
		body string
	}{
		{
			name: "values",
			body: `{"value":[{"name":"wait_timeout","properties":{"value":"100"}},` +
				`{"name":"slow_query_log","properties":{"value":"ON"}}]}`,
			want: map[string]string{"wait_timeout": "100", "slow_query_log": "ON"},
		},
		{
			name: "reset all",
			body: `{"resetAllToDefault":"True"}`,
			want: map[string]string{
				"wait_timeout":         "28800",
				"slow_query_log":       "OFF",
				"character_set_server": "utf8mb4",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eng := newFakeFlexEngine()
			s := newFlexStack(t, eng)
			s.myCreate(t, "srv")

			status, body := s.raw(t, http.MethodPost, flexServerPath(myNS, "srv")+"/updateConfigurations", tt.body,
				flexAPIMy)
			require.Equal(t, http.StatusOK, status, body)

			for name, want := range tt.want {
				got, ok := eng.param(flexKey(myNS, "srv"), name)
				require.True(t, ok, name)
				assert.Equal(t, want, got.Value, name)
			}

			_, ok := eng.param(flexKey(myNS, "srv"), "require_secure_transport")
			assert.False(t, ok, "the platform only parameter is never pushed")
		})
	}
}

func TestFlexRestartWithFailover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		kind     string
		body     string
		wantZone string
		status   int
	}{
		{"pg plain", pgNS, `{}`, "1", http.StatusAccepted},
		{
			"pg failover",
			pgNS,
			`{"restartWithFailover":true,"failoverMode":"PlannedFailover"}`,
			"2",
			http.StatusAccepted,
		},
		{"pg bad mode", pgNS, `{"restartWithFailover":true,"failoverMode":"Sideways"}`, "1", http.StatusBadRequest},
		{"mysql plain", myNS, `{}`, "1", http.StatusAccepted},
		{"mysql failover", myNS, `{"restartWithFailover":"Enabled","maxFailoverSeconds":60}`, "2", http.StatusAccepted},
		{
			"mysql negative",
			myNS,
			`{"restartWithFailover":"Enabled","maxFailoverSeconds":-1}`,
			"1",
			http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			eng := newFakeFlexEngine()
			s := newFlexStack(t, eng)
			api := map[string]string{pgNS: flexAPIPG, myNS: flexAPIMy}[tt.kind]
			path := flexServerPath(tt.kind, "hasrv")

			createHA(t, s, tt.kind, "hasrv")

			status, resp := s.raw(t, http.MethodPost, path+"/restart", tt.body, api)
			assert.Equal(t, tt.status, status, resp)

			_, got := s.raw(t, http.MethodGet, path, "", api)
			props, _ := got["properties"].(map[string]any)
			assert.Equal(t, tt.wantZone, props["availabilityZone"])
		})
	}
}

func TestFlexRestartWithFailoverNeedsHA(t *testing.T) {
	t.Parallel()

	eng := newFakeFlexEngine()
	s := newFlexStack(t, eng)
	s.pgCreate(t, "plain")

	status, resp := s.raw(t, http.MethodPost, flexServerPath(pgNS, "plain")+"/restart",
		`{"restartWithFailover":true}`, flexAPIPG)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "InvalidParameterValue", flexErrCode(resp))
}

func TestFlexStopStartFollowTheBackendClock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		action string
		want   []string
	}{
		{"stop", "stop", []string{"Stopping", "Stopped"}},
		{"restart", "restart", []string{"Updating", "Ready"}},
	}

	for _, kind := range flexKindCases() {
		for _, tt := range tests {
			t.Run(kind.name+" "+tt.name, func(t *testing.T) {
				t.Parallel()

				s := newFlexStack(t, nil)
				kind.create(t, s, "srv")

				clk := newFlexClock()
				clk.Advance(time.Hour)
				s.pg.SetClock(clk.Now)
				s.my.SetClock(clk.Now)

				status, _ := s.raw(t, http.MethodPost, flexServerPath(kind.ns, "srv")+"/"+tt.action, "", kind.api)
				require.Equal(t, http.StatusAccepted, status)

				state := func() any {
					_, body := s.raw(t, http.MethodGet, flexServerPath(kind.ns, "srv"), "", kind.api)
					props, _ := body["properties"].(map[string]any)

					return props["state"]
				}

				assert.Equal(t, tt.want[0], state(), "the operation is in flight until the clock passes it")

				clk.Advance(time.Second)
				assert.Equal(t, tt.want[1], state())
			})
		}
	}
}

func createHA(t *testing.T, s *flexStack, ns, name string) {
	t.Helper()

	if ns == pgNS {
		srv := pgServer()
		srv.SKU = &armpostgresqlflexibleservers.SKU{
			Name: new("Standard_D2ds_v5"), Tier: new(armpostgresqlflexibleservers.SKUTierGeneralPurpose),
		}
		srv.Properties.AvailabilityZone = new("1")
		srv.Properties.HighAvailability = &armpostgresqlflexibleservers.HighAvailability{
			Mode:                    new(armpostgresqlflexibleservers.HighAvailabilityModeZoneRedundant),
			StandbyAvailabilityZone: new("2"),
		}

		p, err := s.pgFactory(t).NewServersClient().BeginCreate(t.Context(), flexRG, name, srv, nil)
		require.NoError(t, err)
		_, err = p.PollUntilDone(t.Context(), pollFast())
		require.NoError(t, err)

		return
	}

	srv := mySrv()
	srv.SKU = &armmysqlflexibleservers.MySQLServerSKU{
		Name: new("Standard_D2ds_v4"), Tier: new(armmysqlflexibleservers.ServerSKUTierGeneralPurpose),
	}
	srv.Properties.AvailabilityZone = new("1")
	srv.Properties.HighAvailability = &armmysqlflexibleservers.HighAvailability{
		Mode:                    new(armmysqlflexibleservers.HighAvailabilityModeZoneRedundant),
		StandbyAvailabilityZone: new("2"),
	}

	p, err := s.myFactory(t).NewServersClient().BeginCreate(t.Context(), flexRG, name, srv, nil)
	require.NoError(t, err)
	_, err = p.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
}
