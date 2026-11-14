package azurearm_test

import (
	"bytes"
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const ehARM = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.EventHub/namespaces/"

type ehDREnv struct {
	dp *fakeEventHubs
	h  *azurearm.Handler
}

func newEHDREnv(t *testing.T) *ehDREnv {
	t.Helper()

	dp := newFakeEventHubs()
	e := &ehDREnv{dp: dp, h: azurearm.NewTestHandlerWithEventHubs(dp)}

	e.mustPut(t, "pri", map[string]any{"location": "eastus", "sku": map[string]any{"name": "Standard"}})
	e.mustPut(t, "sec", map[string]any{"location": "westus", "sku": map[string]any{"name": "Standard"}})

	return e
}

func (e *ehDREnv) do(t *testing.T, method, path string, body any) (int, map[string]any) {
	t.Helper()

	var raw []byte

	if body != nil {
		var err error

		raw, err = json.Marshal(body, jsonx.V1())
		require.NoError(t, err)
	}

	sep := "?"
	if strings.Contains(path, "?") {
		sep = "&"
	}

	req := httptest.NewRequestWithContext(t.Context(), method, path+sep+"api-version=2024-01-01", bytes.NewReader(raw))
	rec := httptest.NewRecorder()
	newEchoServer(e.h).ServeHTTP(rec, req)

	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1())

	return rec.Code, out
}

func (e *ehDREnv) mustPut(t *testing.T, path string, body any) {
	t.Helper()

	code, out := e.do(t, http.MethodPut, ehARM+path, body)
	require.Less(t, code, http.StatusMultipleChoices, out)
}

func drBody(partner string) map[string]any {
	return map[string]any{"properties": map[string]any{"partnerNamespace": ehARM + partner}}
}

func TestEventHubDisasterRecoveryPairing(t *testing.T) {
	t.Parallel()

	e := newEHDREnv(t)
	e.mustPut(t, "pri/eventhubs/orders", map[string]any{"properties": map[string]any{"partitionCount": float64(2)}})
	e.mustPut(t, "pri/eventhubs/orders/consumergroups/cg1", map[string]any{})

	status, created := e.do(t, http.MethodPut, ehARM+"pri/disasterRecoveryConfigs/orders-alias", drBody("sec"))
	require.Equal(t, http.StatusOK, status, created)

	p := props(created)
	assert.Equal(t, "Primary", p["role"])
	assert.Equal(t, "Succeeded", p["provisioningState"])
	assert.Equal(t, ehARM+"sec", p["partnerNamespace"])
	assert.EqualValues(t, 0, p["pendingReplicationOperationsCount"])
	assert.Equal(t, "orders-alias", created["name"])

	t.Run("secondary mirrors the alias", func(t *testing.T) {
		t.Parallel()

		code, body := e.do(t, http.MethodGet, ehARM+"sec/disasterRecoveryConfigs/orders-alias", nil)
		require.Equal(t, http.StatusOK, code, body)
		assert.Equal(t, "Secondary", props(body)["role"])
		assert.Equal(t, ehARM+"pri", props(body)["partnerNamespace"])
	})

	t.Run("metadata replicated", func(t *testing.T) {
		t.Parallel()

		code, hub := e.do(t, http.MethodGet, ehARM+"sec/eventhubs/orders", nil)
		require.Equal(t, http.StatusOK, code, hub)
		assert.EqualValues(t, 2, props(hub)["partitionCount"])

		code, cg := e.do(t, http.MethodGet, ehARM+"sec/eventhubs/orders/consumergroups/cg1", nil)
		assert.Equal(t, http.StatusOK, code, cg)
	})

	t.Run("list", func(t *testing.T) {
		t.Parallel()

		code, list := e.do(t, http.MethodGet, ehARM+"pri/disasterRecoveryConfigs", nil)
		require.Equal(t, http.StatusOK, code)
		assert.Len(t, list["value"], 1)
	})
}

func TestEventHubDisasterRecoverySecondaryReadOnly(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want int
	}{
		{name: "create hub on secondary", path: "sec/eventhubs/h1", want: http.StatusBadRequest},
		{name: "create hub on primary", path: "pri/eventhubs/h1", want: http.StatusOK},
		{name: "create group on secondary", path: "sec/eventhubs/h1/consumergroups/g", want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEHDREnv(t)
			status, out := e.do(t, http.MethodPut, ehARM+"pri/disasterRecoveryConfigs/aliasone", drBody("sec"))
			require.Equal(t, http.StatusOK, status, out)

			status, out = e.do(t, http.MethodPut, ehARM+tt.path, map[string]any{})
			assert.Equal(t, tt.want, status, out)
		})
	}

	t.Run("replication follows the primary", func(t *testing.T) {
		t.Parallel()

		e := newEHDREnv(t)
		status, out := e.do(t, http.MethodPut, ehARM+"pri/disasterRecoveryConfigs/aliasone", drBody("sec"))
		require.Equal(t, http.StatusOK, status, out)

		e.mustPut(t, "pri/eventhubs/h1", map[string]any{})

		status, _ = e.do(t, http.MethodGet, ehARM+"sec/eventhubs/h1", nil)
		assert.Equal(t, http.StatusOK, status)

		status, _ = e.do(t, http.MethodDelete, ehARM+"pri/eventhubs/h1", nil)
		require.Less(t, status, http.StatusMultipleChoices)

		status, _ = e.do(t, http.MethodGet, ehARM+"sec/eventhubs/h1", nil)
		assert.Equal(t, http.StatusNotFound, status)

		status, _ = e.do(t, http.MethodDelete, ehARM+"sec/eventhubs/h1", nil)
		assert.Equal(t, http.StatusBadRequest, status)
	})
}

func TestEventHubDisasterRecoveryValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*testing.T, *ehDREnv)
		body  map[string]any
		name  string
		alias string
		want  int
	}{
		{name: "ok", alias: "valid-alias", body: drBody("sec"), want: http.StatusOK},
		{name: "missing partner", alias: "valid-alias", body: drBody("nope"), want: http.StatusBadRequest},
		{name: "partner not an id", alias: "valid-alias",
			body: map[string]any{"properties": map[string]any{"partnerNamespace": "sec"}}, want: http.StatusBadRequest},
		{name: "self", alias: "valid-alias", body: drBody("pri"), want: http.StatusBadRequest},
		{name: "short alias", alias: "abc", body: drBody("sec"), want: http.StatusBadRequest},
		{name: "alias equals other namespace", alias: "othername", body: drBody("sec"), want: http.StatusConflict,
			setup: func(t *testing.T, e *ehDREnv) {
				t.Helper()
				e.mustPut(t, "othername", map[string]any{"location": "brazilsouth"})
			}},
		{name: "same region", alias: "valid-alias", body: drBody("same"), want: http.StatusBadRequest,
			setup: func(t *testing.T, e *ehDREnv) {
				t.Helper()
				e.mustPut(t, "same", map[string]any{"location": "eastus", "sku": map[string]any{"name": "Standard"}})
			}},
		{name: "tier mismatch", alias: "valid-alias", body: drBody("prem"), want: http.StatusBadRequest,
			setup: func(t *testing.T, e *ehDREnv) {
				t.Helper()
				e.mustPut(
					t,
					"prem",
					map[string]any{"location": "northeurope", "sku": map[string]any{"name": "Premium"}},
				)
			}},
		{name: "basic tier", alias: "valid-alias", body: drBody("basic2"), want: http.StatusBadRequest,
			setup: func(t *testing.T, e *ehDREnv) {
				t.Helper()
				e.mustPut(t, "basic1", map[string]any{"location": "eastus2", "sku": map[string]any{"name": "Basic"}})
				e.mustPut(t, "basic2", map[string]any{"location": "westus2", "sku": map[string]any{"name": "Basic"}})
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEHDREnv(t)
			if tt.setup != nil {
				tt.setup(t, e)
			}

			code, out := e.do(t, http.MethodPut, ehARM+"pri/disasterRecoveryConfigs/"+tt.alias, tt.body)
			assert.Equal(t, tt.want, code, out)
		})
	}

	t.Run("alternate name allows alias equal to namespace", func(t *testing.T) {
		t.Parallel()

		e := newEHDREnv(t)
		e.mustPut(t, "myalias", map[string]any{"location": "uksouth", "sku": map[string]any{"name": "Standard"}})

		body := drBody("sec")
		props(body)["alternateName"] = "pri-alt"

		code, out := e.do(t, http.MethodPut, ehARM+"myalias/disasterRecoveryConfigs/myalias", body)
		require.Equal(t, http.StatusOK, code, out)
		assert.Equal(t, "pri-alt", props(out)["alternateName"])
	})

	t.Run("alias used by another pairing", func(t *testing.T) {
		t.Parallel()

		e := newEHDREnv(t)
		e.mustPut(t, "tri", map[string]any{"location": "westeurope", "sku": map[string]any{"name": "Standard"}})
		e.mustPut(t, "qua", map[string]any{"location": "japaneast", "sku": map[string]any{"name": "Standard"}})

		code, out := e.do(t, http.MethodPut, ehARM+"pri/disasterRecoveryConfigs/sharedalias", drBody("sec"))
		require.Equal(t, http.StatusOK, code, out)

		body := drBody("qua")
		code, _ = e.do(t, http.MethodPut, ehARM+"tri/disasterRecoveryConfigs/sharedalias", body)
		assert.Equal(t, http.StatusConflict, code)

		code, _ = e.do(t, http.MethodPut, ehARM+"tri/disasterRecoveryConfigs/otheralias", drBody("sec"))
		assert.Equal(t, http.StatusBadRequest, code, "the secondary already belongs to a pairing")
	})
}

func TestEventHubDisasterRecoveryBreakAndFailover(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) *ehDREnv {
		t.Helper()

		e := newEHDREnv(t)
		code, out := e.do(t, http.MethodPut, ehARM+"pri/disasterRecoveryConfigs/failalias", drBody("sec"))
		require.Equal(t, http.StatusOK, code, out)

		return e
	}

	t.Run("break pairing", func(t *testing.T) {
		t.Parallel()

		e := setup(t)

		code, _ := e.do(t, http.MethodPost, ehARM+"sec/disasterRecoveryConfigs/failalias/breakPairing", nil)
		assert.Equal(t, http.StatusBadRequest, code, "only the primary breaks the pairing")

		code, _ = e.do(t, http.MethodPost, ehARM+"pri/disasterRecoveryConfigs/failalias/breakPairing", nil)
		require.Equal(t, http.StatusOK, code)

		_, got := e.do(t, http.MethodGet, ehARM+"pri/disasterRecoveryConfigs/failalias", nil)
		assert.Equal(t, "PrimaryNotReplicating", props(got)["role"])

		code, _ = e.do(t, http.MethodGet, ehARM+"sec/disasterRecoveryConfigs/failalias", nil)
		assert.Equal(t, http.StatusNotFound, code)

		code, out := e.do(t, http.MethodPut, ehARM+"sec/eventhubs/free", map[string]any{})
		assert.Equal(t, http.StatusOK, code, out)
	})

	t.Run("fail over", func(t *testing.T) {
		t.Parallel()

		e := setup(t)

		code, _ := e.do(t, http.MethodPost, ehARM+"pri/disasterRecoveryConfigs/failalias/failover", nil)
		assert.Equal(t, http.StatusBadRequest, code, "fail over starts on the secondary")

		code, _ = e.do(t, http.MethodPost, ehARM+"sec/disasterRecoveryConfigs/failalias/failover", nil)
		require.Equal(t, http.StatusOK, code)

		_, got := e.do(t, http.MethodGet, ehARM+"sec/disasterRecoveryConfigs/failalias", nil)
		assert.Equal(t, "PrimaryNotReplicating", props(got)["role"])

		code, _ = e.do(t, http.MethodGet, ehARM+"pri/disasterRecoveryConfigs/failalias", nil)
		assert.Equal(t, http.StatusNotFound, code, "the pairing is removed")
	})

	t.Run("delete", func(t *testing.T) {
		t.Parallel()

		e := setup(t)

		code, _ := e.do(t, http.MethodDelete, ehARM+"pri/disasterRecoveryConfigs/failalias", nil)
		require.Less(t, code, http.StatusMultipleChoices)

		for _, ns := range []string{"pri", "sec"} {
			code, _ = e.do(t, http.MethodGet, ehARM+ns+"/disasterRecoveryConfigs/failalias", nil)
			assert.Equal(t, http.StatusNotFound, code)
		}

		code, _ = e.do(t, http.MethodDelete, ehARM+"pri/disasterRecoveryConfigs/failalias", nil)
		assert.Equal(t, http.StatusNotFound, code)
	})

	t.Run("delete namespace", func(t *testing.T) {
		t.Parallel()

		e := setup(t)

		code, _ := e.do(t, http.MethodDelete, ehARM+"sec", nil)
		require.Less(t, code, http.StatusMultipleChoices)

		_, got := e.do(t, http.MethodGet, ehARM+"pri/disasterRecoveryConfigs/failalias", nil)
		assert.Equal(t, "PrimaryNotReplicating", props(got)["role"])
	})
}

func TestEventHubDisasterRecoveryNameAvailability(t *testing.T) {
	t.Parallel()

	e := newEHDREnv(t)
	status, out := e.do(t, http.MethodPut, ehARM+"pri/disasterRecoveryConfigs/takenalias", drBody("sec"))
	require.Equal(t, http.StatusOK, status, out)

	tests := []struct {
		name   string
		in     string
		reason string
		want   bool
	}{
		{name: "free", in: "freealias", reason: "None", want: true},
		{name: "alias taken", in: "takenalias", reason: "NameInUse"},
		{name: "short", in: "pri", reason: "InvalidName"},
		{name: "invalid", in: "-bad-", reason: "InvalidName"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, got := e.do(t, http.MethodPost, ehARM+"pri/disasterRecoveryConfigs/checkNameAvailability",
				map[string]any{"name": tt.in})
			require.Equal(t, http.StatusOK, code, got)
			assert.Equal(t, tt.want, got["nameAvailable"])
			assert.Equal(t, tt.reason, got["reason"])
		})
	}

	status, _ = e.do(t, http.MethodPost, ehARM+"pri/disasterRecoveryConfigs/checkNameAvailability", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, status)
}

func TestEventHubDisasterRecoveryAuthorizationRules(t *testing.T) {
	t.Parallel()

	e := newEHDREnv(t)
	e.mustPut(t, "pri/authorizationRules/root", map[string]any{"properties": map[string]any{"rights": []any{"Listen"}}})

	code, out := e.do(t, http.MethodPut, ehARM+"pri/disasterRecoveryConfigs/rulesalias", drBody("sec"))
	require.Equal(t, http.StatusOK, code, out)

	base := ehARM + "pri/disasterRecoveryConfigs/rulesalias/authorizationRules"

	code, list := e.do(t, http.MethodGet, base, nil)
	require.Equal(t, http.StatusOK, code, list)
	require.Len(t, list["value"], 1)

	code, one := e.do(t, http.MethodGet, base+"/root", nil)
	require.Equal(t, http.StatusOK, code, one)
	assert.Equal(t, []any{"Listen"}, props(one)["rights"])

	code, keys := e.do(t, http.MethodPost, base+"/root/listKeys", nil)
	require.Equal(t, http.StatusOK, code, keys)
	assert.Equal(t, "pk-root", keys["primaryKey"])

	code, _ = e.do(t, http.MethodGet, base+"/missing", nil)
	assert.Equal(t, http.StatusNotFound, code)

	code, _ = e.do(t, http.MethodGet, ehARM+"pri/disasterRecoveryConfigs/noalias/authorizationRules", nil)
	assert.Equal(t, http.StatusNotFound, code)
}

func TestEventHubAuthorizationRuleList(t *testing.T) {
	t.Parallel()

	e := newEHDREnv(t)
	rights := map[string]any{"properties": map[string]any{"rights": []any{"Listen"}}}
	e.mustPut(t, "pri/authorizationRules/root", rights)
	e.mustPut(t, "pri/eventhubs/hub", map[string]any{})
	e.mustPut(t, "pri/eventhubs/hub/authorizationRules/send", rights)

	code, list := e.do(t, http.MethodGet, ehARM+"pri/authorizationRules", nil)
	require.Equal(t, http.StatusOK, code, list)
	assert.Len(t, list["value"], 1)

	code, list = e.do(t, http.MethodGet, ehARM+"pri/eventhubs/hub/authorizationRules", nil)
	require.Equal(t, http.StatusOK, code, list)
	assert.Len(t, list["value"], 1)
}

func TestEventHubDisasterRecoveryPersistence(t *testing.T) {
	t.Parallel()

	e := newEHDREnv(t)
	code, out := e.do(t, http.MethodPut, ehARM+"pri/disasterRecoveryConfigs/persist-alias", drBody("sec"))
	require.Equal(t, http.StatusOK, code, out)

	p, ok := azurearm.EventHubProviderOf(e.h)
	require.True(t, ok)

	snap := azurearm.EventHubSnapshotJSON(p)

	p2 := azurearm.NewEventHubProvider(azurearm.EventHubEndpointConfig{}, newFakeEventHubs())
	azurearm.EventHubRestoreJSON(t, p2, snap)

	id := ehID([]string{"namespaces", "disasterRecoveryConfigs"}, "sec", "persist-alias")
	got, err := p2.Get(t.Context(), id)
	require.NoError(t, err)
	assert.Equal(t, "Secondary", props(got)["role"])
}
