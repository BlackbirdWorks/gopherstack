package azurearm_test

import (
	"net/http"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type legacyEntry struct {
	body map[string]any
	ns   string
	path string
	list string
}

func legacyBody(loc string, props map[string]any) map[string]any {
	b := map[string]any{"properties": props}
	if loc != "" {
		b["location"] = loc
	}

	return b
}

func legacyEntries() []legacyEntry {
	n := "Microsoft.Network"
	c := "Microsoft.Compute"
	a := "Microsoft.App"
	empty := map[string]any{}
	host := legacyBody("westus", empty)
	host["sku"] = map[string]any{"name": "DSv3-Type1"}

	return []legacyEntry{
		{ns: n, path: "privateLinkServices/pls1", list: "privateLinkServices", body: legacyBody("westus", empty)},
		{ns: n, path: "privateLinkServices/pls-noloc", list: "privateLinkServices", body: legacyBody("", empty)},
		{ns: n, path: "routeFilters/rf1", list: "routeFilters", body: legacyBody("westus", map[string]any{
			"rules": []any{map[string]any{"name": "r0", "properties": map[string]any{"access": "Allow"}}},
		})},
		{ns: n, path: "routeFilters/rf1/routeFilterRules/r1", list: "routeFilters/rf1/routeFilterRules",
			body: legacyBody("", map[string]any{"access": "Allow"})},
		{
			ns:   n,
			path: "serviceEndpointPolicies/sep1",
			list: "serviceEndpointPolicies",
			body: legacyBody("westus", empty),
		},
		{ns: n, path: "serviceEndpointPolicies/sep1/serviceEndpointPolicyDefinitions/d1",
			list: "serviceEndpointPolicies/sep1/serviceEndpointPolicyDefinitions", body: legacyBody("", empty)},
		{ns: n, path: "networkWatchers/w1", list: "networkWatchers", body: legacyBody("westus", empty)},
		{ns: n, path: "networkWatchers/w1/flowLogs/fl1", list: "networkWatchers/w1/flowLogs",
			body: legacyBody("", map[string]any{"targetResourceId": "/gone", "storageId": "/gone"})},
		{ns: n, path: "networkWatchers/w1/packetCaptures/pc1", list: "networkWatchers/w1/packetCaptures",
			body: legacyBody("", empty)},
		{ns: n, path: "virtualNetworkTaps/tap1", list: "virtualNetworkTaps", body: legacyBody("", empty)},
		{ns: n, path: "ddosProtectionPlans/ddos1", list: "ddosProtectionPlans", body: legacyBody("westus", empty)},
		{
			ns:   c,
			path: "proximityPlacementGroups/ppg1",
			list: "proximityPlacementGroups",
			body: legacyBody("westus", empty),
		},
		{ns: c, path: "sshPublicKeys/key1", list: "sshPublicKeys", body: legacyBody("westus", empty)},
		{ns: c, path: "capacityReservationGroups/crg1", list: "capacityReservationGroups", body: legacyBody("", empty)},
		{ns: c, path: "capacityReservationGroups/crg1/capacityReservations/cr1",
			list: "capacityReservationGroups/crg1/capacityReservations", body: legacyBody("westus", empty)},
		{ns: c, path: "hostGroups/hg1", list: "hostGroups", body: legacyBody("westus", empty)},
		{ns: c, path: "hostGroups/hg1/hosts/h1", list: "hostGroups/hg1/hosts", body: host},
		{ns: c, path: "diskEncryptionSets/des1", list: "diskEncryptionSets", body: legacyBody("westus", empty)},
		{ns: c, path: "diskAccesses/da1", list: "diskAccesses", body: legacyBody("westus", empty)},
		{
			ns:   c,
			path: "restorePointCollections/rpc1",
			list: "restorePointCollections",
			body: legacyBody("westus", empty),
		},
		{
			ns:   c,
			path: "restorePointCollections/rpc1/restorePoints/rp1",
			list: "restorePointCollections/rpc1/restorePoints",
			body: legacyBody("", empty),
		},
		{ns: c, path: "galleries/gal1", list: "galleries", body: legacyBody("westus", empty)},
		{
			ns:   c,
			path: "galleries/gal1/applications/app1",
			list: "galleries/gal1/applications",
			body: legacyBody("westus", empty),
		},
		{
			ns:   c,
			path: "galleries/gal1/applications/app1/versions/1.0.0",
			list: "galleries/gal1/applications/app1/versions",
			body: legacyBody("westus", empty),
		},
		{ns: a, path: "sessionPools/pool1", list: "sessionPools", body: legacyBody("", empty)},
		{ns: a, path: "managedEnvironments/env1/daprComponents/d1", list: "managedEnvironments/env1/daprComponents",
			body: legacyBody("", empty)},
		{ns: a, path: "managedEnvironments/env1/certificates/c1", list: "managedEnvironments/env1/certificates",
			body: legacyBody("", empty)},
		{ns: a, path: "managedEnvironments/env1/javaComponents/j1", list: "managedEnvironments/env1/javaComponents",
			body: legacyBody("", empty)},
		{ns: a, path: "containerApps/app1/authConfigs/current", list: "containerApps/app1/authConfigs",
			body: legacyBody("", empty)},
		{ns: a, path: "containerApps/app1/sourcecontrols/current", list: "containerApps/app1/sourcecontrols",
			body: legacyBody("", map[string]any{"repoUrl": "x"})},
		{
			ns:   "Microsoft.ContainerService",
			path: "snapshots/snap1",
			list: "snapshots",
			body: legacyBody("westus", empty),
		},
	}
}

func TestLegacyGenericEntriesMigrateOnRestore(t *testing.T) {
	t.Parallel()

	const api = "?api-version=2024-03-01"

	old := newNetStack(t)
	ctx := t.Context()

	envPath := netProv("Microsoft.App", "managedEnvironments", "env1")
	status, _ := old.rawJSON(t, http.MethodPut, envPath+api, `{"location":"local","properties":{}}`)
	require.Less(t, status, 300)

	status, _ = old.rawJSON(t, http.MethodPut, netProv("Microsoft.App", "containerApps", "app1")+api,
		`{"location":"local","properties":{"environmentId":"`+
			"/subscriptions/"+aciSub+"/resourceGroups/"+netRG+`/providers/Microsoft.App/managedEnvironments/env1`+
			`","template":{"containers":[{"name":"c","image":"nginx"}]}}}`)
	require.Less(t, status, 300)

	entries := legacyEntries()
	for _, e := range entries {
		require.NoError(t, old.handler.PutLegacyGeneric(netProvPath(e.ns, e.path), e.body), e.path)
	}

	checkAll := func(t *testing.T, s *netStack) {
		t.Helper()

		for _, e := range entries {
			url := netProvPath(e.ns, e.path) + api
			code, got := s.rawJSON(t, http.MethodGet, url, "")
			require.Equal(t, http.StatusOK, code, e.path)
			assert.NotEmpty(t, got["name"], e.path)

			code, list := s.rawJSON(t, http.MethodGet, netProvPath(e.ns, e.list)+api, "")
			require.Equal(t, http.StatusOK, code, e.list)

			values, _ := list["value"].([]any)
			found := false

			for _, v := range values {
				m, _ := v.(map[string]any)
				found = found || m["name"] == got["name"]
			}

			assert.True(t, found, e.path)
		}
	}

	first := newNetStack(t)
	require.NoError(t, first.handler.Restore(ctx, old.handler.Snapshot(ctx)))
	checkAll(t, first)

	second := newNetStack(t)
	require.NoError(t, second.handler.Restore(ctx, first.handler.Snapshot(ctx)))
	checkAll(t, second)

	for _, e := range slices.Backward(entries) {
		url := netProvPath(e.ns, e.path) + api

		del, _ := second.rawJSON(t, http.MethodDelete, url, "")
		assert.Less(t, del, 300, e.path)

		gone, _ := second.rawJSON(t, http.MethodGet, url, "")
		assert.Equal(t, http.StatusNotFound, gone, e.path)
	}
}

func netProvPath(ns, rest string) string {
	return "/subscriptions/" + aciSub + "/resourceGroups/" + netRG + "/providers/" + ns + "/" + rest
}
