package azurearm_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

var errNoRights = errors.New("no rights")

type fakeEventHubs struct {
	hubs   map[string]azurearm.EventHubInfo
	rules  map[string]azurearm.EventHubRule
	groups map[string]bool
	nss    map[string]bool
}

func newFakeEventHubs() *fakeEventHubs {
	return &fakeEventHubs{
		hubs: map[string]azurearm.EventHubInfo{}, rules: map[string]azurearm.EventHubRule{},
		groups: map[string]bool{}, nss: map[string]bool{},
	}
}

func (f *fakeEventHubs) EnsureNamespace(n string) error {
	f.nss[n] = true

	return nil
}

func (f *fakeEventHubs) DeleteNamespace(n string) error {
	delete(f.nss, n)

	return nil
}

func (f *fakeEventHubs) CreateHub(ns, name string, count int, ret time.Duration) (azurearm.EventHubInfo, error) {
	if count == 0 {
		count = 4
	}

	if ret == 0 {
		ret = 24 * time.Hour
	}

	ids := make([]string, count)
	for i := range ids {
		ids[i] = strconv.Itoa(i)
	}

	info := azurearm.EventHubInfo{CreatedAt: time.Unix(1700000000, 0), PartitionIDs: ids, Retention: ret}
	f.hubs[ns+"/"+name] = info

	return info, nil
}

func (f *fakeEventHubs) HubInfo(ns, name string) (azurearm.EventHubInfo, bool) {
	i, ok := f.hubs[ns+"/"+name]

	return i, ok
}

func (f *fakeEventHubs) DeleteHub(ns, name string) error {
	delete(f.hubs, ns+"/"+name)

	return nil
}

func (f *fakeEventHubs) CreateConsumerGroup(ns, hub, name string) error {
	f.groups[ns+"/"+hub+"/"+name] = true

	return nil
}

func (f *fakeEventHubs) DeleteConsumerGroup(ns, hub, name string) error {
	delete(f.groups, ns+"/"+hub+"/"+name)

	return nil
}

func (f *fakeEventHubs) PutRule(ns, hub, name string, rights []string) (azurearm.EventHubRule, error) {
	if len(rights) == 0 {
		return azurearm.EventHubRule{}, errNoRights
	}

	r := azurearm.EventHubRule{PrimaryKey: "pk-" + name, SecondaryKey: "sk-" + name, Rights: rights}
	f.rules[ns+"/"+hub+"/"+name] = r

	return r, nil
}

func (f *fakeEventHubs) GetRule(ns, hub, name string) (azurearm.EventHubRule, bool) {
	r, ok := f.rules[ns+"/"+hub+"/"+name]

	return r, ok
}

func (f *fakeEventHubs) RegenerateRuleKey(ns, hub, name, keyType, key string) (azurearm.EventHubRule, error) {
	r, ok := f.rules[ns+"/"+hub+"/"+name]
	if !ok {
		return azurearm.EventHubRule{}, azurearm.ErrEventHubRuleNotFound
	}

	if key == "" {
		key = "fresh-" + keyType
	}

	switch keyType {
	case "PrimaryKey":
		r.PrimaryKey = key
	case "SecondaryKey":
		r.SecondaryKey = key
	default:
		return azurearm.EventHubRule{}, errNoRights
	}

	f.rules[ns+"/"+hub+"/"+name] = r

	return r, nil
}

func (f *fakeEventHubs) DeleteRule(ns, hub, name string) error {
	delete(f.rules, ns+"/"+hub+"/"+name)

	return nil
}

func ehID(types []string, names ...string) azurearm.ResourceID {
	return azurearm.ResourceID{
		SubscriptionID: "sub", ResourceGroup: "rg", Namespace: "Microsoft.EventHub", Types: types, Names: names,
	}
}

func props(body map[string]any) map[string]any {
	p, _ := body["properties"].(map[string]any)

	return p
}

func TestEventHubProvider_Lifecycle(t *testing.T) {
	t.Parallel()

	dp := newFakeEventHubs()
	p := azurearm.NewEventHubProvider(azurearm.EventHubEndpointConfig{Override: "host.example:15673"}, dp)
	ctx := azurearm.WithRequestHost(t.Context(), "myhost")

	nsID := ehID([]string{"namespaces"}, "ns")
	hubID := ehID([]string{"namespaces", "eventhubs"}, "ns", "hub")
	cgID := ehID([]string{"namespaces", "eventhubs", "consumergroups"}, "ns", "hub", "cg")
	nsRule := ehID([]string{"namespaces", "authorizationRules"}, "ns", "root")
	hubRule := ehID([]string{"namespaces", "eventhubs", "authorizationRules"}, "ns", "hub", "send")

	_, err := p.Put(ctx, hubID, map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrEventHubNamespaceNotFound, "parent must exist")

	body, err := p.Put(ctx, nsID, map[string]any{"location": "westus", "sku": map[string]any{"name": "Basic"}})
	require.NoError(t, err)
	assert.Equal(t, "westus", body["location"])
	assert.True(t, dp.nss["ns"])
	assert.Equal(t, "http://myhost:10005/", props(body)["serviceBusEndpoint"])

	body, err = p.Put(ctx, hubID, map[string]any{"properties": map[string]any{
		"partitionCount": float64(3), "messageRetentionInDays": float64(2), "status": "Active", "userMetadata": "meta",
	}})
	require.NoError(t, err)

	hp := props(body)
	assert.Equal(t, 3, hp["partitionCount"])
	assert.Equal(t, 2, hp["messageRetentionInDays"])
	assert.Equal(t, []string{"0", "1", "2"}, hp["partitionIds"])
	assert.Equal(t, "meta", hp["userMetadata"])
	assert.Equal(t, 48, hp["retentionDescription"].(map[string]any)["retentionTimeInHours"])

	body, err = p.Put(ctx, hubID, map[string]any{"properties": map[string]any{
		"partitionCount": float64(2), "retentionDescription": map[string]any{"retentionTimeInHours": float64(6)},
	}})
	require.NoError(t, err)
	assert.Equal(t, 1, props(body)["messageRetentionInDays"], "sub-day retention rounds up")

	_, err = p.Put(ctx, cgID, map[string]any{"properties": map[string]any{"userMetadata": "cgmeta"}})
	require.NoError(t, err)
	assert.True(t, dp.groups["ns/hub/cg"])

	got, err := p.Get(ctx, cgID)
	require.NoError(t, err)
	assert.Equal(t, "cgmeta", props(got)["userMetadata"])

	rights := map[string]any{"properties": map[string]any{"rights": []any{"Listen", "Send"}}}
	_, err = p.Put(ctx, nsRule, rights)
	require.NoError(t, err)
	_, err = p.Put(ctx, hubRule, map[string]any{"properties": map[string]any{"rights": []any{}}})
	require.Error(t, err)
	require.ErrorIs(t, err, azurearm.ErrInvalidRequestBody)

	_, err = p.Put(ctx, hubRule, rights)
	require.NoError(t, err)

	keys, err := p.ListKeys(ctx, hubRule)
	require.NoError(t, err)
	assert.Equal(t, "pk-send", keys["primaryKey"])
	assert.Equal(
		t,
		"Endpoint=sb://host.example:15673/;SharedAccessKeyName=send;SharedAccessKey=pk-send;EntityPath=hub;"+
			"UseDevelopmentEmulator=true",
		keys["primaryConnectionString"],
	)

	_, err = p.ListKeys(ctx, hubID)
	require.ErrorIs(t, err, azurearm.ErrResourceNotFound)

	list, err := p.List(ctx, ehID([]string{"namespaces", "eventhubs"}, "ns"))
	require.NoError(t, err)
	require.Len(t, list, 1)

	nrs, err := p.Get(ctx, ehID([]string{"namespaces", "networkRuleSets"}, "ns", "default"))
	require.NoError(t, err)
	assert.Equal(t, "Allow", props(nrs)["defaultAction"])

	require.NoError(t, p.Delete(ctx, hubRule))
	_, err = p.Get(ctx, hubRule)
	require.ErrorIs(t, err, azurearm.ErrEventHubRuleNotFound)

	require.NoError(t, p.Delete(ctx, cgID))
	_, err = p.Get(ctx, cgID)
	require.ErrorIs(t, err, azurearm.ErrEventHubConsumerGroupNotFound)

	require.NoError(t, p.Delete(ctx, hubID))
	_, err = p.Get(ctx, hubID)
	require.ErrorIs(t, err, azurearm.ErrEventHubNotFound)

	require.NoError(t, p.Delete(ctx, nsID))
	require.ErrorIs(t, p.Delete(ctx, nsID), azurearm.ErrEventHubNamespaceNotFound)
	assert.False(t, dp.nss["ns"])
}

func TestEventHubProvider_GroupCascadeAndPersistence(t *testing.T) {
	t.Parallel()

	dp := newFakeEventHubs()
	p := azurearm.NewEventHubProvider(azurearm.EventHubEndpointConfig{}, dp)
	ctx := context.Background()

	_, err := p.Put(ctx, ehID([]string{"namespaces"}, "ns"), map[string]any{"tags": map[string]any{"a": "b"}})
	require.NoError(t, err)
	_, err = p.Put(ctx, ehID([]string{"namespaces", "eventhubs"}, "ns", "hub"), map[string]any{})
	require.NoError(t, err)

	snap := azurearm.EventHubSnapshotJSON(p)

	p2 := azurearm.NewEventHubProvider(azurearm.EventHubEndpointConfig{}, dp)
	azurearm.EventHubRestoreJSON(t, p2, snap)

	got, err := p2.Get(ctx, ehID([]string{"namespaces"}, "ns"))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "b"}, got["tags"])

	_, err = p2.Get(ctx, ehID([]string{"namespaces", "eventhubs"}, "ns", "hub"))
	require.NoError(t, err)

	p2.DeleteResourcesInGroup(ctx, "RG")
	assert.False(t, dp.nss["ns"], "namespace removed from the data plane with its group")

	_, err = p2.Get(ctx, ehID([]string{"namespaces"}, "ns"))
	require.ErrorIs(t, err, azurearm.ErrEventHubNamespaceNotFound)

	p.Reset()
	_, err = p.Get(ctx, ehID([]string{"namespaces"}, "ns"))
	require.ErrorIs(t, err, azurearm.ErrEventHubNamespaceNotFound)

	_, err = p.Get(ctx, ehID([]string{"namespaces", "bogus"}, "ns", "x"))
	require.ErrorIs(t, err, azurearm.ErrResourceNotFound)
}

func TestEventHubRegenerateKeys(t *testing.T) {
	t.Parallel()

	const base = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.EventHub/namespaces/ns"

	tests := []struct {
		body     map[string]any
		name     string
		path     string
		wantKey  string
		wantCode int
		wantNew  bool
	}{
		{name: "namespace primary", path: "/authorizationRules/root", body: map[string]any{"keyType": "PrimaryKey"},
			wantCode: http.StatusOK, wantNew: true},
		{name: "hub secondary explicit", path: "/eventhubs/hub/authorizationRules/send",
			body:     map[string]any{"keyType": "SecondaryKey", "key": "my-own-key"},
			wantCode: http.StatusOK, wantKey: "my-own-key"},
		{name: "bad key type", path: "/authorizationRules/root", body: map[string]any{"keyType": "Tertiary"},
			wantCode: http.StatusBadRequest},
		{name: "missing rule", path: "/authorizationRules/nope", body: map[string]any{"keyType": "PrimaryKey"},
			wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dp := newFakeEventHubs()
			h := azurearm.NewTestHandlerWithEventHubs(dp)
			ctx := t.Context()
			rights := map[string]any{"properties": map[string]any{"rights": []any{"Listen", "Send"}}}
			do := func(method, path string, body any) (int, map[string]any) {
				raw, err := json.Marshal(body)
				require.NoError(t, err)

				req := httptest.NewRequestWithContext(ctx, method, path+"?api-version=2024-01-01",
					bytes.NewReader(raw))
				rec := httptest.NewRecorder()
				newEchoServer(h).ServeHTTP(rec, req)

				var out map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &out)

				return rec.Code, out
			}

			for _, step := range []struct {
				body any
				path string
			}{
				{path: "", body: map[string]any{"location": "l"}},
				{path: "/eventhubs/hub", body: map[string]any{}},
				{path: "/authorizationRules/root", body: rights},
				{path: "/eventhubs/hub/authorizationRules/send", body: rights},
			} {
				code, out := do(http.MethodPut, base+step.path, step.body)
				require.Less(t, code, http.StatusMultipleChoices, out)
			}

			code, out := do(http.MethodPost, base+tt.path+"/regenerateKeys", tt.body)
			require.Equal(t, tt.wantCode, code, out)

			if tt.wantCode != http.StatusOK {
				return
			}

			field := "primaryKey"
			if tt.body["keyType"] == "SecondaryKey" {
				field = "secondaryKey"
			}

			if tt.wantNew {
				assert.Equal(t, "fresh-PrimaryKey", out[field])
			} else {
				assert.Equal(t, tt.wantKey, out[field])
			}

			_, listed := do(http.MethodPost, base+tt.path+"/listKeys", nil)
			assert.Equal(t, out[field], listed[field])
		})
	}
}

func TestEventHubNamespaceIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		identity map[string]any
		name     string
		wantPID  bool
	}{
		{name: "system assigned", identity: map[string]any{"type": "SystemAssigned"}, wantPID: true},
		{name: "user assigned", identity: map[string]any{
			"type":                   "UserAssigned",
			"userAssignedIdentities": map[string]any{"/subscriptions/s/uai": map[string]any{}},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := azurearm.NewEventHubProvider(azurearm.EventHubEndpointConfig{}, newFakeEventHubs())
			nsID := ehID([]string{"namespaces"}, "ns")

			body, err := p.Put(t.Context(), nsID, map[string]any{"identity": tt.identity})
			require.NoError(t, err)

			got, _ := body["identity"].(map[string]any)
			assert.Equal(t, tt.identity["type"], got["type"])
			assert.Equal(t, tt.wantPID, got["principalId"] != nil)

			snap := azurearm.EventHubSnapshotJSON(p)
			p2 := azurearm.NewEventHubProvider(azurearm.EventHubEndpointConfig{}, newFakeEventHubs())
			azurearm.EventHubRestoreJSON(t, p2, snap)

			again, err := p2.Get(t.Context(), nsID)
			require.NoError(t, err)
			assert.Equal(t, tt.identity["type"], again["identity"].(map[string]any)["type"])

			body, err = p.Put(t.Context(), nsID, map[string]any{})
			require.NoError(t, err)
			assert.Contains(t, body, "identity", "an update without identity keeps it")
		})
	}
}
