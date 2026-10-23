package azurearm_test

import (
	"context"
	"errors"
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
