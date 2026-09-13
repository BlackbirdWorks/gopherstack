package azurearm_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

// fakeServiceBusEntities is a spy ServiceBusEntities implementation, used to
// assert exactly which of ServiceBusProvider's Put paths call into the data
// plane (namespace Put must NOT; queue/topic/subscription Put MUST).
type fakeServiceBusEntities struct {
	queues        map[string]bool
	topics        map[string]bool
	subscriptions map[string]bool

	calls []string
}

func newFakeServiceBusEntities() *fakeServiceBusEntities {
	return &fakeServiceBusEntities{
		queues:        make(map[string]bool),
		topics:        make(map[string]bool),
		subscriptions: make(map[string]bool),
	}
}

func (f *fakeServiceBusEntities) CreateQueue(name string, _, _ time.Duration, _ int) error {
	f.calls = append(f.calls, "CreateQueue:"+name)
	f.queues[name] = true

	return nil
}

func (f *fakeServiceBusEntities) DeleteQueue(name string) error {
	f.calls = append(f.calls, "DeleteQueue:"+name)
	delete(f.queues, name)

	return nil
}

func (f *fakeServiceBusEntities) QueueExists(name string) bool { return f.queues[name] }

func (f *fakeServiceBusEntities) CreateTopic(name string, _ time.Duration) error {
	f.calls = append(f.calls, "CreateTopic:"+name)
	f.topics[name] = true

	return nil
}

func (f *fakeServiceBusEntities) DeleteTopic(name string) error {
	f.calls = append(f.calls, "DeleteTopic:"+name)
	delete(f.topics, name)

	return nil
}

func (f *fakeServiceBusEntities) TopicExists(name string) bool { return f.topics[name] }

func (f *fakeServiceBusEntities) CreateSubscription(topic, name string, _ time.Duration, _ int) error {
	f.calls = append(f.calls, "CreateSubscription:"+topic+"/"+name)
	f.subscriptions[topic+"/"+name] = true

	return nil
}

func (f *fakeServiceBusEntities) DeleteSubscription(topic, name string) error {
	f.calls = append(f.calls, "DeleteSubscription:"+topic+"/"+name)
	delete(f.subscriptions, topic+"/"+name)

	return nil
}

func (f *fakeServiceBusEntities) SubscriptionExists(topic, name string) bool {
	return f.subscriptions[topic+"/"+name]
}

var _ azurearm.ServiceBusEntities = (*fakeServiceBusEntities)(nil)

func sbNamespaceID(rg, name string) azurearm.ResourceID {
	return azurearm.ResourceID{
		SubscriptionID: "sub1", ResourceGroup: rg, Namespace: "Microsoft.ServiceBus",
		Types: []string{"namespaces"}, Names: []string{name},
	}
}

func sbQueueID(rg, ns, name string) azurearm.ResourceID {
	return azurearm.ResourceID{
		SubscriptionID: "sub1", ResourceGroup: rg, Namespace: "Microsoft.ServiceBus",
		Types: []string{"namespaces", "queues"}, Names: []string{ns, name},
	}
}

// sbTopicID's ResourceGroup and topic name are always "rg1"/"t1" -- every
// test call site needs the same values, so they're hardcoded rather than
// parameters.
func sbTopicID(ns string) azurearm.ResourceID {
	return azurearm.ResourceID{
		SubscriptionID: "sub1", ResourceGroup: "rg1", Namespace: "Microsoft.ServiceBus",
		Types: []string{"namespaces", "topics"}, Names: []string{ns, "t1"},
	}
}

func sbAuthRuleID(rg, ns, name string) azurearm.ResourceID {
	return azurearm.ResourceID{
		SubscriptionID: "sub1", ResourceGroup: rg, Namespace: "Microsoft.ServiceBus",
		Types: []string{"namespaces", "authorizationRules"}, Names: []string{ns, name},
	}
}

// sbSubscriptionID's ResourceGroup is always "rg1" -- every test call site
// needs the same resource group, so it's hardcoded rather than a parameter.
func sbSubscriptionID(ns, topic, name string) azurearm.ResourceID {
	return azurearm.ResourceID{
		SubscriptionID: "sub1", ResourceGroup: "rg1", Namespace: "Microsoft.ServiceBus",
		Types: []string{"namespaces", "topics", "subscriptions"}, Names: []string{ns, topic, name},
	}
}

// sbNetworkRuleSetID addresses namespaces/{ns}/networkRuleSets/default -- the
// only name real Azure (and terraform-provider-azurerm@v4.81.0's
// NamespacesClient.GetNetworkRuleSet, which hardcodes the "default" path
// segment) ever uses. ResourceGroup is likewise always "rg1" -- every test
// call site needs the same resource group, so it's hardcoded rather than a
// parameter.
func sbNetworkRuleSetID(ns string) azurearm.ResourceID {
	return azurearm.ResourceID{
		SubscriptionID: "sub1", ResourceGroup: "rg1", Namespace: "Microsoft.ServiceBus",
		Types: []string{"namespaces", "networkRuleSets"}, Names: []string{ns, "default"},
	}
}

func TestServiceBusProvider_NamespacePutGetDelete(t *testing.T) {
	t.Parallel()

	fake := newFakeServiceBusEntities()
	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{Port: 10003}, fake)
	ctx := t.Context()
	id := sbNamespaceID("rg1", "ns1")

	body, err := sp.Put(ctx, id, map[string]any{
		"location": "westus",
		"tags":     map[string]any{"env": "dev"},
		"sku":      map[string]any{"name": "Standard", "tier": "Standard"},
	})
	require.NoError(t, err)
	assert.Equal(t, "ns1", body["name"])
	assert.Equal(t, "Microsoft.ServiceBus/namespaces", body["type"])
	assert.Equal(t, "westus", body["location"])

	props, ok := body["properties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Succeeded", props["provisioningState"])
	assert.Equal(t, "Active", props["status"])
	assert.NotEmpty(t, props["serviceBusEndpoint"])

	// Critical assertion: namespace Put must NOT call into ServiceBusEntities.
	assert.Empty(t, fake.calls, "namespace Put must not call ServiceBusEntities")

	got, err := sp.Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, body["id"], got["id"])

	require.NoError(t, sp.Delete(ctx, id))

	_, err = sp.Get(ctx, id)
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)

	assert.Empty(t, fake.calls, "namespace Delete must not call ServiceBusEntities")
}

func TestServiceBusProvider_Namespace_NotFound(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)

	_, err := sp.Get(t.Context(), sbNamespaceID("rg1", "missing"))
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)

	err = sp.Delete(t.Context(), sbNamespaceID("rg1", "missing"))
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)
}

func TestServiceBusProvider_QueuePutGetDelete(t *testing.T) {
	t.Parallel()

	fake := newFakeServiceBusEntities()
	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, fake)
	ctx := t.Context()

	_, err := sp.Put(ctx, sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)
	fake.calls = nil // reset after namespace create, so the queue assertion below is unambiguous

	id := sbQueueID("rg1", "ns1", "q1")
	body, err := sp.Put(ctx, id, map[string]any{
		"properties": map[string]any{
			"lockDuration":             "PT30S",
			"defaultMessageTimeToLive": "P1D",
			"maxDeliveryCount":         float64(10),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "q1", body["name"])
	assert.Equal(t, "Microsoft.ServiceBus/namespaces/queues", body["type"])

	props, ok := body["properties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "PT30S", props["lockDuration"])
	assert.Equal(t, "P1D", props["defaultMessageTimeToLive"])
	assert.InDelta(t, 10, props["maxDeliveryCount"], 0)

	// Critical assertion: queue Put MUST call into ServiceBusEntities.
	require.Contains(t, fake.calls, "CreateQueue:q1")
	assert.True(t, fake.QueueExists("q1"))

	got, err := sp.Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, body["id"], got["id"])

	require.NoError(t, sp.Delete(ctx, id))
	assert.Contains(t, fake.calls, "DeleteQueue:q1")
	assert.False(t, fake.QueueExists("q1"))

	_, err = sp.Get(ctx, id)
	require.ErrorIs(t, err, azurearm.ErrServiceBusQueueNotFound)
}

func TestServiceBusProvider_Queue_ParentNamespaceMissing(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)

	_, err := sp.Put(t.Context(), sbQueueID("rg1", "missingns", "q1"), map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)
}

func TestServiceBusProvider_TopicPutGetDelete(t *testing.T) {
	t.Parallel()

	fake := newFakeServiceBusEntities()
	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, fake)
	ctx := t.Context()

	_, err := sp.Put(ctx, sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)
	fake.calls = nil

	id := sbTopicID("ns1")
	body, err := sp.Put(ctx, id, map[string]any{
		"properties": map[string]any{"defaultMessageTimeToLive": "P1D"},
	})
	require.NoError(t, err)
	assert.Equal(t, "t1", body["name"])
	assert.Equal(t, "Microsoft.ServiceBus/namespaces/topics", body["type"])

	props, ok := body["properties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "P1D", props["defaultMessageTimeToLive"])

	// Critical assertion: topic Put MUST call into ServiceBusEntities.
	require.Contains(t, fake.calls, "CreateTopic:t1")
	assert.True(t, fake.TopicExists("t1"))

	got, err := sp.Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, body["id"], got["id"])

	require.NoError(t, sp.Delete(ctx, id))
	assert.Contains(t, fake.calls, "DeleteTopic:t1")

	_, err = sp.Get(ctx, id)
	require.ErrorIs(t, err, azurearm.ErrServiceBusTopicNotFound)
}

func TestServiceBusProvider_Topic_ParentNamespaceMissing(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)

	_, err := sp.Put(t.Context(), sbTopicID("missingns"), map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)
}

func TestServiceBusProvider_SubscriptionPutGetDelete(t *testing.T) {
	t.Parallel()

	fake := newFakeServiceBusEntities()
	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, fake)
	ctx := t.Context()

	_, err := sp.Put(ctx, sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)
	_, err = sp.Put(ctx, sbTopicID("ns1"), map[string]any{})
	require.NoError(t, err)
	fake.calls = nil

	id := sbSubscriptionID("ns1", "t1", "sub1")
	body, err := sp.Put(ctx, id, map[string]any{
		"properties": map[string]any{
			"lockDuration":     "PT1M",
			"maxDeliveryCount": float64(5),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "sub1", body["name"])
	assert.Equal(t, "Microsoft.ServiceBus/namespaces/topics/subscriptions", body["type"])

	props, ok := body["properties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "PT1M", props["lockDuration"])
	assert.InDelta(t, 5, props["maxDeliveryCount"], 0)

	// Critical assertion: subscription Put MUST call into ServiceBusEntities.
	require.Contains(t, fake.calls, "CreateSubscription:t1/sub1")
	assert.True(t, fake.SubscriptionExists("t1", "sub1"))

	got, err := sp.Get(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, body["id"], got["id"])

	require.NoError(t, sp.Delete(ctx, id))
	assert.Contains(t, fake.calls, "DeleteSubscription:t1/sub1")

	_, err = sp.Get(ctx, id)
	require.ErrorIs(t, err, azurearm.ErrServiceBusSubscriptionNotFound)
}

func TestServiceBusProvider_Subscription_ParentTopicMissing(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)
	ctx := t.Context()

	_, err := sp.Put(ctx, sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)

	_, err = sp.Put(ctx, sbSubscriptionID("ns1", "missingtopic", "s1"), map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrServiceBusTopicNotFound)
}

func TestServiceBusProvider_Subscription_ParentNamespaceMissing(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)

	_, err := sp.Put(t.Context(), sbSubscriptionID("missingns", "t1", "s1"), map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)
}

func TestServiceBusProvider_AuthRule_ListKeys(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{Port: 10003}, nil)
	ctx := t.Context()

	_, err := sp.Put(ctx, sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)

	ruleID := sbAuthRuleID("rg1", "ns1", "RootManageSharedAccessKey")
	body, err := sp.Put(ctx, ruleID, map[string]any{})
	require.NoError(t, err)
	assert.Equal(t, "RootManageSharedAccessKey", body["name"])

	resp, err := sp.ListKeys(ctx, ruleID)
	require.NoError(t, err)
	assert.Equal(t, "RootManageSharedAccessKey", resp["keyName"])
	assert.NotEmpty(t, resp["primaryKey"])
	assert.Equal(t, resp["primaryKey"], resp["secondaryKey"])
	assert.NotEmpty(t, resp["primaryConnectionString"])
	assert.Equal(t, resp["primaryConnectionString"], resp["secondaryConnectionString"])
	assert.Contains(t, resp["primaryConnectionString"], "Endpoint=")
}

func TestServiceBusProvider_AuthRule_ParentNamespaceMissing(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)

	_, err := sp.ListKeys(t.Context(), sbAuthRuleID("rg1", "missingns", "rule1"))
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)
}

func TestServiceBusProvider_ListKeys_TypeValidation(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)

	_, err := sp.ListKeys(t.Context(), sbNamespaceID("rg1", "ns1"))
	require.Error(t, err)
}

func TestServiceBusProvider_List(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)
	ctx := t.Context()

	_, err := sp.Put(ctx, sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)
	_, err = sp.Put(ctx, sbQueueID("rg1", "ns1", "zeta"), map[string]any{})
	require.NoError(t, err)
	_, err = sp.Put(ctx, sbQueueID("rg1", "ns1", "alpha"), map[string]any{})
	require.NoError(t, err)

	values, err := sp.List(ctx, sbQueueID("rg1", "ns1", ""))
	require.NoError(t, err)
	require.Len(t, values, 2)
	assert.Equal(t, "alpha", values[0]["name"])
	assert.Equal(t, "zeta", values[1]["name"])
}

func TestServiceBusProvider_List_ScopedToResourceGroup(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)
	ctx := t.Context()

	_, err := sp.Put(ctx, sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)
	_, err = sp.Put(ctx, sbNamespaceID("rg2", "ns2"), map[string]any{"location": "westus"})
	require.NoError(t, err)

	values, err := sp.List(ctx, sbNamespaceID("rg1", ""))
	require.NoError(t, err)
	require.Len(t, values, 1)
	assert.Equal(t, "ns1", values[0]["name"])
}

// TestServiceBusProvider_ListQueues_ScopedToResourceGroup asserts that
// List(queue) filters by id.ResourceGroup exactly like List(namespace) does
// -- queues/topics/subscriptions in a different resource group must not leak
// into another group's listing (previously an unfiltered map iteration, a
// real bug found during M9 QC).
func TestServiceBusProvider_ListQueues_ScopedToResourceGroup(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)
	ctx := t.Context()

	_, err := sp.Put(ctx, sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)
	_, err = sp.Put(ctx, sbNamespaceID("rg2", "ns2"), map[string]any{"location": "westus"})
	require.NoError(t, err)

	_, err = sp.Put(ctx, sbQueueID("rg1", "ns1", "q1"), map[string]any{})
	require.NoError(t, err)
	_, err = sp.Put(ctx, sbQueueID("rg2", "ns2", "q2"), map[string]any{})
	require.NoError(t, err)

	values, err := sp.List(ctx, sbQueueID("rg1", "ns1", ""))
	require.NoError(t, err)
	require.Len(t, values, 1)
	assert.Equal(t, "q1", values[0]["name"])
	assert.Contains(t, values[0]["id"], "/resourceGroups/rg1/")
}

// TestServiceBusProvider_Get_WrongResourceGroup asserts that a namespace (and
// its children) created under one resource group are not visible or
// deletable via a different resource group's path.
func TestServiceBusProvider_Get_WrongResourceGroup(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)
	ctx := t.Context()

	_, err := sp.Put(ctx, sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)
	_, err = sp.Put(ctx, sbQueueID("rg1", "ns1", "q1"), map[string]any{})
	require.NoError(t, err)

	_, err = sp.Get(ctx, sbNamespaceID("rg2", "ns1"))
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)

	err = sp.Delete(ctx, sbNamespaceID("rg2", "ns1"))
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)

	// A queue put via rg2's path, even naming the same namespace/queue names,
	// must not see rg1's namespace as an existing parent.
	_, err = sp.Put(ctx, sbQueueID("rg2", "ns1", "q1"), map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)

	_, err = sp.Get(ctx, sbQueueID("rg2", "ns1", "q1"))
	require.ErrorIs(t, err, azurearm.ErrServiceBusQueueNotFound)
}

func TestServiceBusProvider_Reset(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)
	ctx := t.Context()

	_, err := sp.Put(ctx, sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)
	_, err = sp.Put(ctx, sbQueueID("rg1", "ns1", "q1"), map[string]any{})
	require.NoError(t, err)

	sp.Reset()

	_, err = sp.Get(ctx, sbNamespaceID("rg1", "ns1"))
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)

	_, err = sp.Get(ctx, sbQueueID("rg1", "ns1", "q1"))
	require.ErrorIs(t, err, azurearm.ErrServiceBusQueueNotFound)
}

func TestServiceBusProvider_DeleteResourcesInGroup(t *testing.T) {
	t.Parallel()

	fake := newFakeServiceBusEntities()
	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, fake)
	ctx := t.Context()

	_, err := sp.Put(ctx, sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)
	_, err = sp.Put(ctx, sbQueueID("rg1", "ns1", "q1"), map[string]any{})
	require.NoError(t, err)
	_, err = sp.Put(ctx, sbTopicID("ns1"), map[string]any{})
	require.NoError(t, err)
	_, err = sp.Put(ctx, sbSubscriptionID("ns1", "t1", "s1"), map[string]any{})
	require.NoError(t, err)

	_, err = sp.Put(ctx, sbNamespaceID("rg2", "ns2"), map[string]any{"location": "westus"})
	require.NoError(t, err)

	sp.DeleteResourcesInGroup(ctx, "rg1")

	_, err = sp.Get(ctx, sbNamespaceID("rg1", "ns1"))
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)
	_, err = sp.Get(ctx, sbQueueID("rg1", "ns1", "q1"))
	require.ErrorIs(t, err, azurearm.ErrServiceBusQueueNotFound)
	_, err = sp.Get(ctx, sbTopicID("ns1"))
	require.ErrorIs(t, err, azurearm.ErrServiceBusTopicNotFound)
	_, err = sp.Get(ctx, sbSubscriptionID("ns1", "t1", "s1"))
	require.ErrorIs(t, err, azurearm.ErrServiceBusSubscriptionNotFound)

	assert.Contains(t, fake.calls, "DeleteQueue:q1")
	assert.Contains(t, fake.calls, "DeleteTopic:t1")
	assert.Contains(t, fake.calls, "DeleteSubscription:t1/s1")

	// rg2's namespace must survive.
	_, err = sp.Get(ctx, sbNamespaceID("rg2", "ns2"))
	require.NoError(t, err)
}

func TestServiceBusProvider_TypeValidation(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)
	ctx := t.Context()

	badID := azurearm.ResourceID{
		SubscriptionID: "sub1", ResourceGroup: "rg1", Namespace: "Microsoft.ServiceBus",
		Types: []string{"notNamespaces"}, Names: []string{"whatever"},
	}

	_, err := sp.Put(ctx, badID, map[string]any{})
	require.Error(t, err)

	_, err = sp.Get(ctx, badID)
	require.Error(t, err)

	err = sp.Delete(ctx, badID)
	require.Error(t, err)

	_, err = sp.ListKeys(ctx, badID)
	require.Error(t, err)
}

func TestServiceBusProvider_NamespaceAndResourceTypes(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)

	assert.Equal(t, "Microsoft.ServiceBus", sp.Namespace())

	types := sp.ResourceTypes()
	require.NotEmpty(t, types)

	for _, def := range types {
		assert.NotEmpty(t, def.APIVersions)
	}
}

func TestServiceBusProvider_EndpointOverride(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{
		Override: "servicebus.example.com:18003",
	}, nil)

	body, err := sp.Put(t.Context(), sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)

	props, ok := body["properties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "http://servicebus.example.com:18003/", props["serviceBusEndpoint"])
}

// TestServiceBusProvider_NetworkRuleSet_GetWithoutPut is the substitute
// confirmation for the CI failure this fix addresses (PR #2466's
// terraform-tests (6) job): "retrieving network rule set Namespace ...
// unexpected status 404". terraform-provider-azurerm@v4.81.0's
// resourceServiceBusNamespaceFlatten (internal/services/servicebus/
// servicebus_namespace_resource.go) calls client.GetNetworkRuleSet on every
// namespace Read, unconditionally -- regardless of SKU and regardless of
// whether the caller's config ever sets a `network_rule_set` block (this
// milestone's own fixture, test/terraform/azure/servicebus_test.go, sets
// neither). A GET against namespaces/{ns}/networkRuleSets/default therefore
// has to succeed even when no Put was ever made against that sub-resource,
// which this test asserts directly by calling Get with no prior
// networkRuleSets Put -- only the sibling namespace Put that real Terraform
// always does first.
func TestServiceBusProvider_NetworkRuleSet_GetWithoutPut(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)
	ctx := t.Context()

	_, err := sp.Put(ctx, sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)

	id := sbNetworkRuleSetID("ns1")

	got, err := sp.Get(ctx, id)
	require.NoError(t, err, "GET networkRuleSets/default must succeed even when no PUT was ever made against it")
	assert.Equal(t, "default", got["name"])
	assert.Equal(t, "Microsoft.ServiceBus/namespaces/networkRuleSets", got["type"])

	props, ok := got["properties"].(map[string]any)
	require.True(t, ok)
	// Defaults matching a namespace with no custom network rules configured
	// in real Azure -- see buildNetworkRuleSetBody's doc comment.
	assert.Equal(t, "Allow", props["defaultAction"])
	assert.Equal(t, "Enabled", props["publicNetworkAccess"])
	assert.Equal(t, false, props["trustedServiceAccessEnabled"])
	assert.Empty(t, props["ipRules"])
	assert.Empty(t, props["virtualNetworkRules"])
}

// TestServiceBusProvider_NetworkRuleSet_PutThenGet confirms a Put'd
// networkRuleSets/default body decodes into
// hashicorp/go-azure-sdk@resource-manager/servicebus/2024-01-01/namespaces's
// NetworkRuleSetProperties field names and round-trips through a subsequent
// Get, exercising the path terraform-provider-azurerm's
// createNetworkRuleSetForNamespace takes when a caller's config does set a
// `network_rule_set` block.
func TestServiceBusProvider_NetworkRuleSet_PutThenGet(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)
	ctx := t.Context()

	_, err := sp.Put(ctx, sbNamespaceID("rg1", "ns1"), map[string]any{"location": "westus"})
	require.NoError(t, err)

	id := sbNetworkRuleSetID("ns1")

	putBody, err := sp.Put(ctx, id, map[string]any{
		"properties": map[string]any{
			"defaultAction":               "Deny",
			"publicNetworkAccess":         "Disabled",
			"trustedServiceAccessEnabled": true,
			"ipRules":                     []any{map[string]any{"ipMask": "10.0.0.0/24", "action": "Allow"}},
		},
	})
	require.NoError(t, err)

	props, ok := putBody["properties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Deny", props["defaultAction"])
	assert.Equal(t, "Disabled", props["publicNetworkAccess"])
	assert.Equal(t, true, props["trustedServiceAccessEnabled"])
	assert.Len(t, props["ipRules"], 1)

	got, err := sp.Get(ctx, id)
	require.NoError(t, err)
	gotProps, ok := got["properties"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Deny", gotProps["defaultAction"])
}

func TestServiceBusProvider_NetworkRuleSet_ParentNamespaceMissing(t *testing.T) {
	t.Parallel()

	sp := azurearm.NewServiceBusProvider(azurearm.ServiceBusEndpointConfig{}, nil)

	_, err := sp.Get(t.Context(), sbNetworkRuleSetID("missingns"))
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)

	_, err = sp.Put(t.Context(), sbNetworkRuleSetID("missingns"), map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrServiceBusNamespaceNotFound)
}
