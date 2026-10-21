package azurearm_test

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

var errBadSchema = errors.New("bad schema")

type fakeEventGrid struct {
	topics map[string]string
	subs   map[string]map[string]any
	pulls  map[string]map[string]any
	nsKeys map[string]string
	nsts   map[string]bool
	nss    map[string]bool
}

func newFakeEventGrid() *fakeEventGrid {
	return &fakeEventGrid{
		topics: map[string]string{}, subs: map[string]map[string]any{}, pulls: map[string]map[string]any{},
		nsKeys: map[string]string{}, nsts: map[string]bool{}, nss: map[string]bool{},
	}
}

func (f *fakeEventGrid) PutTopic(name, _, schema string) (azurearm.EventGridTopic, error) {
	if schema == "Custom" {
		return azurearm.EventGridTopic{}, errBadSchema
	}

	if schema == "" {
		schema = "EventGridSchema"
	}

	f.topics[name] = schema

	return azurearm.EventGridTopic{InputSchema: schema}, nil
}

func (f *fakeEventGrid) TopicInfo(name string) (azurearm.EventGridTopic, bool) {
	s, ok := f.topics[name]

	return azurearm.EventGridTopic{InputSchema: s}, ok
}

func (f *fakeEventGrid) DeleteTopic(name string) error {
	delete(f.topics, name)

	return nil
}

func (f *fakeEventGrid) TopicKeys(string) (string, string, error) { return "k1", "k2", nil }

func (f *fakeEventGrid) RegenerateTopicKey(_, keyName string) (string, string, error) {
	if keyName == "key1" {
		return "new1", "k2", nil
	}

	return "k1", "new2", nil
}

func (f *fakeEventGrid) PutSubscription(
	_ context.Context,
	topic, name string,
	props map[string]any,
) (map[string]any, error) {
	out := map[string]any{"topic": topic, "provisioningState": "Succeeded"}
	maps.Copy(out, props)

	f.subs[topic+"/"+name] = out

	return out, nil
}

func (f *fakeEventGrid) GetSubscription(topic, name string) (map[string]any, bool) {
	s, ok := f.subs[topic+"/"+name]

	return s, ok
}

func (f *fakeEventGrid) DeleteSubscription(topic, name string) error {
	delete(f.subs, topic+"/"+name)

	return nil
}

func (f *fakeEventGrid) SubscriptionFullURL(topic, name string) (string, error) {
	return "https://hook.example/" + topic + "/" + name + "?code=x", nil
}

func (f *fakeEventGrid) PutNamespace(name string) error {
	f.nss[name] = true

	return nil
}

func (f *fakeEventGrid) DeleteNamespace(name string) error {
	delete(f.nss, name)

	return nil
}

func (f *fakeEventGrid) NamespaceKeys(string) (string, string, error) { return "n1", "n2", nil }

func (f *fakeEventGrid) RegenerateNamespaceKey(string, string) (string, string, error) {
	return "r1", "n2", nil
}

func (f *fakeEventGrid) PutNamespaceTopic(ns, name string) (time.Time, error) {
	f.nsts[ns+"/"+name] = true

	return time.Unix(1700000000, 0), nil
}

func (f *fakeEventGrid) NamespaceTopicCreated(ns, name string) (time.Time, bool) {
	return time.Unix(1700000000, 0), f.nsts[ns+"/"+name]
}

func (f *fakeEventGrid) DeleteNamespaceTopic(ns, name string) error {
	delete(f.nsts, ns+"/"+name)

	return nil
}

func (f *fakeEventGrid) PutPullSubscription(ns, topic, name string, props map[string]any) (map[string]any, error) {
	out := map[string]any{"provisioningState": "Succeeded", "echo": props["x"]}
	f.pulls[ns+"/"+topic+"/"+name] = out

	return out, nil
}

func (f *fakeEventGrid) GetPullSubscription(ns, topic, name string) (map[string]any, bool) {
	s, ok := f.pulls[ns+"/"+topic+"/"+name]

	return s, ok
}

func (f *fakeEventGrid) DeletePullSubscription(ns, topic, name string) error {
	delete(f.pulls, ns+"/"+topic+"/"+name)

	return nil
}

func egID(types []string, names ...string) azurearm.ResourceID {
	return azurearm.ResourceID{
		SubscriptionID: "sub", ResourceGroup: "rg", Namespace: "Microsoft.EventGrid", Types: types, Names: names,
	}
}

func topicSubID(topic, name string) azurearm.ResourceID {
	return egID([]string{"topics", "providers", "eventSubscriptions"}, topic, "Microsoft.EventGrid", name)
}

func TestEventGridProvider_TopicLifecycle(t *testing.T) {
	t.Parallel()

	dp := newFakeEventGrid()
	p := azurearm.NewEventGridProvider(azurearm.EventGridEndpointConfig{}, dp)
	ctx := azurearm.WithRequestHost(t.Context(), "myhost")
	topic := egID([]string{"topics"}, "tp")

	body, err := p.Put(ctx, topic, map[string]any{
		"location": "westus", "tags": map[string]any{"a": "b"},
		"properties": map[string]any{"inputSchema": "CloudEventSchemaV1_0", "publicNetworkAccess": "Disabled"},
	})
	require.NoError(t, err)
	assert.Equal(t, "westus", body["location"])
	assert.Equal(t, "Microsoft.EventGrid/topics", body["type"])

	pr := props(body)
	assert.Equal(t, "https://myhost:10007/api/events", pr["endpoint"])
	assert.Equal(t, "CloudEventSchemaV1_0", pr["inputSchema"])
	assert.Equal(t, "Disabled", pr["publicNetworkAccess"], "unmodelled properties round-trip")
	assert.Equal(t, "Succeeded", pr["provisioningState"])

	_, err = p.Put(
		ctx,
		egID([]string{"topics"}, "bad"),
		map[string]any{"properties": map[string]any{"inputSchema": "Custom"}},
	)
	require.ErrorIs(t, err, azurearm.ErrInvalidRequestBody)

	keys, err := p.ListKeys(ctx, topic)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"key1": "k1", "key2": "k2"}, keys)

	keys, err = p.RegenerateKey(ctx, topic, "key1")
	require.NoError(t, err)
	assert.Equal(t, "new1", keys["key1"])

	_, err = p.RegenerateKey(ctx, topic, "key3")
	require.ErrorIs(t, err, azurearm.ErrInvalidRequestBody)

	_, err = p.ListKeys(ctx, egID([]string{"topics"}, "nope"))
	require.ErrorIs(t, err, azurearm.ErrEventGridTopicNotFound)

	list, err := p.List(ctx, egID([]string{"topics"}))
	require.NoError(t, err)
	require.Len(t, list, 1)

	require.NoError(t, p.Delete(ctx, topic))
	require.ErrorIs(t, p.Delete(ctx, topic), azurearm.ErrEventGridTopicNotFound)
	_, err = p.Get(ctx, topic)
	require.ErrorIs(t, err, azurearm.ErrEventGridTopicNotFound)
	assert.Empty(t, dp.topics)
}

func TestEventGridProvider_EventSubscriptions(t *testing.T) {
	t.Parallel()

	dp := newFakeEventGrid()
	p := azurearm.NewEventGridProvider(azurearm.EventGridEndpointConfig{}, dp)
	ctx := t.Context()
	sub := topicSubID("tp", "sub1")

	_, err := p.Put(ctx, sub, map[string]any{"properties": map[string]any{}})
	require.ErrorIs(t, err, azurearm.ErrEventGridTopicNotFound, "the scope topic must exist")

	_, err = p.Put(ctx, egID([]string{"topics"}, "tp"), map[string]any{})
	require.NoError(t, err)

	body, err := p.Put(ctx, sub, map[string]any{"properties": map[string]any{"labels": []any{"x"}}})
	require.NoError(t, err)
	assert.Equal(t, "Microsoft.EventGrid/eventSubscriptions", body["type"])
	assert.Equal(t, "sub1", body["name"])
	assert.Equal(
		t,
		"/subscriptions/sub/resourceGroups/rg/providers/Microsoft.EventGrid/topics/tp"+
			"/providers/Microsoft.EventGrid/eventSubscriptions/sub1",
		body["id"],
	)
	assert.Equal(t, []any{"x"}, props(body)["labels"])

	got, err := p.Get(ctx, sub)
	require.NoError(t, err)
	assert.Equal(t, "Succeeded", props(got)["provisioningState"])

	full, err := p.FullURL(ctx, sub)
	require.NoError(t, err)
	assert.Equal(t, "https://hook.example/tp/sub1?code=x", full["endpointUrl"])

	_, err = p.FullURL(ctx, egID([]string{"topics"}, "tp"))
	require.ErrorIs(t, err, azurearm.ErrResourceNotFound)

	// Deleting the topic removes its subscriptions.
	require.NoError(t, p.Delete(ctx, egID([]string{"topics"}, "tp")))
	_, err = p.Get(ctx, sub)
	require.ErrorIs(t, err, azurearm.ErrEventGridSubscriptionNotFound)
}

func TestEventGridProvider_NamespaceHierarchy(t *testing.T) {
	t.Parallel()

	dp := newFakeEventGrid()
	p := azurearm.NewEventGridProvider(azurearm.EventGridEndpointConfig{Override: "eg:1"}, dp)
	ctx := t.Context()
	ns := egID([]string{"namespaces"}, "ns")
	nt := egID([]string{"namespaces", "topics"}, "ns", "nt")
	ps := egID([]string{"namespaces", "topics", "eventSubscriptions"}, "ns", "nt", "ps")

	_, err := p.Put(ctx, nt, map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrEventGridNamespaceNotFound)

	body, err := p.Put(ctx, ns, map[string]any{"location": "westus"})
	require.NoError(t, err)
	assert.Equal(t, "eg:1", props(body)["topicsConfiguration"].(map[string]any)["hostname"])
	assert.Equal(t, "Standard", body["sku"].(map[string]any)["name"])

	_, err = p.Put(ctx, ps, map[string]any{})
	require.ErrorIs(t, err, azurearm.ErrEventGridNamespaceTopicNotFound)

	_, err = p.Put(ctx, nt, map[string]any{})
	require.NoError(t, err)

	body, err = p.Put(ctx, ps, map[string]any{"properties": map[string]any{"x": "y"}})
	require.NoError(t, err)
	assert.Equal(t, "y", props(body)["echo"])
	assert.Equal(t, "Microsoft.EventGrid/namespaces/topics/eventSubscriptions", body["type"])

	keys, err := p.ListKeys(ctx, ns)
	require.NoError(t, err)
	assert.Equal(t, "n1", keys["key1"])

	require.NoError(t, p.Delete(ctx, ns))
	_, err = p.Get(ctx, ps)
	require.ErrorIs(t, err, azurearm.ErrEventGridSubscriptionNotFound)
	assert.Empty(t, dp.nss)

	_, err = p.Get(ctx, egID([]string{"bogus"}, "x"))
	require.ErrorIs(t, err, azurearm.ErrResourceNotFound)
}

func TestEventGridProvider_PersistenceAndGroupDelete(t *testing.T) {
	t.Parallel()

	dp := newFakeEventGrid()
	p := azurearm.NewEventGridProvider(azurearm.EventGridEndpointConfig{}, dp)
	ctx := t.Context()
	topic := egID([]string{"topics"}, "tp")

	_, err := p.Put(ctx, topic, map[string]any{"tags": map[string]any{"a": "b"}})
	require.NoError(t, err)
	_, err = p.Put(ctx, topicSubID("tp", "s"), map[string]any{})
	require.NoError(t, err)

	p2 := azurearm.NewEventGridProvider(azurearm.EventGridEndpointConfig{}, dp)
	azurearm.EventGridRestoreJSON(t, p2, azurearm.EventGridSnapshotJSON(p))

	got, err := p2.Get(ctx, topic)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "b"}, got["tags"])

	_, err = p2.Get(ctx, topicSubID("tp", "s"))
	require.NoError(t, err)

	p2.DeleteResourcesInGroup(ctx, "RG")
	assert.Empty(t, dp.topics)

	p.Reset()
	_, err = p.Get(ctx, topic)
	require.ErrorIs(t, err, azurearm.ErrEventGridTopicNotFound)
}

const (
	egTestBase    = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.EventGrid/topics/tp"
	egTestSubPath = egTestBase + "/providers/Microsoft.EventGrid/eventSubscriptions/s1"
	egAPIVersion  = "?api-version=2025-02-15"
)

func newEventGridHandler(t *testing.T) *azurearm.Handler {
	t.Helper()

	h := azurearm.NewTestHandlerWithEventGrid(newFakeEventGrid(), azurearm.DefaultSettings())

	st, _ := doRequest(t, h, http.MethodPut, "/subscriptions/sub/resourceGroups/rg", []byte(`{"location":"local"}`))
	require.Equal(t, http.StatusCreated, st)

	st, _ = doRequest(t, h, http.MethodPut, egTestBase+egAPIVersion, []byte(`{"location":"local"}`))
	require.Equal(t, http.StatusCreated, st, "go-azure-sdk accepts only 201 for Event Grid PUTs")

	st, sb := doRequest(t, h, http.MethodPut, egTestSubPath+egAPIVersion, []byte(`{"properties":{}}`))
	require.Equal(t, http.StatusCreated, st, "%v", sb)

	return h
}

func TestEventGridHandlerRoutes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		body   string
		field  string
		want   string
		status int
	}{
		{name: "list keys", path: egTestBase + "/listKeys", field: "key1", want: "k1", status: http.StatusOK},
		{
			name: "regenerate", path: egTestBase + "/regenerateKey", body: `{"keyName":"key2"}`,
			field: "key2", want: "new2", status: http.StatusOK,
		},
		{
			name: "regenerate bad name", path: egTestBase + "/regenerateKey", body: `{"keyName":"x"}`,
			status: http.StatusBadRequest,
		},
		{
			name: "full url", path: egTestSubPath + "/getFullUrl", field: "endpointUrl",
			want: "https://hook.example/tp/s1?code=x", status: http.StatusOK,
		},
		{name: "missing topic keys", path: egTestBase + "x/listKeys", status: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newEventGridHandler(t)

			status, out := doRequest(t, h, http.MethodPost, tt.path+egAPIVersion, []byte(tt.body))
			require.Equal(t, tt.status, status, "%v", out)

			if tt.field != "" {
				assert.Equal(t, tt.want, out[tt.field])
			}
		})
	}
}

func TestEventGridHandlerSubscriptionCRUD(t *testing.T) {
	t.Parallel()

	h := newEventGridHandler(t)

	st, got := doRequest(t, h, http.MethodGet, egTestSubPath+egAPIVersion, nil)
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, "s1", got["name"])

	req := httptest.NewRequest(http.MethodDelete, egTestSubPath+egAPIVersion, http.NoBody)
	req.Host = "host:10006"
	rec := httptest.NewRecorder()
	newEchoServer(h).ServeHTTP(rec, req)

	assert.Equal(t, http.StatusAccepted, rec.Code)

	op := rec.Header().Get("Azure-Asyncoperation")
	require.Contains(t, op, "/providers/Microsoft.EventGrid/locations/local/operationsStatus/")

	opURL, err := url.Parse(op)
	require.NoError(t, err)

	st, status := doRequest(t, h, http.MethodGet, opURL.Path, nil)
	require.Equal(t, http.StatusOK, st)
	assert.Equal(t, "Succeeded", status["status"])

	st, _ = doRequest(t, h, http.MethodGet, egTestSubPath+egAPIVersion, nil)
	assert.Equal(t, http.StatusNotFound, st)
}
