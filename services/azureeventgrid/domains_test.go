package azureeventgrid_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/messaging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eg "github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

const domainRID = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.EventGrid/domains/d1"

func queueDest(queue string) map[string]any {
	return map[string]any{"destination": map[string]any{
		"endpointType": "StorageQueue",
		"properties":   map[string]any{"resourceId": storageID, "queueName": queue},
	}}
}

func (e *env) domain(t *testing.T, schema string) eg.Keys {
	t.Helper()

	_, err := e.backend.PutDomain("d1", domainRID, schema, nil, nil)
	require.NoError(t, err)

	keys, err := e.backend.DomainKeys("d1")
	require.NoError(t, err)

	return keys
}

func domainEvent(topic, id, subject string) map[string]any {
	return map[string]any{
		"topic": topic, "id": id, "subject": subject, "eventType": "App.Thing",
		"eventTime": "2026-10-20T12:00:00Z", "dataVersion": "1", "data": map[string]any{"n": 1},
	}
}

func queues(rec *recorder) map[string]int {
	out := map[string]int{}
	for _, s := range rec.all() {
		out[s.queue]++
	}

	return out
}

func TestDomainPublishRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want   map[string]int
		name   string
		events []map[string]any
	}{
		{
			name:   "topic event reaches domain and topic subscriptions",
			events: []map[string]any{domainEvent("orders", "1", "/o/1")},
			want:   map[string]int{"all": 1, "orders": 1},
		},
		{
			name: "events fan out by topic",
			events: []map[string]any{
				domainEvent(
					"orders",
					"1",
					"/o/1",
				), domainEvent("users", "2", "/u/1"), domainEvent("users", "3", "/u/2"),
			},
			want: map[string]int{"all": 3, "orders": 1, "users": 2},
		},
		{
			name:   "topic without domain topic reaches only the domain scope",
			events: []map[string]any{domainEvent("ghost", "1", "/g")},
			want:   map[string]int{"all": 1},
		},
		{
			name:   "subject filter on a domain topic subscription",
			events: []map[string]any{domainEvent("orders", "1", "/skip/1")},
			want:   map[string]int{"all": 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, false)
			keys := e.domain(t, "")
			ctx := t.Context()

			_, err := e.backend.PutScopedSubscription(ctx, eg.Scope{Domain: "d1"}, "all", queueDest("all"))
			require.NoError(t, err)

			ordersProps := queueDest("orders")
			ordersProps["filter"] = map[string]any{"subjectBeginsWith": "/o"}
			_, err = e.backend.PutScopedSubscription(ctx, eg.Scope{Domain: "d1", Topic: "orders"}, "s", ordersProps)
			require.NoError(t, err)
			_, err = e.backend.PutScopedSubscription(
				ctx,
				eg.Scope{Domain: "d1", Topic: "users"},
				"s",
				queueDest("users"),
			)
			require.NoError(t, err)

			anys := make([]any, 0, len(tt.events))
			for _, ev := range tt.events {
				anys = append(anys, ev)
			}

			_, err = e.keyClient(t, keys.Key1).PublishCustomEventEvents(ctx, anys, nil)
			require.NoError(t, err)
			e.backend.RunDue(ctx)

			assert.Equal(t, tt.want, queues(e.rec))

			for _, s := range e.rec.all() {
				var got map[string]any

				require.NoError(t, json.Unmarshal([]byte(s.text), &got))
				assert.True(t, strings.HasPrefix(got["topic"].(string), domainRID+"/topics/"), "%v", got["topic"])
				assert.Equal(t, "1", got["metadataVersion"])
			}
		})
	}
}

func TestDomainPublishRejectsMissingTopic(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	keys := e.domain(t, "")
	ev := domainEvent("", "1", "/s")
	delete(ev, "topic")

	_, err := e.keyClient(t, keys.Key1).PublishCustomEventEvents(t.Context(), []any{ev}, nil)
	assert.Equal(t, http.StatusBadRequest, statusOf(err))
}

func TestDomainCloudEventsUseSourceAsTopic(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	keys := e.domain(t, eg.SchemaCloudEvent)

	_, err := e.backend.PutScopedSubscription(
		t.Context(),
		eg.Scope{Domain: "d1", Topic: "orders"},
		"s",
		queueDest("orders"),
	)
	require.NoError(t, err)

	mk := func(source string) messaging.CloudEvent {
		ev, mkErr := messaging.NewCloudEvent(source, "my.type", map[string]any{"k": "v"}, nil)
		require.NoError(t, mkErr)

		return ev
	}

	_, err = e.keyClient(t, keys.Key1).
		PublishCloudEvents(t.Context(), []messaging.CloudEvent{mk("orders"), mk("users")}, nil)
	require.NoError(t, err)
	e.backend.RunDue(t.Context())

	assert.Equal(t, map[string]int{"orders": 1}, queues(e.rec))
}

func TestDomainTopicLifecycle(t *testing.T) {
	t.Parallel()

	no, yes := false, true

	tests := []struct {
		createErr  error
		autoCreate *bool
		autoDelete *bool
		name       string
		topicAfter bool
	}{
		{name: "defaults create and delete", topicAfter: false},
		{name: "keep topic without auto delete", autoDelete: &no, topicAfter: true},
		{name: "explicit true", autoCreate: &yes, autoDelete: &yes, topicAfter: false},
		{name: "no auto create needs the topic", autoCreate: &no, createErr: eg.ErrDomainTopicNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, _ := newBackend(t)
			_, err := b.PutDomain("d1", domainRID, "", tt.autoCreate, tt.autoDelete)
			require.NoError(t, err)

			sc := eg.Scope{Domain: "d1", Topic: "orders"}
			_, err = b.PutScopedSubscription(t.Context(), sc, "s", queueDest("q"))
			require.ErrorIs(t, err, tt.createErr)

			if tt.createErr != nil {
				_, err = b.PutDomainTopic("d1", "orders")
				require.NoError(t, err)
				_, err = b.PutScopedSubscription(t.Context(), sc, "s", queueDest("q"))
				require.NoError(t, err)
			}

			_, err = b.DomainTopic("d1", "orders")
			require.NoError(t, err, "topic exists with its first subscription")

			require.NoError(t, b.DeleteScopedSubscription(sc, "s"))

			_, err = b.DomainTopic("d1", "orders")
			if tt.topicAfter {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, eg.ErrDomainTopicNotFound)
			}
		})
	}
}

func TestDomainCRUDAndKeys(t *testing.T) {
	t.Parallel()

	b, _, _ := newBackend(t)

	_, err := b.PutDomain("bad name", domainRID, "", nil, nil)
	require.ErrorIs(t, err, eg.ErrInvalidName)

	info, err := b.PutDomain("d1", domainRID, "", nil, nil)
	require.NoError(t, err)
	assert.Equal(t, eg.SchemaEventGrid, info.InputSchema)
	assert.True(t, info.AutoCreate)
	assert.True(t, info.AutoDelete)

	keys, err := b.DomainKeys("d1")
	require.NoError(t, err)

	rot, err := b.RegenerateDomainKey("d1", "KEY1")
	require.NoError(t, err)
	assert.NotEqual(t, keys.Key1, rot.Key1)
	assert.Equal(t, keys.Key2, rot.Key2)

	_, err = b.RegenerateDomainKey("d1", "key3")
	require.ErrorIs(t, err, eg.ErrInvalidName)

	for _, n := range []string{"b", "a"} {
		_, err = b.PutDomainTopic("d1", n)
		require.NoError(t, err)
	}

	topics, err := b.DomainTopics("d1")
	require.NoError(t, err)
	require.Len(t, topics, 2)
	assert.Equal(t, "a", topics[0].Name)

	require.NoError(t, b.DeleteDomainTopic("d1", "a"))
	require.ErrorIs(t, b.DeleteDomainTopic("d1", "a"), eg.ErrDomainTopicNotFound)
	_, err = b.PutDomainTopic("nope", "x")
	require.ErrorIs(t, err, eg.ErrDomainNotFound)

	require.NoError(t, b.DeleteDomain("d1"))
	_, err = b.Domain("d1")
	require.ErrorIs(t, err, eg.ErrDomainNotFound)
	require.ErrorIs(t, b.DeleteDomain("d1"), eg.ErrDomainNotFound)
}

func TestDomainPublishAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key  func(domain, topic eg.Keys) string
		name string
		want int
		recv int
	}{
		{name: "domain key", key: func(d, _ eg.Keys) string { return d.Key2 }, recv: 1},
		{name: "topic key publishes to the topic", key: func(_, tk eg.Keys) string { return tk.Key1 }},
		{name: "wrong key", key: func(eg.Keys, eg.Keys) string { return "nope" }, want: http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			keys := e.domain(t, "")
			topicKeys, err := e.backend.TopicKeys("t1")
			require.NoError(t, err)

			_, err = e.backend.PutScopedSubscription(t.Context(), eg.Scope{Domain: "d1"}, "all", queueDest("domain"))
			require.NoError(t, err)

			_, err = e.keyClient(t, tt.key(keys, topicKeys)).PublishCustomEventEvents(t.Context(),
				[]any{domainEvent("orders", "1", "/s")}, nil)
			assert.Equal(t, tt.want, statusOf(err))

			e.backend.RunDue(t.Context())
			assert.Equal(t, tt.recv, queues(e.rec)["domain"])
		})
	}
}

func TestDomainSubscriptionWebhookBatchingAndHeaders(t *testing.T) {
	t.Parallel()

	b, _, _ := newBackend(t)
	_, err := b.PutDomain("d1", domainRID, "", nil, nil)
	require.NoError(t, err)

	h := newHook(t, func(int) int { return http.StatusOK })
	sc := eg.Scope{Domain: "d1", Topic: "orders"}

	mapping := map[string]any{
		"name":       "X-Secret",
		"type":       "Static",
		"properties": map[string]any{"value": "v", "isSecret": true},
	}
	_, err = b.PutScopedSubscription(t.Context(), sc, "s", webhookDest(h.srv.URL, map[string]any{
		"maxEventsPerBatch": 10, "deliveryAttributeMappings": []any{mapping},
	}))
	require.NoError(t, err)

	attrs, err := b.DeliveryAttributes(sc, "s")
	require.NoError(t, err)
	require.Len(t, attrs, 1)
	assert.Equal(t, "v", attrs[0]["properties"].(map[string]any)["value"])

	var events []eg.Event

	for _, id := range []string{"a", "b", "c"} {
		raw, _ := json.Marshal(domainEvent("orders", id, "/s"))
		parsed, perr := eg.ParseEvents(raw, eg.SchemaEventGrid)
		require.NoError(t, perr)

		events = append(events, parsed...)
	}

	require.NoError(t, b.PublishDomain("d1", events))
	b.RunDue(t.Context())
	require.Equal(t, 1, h.count())

	h.mu.Lock()
	defer h.mu.Unlock()

	var arr []map[string]any

	require.NoError(t, json.Unmarshal(h.bodies[0], &arr))
	assert.Len(t, arr, 3)
	assert.Equal(t, "v", h.requests[0].Header.Get("X-Secret"))
	assert.Equal(t, domainRID+"/topics/orders", arr[0]["topic"])
}

func TestDomainSnapshotRestore(t *testing.T) {
	t.Parallel()

	b, clk, rec := newBackend(t)
	no := false
	_, err := b.PutDomain("d1", domainRID, eg.SchemaCloudEvent, nil, &no)
	require.NoError(t, err)

	sc := eg.Scope{Domain: "d1", Topic: "orders"}
	_, err = b.PutScopedSubscription(t.Context(), sc, "s", queueDest("q"))
	require.NoError(t, err)

	raw := []byte(`{"specversion":"1.0","id":"1","source":"orders","type":"t"}`)
	events, err := eg.ParseEvents(raw, eg.SchemaCloudEvent)
	require.NoError(t, err)
	require.NoError(t, b.PublishDomain("d1", events))

	b2 := eg.NewInMemoryBackend()
	eg.SetNow(b2, clk.Now)
	b2.SetTargets(rec)
	require.NoError(t, b2.Restore(t.Context(), b.Snapshot(t.Context())))

	info, err := b2.Domain("d1")
	require.NoError(t, err)
	assert.Equal(t, eg.SchemaCloudEvent, info.InputSchema)
	assert.True(t, info.AutoCreate)
	assert.False(t, info.AutoDelete)

	k1, _ := b.DomainKeys("d1")
	k2, _ := b2.DomainKeys("d1")
	assert.Equal(t, k1, k2)

	_, err = b2.ScopedSubscription(sc, "s")
	require.NoError(t, err)
	assert.Equal(t, 1, b2.RunDue(t.Context()))
	assert.Equal(t, map[string]int{"q": 1}, queues(rec))
}
