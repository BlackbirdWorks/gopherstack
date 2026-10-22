package azureeventgrid_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

type hook struct {
	srv       *httptest.Server
	code      func(n int) int
	requests  []*http.Request
	bodies    [][]byte
	shakeBody []byte
	shakes    int
	manual    bool
	mu        sync.Mutex
}

func newHook(t *testing.T, code func(n int) int) *hook {
	t.Helper()

	h := &hook{code: code}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		h.mu.Lock()
		defer h.mu.Unlock()

		if r.Header.Get("Aeg-Event-Type") == "SubscriptionValidation" {
			h.shakes++
			h.shakeBody = body

			if !h.manual {
				var ev []map[string]any

				_ = json.Unmarshal(body, &ev)
				code, _ := ev[0]["data"].(map[string]any)["validationCode"].(string)
				_ = json.NewEncoder(w).Encode(map[string]string{"validationResponse": code})
			}

			return
		}

		h.requests = append(h.requests, r)
		h.bodies = append(h.bodies, body)
		w.WriteHeader(h.code(len(h.requests)))
	}))

	t.Cleanup(h.srv.Close)

	return h
}

func (h *hook) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()

	return len(h.requests)
}

func TestWebhookHandshake(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		schema    string
		respond   func(w http.ResponseWriter, r *http.Request, code string)
		wantState string
		wantErr   bool
	}{
		{
			name:      "echo validation code",
			schema:    azureeventgrid.SchemaEventGrid,
			wantState: azureeventgrid.StateSucceeded,
			respond: func(w http.ResponseWriter, _ *http.Request, code string) {
				_ = json.NewEncoder(w).Encode(map[string]string{"validationResponse": code})
			},
		},
		{
			name: "plain 200 needs manual validation", schema: azureeventgrid.SchemaEventGrid,
			wantState: azureeventgrid.StateAwaitingManual,
			respond:   func(w http.ResponseWriter, _ *http.Request, _ string) { w.WriteHeader(http.StatusOK) },
		},
		{
			name: "wrong code echoed needs manual validation", schema: azureeventgrid.SchemaEventGrid,
			wantState: azureeventgrid.StateAwaitingManual,
			respond: func(w http.ResponseWriter, _ *http.Request, _ string) {
				_, _ = w.Write([]byte(`{"validationResponse":"nope"}`))
			},
		},
		{
			name:    "server error fails",
			schema:  azureeventgrid.SchemaEventGrid,
			wantErr: true,
			respond: func(w http.ResponseWriter, _ *http.Request, _ string) { w.WriteHeader(http.StatusInternalServerError) },
		},
		{
			name:      "cloudevents allowed origin",
			schema:    azureeventgrid.SchemaCloudEvent,
			wantState: azureeventgrid.StateSucceeded,
			respond: func(w http.ResponseWriter, r *http.Request, _ string) {
				w.Header().Set("Webhook-Allowed-Origin", r.Header.Get("Webhook-Request-Origin"))
			},
		},
		{
			name: "cloudevents without allowed origin", schema: azureeventgrid.SchemaCloudEvent,
			wantState: azureeventgrid.StateAwaitingManual,
			respond:   func(w http.ResponseWriter, _ *http.Request, _ string) { w.WriteHeader(http.StatusOK) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, _ := newBackend(t)
			_, err := b.PutTopic("t1", topicRID, tt.schema)
			require.NoError(t, err)

			var seen struct {
				headers http.Header
				method  string
				event   []map[string]any
				mu      sync.Mutex
			}

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)

				seen.mu.Lock()
				seen.headers, seen.method = r.Header.Clone(), r.Method
				_ = json.Unmarshal(body, &seen.event)
				seen.mu.Unlock()

				code := ""
				if len(seen.event) > 0 {
					code, _ = seen.event[0]["data"].(map[string]any)["validationCode"].(string)
				}

				tt.respond(w, r, code)
			}))
			defer srv.Close()

			_, err = b.PutSubscription(t.Context(), "t1", "s1", webhookProps(srv.URL, nil))
			if tt.wantErr {
				require.ErrorIs(t, err, azureeventgrid.ErrWebhookValidation)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantState, azureeventgrid.SubscriptionState(b, "t1", "s1"))

			seen.mu.Lock()
			defer seen.mu.Unlock()

			if tt.schema == azureeventgrid.SchemaEventGrid {
				assert.Equal(t, "SubscriptionValidation", seen.headers.Get("Aeg-Event-Type"))
				assert.Equal(t, "Microsoft.EventGrid.SubscriptionValidationEvent", seen.event[0]["eventType"])
				assert.Equal(t, topicRID, seen.event[0]["topic"])
			} else {
				assert.Equal(t, http.MethodOptions, seen.method)
			}
		})
	}
}

func TestManualValidationUnlocksDelivery(t *testing.T) {
	t.Parallel()

	b, _, _ := newBackend(t)
	h := newHook(t, func(int) int { return http.StatusOK })
	h.manual = true

	_, err := b.PutSubscription(t.Context(), "t1", "s1", webhookProps(h.srv.URL, nil))
	require.NoError(t, err)
	require.Equal(t, azureeventgrid.StateAwaitingManual, azureeventgrid.SubscriptionState(b, "t1", "s1"))

	var validationURL string

	h.mu.Lock()

	var ev []map[string]any

	require.NoError(t, json.Unmarshal(h.shakeBody, &ev))

	validationURL, _ = ev[0]["data"].(map[string]any)["validationUrl"].(string)
	h.mu.Unlock()

	require.Contains(t, validationURL, "/validate?id=")

	require.NoError(t, b.Publish("t1", []azureeventgrid.Event{egEvent("e1", "a")}))
	assert.Equal(t, 0, azureeventgrid.PendingCount(b, "t1", "s1"), "no delivery while awaiting validation")

	code := validationURL[len("https://localhost/validate?id="):]
	assert.False(t, b.ConfirmValidation("wrong"))
	require.True(t, b.ConfirmValidation(code))
	assert.Equal(t, azureeventgrid.StateSucceeded, azureeventgrid.SubscriptionState(b, "t1", "s1"))

	require.NoError(t, b.Publish("t1", []azureeventgrid.Event{egEvent("e2", "a")}))
	assert.Equal(t, 1, b.RunDue(t.Context()))
}

func TestWebhookDeliveryShape(t *testing.T) {
	t.Parallel()

	b, _, _ := newBackend(t)
	h := newHook(t, func(int) int { return http.StatusOK })

	_, err := b.PutSubscription(t.Context(), "t1", "My-Sub", webhookProps(h.srv.URL, map[string]any{
		"filter": map[string]any{"subjectBeginsWith": "/keep"},
	}))
	require.NoError(t, err)

	require.NoError(t, b.Publish("t1", []azureeventgrid.Event{egEvent("e1", "/keep/1"), egEvent("e2", "/drop/2")}))
	assert.Equal(t, 1, b.RunDue(t.Context()))
	assert.Equal(t, 0, azureeventgrid.PendingCount(b, "t1", "My-Sub"))
	require.Equal(t, 1, h.count())

	h.mu.Lock()
	defer h.mu.Unlock()

	assert.Equal(t, "Notification", h.requests[0].Header.Get("Aeg-Event-Type"))
	assert.Equal(t, "MY-SUB", h.requests[0].Header.Get("Aeg-Subscription-Name"))
	assert.Equal(t, "0", h.requests[0].Header.Get("Aeg-Delivery-Count"))

	var got []map[string]any

	require.NoError(t, json.Unmarshal(h.bodies[0], &got))
	require.Len(t, got, 1)
	assert.Equal(t, "e1", got[0]["id"])
	assert.Equal(t, topicRID, got[0]["topic"])
	assert.Equal(t, "1", got[0]["metadataVersion"])
}

func TestRetryAndDeadLetter(t *testing.T) {
	t.Parallel()

	dl := map[string]any{"endpointType": "StorageBlob", "properties": map[string]any{
		"resourceId": storageID, "blobContainerName": "dead",
	}}

	tests := []struct {
		retry      map[string]any
		name       string
		wantReason string
		steps      []time.Duration
		code       int
		wantCalls  int
		wantDead   bool
	}{
		{
			name: "exhausts max attempts", code: 500,
			retry:     map[string]any{"maxDeliveryAttempts": 3, "eventTimeToLiveInMinutes": 1440},
			steps:     []time.Duration{0, 10 * time.Second, 30 * time.Second},
			wantCalls: 3, wantDead: true, wantReason: "MaxDeliveryAttemptsExceeded",
		},
		{
			name: "event expires before next retry", code: 503,
			retry: map[string]any{"maxDeliveryAttempts": 30, "eventTimeToLiveInMinutes": 1},
			// attempts at 0s, 10s, 40s; the next (+1m) would pass the 1 minute TTL.
			steps:     []time.Duration{0, 10 * time.Second, 30 * time.Second},
			wantCalls: 3, wantDead: true, wantReason: "TimeToLiveExpired",
		},
		{
			name: "bad request dead-letters at once", code: 400,
			retry:     map[string]any{"maxDeliveryAttempts": 30, "eventTimeToLiveInMinutes": 1440},
			steps:     []time.Duration{0},
			wantCalls: 1, wantDead: true, wantReason: "MaxDeliveryAttemptsExceeded",
		},
		{
			name: "success removes the event", code: 200,
			retry:     map[string]any{"maxDeliveryAttempts": 3, "eventTimeToLiveInMinutes": 1440},
			steps:     []time.Duration{0},
			wantCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, clk, rec := newBackend(t)
			h := newHook(t, func(int) int { return tt.code })

			_, err := b.PutSubscription(t.Context(), "t1", "s1", webhookProps(h.srv.URL, map[string]any{
				"retryPolicy": tt.retry, "deadLetterDestination": dl,
			}))
			require.NoError(t, err)
			require.NoError(t, b.Publish("t1", []azureeventgrid.Event{egEvent("e1", "a")}))

			for _, step := range tt.steps {
				clk.Advance(step)
				assert.Equal(t, 1, b.RunDue(t.Context()))
			}

			assert.Equal(t, 0, b.RunDue(t.Context()), "nothing is due right after the last attempt")
			assert.Equal(t, tt.wantCalls, h.count())
			assert.Equal(t, 0, azureeventgrid.PendingCount(b, "t1", "s1"))

			blobs := rec.all()
			if !tt.wantDead {
				assert.Empty(t, blobs)

				return
			}

			require.Len(t, blobs, 1)
			assert.Equal(t, "acct", blobs[0].account)
			assert.Equal(t, "dead", blobs[0].container)
			assert.Contains(t, blobs[0].name, "t1/s1/2026/10/20/")
			assert.Contains(t, blobs[0].name, "e1.json")

			var out map[string]any

			require.NoError(t, json.Unmarshal(blobs[0].body, &out))
			assert.Equal(t, tt.wantReason, out["deadLetterReason"])
			assert.EqualValues(t, tt.wantCalls, out["deliveryAttempts"])
			assert.Equal(t, "e1", out["id"])
			assert.NotEmpty(t, out["lastDeliveryOutcome"])
		})
	}
}

func TestRetryScheduleFollowsBackoff(t *testing.T) {
	t.Parallel()

	b, clk, _ := newBackend(t)
	h := newHook(t, func(int) int { return http.StatusInternalServerError })

	_, err := b.PutSubscription(t.Context(), "t1", "s1", webhookProps(h.srv.URL, nil))
	require.NoError(t, err)
	require.NoError(t, b.Publish("t1", []azureeventgrid.Event{egEvent("e1", "a")}))

	for failed := 1; failed <= 10; failed++ {
		before := clk.Now()

		require.Equal(t, 1, b.RunDue(t.Context()), "attempt %d", failed)
		assert.Equal(t, before.Add(azureeventgrid.Backoff(failed)), azureeventgrid.NextDue(b, "t1", "s1"))
		assert.Equal(t, 0, b.RunDue(t.Context()))

		clk.Advance(azureeventgrid.Backoff(failed))
	}

	// The 11th failure would retry 12h later, past the default 24h TTL: the event is dropped.
	require.Equal(t, 1, b.RunDue(t.Context()))
	assert.Equal(t, 0, azureeventgrid.PendingCount(b, "t1", "s1"))

	assert.Equal(t, 10*time.Second, azureeventgrid.Backoff(1))
	assert.Equal(t, 12*time.Hour, azureeventgrid.Backoff(10))
	assert.Equal(t, 12*time.Hour, azureeventgrid.Backoff(50))
}

func TestTargetDelivery(t *testing.T) {
	t.Parallel()

	sbQ := "/subscriptions/s/resourceGroups/rg/providers/Microsoft.ServiceBus/namespaces/ns/queues/q1"
	sbT := "/subscriptions/s/resourceGroups/rg/providers/Microsoft.ServiceBus/namespaces/ns/topics/tp1"
	ehID := "/subscriptions/s/resourceGroups/rg/providers/Microsoft.EventHub/namespaces/ehns/eventhubs/hub1"

	tests := []struct {
		dest  map[string]any
		check func(t *testing.T, got sent)
		name  string
	}{
		{
			name: "service bus queue",
			dest: map[string]any{"endpointType": "ServiceBusQueue", "properties": map[string]any{"resourceId": sbQ}},
			check: func(t *testing.T, got sent) {
				t.Helper()
				assert.Equal(t, "q1", got.queue)
				assert.JSONEq(t, `"e1"`, string(mustField(t, got.body, "id")))
			},
		},
		{
			name: "service bus topic",
			dest: map[string]any{"endpointType": "ServiceBusTopic", "properties": map[string]any{"resourceId": sbT}},
			check: func(t *testing.T, got sent) {
				t.Helper()
				assert.Equal(t, "tp1", got.topic)
			},
		},
		{
			name: "event hub",
			dest: map[string]any{"endpointType": "EventHub", "properties": map[string]any{"resourceId": ehID}},
			check: func(t *testing.T, got sent) {
				t.Helper()
				assert.Equal(t, "ehns", got.ns)
				assert.Equal(t, "hub1", got.hub)

				var arr []map[string]any

				require.NoError(t, json.Unmarshal(got.body, &arr))
				assert.Equal(t, "e1", arr[0]["id"])
			},
		},
		{
			name: "storage queue",
			dest: map[string]any{"endpointType": "StorageQueue", "properties": map[string]any{
				"resourceId": storageID, "queueName": "events", "queueMessageTimeToLiveInSeconds": 120,
			}},
			check: func(t *testing.T, got sent) {
				t.Helper()
				assert.Equal(t, "acct", got.account)
				assert.Equal(t, "events", got.queue)
				assert.Equal(t, 2*time.Minute, got.ttl)
				assert.JSONEq(t, `"e1"`, string(mustField(t, []byte(got.text), "id")))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, rec := newBackend(t)

			_, err := b.PutSubscription(t.Context(), "t1", "s1", map[string]any{"destination": tt.dest})
			require.NoError(t, err)
			require.NoError(t, b.Publish("t1", []azureeventgrid.Event{egEvent("e1", "a")}))
			assert.Equal(t, 1, b.RunDue(t.Context()))

			got := rec.all()
			require.Len(t, got, 1)
			tt.check(t, got[0])
		})
	}
}

func mustField(t *testing.T, raw []byte, key string) json.RawMessage {
	t.Helper()

	var m map[string]json.RawMessage

	require.NoError(t, json.Unmarshal(raw, &m))

	return m[key]
}

func TestTargetFailureRetries(t *testing.T) {
	t.Parallel()

	b, clk, rec := newBackend(t)
	rec.fail, rec.err = true, azureeventgrid.ErrTargetUnavailable

	dest := map[string]any{
		"endpointType": "StorageQueue",
		"properties":   map[string]any{"resourceId": storageID, "queueName": "q"},
	}

	_, err := b.PutSubscription(t.Context(), "t1", "s1", map[string]any{"destination": dest})
	require.NoError(t, err)
	require.NoError(t, b.Publish("t1", []azureeventgrid.Event{egEvent("e1", "a")}))
	require.Equal(t, 1, b.RunDue(t.Context()))
	require.Equal(t, 1, azureeventgrid.PendingCount(b, "t1", "s1"))

	rec.mu.Lock()
	rec.fail = false
	rec.mu.Unlock()

	clk.Advance(10 * time.Second)
	require.Equal(t, 1, b.RunDue(t.Context()))
	assert.Len(t, rec.all(), 1)
	assert.Equal(t, 0, azureeventgrid.PendingCount(b, "t1", "s1"))
}

func TestSubscriptionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props map[string]any
		name  string
	}{
		{
			name:  "unsupported endpoint",
			props: map[string]any{"destination": map[string]any{"endpointType": "AzureFunction"}},
		},
		{name: "webhook without url", props: map[string]any{"destination": map[string]any{"endpointType": "WebHook"}}},
		{name: "webhook bad scheme", props: webhookProps("ftp://x/y", nil)},
		{name: "queue without name", props: map[string]any{"destination": map[string]any{
			"endpointType": "StorageQueue", "properties": map[string]any{"resourceId": storageID},
		}}},
		{name: "retry out of range", props: map[string]any{
			"destination": map[string]any{
				"endpointType": "StorageQueue",
				"properties":   map[string]any{"resourceId": storageID, "queueName": "q"},
			},
			"retryPolicy": map[string]any{"maxDeliveryAttempts": 99},
		}},
		{name: "cloud schema on eventgrid topic", props: map[string]any{
			"destination": map[string]any{
				"endpointType": "StorageQueue",
				"properties":   map[string]any{"resourceId": storageID, "queueName": "q"},
			},
			"eventDeliverySchema": "CloudEventSchemaV1_0",
		}},
		{name: "dead letter wrong type", props: map[string]any{
			"destination": map[string]any{
				"endpointType": "StorageQueue",
				"properties":   map[string]any{"resourceId": storageID, "queueName": "q"},
			},
			"deadLetterDestination": map[string]any{"endpointType": "StorageQueue"},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, _ := newBackend(t)
			_, err := b.PutSubscription(t.Context(), "t1", "s1", tt.props)
			require.ErrorIs(t, err, azureeventgrid.ErrInvalidSpec)
		})
	}
}

func TestSubscriptionRoundTripAndLifecycle(t *testing.T) {
	t.Parallel()

	b, _, _ := newBackend(t)
	h := newHook(t, func(int) int { return http.StatusOK })
	full := h.srv.URL + "/hook?code=secret"

	got, err := b.PutSubscription(t.Context(), "t1", "s1", webhookProps(full, map[string]any{
		"labels": []any{"x"}, "filter": map[string]any{"includedEventTypes": []any{"App.Thing"}},
	}))
	require.NoError(t, err)

	props := got["destination"].(map[string]any)["properties"].(map[string]any)
	assert.Equal(t, h.srv.URL+"/hook", props["endpointBaseUrl"])
	assert.NotContains(t, props, "endpointUrl")
	assert.Equal(t, "Succeeded", got["provisioningState"])
	assert.Equal(t, topicRID, got["topic"])
	assert.Equal(t, "EventGridSchema", got["eventDeliverySchema"])
	assert.EqualValues(t, 30, got["retryPolicy"].(map[string]any)["maxDeliveryAttempts"])

	// azurerm treats absent subject filter strings as a configured subject_filter block.
	filter, _ := got["filter"].(map[string]any)
	assert.Empty(t, filter["subjectBeginsWith"])
	assert.Empty(t, filter["subjectEndsWith"])
	assert.Equal(t, false, filter["isSubjectCaseSensitive"])

	url, err := b.FullURL("t1", "s1")
	require.NoError(t, err)
	assert.Equal(t, full, url)

	// Re-PUT with the same endpoint does not repeat the handshake.
	_, err = b.PutSubscription(t.Context(), "t1", "s1", webhookProps(full, nil))
	require.NoError(t, err)
	assert.Equal(t, 1, h.shakes)

	require.NoError(t, b.DeleteSubscription("t1", "s1"))
	_, err = b.Subscription("t1", "s1")
	require.ErrorIs(t, err, azureeventgrid.ErrSubscriptionNotFound)
	require.ErrorIs(t, b.DeleteSubscription("t1", "s1"), azureeventgrid.ErrSubscriptionNotFound)

	_, err = b.PutSubscription(t.Context(), "nope", "s1", webhookProps(full, nil))
	require.ErrorIs(t, err, azureeventgrid.ErrTopicNotFound)
}

func TestSnapshotRestoreKeepsPendingDeliveries(t *testing.T) {
	t.Parallel()

	b, clk, rec := newBackend(t)
	dest := map[string]any{
		"endpointType": "StorageQueue",
		"properties":   map[string]any{"resourceId": storageID, "queueName": "q"},
	}

	_, err := b.PutSubscription(t.Context(), "t1", "s1", map[string]any{"destination": dest})
	require.NoError(t, err)
	require.NoError(t, b.Publish("t1", []azureeventgrid.Event{egEvent("e1", "a")}))

	snap := b.Snapshot(t.Context())

	b2 := azureeventgrid.NewInMemoryBackend()
	azureeventgrid.SetNow(b2, clk.Now)
	b2.SetTargets(rec)
	require.NoError(t, b2.Restore(t.Context(), snap))
	assert.Equal(t, 1, azureeventgrid.PendingCount(b2, "t1", "s1"))
	assert.Equal(t, 1, b2.RunDue(t.Context()))
	assert.Len(t, rec.all(), 1)

	keys, err := b2.TopicKeys("t1")
	require.NoError(t, err)

	orig, _ := b.TopicKeys("t1")
	assert.Equal(t, orig, keys)
}
