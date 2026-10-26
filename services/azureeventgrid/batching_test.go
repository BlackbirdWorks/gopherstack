package azureeventgrid_test

import (
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

func webhookDest(url string, destProps map[string]any) map[string]any {
	props := map[string]any{"endpointUrl": url}
	maps.Copy(props, destProps)

	return map[string]any{"destination": map[string]any{"endpointType": "WebHook", "properties": props}}
}

func TestWebhookBatching(t *testing.T) {
	t.Parallel()

	tests := []struct {
		destProps map[string]any
		name      string
		wantSizes []int
		events    int
		padding   int
	}{
		{name: "default is one event per request", events: 3, wantSizes: []int{1, 1, 1}},
		{
			name: "grouped up to maxEventsPerBatch", destProps: map[string]any{"maxEventsPerBatch": 2},
			events: 5, wantSizes: []int{2, 2, 1},
		},
		{
			name: "all events when the cap is higher", destProps: map[string]any{"maxEventsPerBatch": 10},
			events: 4, wantSizes: []int{4},
		},
		{
			name:      "preferred size splits batches",
			destProps: map[string]any{"maxEventsPerBatch": 10, "preferredBatchSizeInKilobytes": 1},
			events:    4, padding: 700, wantSizes: []int{1, 1, 1, 1},
		},
		{
			name:      "preferred size packs several small events",
			destProps: map[string]any{"maxEventsPerBatch": 10, "preferredBatchSizeInKilobytes": 1},
			events:    3, padding: 0, wantSizes: []int{3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, _ := newBackend(t)
			h := newHook(t, func(int) int { return http.StatusOK })

			_, err := b.PutSubscription(t.Context(), "t1", "s1", webhookDest(h.srv.URL, tt.destProps))
			require.NoError(t, err)

			events := make([]azureeventgrid.Event, 0, tt.events)
			for i := range tt.events {
				ev := egEvent("e"+string(rune('a'+i)), "/s")
				ev.Fields["data"] = map[string]any{"pad": strings.Repeat("x", tt.padding)}
				raw, _ := json.Marshal(ev.Fields)
				ev.Raw = raw
				events = append(events, ev)
			}

			require.NoError(t, b.Publish("t1", events))
			b.RunDue(t.Context())

			h.mu.Lock()
			defer h.mu.Unlock()

			sizes := make([]int, 0, len(h.bodies))
			total := 0

			for _, body := range h.bodies {
				var arr []map[string]any

				require.NoError(t, json.Unmarshal(body, &arr))
				sizes = append(sizes, len(arr))
				total += len(arr)
			}

			assert.ElementsMatch(t, tt.wantSizes, sizes)
			assert.Equal(t, tt.events, total)
			assert.Equal(t, 0, azureeventgrid.PendingCount(b, "t1", "s1"))
		})
	}
}

func TestWebhookBatchFailureRetriesWholeBatch(t *testing.T) {
	t.Parallel()

	b, clk, _ := newBackend(t)
	h := newHook(t, func(n int) int {
		if n == 1 {
			return http.StatusInternalServerError
		}

		return http.StatusOK
	})

	_, err := b.PutSubscription(t.Context(), "t1", "s1", webhookDest(h.srv.URL, map[string]any{"maxEventsPerBatch": 5}))
	require.NoError(t, err)

	require.NoError(t, b.Publish("t1", []azureeventgrid.Event{egEvent("a", "/s"), egEvent("b", "/s")}))
	b.RunDue(t.Context())
	assert.Equal(t, 2, azureeventgrid.PendingCount(b, "t1", "s1"))

	clk.Advance(azureeventgrid.Backoff(1))
	b.RunDue(t.Context())
	assert.Equal(t, 0, azureeventgrid.PendingCount(b, "t1", "s1"))

	h.mu.Lock()
	defer h.mu.Unlock()

	require.Len(t, h.bodies, 2)

	var arr []map[string]any

	require.NoError(t, json.Unmarshal(h.bodies[1], &arr))
	assert.Len(t, arr, 2)
	assert.Equal(t, "1", h.requests[1].Header.Get("Aeg-Delivery-Count"))
}

func TestBatchSettingsValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props map[string]any
		name  string
		ok    bool
	}{
		{name: "defaults", props: map[string]any{}, ok: true},
		{
			name: "max", ok: true,
			props: map[string]any{"maxEventsPerBatch": 5000, "preferredBatchSizeInKilobytes": 1024},
		},
		{name: "zero events", props: map[string]any{"maxEventsPerBatch": -1}},
		{name: "too many events", props: map[string]any{"maxEventsPerBatch": 5001}},
		{name: "size too big", props: map[string]any{"preferredBatchSizeInKilobytes": 1025}},
		{name: "size negative", props: map[string]any{"preferredBatchSizeInKilobytes": -3}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, _ := newBackend(t)
			h := newHook(t, func(int) int { return http.StatusOK })

			_, err := b.PutSubscription(t.Context(), "t1", "s1", webhookDest(h.srv.URL, tt.props))
			if tt.ok {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, azureeventgrid.ErrInvalidSpec)
			}
		})
	}
}

func TestDeliveryAttributeMappings(t *testing.T) {
	t.Parallel()

	b, _, _ := newBackend(t)
	h := newHook(t, func(int) int { return http.StatusOK })

	mappings := []any{
		map[string]any{"name": "X-Env", "type": "Static", "properties": map[string]any{"value": "prod"}},
		map[string]any{
			"name": "X-Token", "type": "Static", "properties": map[string]any{"value": "s3cret", "isSecret": true},
		},
		map[string]any{"name": "X-Subject", "type": "Dynamic", "properties": map[string]any{"sourceField": "subject"}},
		map[string]any{"name": "X-N", "type": "Dynamic", "properties": map[string]any{"sourceField": "data.n"}},
		map[string]any{
			"name":       "X-Missing",
			"type":       "Dynamic",
			"properties": map[string]any{"sourceField": "data.nope"},
		},
	}

	props, err := b.PutSubscription(t.Context(), "t1", "s1",
		webhookDest(h.srv.URL, map[string]any{"deliveryAttributeMappings": mappings}))
	require.NoError(t, err)

	dest, _ := props["destination"].(map[string]any)
	dprops, _ := dest["properties"].(map[string]any)
	got, _ := dprops["deliveryAttributeMappings"].([]any)
	require.Len(t, got, 5)

	secret, _ := got[1].(map[string]any)["properties"].(map[string]any)
	assert.NotContains(t, secret, "value", "secret values are withheld from reads")
	assert.Equal(t, true, secret["isSecret"])

	require.NoError(t, b.Publish("t1", []azureeventgrid.Event{egEvent("e1", "/things/9")}))
	b.RunDue(t.Context())
	require.Equal(t, 1, h.count())

	h.mu.Lock()
	defer h.mu.Unlock()

	hdr := h.requests[0].Header
	assert.Equal(t, "prod", hdr.Get("X-Env"))
	assert.Equal(t, "s3cret", hdr.Get("X-Token"))
	assert.Equal(t, "/things/9", hdr.Get("X-Subject"))
	assert.Equal(t, "1", hdr.Get("X-N"))
	assert.Empty(t, hdr.Values("X-Missing"))
}

func TestDeliveryAttributeMappingValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mapping map[string]any
		name    string
	}{
		{name: "no name", mapping: map[string]any{"type": "Static", "properties": map[string]any{"value": "v"}}},
		{name: "bad header name", mapping: map[string]any{
			"name": "bad name", "type": "Static", "properties": map[string]any{"value": "v"},
		}},
		{name: "unknown type", mapping: map[string]any{"name": "X-A", "type": "Other"}},
		{name: "dynamic without field", mapping: map[string]any{
			"name": "X-A", "type": "Dynamic", "properties": map[string]any{},
		}},
		{name: "static without value", mapping: map[string]any{
			"name": "X-A", "type": "Static", "properties": map[string]any{},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _, _ := newBackend(t)
			h := newHook(t, func(int) int { return http.StatusOK })

			_, err := b.PutSubscription(t.Context(), "t1", "s1",
				webhookDest(h.srv.URL, map[string]any{"deliveryAttributeMappings": []any{tt.mapping}}))
			require.ErrorIs(t, err, azureeventgrid.ErrInvalidSpec)
		})
	}
}

func TestCloudEventsBatchedDelivery(t *testing.T) {
	t.Parallel()

	b, _, _ := newBackend(t)
	_, err := b.PutTopic("c1", "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.EventGrid/topics/c1",
		azureeventgrid.SchemaCloudEvent)
	require.NoError(t, err)

	var (
		mu    sync.Mutex
		ctype string
		body  []byte
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			w.Header().Set("Webhook-Allowed-Origin", "*")

			return
		}

		data, _ := io.ReadAll(r.Body)

		mu.Lock()
		ctype, body = r.Header.Get("Content-Type"), data
		mu.Unlock()
	}))
	t.Cleanup(srv.Close)

	_, err = b.PutSubscription(t.Context(), "c1", "s1", webhookDest(srv.URL, map[string]any{"maxEventsPerBatch": 5}))
	require.NoError(t, err)

	raw := []byte(
		`[{"specversion":"1.0","id":"1","source":"/a","type":"t"},{"specversion":"1.0","id":"2","source":"/a","type":"t"}]`,
	)
	events, err := azureeventgrid.ParseEvents(raw, azureeventgrid.SchemaCloudEvent)
	require.NoError(t, err)
	require.NoError(t, b.Publish("c1", events))
	b.RunDue(t.Context())

	mu.Lock()
	defer mu.Unlock()

	assert.Contains(t, ctype, "application/cloudevents-batch+json")

	var arr []map[string]any

	require.NoError(t, json.Unmarshal(body, &arr))
	assert.Len(t, arr, 2)
}
