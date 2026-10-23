package azureeventgrid_test

import (
	"encoding/json"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

const (
	topicRID  = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.EventGrid/topics/t1"
	storageID = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/acct"
)

type fakeClock struct {
	t  time.Time
	mu sync.Mutex
}

func newClock() *fakeClock { return &fakeClock{t: time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

type sent struct {
	queue, topic, ns, hub, account, container, name, text string
	body                                                  []byte
	ttl                                                   time.Duration
}

type recorder struct {
	err  error
	got  []sent
	mu   sync.Mutex
	fail bool
}

func (r *recorder) add(s sent) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.fail {
		return r.err
	}

	r.got = append(r.got, s)

	return nil
}

func (r *recorder) all() []sent {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]sent(nil), r.got...)
}

func (r *recorder) SendServiceBus(queue, topic string, m azureeventgrid.Message) error {
	return r.add(sent{queue: queue, topic: topic, body: m.Body})
}

func (r *recorder) SendEventHub(ns, hub string, m azureeventgrid.Message) error {
	return r.add(sent{ns: ns, hub: hub, body: m.Body})
}

func (r *recorder) SendStorageQueue(account, queue, text string, ttl time.Duration) error {
	return r.add(sent{account: account, queue: queue, text: text, ttl: ttl})
}

func (r *recorder) PutBlob(account, container, blob string, data []byte, _ string) error {
	return r.add(sent{account: account, container: container, name: blob, body: data})
}

func newBackend(t *testing.T) (*azureeventgrid.InMemoryBackend, *fakeClock, *recorder) {
	t.Helper()

	clk, rec := newClock(), &recorder{}
	b := azureeventgrid.NewInMemoryBackend()
	azureeventgrid.SetNow(b, clk.Now)
	b.SetTargets(rec)

	_, err := b.PutTopic("t1", topicRID, "")
	require.NoError(t, err)

	return b, clk, rec
}

func egEvent(id, subject string) azureeventgrid.Event {
	raw, _ := json.Marshal(map[string]any{
		"id": id, "subject": subject, "eventType": "App.Thing", "eventTime": "2026-10-20T12:00:00Z",
		"dataVersion": "1", "data": map[string]any{"n": 1},
	})

	events, err := azureeventgrid.ParseEvents(raw, azureeventgrid.SchemaEventGrid)
	if err != nil {
		panic(err)
	}

	return events[0]
}

func webhookProps(url string, extra map[string]any) map[string]any {
	props := map[string]any{"destination": map[string]any{
		"endpointType": "WebHook", "properties": map[string]any{"endpointUrl": url},
	}}
	maps.Copy(props, extra)

	return props
}
