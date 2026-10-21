package azureeventgrid_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/messaging"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/eventgrid/azeventgrid"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/eventgrid/aznamespaces"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	eg "github.com/blackbirdworks/gopherstack/services/azureeventgrid"
)

type env struct {
	backend *eg.InMemoryBackend
	clock   *fakeClock
	rec     *recorder
	srv     *httptest.Server
}

func newEnv(t *testing.T, validate bool) *env {
	t.Helper()

	b, clk, rec := newBackend(t)
	h := eg.NewHandler(b, eg.Settings{Port: eg.DefaultPort, ValidateSAS: validate})

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	return &env{backend: b, clock: clk, rec: rec, srv: srv}
}

func (e *env) publisher(t *testing.T, cred func() (*azeventgrid.Client, error)) *azeventgrid.Client {
	t.Helper()

	c, err := cred()
	require.NoError(t, err)

	return c
}

func (e *env) clientOpts() azcore.ClientOptions {
	return azcore.ClientOptions{Transport: e.srv.Client(), Retry: policy.RetryOptions{MaxRetries: -1}}
}

func (e *env) keyClient(t *testing.T, key string) *azeventgrid.Client {
	t.Helper()

	return e.publisher(t, func() (*azeventgrid.Client, error) {
		return azeventgrid.NewClientWithSharedKeyCredential(
			e.srv.URL+"/api/events",
			azcore.NewKeyCredential(key),
			&azeventgrid.ClientOptions{ClientOptions: e.clientOpts()},
		)
	})
}

func (e *env) sasClient(t *testing.T, token string) *azeventgrid.Client {
	t.Helper()

	return e.publisher(t, func() (*azeventgrid.Client, error) {
		return azeventgrid.NewClientWithSAS(
			e.srv.URL+"/api/events",
			azcore.NewSASCredential(token),
			&azeventgrid.ClientOptions{ClientOptions: e.clientOpts()},
		)
	})
}

func (e *env) queueSub(t *testing.T) {
	t.Helper()

	dest := map[string]any{
		"endpointType": "StorageQueue",
		"properties":   map[string]any{"resourceId": storageID, "queueName": "q"},
	}
	_, err := e.backend.PutSubscription(t.Context(), "t1", "s1", map[string]any{"destination": dest})
	require.NoError(t, err)
}

func sdkEvent(id string) azeventgrid.Event {
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)

	return azeventgrid.Event{
		ID: &id, Subject: new("/things/1"), EventType: new("App.Thing"), DataVersion: new("1"),
		EventTime: &now, Data: map[string]any{"n": 1},
	}
}

func statusOf(err error) int {
	if re, ok := errors.AsType[*azcore.ResponseError](err); ok {
		return re.StatusCode
	}

	return 0
}

func TestPublisherSDK(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	e.queueSub(t)

	keys, err := e.backend.TopicKeys("t1")
	require.NoError(t, err)

	c := e.keyClient(t, keys.Key1)
	ctx := t.Context()

	_, err = c.PublishEvents(ctx, []azeventgrid.Event{sdkEvent("a"), sdkEvent("b")}, nil)
	require.NoError(t, err)
	_, err = c.PublishCustomEventEvents(ctx, []any{
		map[string]any{
			"id":          "c",
			"subject":     "/x",
			"eventType":   "App.Thing",
			"eventTime":   "2026-10-20T12:00:00Z",
			"dataVersion": "1",
			"data":        5,
		},
	}, nil)
	require.NoError(t, err)

	assert.Equal(t, 3, e.backend.RunDue(ctx))
	assert.Len(t, e.rec.all(), 3)
}

func TestPublisherSDKCloudEvents(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	_, err := e.backend.PutTopic("t1", topicRID, eg.SchemaCloudEvent)
	require.NoError(t, err)
	e.queueSub(t)

	keys, _ := e.backend.TopicKeys("t1")
	c := e.keyClient(t, keys.Key2)

	ev, err := messaging.NewCloudEvent(
		"/src",
		"my.type",
		map[string]any{"k": "v"},
		&messaging.CloudEventOptions{Subject: new("sub")},
	)
	require.NoError(t, err)

	_, err = c.PublishCloudEvents(t.Context(), []messaging.CloudEvent{ev, ev}, nil)
	require.NoError(t, err)
	assert.Equal(t, 2, e.backend.RunDue(t.Context()))

	// An EventGrid-schema event on a CloudEvents topic is rejected.
	_, err = c.PublishEvents(t.Context(), []azeventgrid.Event{sdkEvent("a")}, nil)
	assert.Equal(t, http.StatusBadRequest, statusOf(err))
}

func TestPublisherRejectsMalformedEvents(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	keys, _ := e.backend.TopicKeys("t1")
	c := e.keyClient(t, keys.Key1)

	ev := sdkEvent("a")
	ev.DataVersion = nil

	_, err := c.PublishEvents(t.Context(), []azeventgrid.Event{ev}, nil)
	assert.Equal(t, http.StatusBadRequest, statusOf(err))

	cloud, err := messaging.NewCloudEvent("/src", "my.type", "x", nil)
	require.NoError(t, err)

	_, err = c.PublishCloudEvents(t.Context(), []messaging.CloudEvent{cloud}, nil)
	assert.Equal(t, http.StatusBadRequest, statusOf(err), "cloud events on an EventGrid-schema topic")
}

func TestPublisherAuth(t *testing.T) {
	t.Parallel()

	// cred returns the shared key, or the SAS token when key is empty.
	tests := []struct {
		cred func(e *env, k eg.Keys) (key, token string)
		name string
		want int
	}{
		{name: "key1", cred: func(_ *env, k eg.Keys) (string, string) { return k.Key1, "" }},
		{
			name: "wrong key", want: http.StatusUnauthorized,
			cred: func(*env, eg.Keys) (string, string) { return "bad", "" },
		},
		{name: "valid sas token", cred: func(e *env, k eg.Keys) (string, string) {
			return "", eg.SignToken(e.srv.URL+"/api/events", e.clock.Now().Add(time.Hour), k.Key2)
		}},
		{name: "expired sas token", want: http.StatusUnauthorized, cred: func(e *env, k eg.Keys) (string, string) {
			return "", eg.SignToken(e.srv.URL+"/api/events", e.clock.Now().Add(-time.Hour), k.Key1)
		}},
		{
			name: "sas token signed with another key", want: http.StatusUnauthorized,
			cred: func(e *env, _ eg.Keys) (string, string) {
				return "", eg.SignToken(e.srv.URL+"/api/events", e.clock.Now().Add(time.Hour), "other")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			keys, _ := e.backend.TopicKeys("t1")

			key, token := tt.cred(e, keys)

			c := e.sasClient(t, token)
			if key != "" {
				c = e.keyClient(t, key)
			}

			_, err := c.PublishEvents(t.Context(), []azeventgrid.Event{sdkEvent("a")}, nil)

			if tt.want == 0 {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.want, statusOf(err))
		})
	}
}

func TestPublisherKeyRegenerationRevokesOldKey(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	old, _ := e.backend.TopicKeys("t1")

	_, err := e.backend.RegenerateTopicKey("t1", "key1")
	require.NoError(t, err)

	_, err = e.keyClient(t, old.Key1).PublishEvents(t.Context(), []azeventgrid.Event{sdkEvent("a")}, nil)
	assert.Equal(t, http.StatusUnauthorized, statusOf(err))

	cur, _ := e.backend.TopicKeys("t1")
	assert.NotEqual(t, old.Key1, cur.Key1)
	assert.Equal(t, old.Key2, cur.Key2)

	_, err = e.keyClient(t, cur.Key1).PublishEvents(t.Context(), []azeventgrid.Event{sdkEvent("a")}, nil)
	require.NoError(t, err)

	_, err = e.backend.RegenerateTopicKey("t1", "key3")
	require.Error(t, err)
}

func (e *env) nsSetup(t *testing.T) eg.Keys {
	t.Helper()

	require.NoError(t, e.backend.PutNamespace("ns1"))
	_, err := e.backend.PutNamespaceTopic("ns1", "nt")
	require.NoError(t, err)
	_, err = e.backend.PutPullSubscription("ns1", "nt", "ps", map[string]any{
		"filtersConfiguration": map[string]any{"includedEventTypes": []any{"my.type"}},
	})
	require.NoError(t, err)

	keys, err := e.backend.NamespaceKeys("ns1")
	require.NoError(t, err)

	return keys
}

func (e *env) sender(t *testing.T, key string) *aznamespaces.SenderClient {
	t.Helper()

	c, err := aznamespaces.NewSenderClientWithSharedKeyCredential(e.srv.URL, "nt", azcore.NewKeyCredential(key),
		&aznamespaces.SenderClientOptions{ClientOptions: e.clientOpts()})
	require.NoError(t, err)

	return c
}

func (e *env) receiver(t *testing.T, key string) *aznamespaces.ReceiverClient {
	t.Helper()

	c, err := aznamespaces.NewReceiverClientWithSharedKeyCredential(e.srv.URL, "nt", "ps", azcore.NewKeyCredential(key),
		&aznamespaces.ReceiverClientOptions{ClientOptions: e.clientOpts()})
	require.NoError(t, err)

	return c
}

func TestNamespacePullDelivery(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	keys := e.nsSetup(t)
	ctx := t.Context()
	snd, rcv := e.sender(t, keys.Key1), e.receiver(t, keys.Key2)

	match, err := messaging.NewCloudEvent("/src", "my.type", map[string]any{"n": 1}, nil)
	require.NoError(t, err)

	other, err := messaging.NewCloudEvent("/src", "other.type", "x", nil)
	require.NoError(t, err)

	_, err = snd.SendEvent(ctx, &match, nil)
	require.NoError(t, err)

	_, err = snd.SendEvents(ctx, []*messaging.CloudEvent{&other, &match}, nil)
	require.NoError(t, err)

	nowait := new(int32)
	two := new(int32)
	*two = 2

	got, err := rcv.ReceiveEvents(ctx, &aznamespaces.ReceiveEventsOptions{MaxEvents: two, MaxWaitTime: nowait})
	require.NoError(t, err)
	require.Len(t, got.Details, 2, "filtered event is not delivered")
	assert.Equal(t, int32(1), *got.Details[0].BrokerProperties.DeliveryCount)
	assert.Equal(t, "my.type", got.Details[0].Event.Type)

	tokens := []string{*got.Details[0].BrokerProperties.LockToken, *got.Details[1].BrokerProperties.LockToken}

	// Everything is locked now.
	empty, err := rcv.ReceiveEvents(ctx, &aznamespaces.ReceiveEventsOptions{MaxWaitTime: nowait})
	require.NoError(t, err)
	assert.Empty(t, empty.Details)

	rel, err := rcv.ReleaseEvents(ctx, tokens[:1], nil)
	require.NoError(t, err)
	assert.Equal(t, tokens[:1], rel.SucceededLockTokens)

	renew, err := rcv.RenewEventLocks(ctx, tokens[1:], nil)
	require.NoError(t, err)
	assert.Equal(t, tokens[1:], renew.SucceededLockTokens)

	again, err := rcv.ReceiveEvents(ctx, &aznamespaces.ReceiveEventsOptions{MaxWaitTime: nowait})
	require.NoError(t, err)
	require.Len(t, again.Details, 1)
	assert.Equal(t, int32(2), *again.Details[0].BrokerProperties.DeliveryCount, "released event is redelivered")

	ack, err := rcv.AcknowledgeEvents(ctx, []string{*again.Details[0].BrokerProperties.LockToken, "bogus"}, nil)
	require.NoError(t, err)
	assert.Len(t, ack.SucceededLockTokens, 1)
	require.Len(t, ack.FailedLockTokens, 1)
	assert.Equal(t, "bogus", *ack.FailedLockTokens[0].LockToken)

	// The other lock expires with the clock and the event comes back.
	e.clock.Advance(2 * time.Minute)

	back, err := rcv.ReceiveEvents(ctx, &aznamespaces.ReceiveEventsOptions{MaxWaitTime: nowait})
	require.NoError(t, err)
	require.Len(t, back.Details, 1)
	assert.Equal(t, int32(2), *back.Details[0].BrokerProperties.DeliveryCount)

	rej, err := rcv.RejectEvents(ctx, []string{*back.Details[0].BrokerProperties.LockToken}, nil)
	require.NoError(t, err)
	assert.Len(t, rej.SucceededLockTokens, 1)

	final, err := rcv.ReceiveEvents(ctx, &aznamespaces.ReceiveEventsOptions{MaxWaitTime: nowait})
	require.NoError(t, err)
	assert.Empty(t, final.Details)
}

func TestNamespaceAuthAndErrors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	keys := e.nsSetup(t)
	ev, _ := messaging.NewCloudEvent("/src", "my.type", "x", nil)

	_, err := e.sender(t, "bad").SendEvent(t.Context(), &ev, nil)
	assert.Equal(t, http.StatusUnauthorized, statusOf(err))

	_, err = e.sender(t, keys.Key1).SendEvent(t.Context(), &ev, nil)
	require.NoError(t, err)

	missing, err := aznamespaces.NewSenderClientWithSharedKeyCredential(
		e.srv.URL,
		"nope",
		azcore.NewKeyCredential(keys.Key1),
		&aznamespaces.SenderClientOptions{ClientOptions: e.clientOpts()},
	)
	require.NoError(t, err)

	_, err = missing.SendEvent(t.Context(), &ev, nil)
	assert.Equal(t, http.StatusNotFound, statusOf(err))
}
