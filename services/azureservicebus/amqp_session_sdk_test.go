package azureservicebus_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
)

func sessionCfg() azureservicebus.EntityConfig {
	return azureservicebus.EntityConfig{RequiresSession: true, LockDuration: 30 * time.Second}
}

type fakeClock struct{ offset atomic.Int64 }

func (f *fakeClock) now() time.Time {
	return time.Unix(1_800_000_000, 0).Add(time.Duration(f.offset.Load()))
}

func (f *fakeClock) advance(d time.Duration) { f.offset.Add(int64(d)) }

func sessionEnv(t *testing.T) (*amqpEnv, *azservicebus.Client) {
	t.Helper()

	e := newAMQPEnv(t, false)
	_, err := e.b.CreateQueue("sq", sessionCfg())
	require.NoError(t, err)

	return e, e.plainClient(t, azureservicebus.DefaultKeyValue)
}

func sendSession(t *testing.T, c *azservicebus.Client, entity, session string, bodies ...string) {
	t.Helper()

	for _, body := range bodies {
		sendOne(t, c, entity, &azservicebus.Message{Body: []byte(body), SessionID: new(session)})
	}
}

func acceptSession(t *testing.T, c *azservicebus.Client, id string) *azservicebus.SessionReceiver {
	t.Helper()

	r, err := c.AcceptSessionForQueue(sdkCtx(t), "sq", id, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })

	return r
}

func receiveSessionN(t *testing.T, r *azservicebus.SessionReceiver, n int) []*azservicebus.ReceivedMessage {
	t.Helper()

	var out []*azservicebus.ReceivedMessage

	ctx := sdkCtx(t)

	for len(out) < n {
		msgs, err := r.ReceiveMessages(ctx, n-len(out), nil)
		require.NoError(t, err)

		out = append(out, msgs...)
	}

	return out
}

func bodies(msgs []*azservicebus.ReceivedMessage) []string {
	out := make([]string, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, string(m.Body))
	}

	return out
}

type sessionScenario struct {
	run  func(t *testing.T)
	name string
}

func TestAMQP_Sessions(t *testing.T) {
	t.Parallel()

	tests := []sessionScenario{
		{name: "accept specific session", run: scenarioAcceptSpecific},
		{name: "accept next session", run: scenarioAcceptNext},
		{name: "fifo within session", run: scenarioSessionFIFO},
		{name: "session state round trip", run: scenarioSessionState},
		{name: "renew session lock", run: scenarioRenewSession},
		{name: "lock contention", run: scenarioSessionContention},
		{name: "send without session id", run: scenarioSendNoSession},
		{name: "plain receiver refused", run: scenarioPlainReceiverRefused},
		{name: "accept next times out", run: scenarioAcceptNextTimeout},
		{name: "subscription sessions", run: scenarioSubscriptionSessions},
		{name: "lock expiry releases session", run: scenarioSessionExpiry},
		{name: "settle after expiry fails", run: scenarioSettleAfterExpiry},
		{name: "peek stays in session", run: scenarioSessionPeek},
		{name: "close releases session", run: scenarioSessionCloseReleases},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t)
		})
	}
}

func scenarioAcceptSpecific(t *testing.T) {
	t.Helper()

	_, c := sessionEnv(t)
	sendSession(t, c, "sq", "s1", "a1", "a2")
	sendSession(t, c, "sq", "s2", "b1")

	r := acceptSession(t, c, "s2")
	assert.Equal(t, "s2", r.SessionID())
	assert.True(t, r.LockedUntil().After(time.Now()))

	msgs := receiveSessionN(t, r, 1)
	assert.Equal(t, []string{"b1"}, bodies(msgs))
	require.NoError(t, r.CompleteMessage(sdkCtx(t), msgs[0], nil))
}

func scenarioAcceptNext(t *testing.T) {
	t.Helper()

	_, c := sessionEnv(t)
	sendSession(t, c, "sq", "s1", "a1")
	sendSession(t, c, "sq", "s2", "b1")

	r1, err := c.AcceptNextSessionForQueue(sdkCtx(t), "sq", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r1.Close(context.Background()) })

	r2, err := c.AcceptNextSessionForQueue(sdkCtx(t), "sq", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r2.Close(context.Background()) })

	assert.NotEqual(t, r1.SessionID(), r2.SessionID())
	assert.ElementsMatch(t, []string{"s1", "s2"}, []string{r1.SessionID(), r2.SessionID()})

	got1 := receiveSessionN(t, r1, 1)
	got2 := receiveSessionN(t, r2, 1)

	assert.Equal(t, r1.SessionID(), *got1[0].SessionID)
	assert.Equal(t, r2.SessionID(), *got2[0].SessionID)
}

func scenarioSessionFIFO(t *testing.T) {
	t.Helper()

	_, c := sessionEnv(t)
	want := []string{"m1", "m2", "m3", "m4", "m5"}
	sendSession(t, c, "sq", "s1", want...)
	sendSession(t, c, "sq", "other", "x1")

	r := acceptSession(t, c, "s1")
	msgs := receiveSessionN(t, r, len(want))
	assert.Equal(t, want, bodies(msgs))

	for _, m := range msgs {
		require.NoError(t, r.CompleteMessage(sdkCtx(t), m, nil))
	}
}

func scenarioSessionState(t *testing.T) {
	t.Helper()

	_, c := sessionEnv(t)
	r := acceptSession(t, c, "s1")

	state, err := r.GetSessionState(sdkCtx(t), nil)
	require.NoError(t, err)
	assert.Nil(t, state)

	require.NoError(t, r.SetSessionState(sdkCtx(t), []byte("progress=3"), nil))

	state, err = r.GetSessionState(sdkCtx(t), nil)
	require.NoError(t, err)
	assert.Equal(t, []byte("progress=3"), state)

	require.NoError(t, r.SetSessionState(sdkCtx(t), nil, nil))

	state, err = r.GetSessionState(sdkCtx(t), nil)
	require.NoError(t, err)
	assert.Nil(t, state)
}

func scenarioRenewSession(t *testing.T) {
	t.Helper()

	e, c := sessionEnv(t)
	clk := &fakeClock{}
	azureservicebus.SetNowFunc(e.b, clk.now)

	r := acceptSession(t, c, "s1")
	before := r.LockedUntil()

	clk.advance(10 * time.Second)

	require.NoError(t, r.RenewSessionLock(sdkCtx(t), nil))
	assert.Greater(t, r.LockedUntil().UnixMilli(), before.UnixMilli())
}

func scenarioSessionContention(t *testing.T) {
	t.Helper()

	_, c := sessionEnv(t)
	acceptSession(t, c, "s1")

	_, err := c.AcceptSessionForQueue(sdkCtx(t), "sq", "s1", nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "session-cannot-be-locked")
}

func scenarioSendNoSession(t *testing.T) {
	t.Helper()

	_, c := sessionEnv(t)

	s, err := c.NewSender("sq", nil)
	require.NoError(t, err)

	defer s.Close(context.Background())

	err = s.SendMessage(sdkCtx(t), &azservicebus.Message{Body: []byte("x")}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SessionId was not set")
}

func scenarioPlainReceiverRefused(t *testing.T) {
	t.Helper()

	_, c := sessionEnv(t)

	r, err := c.NewReceiverForQueue("sq", nil)
	require.NoError(t, err)

	defer r.Close(context.Background())

	_, err = r.ReceiveMessages(sdkCtx(t), 1, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "non-sessionful")
}

func scenarioAcceptNextTimeout(t *testing.T) {
	t.Helper()

	_, c := sessionEnv(t)

	ctx, cancel := context.WithTimeout(t.Context(), 700*time.Millisecond)
	defer cancel()

	_, err := c.AcceptNextSessionForQueue(ctx, "sq", nil)
	require.Error(t, err)
}

func scenarioSubscriptionSessions(t *testing.T) {
	t.Helper()

	e := newAMQPEnv(t, false)
	_, err := e.b.CreateTopic("tp")
	require.NoError(t, err)
	_, err = e.b.CreateSubscription("tp", "sub", sessionCfg())
	require.NoError(t, err)

	c := e.plainClient(t, azureservicebus.DefaultKeyValue)
	sendSession(t, c, "tp", "s1", "t1", "t2")

	r, err := c.AcceptSessionForSubscription(sdkCtx(t), "tp", "sub", "s1", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })

	assert.Equal(t, []string{"t1", "t2"}, bodies(receiveSessionN(t, r, 2)))

	sendSession(t, c, "tp", "s2", "u1")

	n, err := c.AcceptNextSessionForSubscription(sdkCtx(t), "tp", "sub", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = n.Close(context.Background()) })

	assert.Equal(t, "s2", n.SessionID())

	s, serr := c.NewSender("tp", nil)
	require.NoError(t, serr)

	defer s.Close(context.Background())

	require.Error(t, s.SendMessage(sdkCtx(t), &azservicebus.Message{Body: []byte("x")}, nil))
}

func scenarioSessionExpiry(t *testing.T) {
	t.Helper()

	e, c := sessionEnv(t)
	clk := &fakeClock{}
	azureservicebus.SetNowFunc(e.b, clk.now)
	sendSession(t, c, "sq", "s1", "a")

	acceptSession(t, c, "s1")

	clk.advance(31 * time.Second)

	r2, err := c.AcceptSessionForQueue(sdkCtx(t), "sq", "s1", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r2.Close(context.Background()) })

	assert.Equal(t, []string{"a"}, bodies(receiveSessionN(t, r2, 1)))
}

func scenarioSettleAfterExpiry(t *testing.T) {
	t.Helper()

	e, c := sessionEnv(t)
	clk := &fakeClock{}
	azureservicebus.SetNowFunc(e.b, clk.now)
	sendSession(t, c, "sq", "s1", "a")

	r := acceptSession(t, c, "s1")
	msg := receiveSessionN(t, r, 1)[0]

	clk.advance(31 * time.Second)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	require.Error(t, r.CompleteMessage(ctx, msg, nil), "settling with an expired session lock must fail")

	peeked, err := e.b.Peek(azureservicebus.EntityRef{Queue: "sq"}, false, 0, 10)
	require.NoError(t, err)
	assert.Len(t, peeked, 1, "the message must not have been completed")
}

func scenarioSessionPeek(t *testing.T) {
	t.Helper()

	_, c := sessionEnv(t)
	sendSession(t, c, "sq", "s1", "a1", "a2")
	sendSession(t, c, "sq", "s2", "b1")

	r := acceptSession(t, c, "s1")

	peeked, err := r.PeekMessages(sdkCtx(t), 10, nil)
	require.NoError(t, err)
	assert.Equal(t, []string{"a1", "a2"}, bodies(peeked))
}

func scenarioSessionCloseReleases(t *testing.T) {
	t.Helper()

	_, c := sessionEnv(t)
	sendSession(t, c, "sq", "s1", "a")

	r, err := c.AcceptSessionForQueue(sdkCtx(t), "sq", "s1", nil)
	require.NoError(t, err)
	require.NoError(t, r.Close(context.Background()))

	require.Eventually(t, func() bool {
		r2, aerr := c.AcceptSessionForQueue(sdkCtx(t), "sq", "s1", nil)
		if aerr != nil {
			return false
		}

		_ = r2.Close(context.Background())

		return true
	}, 10*time.Second, 50*time.Millisecond)
}
