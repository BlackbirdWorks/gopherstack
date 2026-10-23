package azureservicebus_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
)

const sdkTimeout = 20 * time.Second

type amqpEnv struct {
	h         *azureservicebus.Handler
	b         *azureservicebus.InMemoryBackend
	pool      *x509.CertPool
	plainAddr string
	tlsAddr   string
}

func listenLocal(t *testing.T) net.Listener {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	return ln
}

func newAMQPEnv(t *testing.T, validateSAS bool) *amqpEnv {
	t.Helper()

	b := azureservicebus.NewInMemoryBackend()
	h := azureservicebus.NewHandler(b)

	if validateSAS {
		h.WithSASValidation(azureservicebus.DefaultKeyValue)
	}

	cert, err := devtls.GenerateSelfSignedCert()
	require.NoError(t, err)

	h.AMQPCert = &cert

	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	plain, secure := listenLocal(t), listenLocal(t)
	env := &amqpEnv{h: h, b: b, plainAddr: plain.Addr().String(), tlsAddr: secure.Addr().String(), pool: pool}

	require.NoError(t, h.ServeAMQP(plain, false))
	require.NoError(t, h.ServeAMQP(secure, true))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	return env
}

func (e *amqpEnv) plainClient(t *testing.T, key string) *azservicebus.Client {
	t.Helper()

	cs := fmt.Sprintf("Endpoint=sb://%s;SharedAccessKeyName=%s;SharedAccessKey=%s;UseDevelopmentEmulator=true",
		e.plainAddr, azureservicebus.DefaultKeyName, key)

	c, err := azservicebus.NewClientFromConnectionString(cs, nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	return c
}

func (e *amqpEnv) tlsClient(t *testing.T) *azservicebus.Client {
	t.Helper()

	_, port, err := net.SplitHostPort(e.tlsAddr)
	require.NoError(t, err)

	cs := fmt.Sprintf("Endpoint=sb://localhost:%s;SharedAccessKeyName=%s;SharedAccessKey=%s",
		port, azureservicebus.DefaultKeyName, azureservicebus.DefaultKeyValue)

	c, err := azservicebus.NewClientFromConnectionString(cs, &azservicebus.ClientOptions{
		TLSConfig: &tls.Config{RootCAs: e.pool, ServerName: "localhost", MinVersion: tls.VersionTLS12},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close(context.Background()) })

	return c
}

func sdkCtx(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), sdkTimeout)
	t.Cleanup(cancel)

	return ctx
}

func sendOne(t *testing.T, c *azservicebus.Client, entity string, msg *azservicebus.Message) {
	t.Helper()

	s, err := c.NewSender(entity, nil)
	require.NoError(t, err)

	defer s.Close(context.Background())

	require.NoError(t, s.SendMessage(sdkCtx(t), msg, nil))
}

func queueReceiver(
	t *testing.T, c *azservicebus.Client, opts *azservicebus.ReceiverOptions,
) *azservicebus.Receiver {
	t.Helper()

	r, err := c.NewReceiverForQueue("q1", opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = r.Close(context.Background()) })

	return r
}

func receiveN(t *testing.T, r *azservicebus.Receiver, n int) []*azservicebus.ReceivedMessage {
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

type scenario struct {
	run  func(t *testing.T, e *amqpEnv)
	name string
}

func TestAMQP_SDKScenarios(t *testing.T) {
	t.Parallel()

	tests := []scenario{
		{name: "send receive complete", run: scenarioSendReceiveComplete},
		{name: "batch send", run: scenarioBatch},
		{name: "abandon redelivers", run: scenarioAbandon},
		{name: "dead letter with reason", run: scenarioDeadLetter},
		{name: "receive and delete", run: scenarioReceiveAndDelete},
		{name: "peek", run: scenarioPeek},
		{name: "schedule and cancel", run: scenarioScheduleCancel},
		{name: "scheduled delivers", run: scenarioScheduledDelivers},
		{name: "renew lock", run: scenarioRenewLock},
		{name: "defer and receive deferred", run: scenarioDefer},
		{name: "topic fan out", run: scenarioTopicFanOut},
		{name: "rest to amqp", run: scenarioRESTToAMQP},
		{name: "amqp to rest", run: scenarioAMQPToREST},
		{name: "tls transport", run: scenarioTLS},
		{name: "unknown entity", run: scenarioUnknownEntity},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, newAMQPEnv(t, false))
		})
	}
}

func scenarioSendReceiveComplete(t *testing.T, e *amqpEnv) {
	t.Helper()

	_, err := e.b.CreateQueue("q1")
	require.NoError(t, err)

	c := e.plainClient(t, azureservicebus.DefaultKeyValue)
	sendOne(t, c, "q1", &azservicebus.Message{
		Body: []byte("hello"), MessageID: new("m-1"), Subject: new("subj"), ContentType: new("text/plain"),
		CorrelationID: new("corr"), SessionID: new("sess"), To: new("dest"), ReplyTo: new("rt"),
		ApplicationProperties: map[string]any{"color": "red", "n": int64(7)},
	})

	r := queueReceiver(t, c, nil)
	got := receiveN(t, r, 1)[0]

	assert.Equal(t, "hello", string(got.Body))
	assert.Equal(t, "m-1", got.MessageID)
	assert.Equal(t, "subj", *got.Subject)
	assert.Equal(t, "text/plain", *got.ContentType)
	assert.Equal(t, "corr", *got.CorrelationID)
	assert.Equal(t, "sess", *got.SessionID)
	assert.Equal(t, "red", got.ApplicationProperties["color"])
	assert.EqualValues(t, 7, got.ApplicationProperties["n"])
	assert.EqualValues(t, 1, got.DeliveryCount)
	assert.NotNil(t, got.SequenceNumber)
	assert.NotNil(t, got.LockedUntil)

	require.NoError(t, r.CompleteMessage(sdkCtx(t), got, nil))

	info, err := e.b.GetQueueInfo("q1")
	require.NoError(t, err)
	assert.Zero(t, info.ActiveMessageCount)
}

func scenarioBatch(t *testing.T, e *amqpEnv) {
	t.Helper()

	_, err := e.b.CreateQueue("q1")
	require.NoError(t, err)

	c := e.plainClient(t, azureservicebus.DefaultKeyValue)
	s, err := c.NewSender("q1", nil)
	require.NoError(t, err)

	defer s.Close(context.Background())

	batch, err := s.NewMessageBatch(sdkCtx(t), nil)
	require.NoError(t, err)

	for i := range 3 {
		require.NoError(t, batch.AddMessage(&azservicebus.Message{Body: fmt.Appendf(nil, "b%d", i)}, nil))
	}

	require.NoError(t, s.SendMessageBatch(sdkCtx(t), batch, nil))

	got := receiveN(t, queueReceiver(t, c, nil), 3)
	assert.Equal(t, "b0", string(got[0].Body))
	assert.Equal(t, "b2", string(got[2].Body))
}

func scenarioAbandon(t *testing.T, e *amqpEnv) {
	t.Helper()

	_, err := e.b.CreateQueue("q1")
	require.NoError(t, err)

	c := e.plainClient(t, azureservicebus.DefaultKeyValue)
	sendOne(t, c, "q1", &azservicebus.Message{Body: []byte("x")})

	r := queueReceiver(t, c, nil)
	first := receiveN(t, r, 1)[0]
	require.EqualValues(t, 1, first.DeliveryCount)
	require.NoError(t, r.AbandonMessage(sdkCtx(t), first, nil))

	second := receiveN(t, r, 1)[0]
	assert.EqualValues(t, 2, second.DeliveryCount)
	require.NoError(t, r.CompleteMessage(sdkCtx(t), second, nil))
}

func scenarioDeadLetter(t *testing.T, e *amqpEnv) {
	t.Helper()

	_, err := e.b.CreateQueue("q1")
	require.NoError(t, err)

	c := e.plainClient(t, azureservicebus.DefaultKeyValue)
	sendOne(t, c, "q1", &azservicebus.Message{Body: []byte("bad")})

	r := queueReceiver(t, c, nil)
	msg := receiveN(t, r, 1)[0]
	require.NoError(t, r.DeadLetterMessage(sdkCtx(t), msg, &azservicebus.DeadLetterOptions{
		Reason: new("poison"), ErrorDescription: new("cannot parse"),
	}))

	dlq := queueReceiver(t, c, &azservicebus.ReceiverOptions{SubQueue: azservicebus.SubQueueDeadLetter})
	got := receiveN(t, dlq, 1)[0]

	assert.Equal(t, "bad", string(got.Body))
	assert.Equal(t, "poison", *got.DeadLetterReason)
	assert.Equal(t, "cannot parse", *got.DeadLetterErrorDescription)
	require.NoError(t, dlq.CompleteMessage(sdkCtx(t), got, nil))

	info, err := e.b.GetQueueInfo("q1")
	require.NoError(t, err)
	assert.Zero(t, info.DeadLetterMessageCount)
}

func scenarioReceiveAndDelete(t *testing.T, e *amqpEnv) {
	t.Helper()

	_, err := e.b.CreateQueue("q1")
	require.NoError(t, err)

	c := e.plainClient(t, azureservicebus.DefaultKeyValue)
	sendOne(t, c, "q1", &azservicebus.Message{Body: []byte("gone")})

	r := queueReceiver(t, c, &azservicebus.ReceiverOptions{ReceiveMode: azservicebus.ReceiveModeReceiveAndDelete})
	got := receiveN(t, r, 1)[0]
	assert.Equal(t, "gone", string(got.Body))

	info, err := e.b.GetQueueInfo("q1")
	require.NoError(t, err)
	assert.Zero(t, info.ActiveMessageCount)
}

func scenarioPeek(t *testing.T, e *amqpEnv) {
	t.Helper()

	_, err := e.b.CreateQueue("q1")
	require.NoError(t, err)

	c := e.plainClient(t, azureservicebus.DefaultKeyValue)
	for _, body := range []string{"p1", "p2"} {
		sendOne(t, c, "q1", &azservicebus.Message{Body: []byte(body)})
	}

	r := queueReceiver(t, c, nil)
	peeked, err := r.PeekMessages(sdkCtx(t), 5, nil)
	require.NoError(t, err)
	require.Len(t, peeked, 2)
	assert.Equal(t, "p1", string(peeked[0].Body))

	info, err := e.b.GetQueueInfo("q1")
	require.NoError(t, err)
	assert.Equal(t, 2, info.ActiveMessageCount)
}

func scenarioScheduleCancel(t *testing.T, e *amqpEnv) {
	t.Helper()

	_, err := e.b.CreateQueue("q1")
	require.NoError(t, err)

	c := e.plainClient(t, azureservicebus.DefaultKeyValue)
	s, err := c.NewSender("q1", nil)
	require.NoError(t, err)

	defer s.Close(context.Background())

	seqs, err := s.ScheduleMessages(sdkCtx(t), []*azservicebus.Message{{Body: []byte("later")}},
		time.Now().Add(time.Hour), nil)
	require.NoError(t, err)
	require.Len(t, seqs, 1)

	r := queueReceiver(t, c, nil)
	peeked, err := r.PeekMessages(sdkCtx(t), 5, nil)
	require.NoError(t, err)
	require.Len(t, peeked, 1)
	assert.Equal(t, azservicebus.MessageStateScheduled, peeked[0].State)

	require.NoError(t, s.CancelScheduledMessages(sdkCtx(t), seqs, nil))

	peeked, err = r.PeekMessages(sdkCtx(t), 5, nil)
	require.NoError(t, err)
	assert.Empty(t, peeked)
}

func scenarioScheduledDelivers(t *testing.T, e *amqpEnv) {
	t.Helper()

	_, err := e.b.CreateQueue("q1")
	require.NoError(t, err)

	c := e.plainClient(t, azureservicebus.DefaultKeyValue)
	s, err := c.NewSender("q1", nil)
	require.NoError(t, err)

	defer s.Close(context.Background())

	_, err = s.ScheduleMessages(sdkCtx(t), []*azservicebus.Message{{Body: []byte("soon")}},
		time.Now().Add(1500*time.Millisecond), nil)
	require.NoError(t, err)

	got := receiveN(t, queueReceiver(t, c, nil), 1)[0]
	assert.Equal(t, "soon", string(got.Body))
}

func scenarioRenewLock(t *testing.T, e *amqpEnv) {
	t.Helper()

	_, err := e.b.CreateQueue("q1")
	require.NoError(t, err)

	c := e.plainClient(t, azureservicebus.DefaultKeyValue)
	sendOne(t, c, "q1", &azservicebus.Message{Body: []byte("lock")})

	r := queueReceiver(t, c, nil)
	msg := receiveN(t, r, 1)[0]
	before := *msg.LockedUntil

	require.NoError(t, r.RenewMessageLock(sdkCtx(t), msg, nil))
	assert.False(t, msg.LockedUntil.Before(before), "lock must not move backwards: %v -> %v", before, *msg.LockedUntil)
	require.NoError(t, r.CompleteMessage(sdkCtx(t), msg, nil))
}

func scenarioDefer(t *testing.T, e *amqpEnv) {
	t.Helper()

	_, err := e.b.CreateQueue("q1")
	require.NoError(t, err)

	c := e.plainClient(t, azureservicebus.DefaultKeyValue)
	sendOne(t, c, "q1", &azservicebus.Message{Body: []byte("park")})

	r := queueReceiver(t, c, nil)
	msg := receiveN(t, r, 1)[0]
	seq := *msg.SequenceNumber
	require.NoError(t, r.DeferMessage(sdkCtx(t), msg, nil))

	short, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	none, _ := r.ReceiveMessages(short, 1, nil)
	assert.Empty(t, none, "deferred message must not be delivered normally")

	deferred, err := r.ReceiveDeferredMessages(sdkCtx(t), []int64{seq}, nil)
	require.NoError(t, err)
	require.Len(t, deferred, 1)
	assert.Equal(t, "park", string(deferred[0].Body))
	require.NoError(t, r.CompleteMessage(sdkCtx(t), deferred[0], nil))
}

func scenarioTopicFanOut(t *testing.T, e *amqpEnv) {
	t.Helper()

	_, err := e.b.CreateTopic("t1")
	require.NoError(t, err)

	for _, sub := range []string{"s1", "s2"} {
		_, err = e.b.CreateSubscription("t1", sub)
		require.NoError(t, err)
	}

	c := e.plainClient(t, azureservicebus.DefaultKeyValue)
	sendOne(t, c, "t1", &azservicebus.Message{Body: []byte("fan")})

	for _, sub := range []string{"s1", "s2"} {
		r, rerr := c.NewReceiverForSubscription("t1", sub, nil)
		require.NoError(t, rerr)

		got := receiveN(t, r, 1)[0]
		assert.Equal(t, "fan", string(got.Body), sub)
		require.NoError(t, r.CompleteMessage(sdkCtx(t), got, nil))
		require.NoError(t, r.Close(context.Background()))
	}
}

func scenarioRESTToAMQP(t *testing.T, e *amqpEnv) {
	t.Helper()

	rec := doRequest(t, e.h, http.MethodPut, "/q1", nil, nil)
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, e.h, http.MethodPost, "/q1/messages", []byte("from-rest"),
		map[string]string{"Brokerproperties": `{"MessageId":"rest-1","Label":"lbl"}`, "X-Custom": "cv"})
	require.Equal(t, http.StatusCreated, rec.Code)

	got := receiveN(t, queueReceiver(t, e.plainClient(t, azureservicebus.DefaultKeyValue), nil), 1)[0]
	assert.Equal(t, "from-rest", string(got.Body))
	assert.Equal(t, "rest-1", got.MessageID)
	assert.Equal(t, "lbl", *got.Subject)
}

func scenarioAMQPToREST(t *testing.T, e *amqpEnv) {
	t.Helper()

	rec := doRequest(t, e.h, http.MethodPut, "/q1", nil, nil)
	require.Equal(t, http.StatusCreated, rec.Code)

	sendOne(t, e.plainClient(t, azureservicebus.DefaultKeyValue), "q1",
		&azservicebus.Message{Body: []byte("from-amqp"), MessageID: new("amqp-1")})

	rec = doRequest(t, e.h, http.MethodPost, "/q1/messages/head", nil, nil)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Equal(t, "from-amqp", rec.Body.String())
}

func scenarioTLS(t *testing.T, e *amqpEnv) {
	t.Helper()

	_, err := e.b.CreateQueue("q1")
	require.NoError(t, err)

	c := e.tlsClient(t)
	sendOne(t, c, "q1", &azservicebus.Message{Body: []byte("secure")})

	r := queueReceiver(t, c, nil)
	got := receiveN(t, r, 1)[0]
	assert.Equal(t, "secure", string(got.Body))
	require.NoError(t, r.CompleteMessage(sdkCtx(t), got, nil))
}

func scenarioUnknownEntity(t *testing.T, e *amqpEnv) {
	t.Helper()

	c := e.plainClient(t, azureservicebus.DefaultKeyValue)

	s, err := c.NewSender("missing", nil)
	require.NoError(t, err)

	defer s.Close(context.Background())

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	require.Error(t, s.SendMessage(ctx, &azservicebus.Message{Body: []byte("x")}, nil))
}

func TestAMQP_SASValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		wantErr bool
	}{
		{name: "valid key", key: azureservicebus.DefaultKeyValue},
		{name: "wrong key", key: "d3Jvbmcta2V5LXdyb25nLWtleS13cm9uZy1rZXk=", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newAMQPEnv(t, true)
			_, err := e.b.CreateQueue("q1")
			require.NoError(t, err)

			s, err := e.plainClient(t, tt.key).NewSender("q1", nil)
			require.NoError(t, err)

			defer s.Close(context.Background())

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()

			err = s.SendMessage(ctx, &azservicebus.Message{Body: []byte("x")}, nil)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}
