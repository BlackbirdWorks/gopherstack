package lambda

import (
	"context"
	"encoding/base64"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/moby/moby/client"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/mq"
	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

const mqTestPassword = "s3cretpassword"

type mqRecordingRuntime struct {
	mq.BrokerRuntime
	ids []string
	mu  sync.Mutex
}

func (r *mqRecordingRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	id, err := r.BrokerRuntime.CreateAndStart(ctx, spec)
	if err == nil {
		r.mu.Lock()
		r.ids = append(r.ids, id)
		r.mu.Unlock()
	}

	return id, err
}

func (r *mqRecordingRuntime) Close() error {
	if c, ok := r.BrokerRuntime.(io.Closer); ok {
		return c.Close()
	}

	return nil
}

func (r *mqRecordingRuntime) containerIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.ids...)
}

type mqHarness struct {
	mq         *mq.InMemoryBackend
	rt         *mqRecordingRuntime
	poller     *EventSourcePoller
	lambda     *InMemoryBackend
	deliveries chan []byte
	broker     *mq.Broker
	esmUUID    string
	attempts   int
	mu         sync.Mutex
	failFirst  bool
}

// startMQBroker boots a real broker, wires a poller to it and creates the ESM; skips without Docker.
func startMQBroker(t *testing.T, engine, queue string, vhostCfg []SourceAccessConfiguration) *mqHarness {
	t.Helper()

	testcontainers.SkipIfProviderIsNotHealthy(t)

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	h := &mqHarness{rt: &mqRecordingRuntime{BrokerRuntime: rt}, deliveries: make(chan []byte, 8)}
	h.mq = mq.NewInMemoryBackend("000000000000", "us-east-1")
	h.mq.EnableBrokers(mq.BrokerConfig{Runtime: h.rt, StartTimeout: kafkaStartupTimeout})
	t.Cleanup(h.mq.Close)

	h.broker, err = h.mq.CreateBroker("real-"+queue, "", engine, "", "", false, false, nil, nil,
		[]*mq.User{{Username: "admin", Password: mqTestPassword}}, nil)
	require.NoError(t, err)
	assert.Equal(t, mq.BrokerStateCreating, h.broker.BrokerState)

	require.Eventually(t, func() bool {
		br, descErr := h.mq.DescribeBroker(h.broker.BrokerID)

		return descErr == nil && br.BrokerState == mq.BrokerStateRunning
	}, kafkaStartupTimeout, 500*time.Millisecond)

	sm := secretsmanager.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	secret, err := sm.CreateSecret(t.Context(), &secretsmanager.CreateSecretInput{
		Name: "mq-creds", SecretString: `{"username":"admin","password":"` + mqTestPassword + `"}`,
	})
	require.NoError(t, err)

	b := newKafkaTestBackend(t)
	h.lambda = b
	h.poller = NewEventSourcePoller(b, nil)
	h.poller.SetMQBrokerResolver(h.mq)
	h.poller.SetMQSecretResolver(sm)
	h.poller.kafkaInvoker = func(_ context.Context, _ string, payload []byte) error {
		h.mu.Lock()
		h.attempts++
		fail := h.failFirst && h.attempts == 1
		h.mu.Unlock()

		h.deliveries <- payload

		if fail {
			return errMQTestFunction
		}

		return nil
	}
	b.SetKinesisPoller(h.poller)
	b.StartKinesisPoller(context.Background())

	esm, err := b.CreateEventSourceMapping(&CreateEventSourceMappingInput{
		FunctionName:                   "fn",
		EventSourceARN:                 h.broker.BrokerArn,
		Enabled:                        true,
		Queues:                         []string{queue},
		BatchSize:                      10,
		MaximumBatchingWindowInSeconds: 1,
		SourceAccessConfigurations: append(
			[]SourceAccessConfiguration{{Type: "BASIC_AUTH", URI: secret.ARN}},
			vhostCfg...),
	})
	require.NoError(t, err)

	h.esmUUID = esm.UUID

	return h
}

func (h *mqHarness) awaitPayload(t *testing.T) map[string]any {
	t.Helper()

	return awaitDelivery(t, h.deliveries)
}

func (h *mqHarness) requireContainerRemovedOnDelete(t *testing.T) {
	t.Helper()

	ids := h.rt.containerIDs()
	require.Len(t, ids, 1)

	dc, err := testcontainers.NewDockerClientWithOpts(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = dc.Close() })

	_, err = dc.ContainerInspect(t.Context(), ids[0], client.ContainerInspectOptions{})
	require.NoError(t, err, "broker container runs while the broker exists")

	_, err = h.mq.DeleteBroker(h.broker.BrokerID)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, inspectErr := dc.ContainerInspect(t.Context(), ids[0], client.ContainerInspectOptions{})

		return inspectErr != nil
	}, time.Minute, 500*time.Millisecond, "DeleteBroker removes the broker container")
}

func TestMQESMRealRabbitMQ(t *testing.T) {
	t.Parallel()

	const queue = "pizza"

	h := startMQBroker(t, mq.EngineTypeRabbitMQ, queue, nil)

	engine, addr, ok := h.mq.MQConsumerEndpoint(h.broker.BrokerArn)
	require.True(t, ok)
	assert.Equal(t, mq.EngineTypeRabbitMQ, engine)

	desc, err := h.mq.DescribeBroker(h.broker.BrokerID)
	require.NoError(t, err)
	assert.Equal(t, "amqp://"+addr, desc.BrokerInstances[0].Endpoints[0])

	conn, err := amqp.DialConfig("amqp://"+addr+"/", amqp.Config{
		SASL: []amqp.Authentication{&amqp.PlainAuth{Username: "admin", Password: mqTestPassword}},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ch, err := conn.Channel()
	require.NoError(t, err)

	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	require.NoError(t, err)

	h.mu.Lock()
	h.failFirst = true
	h.mu.Unlock()

	for _, body := range []string{`{"id":1}`, `{"id":2}`} {
		require.NoError(t, ch.PublishWithContext(t.Context(), "", queue, false, false, amqp.Publishing{
			ContentType: "application/json", Body: []byte(body), Headers: amqp.Table{"source": "test", "n": int32(10)},
		}))
	}

	first := h.awaitPayload(t)
	assert.Equal(t, "aws:rmq", first["eventSource"])
	assert.Equal(t, h.broker.BrokerArn, first["eventSourceArn"])

	second := h.awaitPayload(t)
	msgs := rmqMessages(t, second, queue+"::/")
	require.Len(t, msgs, 2, "failed batch is redelivered")

	for i, want := range []string{`{"id":1}`, `{"id":2}`} {
		assert.Equal(t, true, msgs[i]["redelivered"])
		assert.Equal(t, base64.StdEncoding.EncodeToString([]byte(want)), msgs[i]["data"])

		props, isMap := msgs[i]["basicProperties"].(map[string]any)
		require.True(t, isMap)
		assert.Equal(t, "application/json", props["contentType"])
		assert.Equal(
			t,
			map[string]any{"bytes": []any{116.0, 101.0, 115.0, 116.0}},
			props["headers"].(map[string]any)["source"],
		)
	}

	_, err = h.lambda.DeleteEventSourceMapping(h.esmUUID)
	require.NoError(t, err)

	insp, err := conn.Channel()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		q, inspErr := insp.QueueDeclarePassive(queue, true, false, false, false, nil)

		return inspErr == nil && q.Messages == 0
	}, time.Minute, 500*time.Millisecond, "acked messages are gone and nothing was left unacked")

	h.requireContainerRemovedOnDelete(t)
}

func TestMQESMRealActiveMQ(t *testing.T) {
	t.Parallel()

	const queue = "pizza"

	h := startMQBroker(t, mq.EngineTypeActiveMQ, queue, nil)

	engine, addr, ok := h.mq.MQConsumerEndpoint(h.broker.BrokerArn)
	require.True(t, ok)
	assert.Equal(t, mq.EngineTypeActiveMQ, engine)

	desc, err := h.mq.DescribeBroker(h.broker.BrokerID)
	require.NoError(t, err)
	require.Len(t, desc.BrokerInstances[0].Endpoints, 3)
	assert.Equal(t, "stomp://"+addr, desc.BrokerInstances[0].Endpoints[1])

	conn, err := stomp.Dial("tcp", addr, stomp.ConnOpt.Login("admin", mqTestPassword))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Disconnect() })

	h.mu.Lock()
	h.failFirst = true
	h.mu.Unlock()

	for _, body := range []string{"hello", "world"} {
		require.NoError(t, conn.Send("/queue/"+queue, "text/plain", []byte(body),
			stomp.SendOpt.Header("myCustomProperty", "value")))
	}

	ev := h.awaitPayload(t)

	for msgs, _ := ev["messages"].([]any); !redeliveredAll(msgs); msgs, _ = ev["messages"].([]any) {
		ev = h.awaitPayload(t)
	}

	assert.Equal(t, "aws:mq", ev["eventSource"])
	assert.Equal(t, h.broker.BrokerArn, ev["eventSourceArn"])

	msgs, _ := ev["messages"].([]any)
	require.Len(t, msgs, 2)

	first, _ := msgs[0].(map[string]any)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte("hello")), first["data"])
	assert.Equal(t, map[string]any{"physicalName": queue}, first["destination"])
	assert.Equal(t, "value", first["properties"].(map[string]any)["myCustomProperty"])

	_, err = h.lambda.DeleteEventSourceMapping(h.esmUUID)
	require.NoError(t, err)

	sub, err := conn.Subscribe("/queue/"+queue, stomp.AckAuto)
	require.NoError(t, err)

	select {
	case m := <-sub.C:
		require.FailNow(t, "message was left unacked and redelivered", "%v", m.Header)
	case <-time.After(3 * time.Second):
	}

	h.requireContainerRemovedOnDelete(t)
}

func redeliveredAll(msgs []any) bool {
	for _, m := range msgs {
		mm, _ := m.(map[string]any)
		if mm["redelivered"] != true {
			return false
		}
	}

	return true
}

func rmqMessages(t *testing.T, ev map[string]any, key string) []map[string]any {
	t.Helper()

	by, ok := ev["rmqMessagesByQueue"].(map[string]any)
	require.True(t, ok)

	list, ok := by[key].([]any)
	require.True(t, ok)

	out := make([]map[string]any, 0, len(list))

	for _, m := range list {
		mm, isMap := m.(map[string]any)
		require.True(t, isMap)

		out = append(out, mm)
	}

	return out
}
