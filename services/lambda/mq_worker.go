package lambda

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	mqSourceBasicAuth   = "BASIC_AUTH"
	mqSourceVirtualHost = "VIRTUAL_HOST"
	mqEngineActiveMQ    = "ACTIVEMQ"
)

// MQBrokerResolver maps a broker ARN to its engine and consumer address (AMQP for RABBITMQ, STOMP for ACTIVEMQ).
// ok is false for metadata-only and not-yet-running brokers.
type MQBrokerResolver interface {
	MQConsumerEndpoint(brokerARN string) (engine, addr string, ok bool)
}

// MQSecretResolver reads a Secrets Manager secret string by ARN.
type MQSecretResolver interface {
	SecretString(ctx context.Context, secretARN string) (string, error)
}

type mqWorkerSpec struct {
	Filter         *FilterCriteria
	UUID           string
	FunctionARN    string
	EventSource    string
	EventSourceARN string
	Consumer       mqConsumerConfig
	BatchSize      int
	Window         time.Duration
}

type mqWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
	spec   mqWorkerSpec
}

// SetMQBrokerResolver sets the resolver used to poll Amazon MQ event sources.
func (p *EventSourcePoller) SetMQBrokerResolver(r MQBrokerResolver) {
	p.mu.Lock("SetMQBrokerResolver")
	defer p.mu.Unlock()

	p.mqResolver = r
}

// SetMQSecretResolver sets the resolver for BASIC_AUTH secrets of Amazon MQ event sources.
func (p *EventSourcePoller) SetMQSecretResolver(r MQSecretResolver) {
	p.mu.Lock("SetMQSecretResolver")
	defer p.mu.Unlock()

	p.mqSecrets = r
}

func (p *EventSourcePoller) mqSources() (MQBrokerResolver, MQSecretResolver) {
	p.mu.RLock("mqSources")
	defer p.mu.RUnlock()

	return p.mqResolver, p.mqSecrets
}

// SetMQBrokerResolver sets the resolver that maps Amazon MQ broker ARNs to real broker addresses.
func (b *InMemoryBackend) SetMQBrokerResolver(r MQBrokerResolver) {
	if p := b.currentPoller(); p != nil {
		p.SetMQBrokerResolver(r)
	}
}

// SetMQSecretResolver sets the resolver for Amazon MQ BASIC_AUTH secrets.
func (b *InMemoryBackend) SetMQSecretResolver(r MQSecretResolver) {
	if p := b.currentPoller(); p != nil {
		p.SetMQSecretResolver(r)
	}
}

func (b *InMemoryBackend) currentPoller() *EventSourcePoller {
	b.mu.RLock("currentPoller")
	defer b.mu.RUnlock()

	return b.kinesisPoller
}

type mqSecret struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func mqAccessConfig(m *EventSourceMapping, typ string) string {
	for _, c := range m.SourceAccessConfigurations {
		if c.Type == typ {
			return c.URI
		}
	}

	return ""
}

// mqSpecFor builds the worker spec; the reason is a fixed code explaining why a mapping is not polled.
func (p *EventSourcePoller) mqSpecFor(ctx context.Context, m *EventSourceMapping) (mqWorkerSpec, string) {
	if len(m.Queues) == 0 {
		return mqWorkerSpec{}, "no queue configured"
	}

	res, secrets := p.mqSources()
	if res == nil {
		return mqWorkerSpec{}, "no real broker behind this source"
	}

	engine, addr, ok := res.MQConsumerEndpoint(m.EventSourceARN)
	if !ok {
		return mqWorkerSpec{}, "no real broker behind this source"
	}

	creds, ok := p.mqCredentials(ctx, secrets, mqAccessConfig(m, mqSourceBasicAuth))
	if !ok {
		return mqWorkerSpec{}, "BASIC_AUTH secret unavailable"
	}

	vhost := mqAccessConfig(m, mqSourceVirtualHost)
	if vhost == "" {
		vhost = mqDefaultVHost
	}

	source := mqEventSourceActiveMQ
	if engine == mqEngineRabbitMQ {
		source = mqEventSourceRabbitMQ
	}

	return mqWorkerSpec{
		UUID:           m.UUID,
		FunctionARN:    m.FunctionARN,
		EventSource:    source,
		EventSourceARN: m.EventSourceARN,
		Filter:         m.FilterCriteria,
		BatchSize:      min(max(m.BatchSize, 1), kafkaMaxBatchSize),
		Window:         mqWindow(m),
		Consumer: mqConsumerConfig{
			Engine: engine, Addr: addr, Queue: m.Queues[0], VHost: vhost,
			Username: creds.Username, Password: creds.Password,
		},
	}, ""
}

func mqWindow(m *EventSourceMapping) time.Duration {
	if m.MaximumBatchingWindowInSeconds > 0 {
		return time.Duration(m.MaximumBatchingWindowInSeconds) * time.Second
	}

	return kafkaDefaultWindow
}

func (p *EventSourcePoller) mqCredentials(
	ctx context.Context,
	secrets MQSecretResolver,
	secretARN string,
) (mqSecret, bool) {
	var creds mqSecret

	if secrets == nil || secretARN == "" {
		return creds, false
	}

	raw, err := secrets.SecretString(ctx, secretARN)
	if err != nil || json.Unmarshal([]byte(raw), &creds) != nil {
		return creds, false
	}

	return creds, creds.Username != ""
}

func isMQARN(a string) bool { return strings.HasPrefix(a, mqARNPrefix) }

// reconcileMQ starts, restarts and stops Amazon MQ workers to match the enabled mappings.
func (p *EventSourcePoller) reconcileMQ(ctx context.Context, mappings []*EventSourceMapping) {
	wanted := make(map[string]mqWorkerSpec)

	for _, m := range mappings {
		if m.State != ESMStateEnabled || !isMQARN(m.EventSourceARN) {
			continue
		}

		if spec, reason := p.mqSpecFor(ctx, m); reason == "" {
			wanted[m.UUID] = spec
		} else {
			p.noteUnpolled(ctx, m, reason)
		}
	}

	var stopped []*mqWorker

	p.mu.Lock("reconcileMQ")
	for id, w := range p.mqWorkers {
		if spec, ok := wanted[id]; ok && reflect.DeepEqual(spec, w.spec) {
			continue
		}

		w.cancel()
		stopped = append(stopped, w)
		delete(p.mqWorkers, id)
	}
	p.mu.Unlock()

	for _, w := range stopped {
		<-w.done
	}

	for id, spec := range wanted {
		p.startMQWorker(ctx, id, spec)
	}
}

func (p *EventSourcePoller) noteUnpolled(ctx context.Context, m *EventSourceMapping, reason string) {
	p.mu.Lock("noteUnpolled")
	_, seen := p.kafkaUnsupported[m.UUID]
	p.kafkaUnsupported[m.UUID] = struct{}{}
	p.mu.Unlock()

	if !seen {
		logger.Load(ctx).WarnContext(ctx, "esm mq: mapping is not polled",
			"uuid", m.UUID, "source", m.EventSourceARN, "reason", reason)
	}
}

func (p *EventSourcePoller) startMQWorker(ctx context.Context, id string, spec mqWorkerSpec) {
	p.mu.Lock("startMQWorker")
	defer p.mu.Unlock()

	if _, running := p.mqWorkers[id]; running {
		return
	}

	wctx, cancel := context.WithCancel(ctx)
	w := &mqWorker{cancel: cancel, done: make(chan struct{}), spec: spec}
	p.mqWorkers[id] = w

	p.kafkaWG.Go(func() {
		defer close(w.done)
		p.runMQWorker(wctx, spec)
	})
}

func (p *EventSourcePoller) stopMQWorker(id string) {
	if w, ok := p.mqWorkers[id]; ok {
		w.cancel()
		delete(p.mqWorkers, id)
	}
}

func (p *EventSourcePoller) mqConsumerFactoryOrDefault() mqConsumerFactory {
	if p.mqFactory != nil {
		return p.mqFactory
	}

	return newMQConsumer
}

func (p *EventSourcePoller) runMQWorker(ctx context.Context, spec mqWorkerSpec) {
	backoff := kafkaRetryBackoffBase

	for ctx.Err() == nil {
		cons, err := p.mqConsumerFactoryOrDefault()(spec.Consumer, spec.BatchSize)
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "esm mq: consumer setup failed", "uuid", spec.UUID)
			p.lambdaBackend.setESMLastProcessingResult(spec.UUID, "PROBLEM: consumer setup failed")
			sleepCtx(ctx, backoff)
			backoff = nextBackoff(backoff)

			continue
		}

		backoff = kafkaRetryBackoffBase
		p.consumeMQ(ctx, cons, spec)
		cons.Close()
		sleepCtx(ctx, backoff)
	}
}

// consumeMQ delivers batches until ctx ends or the consumer breaks.
func (p *EventSourcePoller) consumeMQ(ctx context.Context, cons mqConsumer, spec mqWorkerSpec) {
	backoff := kafkaRetryBackoffBase

	for ctx.Err() == nil {
		batch, err := p.collectMQBatch(ctx, cons, spec)
		if err != nil {
			if ctx.Err() == nil {
				logger.Load(ctx).WarnContext(ctx, "esm mq: poll failed", "uuid", spec.UUID, "error", err)
			}

			return
		}

		if len(batch) == 0 {
			continue
		}

		if p.deliverMQBatch(ctx, cons, spec, batch) {
			backoff = kafkaRetryBackoffBase

			continue
		}

		sleepCtx(ctx, backoff)
		backoff = nextBackoff(backoff)
	}
}

// collectMQBatch gathers messages until BatchSize, the batching window or the payload limit.
func (p *EventSourcePoller) collectMQBatch(
	ctx context.Context,
	cons mqConsumer,
	spec mqWorkerSpec,
) ([]mqMessage, error) {
	msgs, err := cons.Poll(ctx, spec.BatchSize)
	if err != nil || len(msgs) == 0 {
		return msgs, err
	}

	wctx, cancel := context.WithTimeout(ctx, spec.Window)
	defer cancel()

	for len(msgs) < spec.BatchSize && wctx.Err() == nil && mqBatchBytes(msgs) < kafkaMaxPayloadBytes {
		more, pollErr := cons.Poll(wctx, spec.BatchSize-len(msgs))
		msgs = append(msgs, more...)

		if pollErr != nil {
			break
		}
	}

	return msgs, nil
}

// deliverMQBatch invokes the function per chunk and acks only what succeeded; true when fully acked.
func (p *EventSourcePoller) deliverMQBatch(
	ctx context.Context,
	cons mqConsumer,
	spec mqWorkerSpec,
	batch []mqMessage,
) bool {
	matched, dropped := splitMQByFilter(spec.Filter, batch)
	chunks := splitMQByPayload(matched, kafkaMaxPayloadBytes)

	for i, chunk := range chunks {
		if !p.invokeMQ(ctx, spec, chunk) {
			var unsettled []mqMessage
			for _, rest := range chunks[i:] {
				unsettled = append(unsettled, rest...)
			}

			p.settleMQ(ctx, spec, append(unsettled, dropped...), cons.Requeue)

			return false
		}

		p.settleMQ(ctx, spec, chunk, cons.Ack)
	}

	p.settleMQ(ctx, spec, dropped, cons.Ack)
	p.lambdaBackend.setESMLastProcessingResult(spec.UUID, "OK")

	return true
}

func (p *EventSourcePoller) settleMQ(
	ctx context.Context, spec mqWorkerSpec, msgs []mqMessage, fn func(context.Context, []mqMessage) error,
) {
	if len(msgs) == 0 {
		return
	}

	if err := fn(context.WithoutCancel(ctx), msgs); err != nil {
		logger.Load(ctx).WarnContext(ctx, "esm mq: settling messages failed", "uuid", spec.UUID, "error", err)
	}
}

// invokeMQ reports whether the function accepted the chunk.
func (p *EventSourcePoller) invokeMQ(ctx context.Context, spec mqWorkerSpec, chunk []mqMessage) bool {
	payload, err := buildMQEventPayload(spec.EventSource, spec.EventSourceARN, spec.Consumer.VHost, chunk)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "esm mq: failed to marshal event", "uuid", spec.UUID, "error", err)

		return false
	}

	if invErr := p.invokeKafka(ctx, spec.FunctionARN, payload); invErr != nil {
		logger.Load(ctx).WarnContext(ctx, "esm mq: invocation failed; messages will be redelivered",
			"uuid", spec.UUID, "error", invErr)
		p.lambdaBackend.setESMLastProcessingResult(spec.UUID, "PROBLEM: "+invErr.Error())

		return false
	}

	return true
}
