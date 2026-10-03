package lambda

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	kafkaDefaultWindow    = 500 * time.Millisecond
	kafkaMaxBatchSize     = 10000
	kafkaBackoffFactor    = 2
	kafkaRetryBackoffBase = time.Second
	kafkaRetryBackoffMax  = 30 * time.Second
	kafkaMSKARNPrefix     = "arn:aws:kafka:"
	mqARNPrefix           = "arn:aws:mq:"
)

var errKafkaFunction = errors.New("function returned an error")

// kafkaWorkerSpec is everything a worker needs; a changed spec restarts it.
type kafkaWorkerSpec struct {
	UUID             string
	FunctionARN      string
	EventSource      string
	EventSourceARN   string
	BootstrapServers string
	Filter           *FilterCriteria
	Consumer         kafkaConsumerConfig
	BatchSize        int
	Window           time.Duration
}

type kafkaWorker struct {
	cancel context.CancelFunc
	done   chan struct{}
	spec   kafkaWorkerSpec
}

func kafkaConsumerGroup(m *EventSourceMapping) string {
	if c := m.SelfManagedKafkaEventSourceConfig; c != nil && c.ConsumerGroupID != "" {
		return c.ConsumerGroupID
	}

	if c := m.AmazonManagedKafkaEventSourceConfig; c != nil && c.ConsumerGroupID != "" {
		return c.ConsumerGroupID
	}

	return m.UUID
}

// kafkaSpecFor returns the worker spec for a self-managed Kafka mapping.
func kafkaSpecFor(m *EventSourceMapping) (kafkaWorkerSpec, bool) {
	return buildKafkaSpec(m, kafkaEventSourceSelfManaged, "", kafkaSourceBootstrap(m.SelfManagedEventSource))
}

// mskSpecFor returns the worker spec for an MSK mapping whose cluster has a reachable real broker.
func (p *EventSourcePoller) mskSpecFor(m *EventSourceMapping) (kafkaWorkerSpec, bool) {
	if !strings.HasPrefix(m.EventSourceARN, kafkaMSKARNPrefix) {
		return kafkaWorkerSpec{}, false
	}

	return buildKafkaSpec(m, kafkaEventSourceMSK, m.EventSourceARN, p.mskBrokers(m.EventSourceARN))
}

func buildKafkaSpec(m *EventSourceMapping, source, sourceARN string, brokers []string) (kafkaWorkerSpec, bool) {
	if len(brokers) == 0 || len(m.Topics) == 0 {
		return kafkaWorkerSpec{}, false
	}

	window := time.Duration(m.MaximumBatchingWindowInSeconds) * time.Second
	if window <= 0 {
		window = kafkaDefaultWindow
	}

	batch := min(max(m.BatchSize, 1), kafkaMaxBatchSize)

	var ts time.Time
	if m.StartingPositionTimestamp > 0 {
		sec := int64(m.StartingPositionTimestamp)
		ts = time.Unix(sec, int64((m.StartingPositionTimestamp-float64(sec))*float64(time.Second)))
	}

	return kafkaWorkerSpec{
		UUID:             m.UUID,
		FunctionARN:      m.FunctionARN,
		EventSource:      source,
		EventSourceARN:   sourceARN,
		BootstrapServers: strings.Join(brokers, ","),
		Filter:           m.FilterCriteria,
		BatchSize:        batch,
		Window:           window,
		Consumer: kafkaConsumerConfig{
			Brokers:          brokers,
			Topics:           m.Topics,
			GroupID:          kafkaConsumerGroup(m),
			StartingPosition: m.StartingPosition,
			StartTimestamp:   ts,
		},
	}, true
}

// isUnsupportedBrokerARN reports broker ARNs served by broker workers, not the record pollers.
func isUnsupportedBrokerARN(a string) bool {
	return strings.HasPrefix(a, kafkaMSKARNPrefix) || strings.HasPrefix(a, mqARNPrefix)
}

// reconcileKafka starts, restarts and stops Kafka workers to match the enabled mappings.
func (p *EventSourcePoller) reconcileKafka(ctx context.Context, mappings []*EventSourceMapping) {
	wanted := make(map[string]kafkaWorkerSpec)

	for _, m := range mappings {
		if m.State != ESMStateEnabled {
			continue
		}

		if spec, ok := p.kafkaSpecForMapping(m); ok {
			wanted[m.UUID] = spec
		} else if strings.HasPrefix(m.EventSourceARN, kafkaMSKARNPrefix) {
			p.noteUnsupportedBroker(ctx, m)
		}
	}

	var stopped []*kafkaWorker

	p.mu.Lock("reconcileKafka")
	for id, w := range p.kafkaWorkers {
		if spec, ok := wanted[id]; ok && reflect.DeepEqual(spec, w.spec) {
			continue
		}

		w.cancel()
		stopped = append(stopped, w)
		delete(p.kafkaWorkers, id)
	}
	p.mu.Unlock()

	for _, w := range stopped {
		<-w.done
	}

	for id, spec := range wanted {
		p.startKafkaWorker(ctx, id, spec)
	}
}

func (p *EventSourcePoller) kafkaSpecForMapping(m *EventSourceMapping) (kafkaWorkerSpec, bool) {
	if spec, ok := p.mskSpecFor(m); ok {
		return spec, true
	}

	return kafkaSpecFor(m)
}

func (p *EventSourcePoller) noteUnsupportedBroker(ctx context.Context, m *EventSourceMapping) {
	p.mu.Lock("noteUnsupportedBroker")
	_, seen := p.kafkaUnsupported[m.UUID]
	p.kafkaUnsupported[m.UUID] = struct{}{}
	p.mu.Unlock()

	if !seen {
		logger.Load(ctx).WarnContext(ctx, "esm: no real broker behind this source; mapping is not polled",
			"uuid", m.UUID, "source", m.EventSourceARN)
	}
}

func (p *EventSourcePoller) startKafkaWorker(ctx context.Context, id string, spec kafkaWorkerSpec) {
	p.mu.Lock("startKafkaWorker")
	defer p.mu.Unlock()

	if _, running := p.kafkaWorkers[id]; running {
		return
	}

	wctx, cancel := context.WithCancel(ctx)
	w := &kafkaWorker{cancel: cancel, done: make(chan struct{}), spec: spec}
	p.kafkaWorkers[id] = w

	p.kafkaWG.Go(func() {
		defer close(w.done)
		p.runKafkaWorker(wctx, spec)
	})
}

// stopKafkaWorker cancels one worker without waiting; safe to call under the backend lock.
func (p *EventSourcePoller) stopKafkaWorker(id string) {
	if w, ok := p.kafkaWorkers[id]; ok {
		w.cancel()
		delete(p.kafkaWorkers, id)
	}

	delete(p.kafkaUnsupported, id)
}

// stopAllKafka cancels every worker and waits for them to exit.
func (p *EventSourcePoller) stopAllKafka() {
	p.mu.Lock("stopAllKafka")
	for id := range p.kafkaWorkers {
		p.stopKafkaWorker(id)
	}

	for id := range p.mqWorkers {
		p.stopMQWorker(id)
	}
	p.mu.Unlock()

	p.kafkaWG.Wait()
}

func (p *EventSourcePoller) kafkaConsumerFactoryOrDefault() kafkaConsumerFactory {
	if p.kafkaFactory != nil {
		return p.kafkaFactory
	}

	return newFranzConsumer
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

func nextBackoff(cur time.Duration) time.Duration {
	return min(cur*kafkaBackoffFactor, kafkaRetryBackoffMax)
}

func (p *EventSourcePoller) runKafkaWorker(ctx context.Context, spec kafkaWorkerSpec) {
	backoff := kafkaRetryBackoffBase
	log := logger.Load(ctx)

	for ctx.Err() == nil {
		cons, err := p.kafkaConsumerFactoryOrDefault()(spec.Consumer)
		if err != nil {
			log.WarnContext(ctx, "esm kafka: consumer setup failed", "uuid", spec.UUID, "error", err)
			sleepCtx(ctx, backoff)
			backoff = nextBackoff(backoff)

			continue
		}

		p.consumeKafka(ctx, cons, spec)
		cons.Close()
	}
}

func (p *EventSourcePoller) consumeKafka(ctx context.Context, cons kafkaConsumer, spec kafkaWorkerSpec) {
	backoff := kafkaRetryBackoffBase

	for ctx.Err() == nil {
		batch, err := p.collectKafkaBatch(ctx, cons, spec)
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "esm kafka: poll failed", "uuid", spec.UUID, "error", err)
			p.lambdaBackend.setESMLastProcessingResult(spec.UUID, "PROBLEM: "+err.Error())
			sleepCtx(ctx, backoff)
			backoff = nextBackoff(backoff)

			continue
		}

		backoff = kafkaRetryBackoffBase

		if len(batch) > 0 {
			p.deliverKafkaBatch(ctx, cons, spec, batch)
		}
	}
}

// collectKafkaBatch gathers records until BatchSize, the batching window or the payload limit.
func (p *EventSourcePoller) collectKafkaBatch(
	ctx context.Context, cons kafkaConsumer, spec kafkaWorkerSpec,
) ([]KafkaRecord, error) {
	recs, err := cons.Poll(ctx, spec.BatchSize)
	if err != nil || len(recs) == 0 {
		return recs, err
	}

	wctx, cancel := context.WithTimeout(ctx, spec.Window)
	defer cancel()

	for len(recs) < spec.BatchSize && wctx.Err() == nil && kafkaBatchBytes(recs) < kafkaMaxPayloadBytes {
		more, pollErr := cons.Poll(wctx, spec.BatchSize-len(recs))
		recs = append(recs, more...)

		if pollErr != nil {
			break
		}
	}

	return recs, nil
}

func kafkaBatchBytes(recs []KafkaRecord) int {
	n := 0
	for _, r := range recs {
		n += kafkaRecordSize(r)
	}

	return n
}

// deliverKafkaBatch filters, chunks and invokes a batch, retrying until it succeeds, then commits.
func (p *EventSourcePoller) deliverKafkaBatch(
	ctx context.Context, cons kafkaConsumer, spec kafkaWorkerSpec, batch []KafkaRecord,
) {
	matched, dropped := splitKafkaByFilter(spec.Filter, batch)
	chunks := splitKafkaByPayload(matched, kafkaMaxPayloadBytes)

	for i, chunk := range chunks {
		if !p.invokeKafkaWithRetry(ctx, spec, chunk) {
			return
		}

		commit := chunk
		if i == len(chunks)-1 {
			commit = append(commit, dropped...)
		}

		p.commitKafka(ctx, cons, spec, commit)
	}

	if len(chunks) == 0 {
		p.commitKafka(ctx, cons, spec, dropped)
	}

	p.lambdaBackend.setESMLastProcessingResult(spec.UUID, "OK")
}

func (p *EventSourcePoller) commitKafka(
	ctx context.Context, cons kafkaConsumer, spec kafkaWorkerSpec, recs []KafkaRecord,
) {
	if err := cons.Commit(ctx, recs); err != nil && ctx.Err() == nil {
		logger.Load(ctx).WarnContext(ctx, "esm kafka: offset commit failed", "uuid", spec.UUID, "error", err)
	}
}

// invokeKafkaWithRetry reports whether the chunk was processed; false only when ctx ended first.
func (p *EventSourcePoller) invokeKafkaWithRetry(ctx context.Context, spec kafkaWorkerSpec, chunk []KafkaRecord) bool {
	payload, err := buildKafkaEventPayload(spec.EventSource, spec.EventSourceARN, spec.BootstrapServers, chunk)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "esm kafka: failed to marshal event", "uuid", spec.UUID, "error", err)

		return false
	}

	backoff := kafkaRetryBackoffBase

	for ctx.Err() == nil {
		invErr := p.invokeKafka(ctx, spec.FunctionARN, payload)
		if invErr == nil {
			return true
		}

		logger.Load(ctx).WarnContext(ctx, "esm kafka: invocation failed; retrying batch",
			"uuid", spec.UUID, "error", invErr)
		p.lambdaBackend.setESMLastProcessingResult(spec.UUID, "PROBLEM: "+invErr.Error())
		sleepCtx(ctx, backoff)
		backoff = nextBackoff(backoff)
	}

	return false
}

func (p *EventSourcePoller) invokeKafka(ctx context.Context, functionARN string, payload []byte) error {
	fnName, qualifier := functionNameAndQualifierFromARN(functionARN)
	if fnName == "" {
		fnName = functionARN
	}

	if p.kafkaInvoker != nil {
		return p.kafkaInvoker(ctx, fnName, payload)
	}

	_, _, fnErr, _, err := p.lambdaBackend.InvokeFunctionWithQualifier(
		ctx, fnName, qualifier, "", "", InvocationTypeRequestResponse, payload,
	)
	if err != nil {
		return fmt.Errorf("invoke %s: %w", fnName, err)
	}

	if fnErr != "" {
		return fmt.Errorf("%w: %s", errKafkaFunction, fnErr)
	}

	return nil
}
