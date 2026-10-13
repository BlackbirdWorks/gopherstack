package lambda

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

var errNoKafkaProducer = errors.New("consumer cannot produce to a Kafka on-failure topic")

// kafkaFailureProducer is implemented by consumers that can write to the source cluster.
type kafkaFailureProducer interface {
	ProduceFailed(ctx context.Context, topic string, key, value []byte) error
}

// kafkaRetryLimit is the retry cap for a Kafka mapping: infinite retries are capped when a destination exists.
func kafkaRetryLimit(spec kafkaWorkerSpec) int {
	if !spec.Policy.limited {
		if spec.Destination != "" {
			return kafkaInfiniteRetryCap
		}

		return -1
	}

	return spec.Policy.maxRetries
}

func splitExpiredKafka(recs []KafkaRecord, maxAge time.Duration, now time.Time) ([]KafkaRecord, []KafkaRecord) {
	if maxAge <= 0 {
		return recs, nil
	}

	var fresh, expired []KafkaRecord

	for _, r := range recs {
		if !r.Timestamp.IsZero() && now.Sub(r.Timestamp) > maxAge {
			expired = append(expired, r)
		} else {
			fresh = append(fresh, r)
		}
	}

	return fresh, expired
}

func kafkaOutcome(err error) invokeOutcome {
	out := invokeOutcome{statusCode: statusOK, executedVersion: versionLatest}
	if err == nil {
		return out
	}

	out.err = err
	out.functionError = functionErrorUnhandled

	if errors.Is(err, errKafkaFunction) {
		if _, fe, ok := strings.Cut(err.Error(), errKafkaFunction.Error()+": "); ok {
			out.functionError = fe
		}
	}

	return out
}

// dropKafkaRecords counts records discarded for expiry or exhausted retries and delivers each to OnFailure.
func (p *EventSourcePoller) dropKafkaRecords(
	ctx context.Context, cons kafkaConsumer, spec kafkaWorkerSpec, recs []KafkaRecord, attempts int, last invokeOutcome,
) {
	p.kafkaMetric(spec, esmMetricGroupEventCount, esmMetricDropped, len(recs))

	if spec.Destination == "" {
		return
	}

	delivered := 0

	for _, r := range recs {
		if err := p.deliverKafkaFailure(ctx, cons, spec, r, attempts, last); err != nil {
			logDeliveryFailure(ctx, spec.UUID, spec.Destination, err)

			continue
		}

		delivered++
	}

	p.kafkaMetric(spec, esmMetricGroupEventCount, esmMetricOnFailureDelivered, delivered)
}

func (p *EventSourcePoller) deliverKafkaFailure(
	ctx context.Context, cons kafkaConsumer, spec kafkaWorkerSpec, r KafkaRecord, attempts int, last invokeOutcome,
) error {
	event, err := buildKafkaEventPayload(spec.EventSource, spec.EventSourceARN, spec.BootstrapServers, []KafkaRecord{r})
	if err != nil {
		return fmt.Errorf("build kafka failure payload: %w", err)
	}

	rec := newFailureRecord(&EventSourceMapping{FunctionARN: spec.FunctionARN}, attempts, last)
	rec.KafkaBatchInfo = &kafkaBatchInfo{
		BatchSize:        1,
		EventSourceArn:   spec.EventSourceARN,
		BootstrapServers: spec.BootstrapServers,
		PayloadSize:      len(event),
		RecordInfo: &kafkaRecordInfo{
			Offset:    strconv.FormatInt(r.Offset, 10),
			Timestamp: r.Timestamp.UTC().Format(failureTimestampMillisLayout),
		},
	}

	dest := spec.Destination

	switch {
	case strings.HasPrefix(dest, kafkaDestinationPrefix):
		rec.Payload = json.RawMessage(event)
		body, mErr := marshalFailure(rec)
		if mErr != nil {
			return mErr
		}

		prod, ok := cons.(kafkaFailureProducer)
		if !ok {
			return errNoKafkaProducer
		}

		return prod.ProduceFailed(ctx, strings.TrimPrefix(dest, kafkaDestinationPrefix), r.Key, body)
	case isS3Destination(dest):
		rec.Payload = string(event)
	}

	body, err := marshalFailure(rec)
	if err != nil {
		return err
	}

	return p.lambdaBackend.deliverFailureRecord(ctx, spec.UUID, r.Topic+"-"+strconv.Itoa(int(r.Partition)), dest, body)
}

// deliverKafkaChunk invokes the chunk until it succeeds or its records are dropped; false only when ctx ended.
func (p *EventSourcePoller) deliverKafkaChunk(
	ctx context.Context, cons kafkaConsumer, spec kafkaWorkerSpec, chunk []KafkaRecord, attempts int,
) bool {
	backoff := kafkaRetryBackoffBase
	last := invokeOutcome{}
	lg := newESMLogger(p.lambdaBackend, spec)

	for ctx.Err() == nil {
		fresh, expired := splitExpiredKafka(chunk, spec.Policy.maxAge, time.Now())
		if len(expired) > 0 {
			p.dropKafkaRecords(ctx, cons, spec, expired, attempts, last)

			chunk = fresh
		}

		if len(chunk) == 0 {
			return true
		}

		payload, err := buildKafkaEventPayload(spec.EventSource, spec.EventSourceARN, spec.BootstrapServers, chunk)
		if err != nil {
			logger.Load(ctx).WarnContext(ctx, "esm kafka: failed to marshal event", "uuid", spec.UUID, "error", err)

			return false
		}

		invErr := p.invokeKafka(ctx, spec.FunctionARN, payload)
		attempts++

		p.kafkaMetric(spec, esmMetricGroupEventCount, esmMetricInvoked, len(chunk))

		if invErr == nil {
			return true
		}

		last = kafkaOutcome(invErr)

		p.kafkaMetric(spec, esmMetricGroupEventCount, esmMetricFailedInvoke, len(chunk))
		p.kafkaMetric(spec, esmMetricGroupErrorCount, esmMetricInvokeError, 1)
		lg.warn(ctx, invErr.Error(), last.functionError)
		logger.Load(ctx).WarnContext(ctx, "esm kafka: invocation failed", "uuid", spec.UUID, "error", invErr)
		p.lambdaBackend.setESMLastProcessingResult(spec.UUID, "PROBLEM: "+invErr.Error())

		if spec.Policy.bisect && len(chunk) > 1 {
			mid := len(chunk) / bisectParts

			return p.deliverKafkaChunk(ctx, cons, spec, chunk[:mid], attempts-1) &&
				p.deliverKafkaChunk(ctx, cons, spec, chunk[mid:], attempts-1)
		}

		if limit := kafkaRetryLimit(spec); limit >= 0 && attempts > limit {
			p.dropKafkaRecords(ctx, cons, spec, chunk, attempts, last)

			return true
		}

		sleepCtx(ctx, backoff)
		backoff = nextBackoff(backoff)
	}

	return false
}
