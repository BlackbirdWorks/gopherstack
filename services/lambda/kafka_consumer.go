package lambda

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const kafkaFetchMaxWait = 250 * time.Millisecond

// kafkaConsumerConfig describes the group consumer an ESM worker needs.
type kafkaConsumerConfig struct {
	StartTimestamp   time.Time
	GroupID          string
	StartingPosition string
	Brokers          []string
	Topics           []string
}

// kafkaConsumer reads batches from a Kafka group and commits processed offsets.
type kafkaConsumer interface {
	// Poll returns up to max records; a done ctx ends it without error.
	Poll(ctx context.Context, maxRecords int) ([]KafkaRecord, error)
	// Commit commits the offsets just past recs.
	Commit(ctx context.Context, recs []KafkaRecord) error
	Close()
}

type kafkaConsumerFactory func(cfg kafkaConsumerConfig) (kafkaConsumer, error)

type franzConsumer struct {
	cl *kgo.Client
}

func kafkaResetOffset(cfg kafkaConsumerConfig) kgo.Offset {
	switch cfg.StartingPosition {
	case "LATEST":
		return kgo.NewOffset().AtEnd()
	case "AT_TIMESTAMP":
		return kgo.NewOffset().AfterMilli(cfg.StartTimestamp.UnixMilli())
	default:
		return kgo.NewOffset().AtStart()
	}
}

func newFranzConsumer(cfg kafkaConsumerConfig) (kafkaConsumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.GroupID),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumeResetOffset(kafkaResetOffset(cfg)),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(kafkaFetchMaxWait),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}

	return &franzConsumer{cl: cl}, nil
}

func (c *franzConsumer) Poll(ctx context.Context, maxRecords int) ([]KafkaRecord, error) {
	fetches := c.cl.PollRecords(ctx, maxRecords)
	if fetches.IsClientClosed() {
		return nil, nil
	}

	var firstErr error

	for _, fe := range fetches.Errors() {
		if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
			continue
		}

		if firstErr == nil {
			firstErr = fmt.Errorf("fetch %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
		}
	}

	recs := make([]KafkaRecord, 0, fetches.NumRecords())

	fetches.EachPartition(func(p kgo.FetchTopicPartition) {
		for _, r := range p.Records {
			rec := toKafkaRecord(r)
			rec.HighWatermark = p.HighWatermark
			recs = append(recs, rec)
		}
	})

	if len(recs) > 0 {
		return recs, nil
	}

	return nil, firstErr
}

func toKafkaRecord(r *kgo.Record) KafkaRecord {
	tsType := "CREATE_TIME"
	if r.Attrs.TimestampType() == 1 {
		tsType = "LOG_APPEND_TIME"
	}

	hs := make([]KafkaHeader, len(r.Headers))
	for i, h := range r.Headers {
		hs[i] = KafkaHeader{Key: h.Key, Value: h.Value}
	}

	return KafkaRecord{
		Topic:         r.Topic,
		Partition:     r.Partition,
		Offset:        r.Offset,
		Timestamp:     r.Timestamp,
		TimestampType: tsType,
		Key:           r.Key,
		Value:         r.Value,
		Headers:       hs,
		raw:           r,
	}
}

func (c *franzConsumer) Commit(ctx context.Context, recs []KafkaRecord) error {
	raws := make([]*kgo.Record, 0, len(recs))

	for _, r := range recs {
		if raw, ok := r.raw.(*kgo.Record); ok {
			raws = append(raws, raw)
		}
	}

	if len(raws) == 0 {
		return nil
	}

	if err := c.cl.CommitRecords(ctx, raws...); err != nil {
		return fmt.Errorf("commit offsets: %w", err)
	}

	return nil
}

func (c *franzConsumer) ProduceFailed(ctx context.Context, topic string, key, value []byte) error {
	res := c.cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value})
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("produce to %s: %w", topic, err)
	}

	return nil
}

func (c *franzConsumer) Close() { c.cl.Close() }
