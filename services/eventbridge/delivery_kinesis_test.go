package eventbridge_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

type auditKinesisFirehoseSink struct {
	records map[string][]string
	mu      sync.Mutex
}

func newAuditKinesisFirehoseSink() *auditKinesisFirehoseSink {
	return &auditKinesisFirehoseSink{records: make(map[string][]string)}
}

func (s *auditKinesisFirehoseSink) PutRecord(_ context.Context, streamARN, data string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[streamARN] = append(s.records[streamARN], data)

	return nil
}

func (s *auditKinesisFirehoseSink) RecordsFor(streamARN string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string{}, s.records[streamARN]...)
}

func TestDelivery_KinesisFirehose_DeliversEvent(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sink := newAuditKinesisFirehoseSink()
	streamARN := "arn:aws:firehose:us-east-1:123456789012:deliverystream/my-stream"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{KinesisFirehose: sink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "firehose-rule",
		EventPattern: `{"source":["firehose-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "firehose-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: streamARN},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "firehose-test", DetailType: "T", Detail: `{"key":"val"}`},
	})

	require.Eventually(t, func() bool {
		return len(sink.RecordsFor(streamARN)) > 0
	}, 2*time.Second, 10*time.Millisecond, "Kinesis Firehose should have received a record")

	record := sink.RecordsFor(streamARN)[0]
	assert.Contains(t, record, "firehose-test")
}

func TestDelivery_KinesisFirehose_NilHandlerSkipsGracefully(t *testing.T) {
	t.Parallel()
	b := newBackend()

	streamARN := "arn:aws:firehose:us-east-1:123456789012:deliverystream/no-backend"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "firehose-nil-rule",
		EventPattern: `{"source":["nil-firehose"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "firehose-nil-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: streamARN},
	})
	require.NoError(t, err)

	require.NotPanics(t, func() {
		b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{Source: "nil-firehose", DetailType: "T", Detail: `{}`},
		})
	})
}

type auditFailingKinesisFirehoseSink struct {
	delegate *auditKinesisFirehoseSink
	failARN  string
}

func (f *auditFailingKinesisFirehoseSink) PutRecord(ctx context.Context, streamARN, data string) error {
	if streamARN == f.failARN {
		return errSimulatedFailure
	}

	return f.delegate.PutRecord(ctx, streamARN, data)
}

func TestDelivery_KinesisFirehose_FailureSendsToDLQ(t *testing.T) {
	t.Parallel()
	b := newBackend()

	dlqSink := newMockSQSSender()
	dlqARN := "arn:aws:sqs:us-east-1:123456789012:firehose-dlq"
	streamARN := "arn:aws:firehose:us-east-1:123456789012:deliverystream/failing-stream"

	firehoseSink := newAuditKinesisFirehoseSink()
	failingSink := &auditFailingKinesisFirehoseSink{delegate: firehoseSink, failARN: streamARN}

	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{
		KinesisFirehose: failingSink,
		SQS:             dlqSink,
	})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "firehose-dlq-rule",
		EventPattern: `{"source":["firehose-dlq-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "firehose-dlq-rule", "", []eventbridge.Target{
		{
			ID:               "t1",
			Arn:              streamARN,
			DeadLetterConfig: &eventbridge.DeadLetterConfig{Arn: dlqARN},
			RetryPolicy:      &eventbridge.RetryPolicy{MaximumRetryAttempts: 0},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "firehose-dlq-test", DetailType: "T", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(dlqSink.MessagesFor(dlqARN)) > 0
	}, 2*time.Second, 10*time.Millisecond, "DLQ should have received the failed Firehose event")
}

type auditKinesisStreamSink struct {
	records map[string][]string
	mu      sync.Mutex
}

func newAuditKinesisStreamSink() *auditKinesisStreamSink {
	return &auditKinesisStreamSink{records: make(map[string][]string)}
}

func (s *auditKinesisStreamSink) PutRecord(_ context.Context, streamARN, _ string, data string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[streamARN] = append(s.records[streamARN], data)

	return nil
}

func (s *auditKinesisStreamSink) RecordsFor(streamARN string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string{}, s.records[streamARN]...)
}

func TestDelivery_KinesisStream_DeliversEvent(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sink := newAuditKinesisStreamSink()
	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{KinesisStream: sink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "kinesis-rule",
		EventPattern: `{"source":["kinesis-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "kinesis-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: streamARN},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "kinesis-test", DetailType: "T", Detail: `{"k":"v"}`},
	})

	require.Eventually(t, func() bool {
		return len(sink.RecordsFor(streamARN)) > 0
	}, 2*time.Second, 10*time.Millisecond, "Kinesis Data Stream should have received a record")

	record := sink.RecordsFor(streamARN)[0]
	assert.Contains(t, record, "kinesis-test")
}

func TestDelivery_KinesisStream_NilHandlerSkipsGracefully(t *testing.T) {
	t.Parallel()
	b := newBackend()

	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/no-backend"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "kinesis-nil-rule",
		EventPattern: `{"source":["nil-kinesis"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "kinesis-nil-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: streamARN},
	})
	require.NoError(t, err)

	require.NotPanics(t, func() {
		b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{Source: "nil-kinesis", DetailType: "T", Detail: `{}`},
		})
	})
}

type auditFailingKinesisStreamSink struct {
	delegate *auditKinesisStreamSink
	failARN  string
}

func (f *auditFailingKinesisStreamSink) PutRecord(ctx context.Context, streamARN, partitionKey, data string) error {
	if streamARN == f.failARN {
		return errSimulatedFailure
	}

	return f.delegate.PutRecord(ctx, streamARN, partitionKey, data)
}

func TestDelivery_KinesisStream_FailureSendsToDLQ(t *testing.T) {
	t.Parallel()
	b := newBackend()

	dlqSink := newMockSQSSender()
	dlqARN := "arn:aws:sqs:us-east-1:123456789012:kinesis-dlq"
	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/failing-stream"

	kinesisDelegate := newAuditKinesisStreamSink()
	failingSink := &auditFailingKinesisStreamSink{delegate: kinesisDelegate, failARN: streamARN}

	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{
		KinesisStream: failingSink,
		SQS:           dlqSink,
	})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "kinesis-dlq-rule",
		EventPattern: `{"source":["kinesis-dlq-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "kinesis-dlq-rule", "", []eventbridge.Target{
		{
			ID:               "t1",
			Arn:              streamARN,
			DeadLetterConfig: &eventbridge.DeadLetterConfig{Arn: dlqARN},
			RetryPolicy:      &eventbridge.RetryPolicy{MaximumRetryAttempts: 0},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "kinesis-dlq-test", DetailType: "T", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(dlqSink.MessagesFor(dlqARN)) > 0
	}, 2*time.Second, 10*time.Millisecond, "DLQ should have received the failed Kinesis Stream event")
}
