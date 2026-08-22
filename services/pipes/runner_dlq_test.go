package pipes_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

const (
	dlqKinesisSource = "arn:aws:kinesis:eu-west-1:111122223333:stream/src"
	dlqLambdaTgt     = "arn:aws:lambda:eu-west-1:111122223333:function:tgt"
	dlqQueueARN      = "arn:aws:sqs:eu-west-1:111122223333:dlq"
	dlqEnrichARN     = "arn:aws:lambda:eu-west-1:111122223333:function:enricher"
	dlqSNSTopicARN   = "arn:aws:sns:eu-west-1:111122223333:dlq-topic"
)

type dlqMockSQSSender struct {
	queueURLs []string
	bodies    []string
	mu        sync.Mutex
}

func (m *dlqMockSQSSender) SendMessage(_ context.Context, queueURL, body, _, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.queueURLs = append(m.queueURLs, queueURL)
	m.bodies = append(m.bodies, body)

	return nil
}

func (m *dlqMockSQSSender) sentTo() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]string(nil), m.queueURLs...)
}

type dlqMockSNSPublisher struct {
	topicARNs []string
	mu        sync.Mutex
}

func (m *dlqMockSNSPublisher) PublishMessage(_ context.Context, topicARN, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.topicARNs = append(m.topicARNs, topicARN)

	return nil
}

func (m *dlqMockSNSPublisher) sentTo() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]string(nil), m.topicARNs...)
}

func dlqBackend() *pipes.InMemoryBackend {
	return pipes.NewInMemoryBackend("111122223333", "eu-west-1")
}

// dlqKinesisSourceParams builds SourceParameters for a Kinesis-sourced pipe,
// optionally with a DeadLetterConfig nested under KinesisStreamParameters --
// the only place the real Pipes API allows configuring one (SQS sources have
// no DLQ config at all: see aws-sdk-go-v2/service/pipes/types).
func dlqKinesisSourceParams(dlqARN string) *pipes.SourceParameters {
	kp := &pipes.KinesisStreamSourceParameters{StartingPosition: "TRIM_HORIZON"}
	if dlqARN != "" {
		kp.DeadLetterConfig = &pipes.DeadLetterConfig{Arn: dlqARN}
	}

	return &pipes.SourceParameters{KinesisStreamParameters: kp}
}

func dlqKinesisReaderWithRecord(data string) *fakeKinesisReader {
	return &fakeKinesisReader{
		shardIDs: []string{"shard-1"},
		pending: map[string][]pipes.KinesisRecord{
			"iter-shard-1-TRIM_HORIZON": {{PartitionKey: "pk1", SequenceNumber: "seq1", Data: []byte(data)}},
		},
	}
}

// TestRunner_EnrichmentFailure_RoutesToDLQ verifies that when an enrichment
// invoker is unwired the runner does not silently drop the event: with a DLQ
// configured (nested under KinesisStreamParameters, the only real-API
// location) the failed batch is sent to the DLQ; without a DLQ configured the
// batch is dropped (there is no source-level redelivery for a stream source
// once the shard iterator has advanced past it).
func TestRunner_EnrichmentFailure_RoutesToDLQ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		dlqARN      string
		wantDLQSent bool
	}{
		{name: "enrichment_unwired_with_dlq", dlqARN: dlqQueueARN, wantDLQSent: true},
		{name: "enrichment_unwired_without_dlq_drops_batch", dlqARN: "", wantDLQSent: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := dlqBackend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:             tt.name,
				RoleARN:          "arn:aws:iam::111122223333:role/r",
				Source:           dlqKinesisSource,
				Target:           dlqLambdaTgt,
				Enrichment:       dlqEnrichARN,
				DesiredState:     "RUNNING",
				SourceParameters: dlqKinesisSourceParams(tt.dlqARN),
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name)

			reader := dlqKinesisReaderWithRecord("boom")
			sender := &dlqMockSQSSender{}

			runner := pipes.NewRunner(b)
			runner.SetKinesisReader(reader)
			runner.SetSQSSender(sender)
			// Note: no Lambda invoker wired, so enrichment fails.

			pipes.PollAllPipesOnce(t.Context(), runner)

			if tt.wantDLQSent {
				assert.Equal(t, []string{tt.dlqARN}, sender.sentTo(), "failed batch should be sent to DLQ")
			} else {
				assert.Empty(t, sender.sentTo(), "no DLQ configured: nothing should be sent")
			}
		})
	}
}

// TestRunner_EnrichmentFailure_SNSDLQ verifies SNS-topic dead-letter targets.
func TestRunner_EnrichmentFailure_SNSDLQ(t *testing.T) {
	t.Parallel()

	b := dlqBackend()
	_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
		Name:             "sns-dlq",
		RoleARN:          "arn:aws:iam::111122223333:role/r",
		Source:           dlqKinesisSource,
		Target:           dlqLambdaTgt,
		Enrichment:       dlqEnrichARN,
		DesiredState:     "RUNNING",
		SourceParameters: dlqKinesisSourceParams(dlqSNSTopicARN),
	})
	require.NoError(t, err)
	pipes.WaitPipeRunning(t, b, "sns-dlq")

	reader := dlqKinesisReaderWithRecord("boom")
	sns := &dlqMockSNSPublisher{}

	runner := pipes.NewRunner(b)
	runner.SetKinesisReader(reader)
	runner.SetSNSPublisher(sns)

	pipes.PollAllPipesOnce(t.Context(), runner)

	assert.Equal(t, []string{dlqSNSTopicARN}, sns.sentTo(), "failed batch should be published to SNS DLQ")
}

// TestRunner_TargetFailure_RoutesToDLQ verifies that an unwired target invoker
// also routes to the DLQ rather than silently succeeding.
func TestRunner_TargetFailure_RoutesToDLQ(t *testing.T) {
	t.Parallel()

	b := dlqBackend()
	_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
		Name:             "target-dlq",
		RoleARN:          "arn:aws:iam::111122223333:role/r",
		Source:           dlqKinesisSource,
		Target:           dlqLambdaTgt, // no Lambda invoker wired -> target fails
		DesiredState:     "RUNNING",
		SourceParameters: dlqKinesisSourceParams(dlqQueueARN),
	})
	require.NoError(t, err)
	pipes.WaitPipeRunning(t, b, "target-dlq")

	reader := dlqKinesisReaderWithRecord("boom")
	sender := &dlqMockSQSSender{}

	runner := pipes.NewRunner(b)
	runner.SetKinesisReader(reader)
	runner.SetSQSSender(sender)

	pipes.PollAllPipesOnce(t.Context(), runner)

	assert.Equal(t, []string{dlqQueueARN}, sender.sentTo(), "failed target batch should be sent to DLQ")
}
