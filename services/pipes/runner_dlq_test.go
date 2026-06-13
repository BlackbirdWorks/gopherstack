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
	dlqSource      = "arn:aws:sqs:eu-west-1:111122223333:src"
	dlqLambdaTgt   = "arn:aws:lambda:eu-west-1:111122223333:function:tgt"
	dlqQueueARN    = "arn:aws:sqs:eu-west-1:111122223333:dlq"
	dlqEnrichARN   = "arn:aws:lambda:eu-west-1:111122223333:function:enricher"
	dlqSNSTopicARN = "arn:aws:sns:eu-west-1:111122223333:dlq-topic"
)

// dlqMockSQSReader records deleted receipt handles and serves one batch.
type dlqMockSQSReader struct {
	messages []*pipes.SQSMessage
	deleted  []string
	mu       sync.Mutex
}

func (m *dlqMockSQSReader) ReceivePipeMessages(_ string, _ int) ([]*pipes.SQSMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	msgs := m.messages
	m.messages = nil

	return msgs, nil
}

func (m *dlqMockSQSReader) DeletePipeMessages(_ string, receiptHandles []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleted = append(m.deleted, receiptHandles...)

	return nil
}

func (m *dlqMockSQSReader) deletedHandles() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]string(nil), m.deleted...)
}

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

// TestRunner_EnrichmentFailure_RoutesToDLQ verifies that when an enrichment
// invoker is unwired the runner does not silently drop the event: with a DLQ
// configured the failed batch is sent to the DLQ and the source messages are
// deleted; without a DLQ the source messages are retained for redelivery.
func TestRunner_EnrichmentFailure_RoutesToDLQ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dlq         *pipes.DeadLetterConfig
		name        string
		wantDLQSent bool
		wantDeleted bool
	}{
		{
			name:        "enrichment_unwired_with_sqs_dlq",
			dlq:         &pipes.DeadLetterConfig{Arn: dlqQueueARN},
			wantDLQSent: true,
			wantDeleted: true,
		},
		{
			name:        "enrichment_unwired_without_dlq_retains_messages",
			dlq:         nil,
			wantDLQSent: false,
			wantDeleted: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := dlqBackend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:             tt.name,
				RoleARN:          "arn:aws:iam::111122223333:role/r",
				Source:           dlqSource,
				Target:           dlqLambdaTgt,
				Enrichment:       dlqEnrichARN,
				DesiredState:     "RUNNING",
				DeadLetterConfig: tt.dlq,
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name)

			reader := &dlqMockSQSReader{
				messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: "{}"}},
			}
			sender := &dlqMockSQSSender{}

			runner := pipes.NewRunner(b)
			runner.SetSQSReader(reader)
			runner.SetSQSSender(sender)
			// Note: no Lambda invoker wired, so enrichment fails.

			pipes.PollAllPipesOnce(t.Context(), runner)

			if tt.wantDLQSent {
				assert.Equal(t, []string{dlqQueueARN}, sender.sentTo(), "failed batch should be sent to DLQ")
			} else {
				assert.Empty(t, sender.sentTo(), "no DLQ configured: nothing should be sent")
			}

			if tt.wantDeleted {
				assert.Equal(t, []string{"rh1"}, reader.deletedHandles(),
					"source messages should be deleted after DLQ send")
			} else {
				assert.Empty(t, reader.deletedHandles(),
					"without DLQ the source messages must be retained for redelivery")
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
		Source:           dlqSource,
		Target:           dlqLambdaTgt,
		Enrichment:       dlqEnrichARN,
		DesiredState:     "RUNNING",
		DeadLetterConfig: &pipes.DeadLetterConfig{Arn: dlqSNSTopicARN},
	})
	require.NoError(t, err)
	pipes.WaitPipeRunning(t, b, "sns-dlq")

	reader := &dlqMockSQSReader{
		messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: "{}"}},
	}
	sns := &dlqMockSNSPublisher{}

	runner := pipes.NewRunner(b)
	runner.SetSQSReader(reader)
	runner.SetSNSPublisher(sns)

	pipes.PollAllPipesOnce(t.Context(), runner)

	assert.Equal(t, []string{dlqSNSTopicARN}, sns.sentTo(), "failed batch should be published to SNS DLQ")
	assert.Equal(t, []string{"rh1"}, reader.deletedHandles(), "source messages should be deleted after DLQ publish")
}

// TestRunner_TargetFailure_RoutesToDLQ verifies that an unwired target invoker
// also routes to the DLQ rather than silently succeeding.
func TestRunner_TargetFailure_RoutesToDLQ(t *testing.T) {
	t.Parallel()

	b := dlqBackend()
	_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
		Name:             "target-dlq",
		RoleARN:          "arn:aws:iam::111122223333:role/r",
		Source:           dlqSource,
		Target:           dlqLambdaTgt, // no Lambda invoker wired → target fails
		DesiredState:     "RUNNING",
		DeadLetterConfig: &pipes.DeadLetterConfig{Arn: dlqQueueARN},
	})
	require.NoError(t, err)
	pipes.WaitPipeRunning(t, b, "target-dlq")

	reader := &dlqMockSQSReader{
		messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: "{}"}},
	}
	sender := &dlqMockSQSSender{}

	runner := pipes.NewRunner(b)
	runner.SetSQSReader(reader)
	runner.SetSQSSender(sender)

	pipes.PollAllPipesOnce(t.Context(), runner)

	assert.Equal(t, []string{dlqQueueARN}, sender.sentTo(), "failed target batch should be sent to DLQ")
	assert.Equal(t, []string{"rh1"}, reader.deletedHandles(), "source messages should be deleted after DLQ send")
}
