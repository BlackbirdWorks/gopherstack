package lambda_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestLambda_Poller_SQS_ReceiveCallsStayWithinSQSLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		batchSize int
		messages  int
		wantTaken int
	}{
		{name: "batch above limit", batchSize: 25, messages: 30, wantTaken: 25},
		{name: "queue shorter than batch", batchSize: 25, messages: 12, wantTaken: 12},
		{name: "batch at limit", batchSize: 10, messages: 30, wantTaken: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "f"}))
			_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
				EventSourceARN:                 "arn:aws:sqs:us-east-1:000000000000:q",
				FunctionName:                   "f",
				BatchSize:                      tt.batchSize,
				MaximumBatchingWindowInSeconds: 1,
				Enabled:                        true,
			})
			require.NoError(t, err)

			reader := &limitCheckingSQSReader{}
			for i := range tt.messages {
				reader.messages = append(reader.messages, &lambda.SQSMessage{
					MessageID: fmt.Sprintf("m%d", i), ReceiptHandle: fmt.Sprintf("r%d", i),
				})
			}

			poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
			poller.SetSQSReader(reader)
			lambda.PollOnce(t.Context(), poller)

			assert.LessOrEqual(t, reader.maxRequested, 10)
			assert.Len(t, reader.messages, tt.messages-tt.wantTaken)
		})
	}
}

type limitCheckingSQSReader struct {
	messages     []*lambda.SQSMessage
	maxRequested int
}

func (r *limitCheckingSQSReader) ReceiveMessagesLocal(_ string, maxMessages int) ([]*lambda.SQSMessage, error) {
	r.maxRequested = max(r.maxRequested, maxMessages)

	if maxMessages > 10 {
		return nil, assert.AnError
	}

	n := min(maxMessages, len(r.messages))
	out := r.messages[:n]
	r.messages = r.messages[n:]

	return out, nil
}

func (*limitCheckingSQSReader) DeleteMessagesLocal(string, []string) error { return nil }
