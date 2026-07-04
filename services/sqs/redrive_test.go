package sqs_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestDLQ_Routing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *sqs.InMemoryBackend)
		name string
	}{
		{
			name: "LazyRouting_EagerApply",
			run: func(t *testing.T, b *sqs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "dlq", Endpoint: "localhost"})
				require.NoError(t, err)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: "source", Endpoint: "localhost"})
				require.NoError(t, err)

				qURL := "http://localhost/000000000000/source"
				dlqURL := "http://localhost/000000000000/dlq"

				// 1. Send message
				_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
				require.NoError(t, err)

				// 2. Receive message (ReceiveCount goes to 1)
				out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
				require.NoError(t, err)
				require.Len(t, out.Messages, 1)

				// 3. Return it to queue instantly via ChangeMessageVisibility
				err = b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
					QueueURL:          qURL,
					ReceiptHandle:     out.Messages[0].ReceiptHandle,
					VisibilityTimeout: 0,
				})
				require.NoError(t, err)

				// 4. Set RedrivePolicy with maxReceiveCount=1
				dlqARN := "arn:aws:sqs:us-east-1:000000000000:dlq"
				err = b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
					QueueURL: qURL,
					Attributes: map[string]string{
						"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":1}`,
					},
				})
				require.NoError(t, err)

				// At this point, the message in `source` has ReceiveCount=1, which >= MaxReceiveCount=1.
				dlqOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: dlqURL, MaxNumberOfMessages: 1})
				require.NoError(t, err)

				require.Len(t, dlqOut.Messages, 1, "Message should be in DLQ immediately")
			},
		},
		{
			name: "ReceiveMaxRepro",
			run: func(t *testing.T, b *sqs.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "dlq2", Endpoint: "localhost"})
				require.NoError(t, err)

				dlqARN := "arn:aws:sqs:us-east-1:000000000000:dlq2"
				_, err = b.CreateQueue(&sqs.CreateQueueInput{
					QueueName: "source2",
					Endpoint:  "localhost",
					Attributes: map[string]string{
						"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":1}`,
					},
				})
				require.NoError(t, err)

				qURL := "http://localhost/000000000000/source2"

				_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
				require.NoError(t, err)

				// If maxReceiveCount is 1, AWS says the FIRST receive gets it.
				out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
				require.NoError(t, err)

				require.Len(t, out.Messages, 1, "Message should be received successfully")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sqs.NewInMemoryBackend()
			defer b.Close()
			tt.run(t, b)
		})
	}
}
