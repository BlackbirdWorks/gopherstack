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

// TestRedriveAllowPolicy_Enforcement verifies that a dead-letter queue's
// RedriveAllowPolicy attribute actually constrains which source queues may
// point their RedrivePolicy at it. Previously the attribute was accepted and
// shape-validated (validateRedriveAllowPolicy) but never enforced, making it
// a disguised stub: any value could be set with zero effect on behaviour.
func TestRedriveAllowPolicy_Enforcement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *sqs.InMemoryBackend)
		name string
	}{
		{
			name: "DenyAll_RejectsAnySourceQueue",
			run: func(t *testing.T, b *sqs.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateQueue(&sqs.CreateQueueInput{
					QueueName: "denyall-dlq",
					Endpoint:  "localhost",
					Attributes: map[string]string{
						"RedriveAllowPolicy": `{"redrivePermission":"denyAll"}`,
					},
				})
				require.NoError(t, err)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: "denyall-source", Endpoint: "localhost"})
				require.NoError(t, err)

				dlqARN := "arn:aws:sqs:us-east-1:000000000000:denyall-dlq"
				err = b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
					QueueURL: "http://localhost/000000000000/denyall-source",
					Attributes: map[string]string{
						"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":3}`,
					},
				})
				require.Error(t, err, "denyAll must reject every source queue, including this one")
			},
		},
		{
			name: "ByQueue_AllowsListedSourceQueueArn",
			run: func(t *testing.T, b *sqs.InMemoryBackend) {
				t.Helper()

				sourceARN := "arn:aws:sqs:us-east-1:000000000000:byqueue-allowed-source"

				_, err := b.CreateQueue(&sqs.CreateQueueInput{
					QueueName: "byqueue-dlq",
					Endpoint:  "localhost",
					Attributes: map[string]string{
						"RedriveAllowPolicy": `{"redrivePermission":"byQueue","sourceQueueArns":["` + sourceARN + `"]}`,
					},
				})
				require.NoError(t, err)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{
					QueueName: "byqueue-allowed-source",
					Endpoint:  "localhost",
				})
				require.NoError(t, err)

				dlqARN := "arn:aws:sqs:us-east-1:000000000000:byqueue-dlq"
				err = b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
					QueueURL: "http://localhost/000000000000/byqueue-allowed-source",
					Attributes: map[string]string{
						"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":3}`,
					},
				})
				require.NoError(t, err, "the ARN listed in sourceQueueArns must be permitted")
			},
		},
		{
			name: "ByQueue_RejectsUnlistedSourceQueueArn",
			run: func(t *testing.T, b *sqs.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateQueue(&sqs.CreateQueueInput{
					QueueName: "byqueue-strict-dlq",
					Endpoint:  "localhost",
					Attributes: map[string]string{
						"RedriveAllowPolicy": `{"redrivePermission":"byQueue",` +
							`"sourceQueueArns":["arn:aws:sqs:us-east-1:000000000000:some-other-queue"]}`,
					},
				})
				require.NoError(t, err)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: "byqueue-strict-source", Endpoint: "localhost"})
				require.NoError(t, err)

				dlqARN := "arn:aws:sqs:us-east-1:000000000000:byqueue-strict-dlq"
				err = b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
					QueueURL: "http://localhost/000000000000/byqueue-strict-source",
					Attributes: map[string]string{
						"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":3}`,
					},
				})
				require.Error(t, err, "an ARN absent from sourceQueueArns must be rejected")
			},
		},
		{
			name: "AllowAllDefault_PermitsAnySourceQueue",
			run: func(t *testing.T, b *sqs.InMemoryBackend) {
				t.Helper()

				// No RedriveAllowPolicy attribute at all — AWS's implicit default
				// is allowAll, so setting a RedrivePolicy must still succeed.
				_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "default-allow-dlq", Endpoint: "localhost"})
				require.NoError(t, err)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: "default-allow-source", Endpoint: "localhost"})
				require.NoError(t, err)

				dlqARN := "arn:aws:sqs:us-east-1:000000000000:default-allow-dlq"
				err = b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
					QueueURL: "http://localhost/000000000000/default-allow-source",
					Attributes: map[string]string{
						"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":3}`,
					},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "DenyAll_AlsoRejectedAtCreateQueueTime",
			run: func(t *testing.T, b *sqs.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateQueue(&sqs.CreateQueueInput{
					QueueName: "create-time-dlq",
					Endpoint:  "localhost",
					Attributes: map[string]string{
						"RedriveAllowPolicy": `{"redrivePermission":"denyAll"}`,
					},
				})
				require.NoError(t, err)

				dlqARN := "arn:aws:sqs:us-east-1:000000000000:create-time-dlq"

				// Setting RedrivePolicy directly at CreateQueue time must go through
				// the same enforcement path as SetQueueAttributes.
				_, err = b.CreateQueue(&sqs.CreateQueueInput{
					QueueName: "create-time-source",
					Endpoint:  "localhost",
					Attributes: map[string]string{
						"RedrivePolicy": `{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":3}`,
					},
				})
				require.Error(t, err, "denyAll must also block RedrivePolicy set inline at CreateQueue time")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sqs.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
			defer b.Close()
			tt.run(t, b)
		})
	}
}
