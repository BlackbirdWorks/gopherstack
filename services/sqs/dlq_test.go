package sqs_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func makeRedrivePolicy(dlqARN string, maxReceiveCount int) string {
	b, _ := json.Marshal(map[string]any{
		"deadLetterTargetArn": dlqARN,
		"maxReceiveCount":     maxReceiveCount,
	})

	return string(b)
}

// TestRedrivePolicy_DLQMovement merges the "policy set at creation" and
// "policy set via SetQueueAttributes" scenarios into one table-driven test.
func TestRedrivePolicy_DLQMovement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		msgBody           string
		maxReceiveCount   int
		setPolicyAtCreate bool
	}{
		{
			name:              "policy_set_at_creation",
			maxReceiveCount:   2,
			msgBody:           "hello",
			setPolicyAtCreate: true,
		},
		{
			name:              "policy_set_via_set_queue_attributes",
			maxReceiveCount:   1,
			msgBody:           "test",
			setPolicyAtCreate: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()

			dlqName := "dlq-" + tt.name
			mainName := "main-" + tt.name
			dlqARN := "arn:aws:sqs:us-east-1:000000000000:" + dlqName
			mainURL := "http://localhost/000000000000/" + mainName
			dlqURL := "http://localhost/000000000000/" + dlqName

			_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName, Endpoint: "localhost"})
			require.NoError(t, err)

			if tt.setPolicyAtCreate {
				_, err = b.CreateQueue(&sqs.CreateQueueInput{
					QueueName: mainName,
					Endpoint:  "localhost",
					Attributes: map[string]string{
						"RedrivePolicy": makeRedrivePolicy(dlqARN, tt.maxReceiveCount),
					},
				})
				require.NoError(t, err)
			} else {
				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: mainName, Endpoint: "localhost"})
				require.NoError(t, err)

				err = b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
					QueueURL: mainURL,
					Attributes: map[string]string{
						"RedrivePolicy": makeRedrivePolicy(dlqARN, tt.maxReceiveCount),
					},
				})
				require.NoError(t, err)
			}

			_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: mainURL, MessageBody: tt.msgBody})
			require.NoError(t, err)

			// Receive maxReceiveCount times — message should still be returned each time.
			for i := range tt.maxReceiveCount {
				out, receiveErr := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: mainURL, MaxNumberOfMessages: 1})
				require.NoError(t, receiveErr)
				require.Len(t, out.Messages, 1, "receive %d should return message", i+1)

				visErr := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
					QueueURL:          mainURL,
					ReceiptHandle:     out.Messages[0].ReceiptHandle,
					VisibilityTimeout: 0,
				})
				require.NoError(t, visErr)
			}

			// Next receive should return empty — message has been moved to DLQ.
			out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: mainURL, MaxNumberOfMessages: 1})
			require.NoError(t, err)
			assert.Empty(t, out.Messages, "message should have been moved to DLQ")

			// DLQ should contain the message.
			dlqOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: dlqURL, MaxNumberOfMessages: 1})
			require.NoError(t, err)
			require.Len(t, dlqOut.Messages, 1)
			assert.Equal(t, tt.msgBody, dlqOut.Messages[0].Body)
		})
	}
}

func TestRedrivePolicy_NoMovementWithoutDLQ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		queueName  string
		msgBody    string
		iterations int
	}{
		{
			name:       "no_redrive_policy",
			queueName:  "plain-queue",
			msgBody:    "stay",
			iterations: 5,
		},
		{
			name:       "high_receive_count_no_dlq",
			queueName:  "plain-queue-high",
			msgBody:    "persistent",
			iterations: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()

			_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: tt.queueName, Endpoint: "localhost"})
			require.NoError(t, err)

			qURL := "http://localhost/000000000000/" + tt.queueName

			_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: tt.msgBody})
			require.NoError(t, err)

			// Receive and re-enqueue iterations times — message must always come back.
			for i := range tt.iterations {
				out, receiveErr := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
				require.NoError(t, receiveErr)
				require.Len(t, out.Messages, 1, "iteration %d", i)

				visErr := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
					QueueURL:          qURL,
					ReceiptHandle:     out.Messages[0].ReceiptHandle,
					VisibilityTimeout: 0,
				})
				require.NoError(t, visErr)
			}
		})
	}
}

func TestRedrivePolicy_InvalidJSONIgnored(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		policy  string
		msgBody string
	}{
		{
			name:    "malformed_json",
			policy:  "{not valid json",
			msgBody: "ok",
		},
		{
			name:    "empty_json_object_no_dlq_fields",
			policy:  "{}",
			msgBody: "also ok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()

			queueName := "bad-policy-" + tt.name

			require.NotPanics(t, func() {
				_, err := b.CreateQueue(&sqs.CreateQueueInput{
					QueueName: queueName,
					Endpoint:  "localhost",
					Attributes: map[string]string{
						"RedrivePolicy": tt.policy,
					},
				})
				require.NoError(t, err)
			})

			qURL := "http://localhost/000000000000/" + queueName

			_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: tt.msgBody})
			require.NoError(t, err)

			out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
			require.NoError(t, err)
			require.Len(t, out.Messages, 1)
			assert.Equal(t, tt.msgBody, out.Messages[0].Body)
		})
	}
}

func TestListDeadLetterSourceQueues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(b *sqs.InMemoryBackend) (dlqURL string)
		name         string
		wantURLs     []string
		wantErr      bool
		wantNotFound bool
	}{
		{
			name: "two_source_queues_point_to_dlq",
			setup: func(b *sqs.InMemoryBackend) string {
				_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "dlq", Endpoint: "localhost"})
				if err != nil {
					panic(err)
				}

				dlqAttrs, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
					QueueURL:       "http://localhost/000000000000/dlq",
					AttributeNames: []string{"QueueArn"},
				})
				if err != nil {
					panic(err)
				}

				dlqARN := dlqAttrs.Attributes["QueueArn"]
				policy := makeRedrivePolicy(dlqARN, 3)

				_, err = b.CreateQueue(&sqs.CreateQueueInput{
					QueueName:  "src-a",
					Endpoint:   "localhost",
					Attributes: map[string]string{"RedrivePolicy": policy},
				})
				if err != nil {
					panic(err)
				}

				_, err = b.CreateQueue(&sqs.CreateQueueInput{
					QueueName:  "src-b",
					Endpoint:   "localhost",
					Attributes: map[string]string{"RedrivePolicy": policy},
				})
				if err != nil {
					panic(err)
				}

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: "unrelated", Endpoint: "localhost"})
				if err != nil {
					panic(err)
				}

				return "http://localhost/000000000000/dlq"
			},
			wantURLs: []string{
				"http://localhost/000000000000/src-a",
				"http://localhost/000000000000/src-b",
			},
		},
		{
			name: "no_source_queues",
			setup: func(b *sqs.InMemoryBackend) string {
				_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "lonely-dlq", Endpoint: "localhost"})
				if err != nil {
					panic(err)
				}

				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: "plain", Endpoint: "localhost"})
				if err != nil {
					panic(err)
				}

				return "http://localhost/000000000000/lonely-dlq"
			},
			wantURLs: []string{},
		},
		{
			name: "dlq_not_found",
			setup: func(_ *sqs.InMemoryBackend) string {
				return "http://localhost/000000000000/nonexistent"
			},
			wantErr:      true,
			wantNotFound: true,
		},
		{
			name: "pagination",
			setup: func(b *sqs.InMemoryBackend) string {
				_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "page-dlq", Endpoint: "localhost"})
				if err != nil {
					panic(err)
				}

				dlqAttrs, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
					QueueURL:       "http://localhost/000000000000/page-dlq",
					AttributeNames: []string{"QueueArn"},
				})
				if err != nil {
					panic(err)
				}

				dlqARN := dlqAttrs.Attributes["QueueArn"]
				policy := makeRedrivePolicy(dlqARN, 2)

				for _, name := range []string{"pq-1", "pq-2", "pq-3"} {
					_, err = b.CreateQueue(&sqs.CreateQueueInput{
						QueueName:  name,
						Endpoint:   "localhost",
						Attributes: map[string]string{"RedrivePolicy": policy},
					})
					if err != nil {
						panic(err)
					}
				}

				return "http://localhost/000000000000/page-dlq"
			},
			wantURLs: []string{
				"http://localhost/000000000000/pq-1",
				"http://localhost/000000000000/pq-2",
				"http://localhost/000000000000/pq-3",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()
			dlqURL := tt.setup(b)

			out, err := b.ListDeadLetterSourceQueues(&sqs.ListDeadLetterSourceQueuesInput{
				QueueURL: dlqURL,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.ElementsMatch(t, tt.wantURLs, out.QueueURLs)
		})
	}
}

func TestListDeadLetterSourceQueues_Pagination(t *testing.T) {
	t.Parallel()

	b := sqs.NewInMemoryBackend()

	_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: "p-dlq", Endpoint: "localhost"})
	require.NoError(t, err)

	dlqAttrs, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueURL:       "http://localhost/000000000000/p-dlq",
		AttributeNames: []string{"QueueArn"},
	})
	require.NoError(t, err)

	dlqARN := dlqAttrs.Attributes["QueueArn"]
	policy := makeRedrivePolicy(dlqARN, 2)

	for _, name := range []string{"pp-1", "pp-2", "pp-3"} {
		_, err = b.CreateQueue(&sqs.CreateQueueInput{
			QueueName:  name,
			Endpoint:   "localhost",
			Attributes: map[string]string{"RedrivePolicy": policy},
		})
		require.NoError(t, err)
	}

	dlqURL := "http://localhost/000000000000/p-dlq"

	first, err := b.ListDeadLetterSourceQueues(&sqs.ListDeadLetterSourceQueuesInput{
		QueueURL:   dlqURL,
		MaxResults: 2,
	})
	require.NoError(t, err)
	assert.Len(t, first.QueueURLs, 2)
	assert.NotEmpty(t, first.NextToken)

	second, err := b.ListDeadLetterSourceQueues(&sqs.ListDeadLetterSourceQueuesInput{
		QueueURL:   dlqURL,
		MaxResults: 2,
		NextToken:  first.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, second.QueueURLs, 1)
	assert.Empty(t, second.NextToken)

	allURLs := make([]string, 0, len(first.QueueURLs)+len(second.QueueURLs))
	allURLs = append(allURLs, first.QueueURLs...)
	allURLs = append(allURLs, second.QueueURLs...)
	assert.ElementsMatch(t, []string{
		"http://localhost/000000000000/pp-1",
		"http://localhost/000000000000/pp-2",
		"http://localhost/000000000000/pp-3",
	}, allURLs)
}
