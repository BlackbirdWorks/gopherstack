package sqs_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/events"
	snsbackend "github.com/blackbirdworks/gopherstack/sns"
	"github.com/blackbirdworks/gopherstack/sqs"
)

// newWiredPair returns an SNS backend and SQS backend connected via the event emitter.
func newWiredPair() (*snsbackend.InMemoryBackend, *sqs.InMemoryBackend) {
	snsBk := snsbackend.NewInMemoryBackend()
	sqsBk := sqs.NewInMemoryBackend()

	emitter := events.NewInMemoryEmitter[*events.SNSPublishedEvent]()
	snsBk.SetPublishEmitter(emitter)
	sqsBk.SubscribeToSNS(emitter)

	return snsBk, sqsBk
}

func TestSNSToSQSDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, snsBk *snsbackend.InMemoryBackend, sqsBk *sqs.InMemoryBackend)
	}{
		{
			name: "BasicDelivery",
			run: func(t *testing.T, snsBk *snsbackend.InMemoryBackend, sqsBk *sqs.InMemoryBackend) {
				// Create topic and queue.
				topic, err := snsBk.CreateTopic("my-topic", nil)
				require.NoError(t, err)

				_, err = sqsBk.CreateQueue(&sqs.CreateQueueInput{
					QueueName: "my-queue",
					Endpoint:  "localhost:8000",
				})
				require.NoError(t, err)

				// Subscribe SQS queue to topic.
				queueARN := "arn:aws:sqs:us-east-1:000000000000:my-queue"
				_, err = snsBk.Subscribe(topic.TopicArn, "sqs", queueARN, "")
				require.NoError(t, err)

				// Publish a message.
				_, err = snsBk.Publish(topic.TopicArn, "hello world", "subject", "", nil)
				require.NoError(t, err)

				// Verify message was delivered to the SQS queue.
				out, err := sqsBk.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            "http://localhost:8000/000000000000/my-queue",
					MaxNumberOfMessages: 1,
					WaitTimeSeconds:     0,
				})
				require.NoError(t, err)
				require.Len(t, out.Messages, 1, "expected exactly one message delivered to SQS")

				// The body should be an SNS notification envelope.
				var env map[string]string
				require.NoError(t, json.Unmarshal([]byte(out.Messages[0].Body), &env))
				assert.Equal(t, "Notification", env["Type"])
				assert.Equal(t, topic.TopicArn, env["TopicArn"])
				assert.Equal(t, "hello world", env["Message"])
				assert.Equal(t, "subject", env["Subject"])
			},
		},
		{
			name: "NoDeliveryWithoutSubscription",
			run: func(t *testing.T, snsBk *snsbackend.InMemoryBackend, sqsBk *sqs.InMemoryBackend) {
				topic, err := snsBk.CreateTopic("my-topic", nil)
				require.NoError(t, err)

				_, err = sqsBk.CreateQueue(&sqs.CreateQueueInput{
					QueueName: "my-queue",
					Endpoint:  "localhost:8000",
				})
				require.NoError(t, err)

				// Publish without subscribing the queue.
				_, err = snsBk.Publish(topic.TopicArn, "hello", "", "", nil)
				require.NoError(t, err)

				out, err := sqsBk.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            "http://localhost:8000/000000000000/my-queue",
					MaxNumberOfMessages: 1,
					WaitTimeSeconds:     0,
				})
				require.NoError(t, err)
				assert.Empty(t, out.Messages, "no messages expected without subscription")
			},
		},
		{
			name: "FilterPolicyExcludes",
			run: func(t *testing.T, snsBk *snsbackend.InMemoryBackend, sqsBk *sqs.InMemoryBackend) {
				topic, err := snsBk.CreateTopic("events", nil)
				require.NoError(t, err)

				_, err = sqsBk.CreateQueue(&sqs.CreateQueueInput{
					QueueName: "filtered-queue",
					Endpoint:  "localhost:8000",
				})
				require.NoError(t, err)

				// Subscribe with a filter: only "type" = "order".
				filterPolicy := `{"type": ["order"]}`
				queueARN := "arn:aws:sqs:us-east-1:000000000000:filtered-queue"
				_, err = snsBk.Subscribe(topic.TopicArn, "sqs", queueARN, filterPolicy)
				require.NoError(t, err)

				// Publish a message with type=invoice (should NOT be delivered).
				attrs := map[string]snsbackend.MessageAttribute{
					"type": {DataType: "String", StringValue: "invoice"},
				}
				_, err = snsBk.Publish(topic.TopicArn, "pay invoice", "", "", attrs)
				require.NoError(t, err)

				out, err := sqsBk.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            "http://localhost:8000/000000000000/filtered-queue",
					MaxNumberOfMessages: 1,
					WaitTimeSeconds:     0,
				})
				require.NoError(t, err)
				assert.Empty(t, out.Messages, "filtered message should not be delivered")
			},
		},
		{
			name: "FilterPolicyMatches",
			run: func(t *testing.T, snsBk *snsbackend.InMemoryBackend, sqsBk *sqs.InMemoryBackend) {
				topic, err := snsBk.CreateTopic("events", nil)
				require.NoError(t, err)

				_, err = sqsBk.CreateQueue(&sqs.CreateQueueInput{
					QueueName: "order-queue",
					Endpoint:  "localhost:8000",
				})
				require.NoError(t, err)

				filterPolicy := `{"type": ["order"]}`
				queueARN := "arn:aws:sqs:us-east-1:000000000000:order-queue"
				_, err = snsBk.Subscribe(topic.TopicArn, "sqs", queueARN, filterPolicy)
				require.NoError(t, err)

				attrs := map[string]snsbackend.MessageAttribute{
					"type": {DataType: "String", StringValue: "order"},
				}
				_, err = snsBk.Publish(topic.TopicArn, "new order", "", "", attrs)
				require.NoError(t, err)

				out, err := sqsBk.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            "http://localhost:8000/000000000000/order-queue",
					MaxNumberOfMessages: 1,
					WaitTimeSeconds:     0,
				})
				require.NoError(t, err)
				require.Len(t, out.Messages, 1, "matching message should be delivered")
				assert.Contains(t, out.Messages[0].Body, "new order")
			},
		},
		{
			name: "MultipleQueues",
			run: func(t *testing.T, snsBk *snsbackend.InMemoryBackend, sqsBk *sqs.InMemoryBackend) {
				topic, err := snsBk.CreateTopic("broadcast", nil)
				require.NoError(t, err)

				for _, q := range []string{"queue-a", "queue-b"} {
					_, err = sqsBk.CreateQueue(&sqs.CreateQueueInput{
						QueueName: q,
						Endpoint:  "localhost:8000",
					})
					require.NoError(t, err)

					arn := "arn:aws:sqs:us-east-1:000000000000:" + q
					_, err = snsBk.Subscribe(topic.TopicArn, "sqs", arn, "")
					require.NoError(t, err)
				}

				_, err = snsBk.Publish(topic.TopicArn, "broadcast msg", "", "", nil)
				require.NoError(t, err)

				for _, q := range []string{"queue-a", "queue-b"} {
					out, rcvErr := sqsBk.ReceiveMessage(&sqs.ReceiveMessageInput{
						QueueURL:            "http://localhost:8000/000000000000/" + q,
						MaxNumberOfMessages: 1,
						WaitTimeSeconds:     0,
					})
					require.NoError(t, rcvErr)
					assert.Len(t, out.Messages, 1, "each subscribed queue should receive the message: %s", q)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			snsBk, sqsBk := newWiredPair()
			tt.run(t, snsBk, sqsBk)
		})
	}
}
