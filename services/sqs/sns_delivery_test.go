package sqs_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/events"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/blackbirdworks/gopherstack/services/sqs"
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
		publishAttrs     map[string]snsbackend.MessageAttribute
		wantBodyContains string
		filterPolicy     string
		publishMessage   string
		publishSubject   string
		topicName        string
		name             string
		wantEnvType      string
		wantEnvMessage   string
		wantEnvSubject   string
		queues           []string
		wantMsgCount     int
		subscribe        bool
	}{
		{
			name:           "BasicDelivery",
			topicName:      "my-topic",
			queues:         []string{"my-queue"},
			subscribe:      true,
			publishMessage: "hello world",
			publishSubject: "subject",
			wantMsgCount:   1,
			wantEnvType:    "Notification",
			wantEnvMessage: "hello world",
			wantEnvSubject: "subject",
		},
		{
			name:           "NoDeliveryWithoutSubscription",
			topicName:      "my-topic",
			queues:         []string{"my-queue"},
			subscribe:      false,
			publishMessage: "hello",
			wantMsgCount:   0,
		},
		{
			name:           "FilterPolicyExcludes",
			topicName:      "events",
			queues:         []string{"filtered-queue"},
			subscribe:      true,
			filterPolicy:   `{"type": ["order"]}`,
			publishMessage: "pay invoice",
			publishAttrs: map[string]snsbackend.MessageAttribute{
				"type": {DataType: "String", StringValue: "invoice"},
			},
			wantMsgCount: 0,
		},
		{
			name:           "FilterPolicyMatches",
			topicName:      "events",
			queues:         []string{"order-queue"},
			subscribe:      true,
			filterPolicy:   `{"type": ["order"]}`,
			publishMessage: "new order",
			publishAttrs: map[string]snsbackend.MessageAttribute{
				"type": {DataType: "String", StringValue: "order"},
			},
			wantMsgCount:     1,
			wantBodyContains: "new order",
		},
		{
			name:           "MultipleQueues",
			topicName:      "broadcast",
			queues:         []string{"queue-a", "queue-b"},
			subscribe:      true,
			publishMessage: "broadcast msg",
			wantMsgCount:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			snsBk, sqsBk := newWiredPair()

			topic, err := snsBk.CreateTopic(tt.topicName, nil)
			require.NoError(t, err)

			for _, q := range tt.queues {
				_, err = sqsBk.CreateQueue(&sqs.CreateQueueInput{
					QueueName: q,
					Endpoint:  "localhost:8000",
				})
				require.NoError(t, err)

				if tt.subscribe {
					arn := "arn:aws:sqs:us-east-1:000000000000:" + q
					_, err = snsBk.Subscribe(topic.TopicArn, "sqs", arn, tt.filterPolicy)
					require.NoError(t, err)
				}
			}

			_, err = snsBk.Publish(topic.TopicArn, tt.publishMessage, tt.publishSubject, "", tt.publishAttrs)
			require.NoError(t, err)

			for _, q := range tt.queues {
				out, rcvErr := sqsBk.ReceiveMessage(&sqs.ReceiveMessageInput{
					QueueURL:            "http://localhost:8000/000000000000/" + q,
					MaxNumberOfMessages: 1,
					WaitTimeSeconds:     0,
				})
				require.NoError(t, rcvErr)

				if tt.wantMsgCount == 0 {
					assert.Empty(t, out.Messages)

					continue
				}

				require.Len(t, out.Messages, tt.wantMsgCount)

				if tt.wantBodyContains != "" {
					assert.Contains(t, out.Messages[0].Body, tt.wantBodyContains)
				}

				if tt.wantEnvType != "" {
					var env map[string]string
					require.NoError(t, json.Unmarshal([]byte(out.Messages[0].Body), &env))
					assert.Equal(t, tt.wantEnvType, env["Type"])
					assert.Equal(t, topic.TopicArn, env["TopicArn"])
					assert.Equal(t, tt.wantEnvMessage, env["Message"])
					assert.Equal(t, tt.wantEnvSubject, env["Subject"])
				}
			}
		})
	}
}
