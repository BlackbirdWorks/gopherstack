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

func TestSNSToSQSRawMessageDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		publishAttrs     map[string]snsbackend.MessageAttribute
		publishMessage   string
		publishSubject   string
		name             string
		wantBody         string
		wantAttrDataType string
		wantAttrKey      string
	}{
		{
			name:           "RawDeliveryBodyIsPlainMessage",
			publishMessage: "raw payload",
			publishSubject: "subj",
			wantBody:       "raw payload",
		},
		{
			name:           "RawDeliveryPreservesMessageAttributes",
			publishMessage: "order created",
			publishAttrs: map[string]snsbackend.MessageAttribute{
				"event-type": {DataType: "String", StringValue: "order.created"},
			},
			wantBody:         "order created",
			wantAttrKey:      "event-type",
			wantAttrDataType: "String",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snsBk, sqsBk := newWiredPair()

			topic, err := snsBk.CreateTopic("raw-topic", nil)
			require.NoError(t, err)

			_, err = sqsBk.CreateQueue(&sqs.CreateQueueInput{
				QueueName: "raw-queue",
				Endpoint:  "localhost:8000",
			})
			require.NoError(t, err)

			sub, err := snsBk.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:raw-queue", "")
			require.NoError(t, err)

			err = snsBk.SetSubscriptionAttributes(sub.SubscriptionArn, "RawMessageDelivery", "true")
			require.NoError(t, err)

			_, err = snsBk.Publish(topic.TopicArn, tt.publishMessage, tt.publishSubject, "", tt.publishAttrs)
			require.NoError(t, err)

			out, err := sqsBk.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            "http://localhost:8000/000000000000/raw-queue",
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     0,
			})
			require.NoError(t, err)
			require.Len(t, out.Messages, 1)

			assert.Equal(t, tt.wantBody, out.Messages[0].Body)

			// Ensure the body is NOT an SNS envelope (no "Type": "Notification" wrapper).
			var env map[string]any
			if jsonErr := json.Unmarshal([]byte(out.Messages[0].Body), &env); jsonErr == nil {
				assert.NotEqual(t, "Notification", env["Type"], "raw delivery body must not be SNS envelope")
			}

			if tt.wantAttrKey != "" {
				attr, ok := out.Messages[0].MessageAttributes[tt.wantAttrKey]
				require.True(t, ok, "expected message attribute %q", tt.wantAttrKey)
				assert.Equal(t, tt.wantAttrDataType, attr.DataType)
			}
		})
	}
}

func TestSNSToSQSDLQ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		publishMessage string
		setupDLQ       bool
		wantDLQMsg     bool
	}{
		{
			name:           "DLQReceivesMessageOnDeliveryFailure",
			publishMessage: "failed delivery",
			setupDLQ:       true,
			wantDLQMsg:     true,
		},
		{
			name:           "NoDLQNoRouting",
			publishMessage: "failed delivery",
			setupDLQ:       false,
			wantDLQMsg:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snsBk, sqsBk := newWiredPair()

			topic, err := snsBk.CreateTopic("dlq-topic", nil)
			require.NoError(t, err)

			// Subscribe to a queue that does NOT exist so delivery fails.
			sub, err := snsBk.Subscribe(
				topic.TopicArn,
				"sqs",
				"arn:aws:sqs:us-east-1:000000000000:nonexistent-queue",
				"",
			)
			require.NoError(t, err)

			if tt.setupDLQ {
				// Create the DLQ queue.
				_, err = sqsBk.CreateQueue(&sqs.CreateQueueInput{
					QueueName: "my-dlq",
					Endpoint:  "localhost:8000",
				})
				require.NoError(t, err)

				err = snsBk.SetSubscriptionAttributes(
					sub.SubscriptionArn,
					"RedrivePolicy",
					`{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:my-dlq"}`,
				)
				require.NoError(t, err)
			}

			_, err = snsBk.Publish(topic.TopicArn, tt.publishMessage, "", "", nil)
			require.NoError(t, err)

			if !tt.setupDLQ {
				return
			}

			out, err := sqsBk.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL:            "http://localhost:8000/000000000000/my-dlq",
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     0,
			})
			require.NoError(t, err)

			if tt.wantDLQMsg {
				require.Len(t, out.Messages, 1)
				// DLQ receives the same body that was attempted to the failed queue.
				// Without RawMessageDelivery, that is the SNS envelope JSON.
				var env map[string]string
				require.NoError(t, json.Unmarshal([]byte(out.Messages[0].Body), &env))
				assert.Equal(t, "Notification", env["Type"])
				assert.Equal(t, tt.publishMessage, env["Message"])
			} else {
				assert.Empty(t, out.Messages)
			}
		})
	}
}

func TestSetSubscriptionAttributesRoundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		attrName  string
		attrValue string
		wantValue string
	}{
		{
			name:      "RawMessageDeliveryTrue",
			attrName:  "RawMessageDelivery",
			attrValue: "true",
			wantValue: "true",
		},
		{
			name:      "RawMessageDeliveryFalse",
			attrName:  "RawMessageDelivery",
			attrValue: "false",
			wantValue: "false",
		},
		{
			name:      "RedrivePolicy",
			attrName:  "RedrivePolicy",
			attrValue: `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:my-dlq"}`,
			wantValue: `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:my-dlq"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snsBk := snsbackend.NewInMemoryBackend()
			topic, err := snsBk.CreateTopic("attr-topic", nil)
			require.NoError(t, err)

			sub, err := snsBk.Subscribe(topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:q", "")
			require.NoError(t, err)

			err = snsBk.SetSubscriptionAttributes(sub.SubscriptionArn, tt.attrName, tt.attrValue)
			require.NoError(t, err)

			attrs, err := snsBk.GetSubscriptionAttributes(sub.SubscriptionArn)
			require.NoError(t, err)
			assert.Equal(t, tt.wantValue, attrs[tt.attrName])
		})
	}
}
