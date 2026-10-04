package sns_test

import (
	"encoding/json"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/events"
	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

const msJSON = `{"default":"def","sqs":"for-sqs","lambda":"for-lambda","firehose":"for-firehose",` +
	`"email":"for-email","email-json":"for-email-json","sms":"for-sms","http":"for-http","GCM":"for-gcm"}`

func newSNSSQSPair(t *testing.T) (*snssdk.Client, *sqssdk.Client) {
	t.Helper()

	snsBackend := sns.NewInMemoryBackend()
	sqsBackend := sqs.NewInMemoryBackend()
	t.Cleanup(sqsBackend.Close)

	emitter := events.NewInMemoryEmitter[*events.SNSPublishedEvent]()
	snsBackend.SetPublishEmitter(emitter)
	sqsBackend.SubscribeToSNS(emitter)

	return newTestSNSClient(t, sns.NewHandler(snsBackend)), newTestSQSClient(t, sqsBackend)
}

func subscribeQueue(
	t *testing.T, snsClient *snssdk.Client, sqsClient *sqssdk.Client, topicArn string, raw bool,
) string {
	t.Helper()

	ctx := t.Context()

	q, err := sqsClient.CreateQueue(ctx, &sqssdk.CreateQueueInput{QueueName: aws.String("ms-queue")})
	require.NoError(t, err)

	attrs, err := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl:       q.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	rawVal := "false"
	if raw {
		rawVal = "true"
	}

	_, err = snsClient.Subscribe(ctx, &snssdk.SubscribeInput{
		TopicArn:   aws.String(topicArn),
		Protocol:   aws.String("sqs"),
		Endpoint:   aws.String(attrs.Attributes["QueueArn"]),
		Attributes: map[string]string{"RawMessageDelivery": rawVal},
	})
	require.NoError(t, err)

	return aws.ToString(q.QueueUrl)
}

func receiveBody(t *testing.T, sqsClient *sqssdk.Client, queueURL string, raw bool) string {
	t.Helper()

	out, err := sqsClient.ReceiveMessage(t.Context(), &sqssdk.ReceiveMessageInput{
		QueueUrl: aws.String(queueURL), MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, out.Messages, 1)

	body := aws.ToString(out.Messages[0].Body)
	if raw {
		return body
	}

	var env struct {
		Message string `json:"Message"`
	}

	require.NoError(t, json.Unmarshal([]byte(body), &env))

	return env.Message
}

func TestMessageStructureJSON_SQSFanOut(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		message string
		want    string
		raw     bool
		batch   bool
	}{
		{name: "envelope protocol key", message: msJSON, want: "for-sqs"},
		{name: "raw protocol key", message: msJSON, want: "for-sqs", raw: true},
		{name: "envelope default fallback", message: `{"default":"def"}`, want: "def"},
		{name: "raw default fallback", message: `{"default":"def"}`, want: "def", raw: true},
		{name: "batch envelope", message: msJSON, want: "for-sqs", batch: true},
		{name: "batch raw default", message: `{"default":"def"}`, want: "def", raw: true, batch: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snsClient, sqsClient := newSNSSQSPair(t)
			ctx := t.Context()

			topic, err := snsClient.CreateTopic(ctx, &snssdk.CreateTopicInput{Name: aws.String("ms-topic")})
			require.NoError(t, err)

			queueURL := subscribeQueue(t, snsClient, sqsClient, aws.ToString(topic.TopicArn), tt.raw)

			if tt.batch {
				_, err = snsClient.PublishBatch(ctx, &snssdk.PublishBatchInput{
					TopicArn: topic.TopicArn,
					PublishBatchRequestEntries: []snstypes.PublishBatchRequestEntry{{
						Id: aws.String("e1"), Message: aws.String(tt.message), MessageStructure: aws.String("json"),
					}},
				})
			} else {
				_, err = snsClient.Publish(ctx, &snssdk.PublishInput{
					TopicArn: topic.TopicArn, Message: aws.String(tt.message), MessageStructure: aws.String("json"),
				})
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, receiveBody(t, sqsClient, queueURL, tt.raw))
		})
	}
}

func TestMessageStructureJSON_AllProtocols(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want    map[string]string
		name    string
		message string
	}{
		{
			name:    "per protocol keys",
			message: msJSON,
			want: map[string]string{
				"lambda": "for-lambda", "firehose-raw": "for-firehose", "firehose-env": "for-firehose",
				"email": "for-email", "email-json": "for-email-json", "sms": "for-sms", "app": "for-gcm",
			},
		},
		{
			name:    "default fallback",
			message: `{"default":"def"}`,
			want: map[string]string{
				"lambda": "def", "firehose-raw": "def", "firehose-env": "def",
				"email": "def", "email-json": "def", "sms": "def", "app": "def",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			lambda := &mockLambdaInvoker{}
			firehose := newMockFirehose()
			b.SetLambdaBackend(lambda)
			b.SetFirehoseBackend(firehose)

			topic, err := b.CreateTopic("ms-all", nil)
			require.NoError(t, err)

			subscribe := func(protocol, endpoint string) *sns.Subscription {
				sub, sErr := b.Subscribe(topic.TopicArn, protocol, endpoint, "")
				require.NoError(t, sErr)

				if protocol == "email" || protocol == "email-json" {
					_, sErr = b.ConfirmSubscription(topic.TopicArn, sub.SubscriptionArn)
					require.NoError(t, sErr)
				}

				return sub
			}

			subscribe("lambda", "arn:aws:lambda:us-east-1:000000000000:function:fn")
			subscribe("email", "a@example.com")
			subscribe("email-json", "b@example.com")
			subscribe("sms", "+15555550100")

			raw := subscribe("firehose", "arn:aws:firehose:us-east-1:000000000000:deliverystream/raw")
			require.NoError(t, b.SetSubscriptionAttributes(raw.SubscriptionArn, "RawMessageDelivery", "true"))
			subscribe("firehose", "arn:aws:firehose:us-east-1:000000000000:deliverystream/env")

			app, err := b.CreatePlatformApplication("app", "GCM", map[string]string{"PlatformCredential": "k"})
			require.NoError(t, err)

			ep, err := b.CreatePlatformEndpoint(app.PlatformApplicationArn, "token", nil)
			require.NoError(t, err)
			subscribe("application", ep.EndpointArn)

			client := newTestSNSClient(t, sns.NewHandler(b))
			_, err = client.Publish(t.Context(), &snssdk.PublishInput{
				TopicArn: aws.String(topic.TopicArn), Message: aws.String(tt.message),
				MessageStructure: aws.String("json"),
			})
			require.NoError(t, err)

			var lenv struct {
				Records []struct {
					Sns struct {
						Message string `json:"Message"`
					} `json:"Sns"`
				} `json:"Records"`
			}

			require.NoError(t, json.Unmarshal(lambda.Last().Payload, &lenv))
			require.Len(t, lenv.Records, 1)
			assert.Equal(t, tt.want["lambda"], lenv.Records[0].Sns.Message)

			rawRecs := firehose.RecordsFor("raw")
			require.Len(t, rawRecs, 1)
			assert.Equal(t, tt.want["firehose-raw"], string(rawRecs[0]))

			envRecs := firehose.RecordsFor("env")
			require.Len(t, envRecs, 1)

			var fenv struct {
				Message string `json:"Message"`
			}

			require.NoError(t, json.Unmarshal(envRecs[0], &fenv))
			assert.Equal(t, tt.want["firehose-env"], fenv.Message)

			got := map[string]string{}
			for _, d := range b.DrainEmailDeliveries() {
				got[d.Protocol] = d.Message
			}

			assert.Equal(t, tt.want["email"], got["email"])
			assert.Equal(t, tt.want["email-json"], got["email-json"])

			smsDeliveries := b.DrainSMSDeliveries()
			require.Len(t, smsDeliveries, 1)
			assert.Equal(t, tt.want["sms"], smsDeliveries[0].Message)

			appDeliveries := b.DrainApplicationDeliveries()
			require.Len(t, appDeliveries, 1)
			assert.Equal(t, tt.want["app"], appDeliveries[0].Message)
		})
	}
}

func TestMessageStructureJSON_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		message string
	}{
		{name: "invalid json", message: "not json"},
		{name: "missing default", message: `{"sqs":"x"}`},
		{name: "non string value", message: `{"default":"x","sqs":5}`},
		{name: "non string default", message: `{"default":{"a":1}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			snsClient, _ := newSNSSQSPair(t)
			ctx := t.Context()

			topic, err := snsClient.CreateTopic(ctx, &snssdk.CreateTopicInput{Name: aws.String("ms-val")})
			require.NoError(t, err)

			_, err = snsClient.Publish(ctx, &snssdk.PublishInput{
				TopicArn: topic.TopicArn, Message: aws.String(tt.message), MessageStructure: aws.String("json"),
			})
			var invalid *snstypes.InvalidParameterException
			require.ErrorAs(t, err, &invalid)

			_, err = snsClient.Publish(ctx, &snssdk.PublishInput{
				PhoneNumber: aws.String("+15555550100"), Message: aws.String(tt.message),
				MessageStructure: aws.String("json"),
			})
			require.ErrorAs(t, err, &invalid)

			batch, err := snsClient.PublishBatch(ctx, &snssdk.PublishBatchInput{
				TopicArn: topic.TopicArn,
				PublishBatchRequestEntries: []snstypes.PublishBatchRequestEntry{{
					Id: aws.String("e1"), Message: aws.String(tt.message), MessageStructure: aws.String("json"),
				}},
			})
			require.NoError(t, err)
			require.Len(t, batch.Failed, 1)
			assert.Equal(t, "InvalidParameter", aws.ToString(batch.Failed[0].Code))
		})
	}
}

func TestMessageStructureJSON_Replay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		message string
		want    string
	}{
		{name: "protocol key", message: msJSON, want: "for-lambda"},
		{name: "default fallback", message: `{"default":"def"}`, want: "def"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := newTestBackend(t)
				lambda := &mockLambdaInvoker{}
				b.SetLambdaBackend(lambda)

				tp, err := b.CreateTopic("ms-replay.fifo", map[string]string{
					"ArchivePolicy": `{"MessageRetentionPeriod":30}`,
				})
				require.NoError(t, err)

				_, err = b.Publish(tp.TopicArn, tt.message, "", "json", nil)
				require.NoError(t, err)

				sub, err := b.Subscribe(tp.TopicArn, "lambda", "arn:aws:lambda:us-east-1:000000000000:function:fn", "")
				require.NoError(t, err)

				from := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
				require.NoError(t, b.SetSubscriptionAttributes(
					sub.SubscriptionArn, "ReplayPolicy", `{"replayFromTimestamp":"`+from+`"}`))
				synctest.Wait()

				var env struct {
					Records []struct {
						Sns struct {
							Message string `json:"Message"`
						} `json:"Sns"`
					} `json:"Records"`
				}

				require.Equal(t, 1, lambda.Count())
				require.NoError(t, json.Unmarshal(lambda.Last().Payload, &env))
				assert.Equal(t, tt.want, env.Records[0].Sns.Message)
			})
		})
	}
}
