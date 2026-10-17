package app

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSESv2SNSEventDestinationWiring(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		types []sesv2types.EventType
		want  []string
	}{
		{
			name:  "send and delivery",
			types: []sesv2types.EventType{sesv2types.EventTypeSend, sesv2types.EventTypeDelivery},
			want:  []string{"Delivery", "Send"},
		},
		{
			name:  "send only",
			types: []sesv2types.EventType{sesv2types.EventTypeSend},
			want:  []string{"Send"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			ctx := t.Context()
			snsC := sns.NewFromConfig(fx.cfg)
			sqsC := sqs.NewFromConfig(fx.cfg)
			sesC := sesv2.NewFromConfig(fx.cfg)

			topic, err := snsC.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String("ses-events")})
			require.NoError(t, err)

			q, err := sqsC.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("ses-events-q")})
			require.NoError(t, err)

			attrs, err := sqsC.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				QueueUrl:       q.QueueUrl,
				AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
			})
			require.NoError(t, err)

			_, err = snsC.Subscribe(ctx, &sns.SubscribeInput{
				TopicArn: topic.TopicArn,
				Protocol: aws.String("sqs"),
				Endpoint: aws.String(attrs.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]),
			})
			require.NoError(t, err)

			_, err = sesC.CreateEmailIdentity(ctx, &sesv2.CreateEmailIdentityInput{
				EmailIdentity: aws.String("sender@example.com"),
			})
			require.NoError(t, err)

			_, err = sesC.CreateConfigurationSet(ctx, &sesv2.CreateConfigurationSetInput{
				ConfigurationSetName: aws.String("cs"),
			})
			require.NoError(t, err)

			_, err = sesC.CreateConfigurationSetEventDestination(
				ctx,
				&sesv2.CreateConfigurationSetEventDestinationInput{
					ConfigurationSetName: aws.String("cs"),
					EventDestinationName: aws.String("sns"),
					EventDestination: &sesv2types.EventDestinationDefinition{
						Enabled:            true,
						MatchingEventTypes: tt.types,
						SnsDestination:     &sesv2types.SnsDestination{TopicArn: topic.TopicArn},
					},
				},
			)
			require.NoError(t, err)

			_, err = sesC.SendEmail(ctx, &sesv2.SendEmailInput{
				FromEmailAddress:     aws.String("sender@example.com"),
				ConfigurationSetName: aws.String("cs"),
				Destination:          &sesv2types.Destination{ToAddresses: []string{"rcpt@example.com"}},
				Content: &sesv2types.EmailContent{Simple: &sesv2types.Message{
					Subject: &sesv2types.Content{Data: aws.String("hi")},
					Body:    &sesv2types.Body{Text: &sesv2types.Content{Data: aws.String("body")}},
				}},
			})
			require.NoError(t, err)

			got := map[string]bool{}

			require.Eventually(t, func() bool {
				out, rerr := sqsC.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
					QueueUrl: q.QueueUrl, MaxNumberOfMessages: 10,
				})
				if rerr != nil {
					return false
				}

				for _, m := range out.Messages {
					var env struct {
						Message string `json:"Message"`
					}

					if json.Unmarshal([]byte(aws.ToString(m.Body)), &env) != nil {
						continue
					}

					var ev struct {
						EventType string `json:"eventType"`
					}

					if json.Unmarshal([]byte(env.Message), &ev) == nil {
						got[ev.EventType] = true
					}
				}

				return len(got) >= len(tt.want)
			}, 5*time.Second, 25*time.Millisecond)

			keys := make([]string, 0, len(got))
			for k := range got {
				keys = append(keys, k)
			}

			assert.ElementsMatch(t, tt.want, keys)
		})
	}
}
