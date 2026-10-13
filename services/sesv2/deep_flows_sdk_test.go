package sesv2_test

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

type recordingSNS struct {
	messages []string
	topics   []string
	mu       sync.Mutex
}

func (p *recordingSNS) PublishToTopic(topic, message string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.topics = append(p.topics, topic)
	p.messages = append(p.messages, message)

	return nil
}

func simpleContent() *sesv2types.EmailContent {
	return &sesv2types.EmailContent{Simple: &sesv2types.Message{
		Subject: &sesv2types.Content{Data: aws.String("s")},
		Body:    &sesv2types.Body{Text: &sesv2types.Content{Data: aws.String("b")}},
	}}
}

func TestSendEmail_FriendlyFrom_SDK(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		from    string
		wantErr bool
	}{
		{name: "bare", from: "a@example.com"},
		{name: "friendly", from: "Alice <a@example.com>"},
		{name: "friendly domain", from: "Alice <x@example.org>"},
		{name: "unverified friendly", from: "Mallory <m@nowhere.net>", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newSESv2SDKClient(t, sesv2.NewHandler(sesv2.NewInMemoryBackend()))
			for _, id := range []string{"a@example.com", "example.org"} {
				_, err := client.CreateEmailIdentity(t.Context(), &sesv2sdk.CreateEmailIdentityInput{
					EmailIdentity: aws.String(id),
				})
				require.NoError(t, err)
			}

			_, err := client.SendEmail(t.Context(), &sesv2sdk.SendEmailInput{
				FromEmailAddress: aws.String(tc.from),
				Destination:      &sesv2types.Destination{ToAddresses: []string{"b@example.com"}},
				Content:          simpleContent(),
			})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestSendEmail_SNSEventDestination_SDK(t *testing.T) {
	t.Parallel()

	b := sesv2.NewInMemoryBackend()
	pub := &recordingSNS{}
	b.SetSNSPublisher(pub)
	client := newSESv2SDKClient(t, sesv2.NewHandler(b))
	ctx := t.Context()

	_, err := client.CreateEmailIdentity(
		ctx,
		&sesv2sdk.CreateEmailIdentityInput{EmailIdentity: aws.String("a@example.com")},
	)
	require.NoError(t, err)
	_, err = client.CreateConfigurationSet(ctx, &sesv2sdk.CreateConfigurationSetInput{
		ConfigurationSetName: aws.String("cs"),
	})
	require.NoError(t, err)

	const topic = "arn:aws:sns:us-east-1:123456789012:t"

	_, err = client.CreateConfigurationSetEventDestination(ctx, &sesv2sdk.CreateConfigurationSetEventDestinationInput{
		ConfigurationSetName: aws.String("cs"),
		EventDestinationName: aws.String("d"),
		EventDestination: &sesv2types.EventDestinationDefinition{
			Enabled:            true,
			MatchingEventTypes: []sesv2types.EventType{sesv2types.EventTypeSend, sesv2types.EventTypeDelivery},
			SnsDestination:     &sesv2types.SnsDestination{TopicArn: aws.String(topic)},
		},
	})
	require.NoError(t, err)

	_, err = client.SendEmail(ctx, &sesv2sdk.SendEmailInput{
		FromEmailAddress:     aws.String("a@example.com"),
		Destination:          &sesv2types.Destination{ToAddresses: []string{"b@example.com"}},
		Content:              simpleContent(),
		ConfigurationSetName: aws.String("cs"),
		EmailTags:            []sesv2types.MessageTag{{Name: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	pub.mu.Lock()
	defer pub.mu.Unlock()
	require.Len(t, pub.messages, 2)
	assert.Equal(t, []string{topic, topic}, pub.topics)

	var send, delivery map[string]any
	require.NoError(t, json.Unmarshal([]byte(pub.messages[0]), &send))
	require.NoError(t, json.Unmarshal([]byte(pub.messages[1]), &delivery))
	assert.Equal(t, "Send", send["eventType"])
	assert.Equal(t, "Delivery", delivery["eventType"])

	mail, ok := delivery["mail"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "a@example.com", mail["source"])
	assert.Equal(t, map[string]any{
		"env": []any{"prod"}, "ses:configuration-set": []any{"cs"},
	}, mail["tags"])
}
