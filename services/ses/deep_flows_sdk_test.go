package ses_test

import (
	"encoding/json"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sessdk "github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

func newDeepSES(t *testing.T) (*sessdk.Client, *ses.InMemoryBackend) {
	t.Helper()

	b := ses.NewInMemoryBackend()

	return newTestSESClient(t, ses.NewHandler(b)), b
}

func TestTemplate_HtmlPartRoundTrip_SDK(t *testing.T) {
	t.Parallel()

	client, b := newDeepSES(t)
	_, err := client.CreateTemplate(t.Context(), &sessdk.CreateTemplateInput{Template: &types.Template{
		TemplateName: aws.String("t1"),
		SubjectPart:  aws.String("Hi {{name}}"),
		TextPart:     aws.String("text {{name}}"),
		HtmlPart:     aws.String("<b>{{name}}</b>"),
	}})
	require.NoError(t, err)

	got, err := client.GetTemplate(t.Context(), &sessdk.GetTemplateInput{TemplateName: aws.String("t1")})
	require.NoError(t, err)
	assert.Equal(t, "<b>{{name}}</b>", aws.ToString(got.Template.HtmlPart))

	_, err = client.UpdateTemplate(t.Context(), &sessdk.UpdateTemplateInput{Template: &types.Template{
		TemplateName: aws.String("t1"),
		SubjectPart:  aws.String("Hi {{name}}"),
		HtmlPart:     aws.String("<i>{{name}}</i>"),
	}})
	require.NoError(t, err)

	got, err = client.GetTemplate(t.Context(), &sessdk.GetTemplateInput{TemplateName: aws.String("t1")})
	require.NoError(t, err)
	assert.Equal(t, "<i>{{name}}</i>", aws.ToString(got.Template.HtmlPart))

	_, err = client.VerifyEmailIdentity(t.Context(), &sessdk.VerifyEmailIdentityInput{
		EmailAddress: aws.String("a@example.com"),
	})
	require.NoError(t, err)

	_, err = client.SendTemplatedEmail(t.Context(), &sessdk.SendTemplatedEmailInput{
		Source:       aws.String("a@example.com"),
		Destination:  &types.Destination{ToAddresses: []string{"b@example.com"}},
		Template:     aws.String("t1"),
		TemplateData: aws.String(`{"name":"Bob"}`),
	})
	require.NoError(t, err)

	emails := b.ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "<i>Bob</i>", emails[0].BodyHTML)
}

func TestSend_FriendlyFrom_SDK(t *testing.T) {
	t.Parallel()

	const raw = "From: Alice <a@example.com>\r\nTo: b@example.com\r\nSubject: s\r\n\r\nbody"

	tests := []struct {
		send func(c *sessdk.Client) error
		name string
	}{
		{
			name: "send email",
			send: func(c *sessdk.Client) error {
				_, err := c.SendEmail(t.Context(), &sessdk.SendEmailInput{
					Source:      aws.String("Alice <a@example.com>"),
					Destination: &types.Destination{ToAddresses: []string{"b@example.com"}},
					Message: &types.Message{
						Subject: &types.Content{Data: aws.String("s")},
						Body:    &types.Body{Text: &types.Content{Data: aws.String("b")}},
					},
				})

				return err
			},
		},
		{
			name: "raw email from header",
			send: func(c *sessdk.Client) error {
				_, err := c.SendRawEmail(t.Context(), &sessdk.SendRawEmailInput{
					RawMessage: &types.RawMessage{Data: []byte(raw)},
				})

				return err
			},
		},
		{
			name: "raw email source",
			send: func(c *sessdk.Client) error {
				_, err := c.SendRawEmail(t.Context(), &sessdk.SendRawEmailInput{
					Source:       aws.String("Alice <a@example.com>"),
					Destinations: []string{"b@example.com"},
					RawMessage:   &types.RawMessage{Data: []byte(raw)},
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client, _ := newDeepSES(t)
			_, err := client.VerifyEmailIdentity(t.Context(), &sessdk.VerifyEmailIdentityInput{
				EmailAddress: aws.String("a@example.com"),
			})
			require.NoError(t, err)
			_, err = client.SendEmail(t.Context(), &sessdk.SendEmailInput{
				Source:      aws.String("Mallory <m@example.com>"),
				Destination: &types.Destination{ToAddresses: []string{"b@example.com"}},
				Message: &types.Message{
					Subject: &types.Content{Data: aws.String("s")},
					Body:    &types.Body{Text: &types.Content{Data: aws.String("b")}},
				},
			})
			var rejected *types.MessageRejected
			require.ErrorAs(t, err, &rejected)
			assert.Contains(t, aws.ToString(rejected.Message), "not verified")

			require.NoError(t, tc.send(client))
		})
	}
}

type recordingPublisher struct {
	messages []string
	mu       sync.Mutex
}

func (p *recordingPublisher) PublishToTopic(_, message string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.messages = append(p.messages, message)

	return nil
}

func TestEventDestination_SendAndDeliveryEvents_SDK(t *testing.T) {
	t.Parallel()

	client, b := newDeepSES(t)
	pub := &recordingPublisher{}
	b.SetSNSPublisher(pub)

	ctx := t.Context()
	_, err := client.VerifyEmailIdentity(
		ctx,
		&sessdk.VerifyEmailIdentityInput{EmailAddress: aws.String("a@example.com")},
	)
	require.NoError(t, err)
	_, err = client.CreateConfigurationSet(ctx, &sessdk.CreateConfigurationSetInput{
		ConfigurationSet: &types.ConfigurationSet{Name: aws.String("cs")},
	})
	require.NoError(t, err)
	_, err = client.CreateConfigurationSetEventDestination(ctx, &sessdk.CreateConfigurationSetEventDestinationInput{
		ConfigurationSetName: aws.String("cs"),
		EventDestination: &types.EventDestination{
			Name:               aws.String("d"),
			Enabled:            true,
			MatchingEventTypes: []types.EventType{types.EventTypeSend, types.EventTypeDelivery},
			SNSDestination:     &types.SNSDestination{TopicARN: aws.String("arn:aws:sns:us-east-1:000000000000:t")},
		},
	})
	require.NoError(t, err)

	_, err = client.SendEmail(ctx, &sessdk.SendEmailInput{
		Source:               aws.String("a@example.com"),
		Destination:          &types.Destination{ToAddresses: []string{"b@example.com"}},
		ConfigurationSetName: aws.String("cs"),
		Tags:                 []types.MessageTag{{Name: aws.String("env"), Value: aws.String("prod")}},
		Message: &types.Message{
			Subject: &types.Content{Data: aws.String("s")},
			Body:    &types.Body{Text: &types.Content{Data: aws.String("b")}},
		},
	})
	require.NoError(t, err)

	pub.mu.Lock()
	defer pub.mu.Unlock()
	require.Len(t, pub.messages, 2)

	var send, delivery map[string]any
	require.NoError(t, json.Unmarshal([]byte(pub.messages[0]), &send))
	require.NoError(t, json.Unmarshal([]byte(pub.messages[1]), &delivery))
	assert.Equal(t, "Send", send["eventType"])
	assert.Contains(t, send, "send")
	assert.Equal(t, "Delivery", delivery["eventType"])

	mail, ok := delivery["mail"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "123456789012", mail["sendingAccountId"])
	assert.Equal(t, map[string]any{
		"env":                   []any{"prod"},
		"ses:configuration-set": []any{"cs"},
	}, mail["tags"])
}
