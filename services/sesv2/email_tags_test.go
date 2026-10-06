package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tag(name, value string) sesv2types.MessageTag {
	return sesv2types.MessageTag{Name: aws.String(name), Value: aws.String(value)}
}

func TestEmailTags_SurfaceInMessageInsights(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want map[string]string
		send func(t *testing.T, c *sesv2sdk.Client) string
		name string
	}{
		{name: "send_email", send: func(t *testing.T, c *sesv2sdk.Client) string {
			t.Helper()

			out, err := c.SendEmail(t.Context(), &sesv2sdk.SendEmailInput{
				FromEmailAddress: aws.String("s@example.com"),
				Destination:      &sesv2types.Destination{ToAddresses: []string{"r@example.com"}},
				Content: &sesv2types.EmailContent{Simple: &sesv2types.Message{
					Subject: &sesv2types.Content{Data: aws.String("hi")},
					Body:    &sesv2types.Body{Text: &sesv2types.Content{Data: aws.String("b")}},
				}},
				EmailTags: []sesv2types.MessageTag{tag("campaign", "spring"), tag("env", "test")},
			})
			require.NoError(t, err)

			return aws.ToString(out.MessageId)
		}, want: map[string]string{"campaign": "spring", "env": "test"}},
		{name: "bulk_default_and_replacement", send: func(t *testing.T, c *sesv2sdk.Client) string {
			t.Helper()

			out, err := c.SendBulkEmail(t.Context(), &sesv2sdk.SendBulkEmailInput{
				FromEmailAddress: aws.String("s@example.com"),
				DefaultContent: &sesv2types.BulkEmailContent{Template: &sesv2types.Template{
					TemplateContent: &sesv2types.EmailTemplateContent{Subject: aws.String("s"), Text: aws.String("t")},
					TemplateData:    aws.String("{}"),
				}},
				DefaultEmailTags: []sesv2types.MessageTag{tag("campaign", "spring"), tag("env", "test")},
				BulkEmailEntries: []sesv2types.BulkEmailEntry{{
					Destination:     &sesv2types.Destination{ToAddresses: []string{"r@example.com"}},
					ReplacementTags: []sesv2types.MessageTag{tag("env", "prod")},
				}},
			})
			require.NoError(t, err)
			require.Len(t, out.BulkEmailEntryResults, 1)

			return aws.ToString(out.BulkEmailEntryResults[0].MessageId)
		}, want: map[string]string{"campaign": "spring", "env": "prod"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newSESv2TestHandler(t)
			_, err := b.CreateEmailIdentity("s@example.com", "", nil)
			require.NoError(t, err)

			c := newSESv2SDKClient(t, h)
			id := tt.send(t, c)

			got, err := c.GetMessageInsights(t.Context(), &sesv2sdk.GetMessageInsightsInput{MessageId: aws.String(id)})
			require.NoError(t, err)

			tags := map[string]string{}
			for _, mt := range got.EmailTags {
				tags[aws.ToString(mt.Name)] = aws.ToString(mt.Value)
			}

			assert.Equal(t, tt.want, tags)
		})
	}
}
