package ses_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sessdk "github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

func TestSendBounce_ExplanationAndMessageDsn_RealClient(t *testing.T) {
	t.Parallel()

	arrival := time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

	cases := []struct {
		dsn         *types.MessageDsn
		name        string
		explanation string
		wantContain []string
	}{
		{
			name:        "auto_generated_text",
			wantContain: []string{"to the following recipients failed", "rcpt@example.com", "dns; inbound-smtp."},
		},
		{
			name:        "explanation_replaces_text",
			explanation: "Mailbox is full",
			wantContain: []string{"Mailbox is full"},
		},
		{
			name: "message_dsn_fields",
			dsn: &types.MessageDsn{
				ReportingMta:    aws.String("dns; mta.example.com"),
				ArrivalDate:     aws.Time(arrival),
				ExtensionFields: []types.ExtensionField{{Name: aws.String("X-Trace"), Value: aws.String("abc")}},
			},
			wantContain: []string{
				"Reporting-MTA: dns; mta.example.com",
				"Arrival-Date: 2026-10-04T12:00:00Z",
				"X-Trace: abc",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := ses.NewInMemoryBackend()
			client := newTestSESClient(t, ses.NewHandler(backend))
			ctx := t.Context()

			_, err := client.VerifyEmailIdentity(ctx, &sessdk.VerifyEmailIdentityInput{
				EmailAddress: aws.String("sender@example.com"),
			})
			require.NoError(t, err)

			out, err := client.SendBounce(ctx, &sessdk.SendBounceInput{
				OriginalMessageId: aws.String("orig-1"),
				BounceSender:      aws.String("sender@example.com"),
				Explanation:       aws.String(tc.explanation),
				MessageDsn:        tc.dsn,
				BouncedRecipientInfoList: []types.BouncedRecipientInfo{
					{Recipient: aws.String("rcpt@example.com"), BounceType: types.BounceTypeDoesNotExist},
				},
			})
			require.NoError(t, err)

			email, err := backend.GetEmailByID(aws.ToString(out.MessageId))
			require.NoError(t, err)
			assert.Equal(t, "sender@example.com", email.From)

			for _, want := range tc.wantContain {
				assert.Contains(t, email.BodyText, want)
			}
		})
	}
}
