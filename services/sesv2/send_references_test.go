package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSendEmail_EndpointAndIdentityARNs(t *testing.T) {
	t.Parallel()

	const ownARN = "arn:aws:ses:us-east-1:000000000000:identity/example.com"

	tests := []struct {
		name         string
		endpointID   string
		fromARN      string
		feedbackARN  string
		wantErr      string
		useRealEndID bool
	}{
		{name: "none"},
		{name: "real_endpoint", useRealEndID: true},
		{name: "unknown_endpoint", endpointID: "mre-nope", wantErr: "NotFoundException"},
		{name: "own_identity", fromARN: ownARN},
		{
			name: "missing_identity", wantErr: "NotFoundException",
			fromARN: "arn:aws:ses:us-east-1:000000000000:identity/gone.com",
		},
		{name: "malformed_arn", fromARN: "not-an-arn", wantErr: "BadRequestException"},
		{
			name: "feedback_identity_missing", wantErr: "NotFoundException",
			feedbackARN: "arn:aws:ses:us-east-1:000000000000:identity/gone.com",
		},
		{name: "other_account", fromARN: "arn:aws:ses:us-east-1:111111111111:identity/other.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newSESv2TestHandler(t)
			_, err := b.CreateEmailIdentity("example.com", "", nil)
			require.NoError(t, err)

			in := &sesv2sdk.SendEmailInput{
				FromEmailAddress: aws.String("s@example.com"),
				Destination:      &sesv2types.Destination{ToAddresses: []string{"r@example.com"}},
				Content: &sesv2types.EmailContent{Simple: &sesv2types.Message{
					Subject: &sesv2types.Content{Data: aws.String("hi")},
					Body:    &sesv2types.Body{Text: &sesv2types.Content{Data: aws.String("b")}},
				}},
			}

			if tt.useRealEndID {
				ep, cerr := b.CreateMultiRegionEndpoint("global", []string{"us-west-2"}, nil)
				require.NoError(t, cerr)

				in.EndpointId = aws.String(ep.EndpointID)
			} else if tt.endpointID != "" {
				in.EndpointId = aws.String(tt.endpointID)
			}

			if tt.fromARN != "" {
				in.FromEmailAddressIdentityArn = aws.String(tt.fromARN)
			}

			if tt.feedbackARN != "" {
				in.FeedbackForwardingEmailAddress = aws.String("fb@example.com")
				in.FeedbackForwardingEmailAddressIdentityArn = aws.String(tt.feedbackARN)
			}

			_, err = newSESv2SDKClient(t, h).SendEmail(t.Context(), in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Empty(t, b.ListEmails())

				return
			}

			require.NoError(t, err)
			assert.Len(t, b.ListEmails(), 1)
		})
	}
}
