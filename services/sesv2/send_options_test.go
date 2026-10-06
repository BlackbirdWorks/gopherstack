package sesv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

const (
	optsIdentityARN  = "arn:aws:ses:us-east-1:000000000000:identity/s@example.com"
	optsConfigSetARN = "arn:aws:ses:us-east-1:000000000000:configuration-set/cs1"
)

func TestSendEmail_ReferencedResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want      sesv2.SendOptions
		in        sesv2sdk.SendEmailInput
		name      string
		wantErr   string
		associate []string
	}{
		{
			name: "config_set_and_feedback",
			in: sesv2sdk.SendEmailInput{
				ConfigurationSetName:           aws.String("cs1"),
				FeedbackForwardingEmailAddress: aws.String("fb@example.com"),
			},
			want: sesv2.SendOptions{ConfigurationSetName: "cs1", FeedbackForwardingEmailAddress: "fb@example.com"},
		},
		{
			name:    "unknown_config_set",
			in:      sesv2sdk.SendEmailInput{ConfigurationSetName: aws.String("nope")},
			wantErr: "NotFoundException",
		},
		{
			name: "list_management",
			in: sesv2sdk.SendEmailInput{ListManagementOptions: &sesv2types.ListManagementOptions{
				ContactListName: aws.String("cl"), TopicName: aws.String("news"),
			}},
			want: sesv2.SendOptions{
				ListManagement: &sesv2.ListManagementOptions{ContactListName: "cl", TopicName: "news"},
			},
		},
		{
			name: "unknown_topic",
			in: sesv2sdk.SendEmailInput{ListManagementOptions: &sesv2types.ListManagementOptions{
				ContactListName: aws.String("cl"), TopicName: aws.String("nope"),
			}},
			wantErr: "NotFoundException",
		},
		{
			name: "unknown_contact_list",
			in: sesv2sdk.SendEmailInput{ListManagementOptions: &sesv2types.ListManagementOptions{
				ContactListName: aws.String("nope"),
			}},
			wantErr: "NotFoundException",
		},
		{
			name:    "unknown_tenant",
			in:      sesv2sdk.SendEmailInput{TenantName: aws.String("nope")},
			wantErr: "NotFoundException",
		},
		{
			name:    "tenant_without_associations",
			in:      sesv2sdk.SendEmailInput{TenantName: aws.String("t1")},
			wantErr: "BadRequestException",
		},
		{
			name:      "tenant_missing_config_set_association",
			in:        sesv2sdk.SendEmailInput{TenantName: aws.String("t1"), ConfigurationSetName: aws.String("cs1")},
			associate: []string{optsIdentityARN},
			wantErr:   "BadRequestException",
		},
		{
			name:      "tenant_associated",
			in:        sesv2sdk.SendEmailInput{TenantName: aws.String("t1"), ConfigurationSetName: aws.String("cs1")},
			associate: []string{optsIdentityARN, optsConfigSetARN},
			want:      sesv2.SendOptions{TenantName: "t1", ConfigurationSetName: "cs1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newSESv2TestHandler(t)
			_, err := b.CreateEmailIdentity("s@example.com", "", nil)
			require.NoError(t, err)
			_, err = b.CreateConfigurationSet("cs1", nil)
			require.NoError(t, err)
			_, err = b.CreateContactList("cl", "", nil, []sesv2.Topic{
				{TopicName: "news", DisplayName: "News", DefaultSubscriptionStatus: "OPT_IN"},
			})
			require.NoError(t, err)
			_, err = b.CreateTenant("t1", nil)
			require.NoError(t, err)

			for _, a := range tt.associate {
				require.NoError(t, b.CreateTenantResourceAssociation("t1", a))
			}

			tt.in.FromEmailAddress = aws.String("s@example.com")
			tt.in.Destination = &sesv2types.Destination{ToAddresses: []string{"r@example.com"}}
			tt.in.Content = &sesv2types.EmailContent{Simple: &sesv2types.Message{
				Subject: &sesv2types.Content{Data: aws.String("hi")},
				Body:    &sesv2types.Body{Text: &sesv2types.Content{Data: aws.String("b")}},
			}}

			_, err = newSESv2SDKClient(t, h).SendEmail(t.Context(), &tt.in)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				assert.Empty(t, b.ListEmails())

				return
			}

			require.NoError(t, err)

			emails := b.ListEmails()
			require.Len(t, emails, 1)
			assert.Equal(t, tt.want, emails[0].Options)
		})
	}
}

func TestSendBulkEmail_ReferencedResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cs      string
		wantErr string
	}{
		{name: "config_set_recorded", cs: "cs1"},
		{name: "unknown_config_set", cs: "nope", wantErr: "NotFoundException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newSESv2TestHandler(t)
			_, err := b.CreateEmailIdentity("s@example.com", "", nil)
			require.NoError(t, err)
			_, err = b.CreateConfigurationSet("cs1", nil)
			require.NoError(t, err)

			_, err = newSESv2SDKClient(t, h).SendBulkEmail(t.Context(), &sesv2sdk.SendBulkEmailInput{
				FromEmailAddress:     aws.String("s@example.com"),
				ConfigurationSetName: aws.String(tt.cs),
				DefaultContent: &sesv2types.BulkEmailContent{Template: &sesv2types.Template{
					TemplateContent: &sesv2types.EmailTemplateContent{Subject: aws.String("s"), Text: aws.String("t")},
					TemplateData:    aws.String("{}"),
				}},
				BulkEmailEntries: []sesv2types.BulkEmailEntry{{
					Destination: &sesv2types.Destination{ToAddresses: []string{"r@example.com"}},
				}},
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			emails := b.ListEmails()
			require.Len(t, emails, 1)
			assert.Equal(t, tt.cs, emails[0].Options.ConfigurationSetName)
		})
	}
}
