package ses_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sessdk "github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOperationErrors_CodeAndMessage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(*sessdk.Client) error
		name     string
		wantCode string
		wantMsg  string
	}{
		{
			name: "config set missing",
			call: func(c *sessdk.Client) error {
				_, err := c.DescribeConfigurationSet(t.Context(), &sessdk.DescribeConfigurationSetInput{
					ConfigurationSetName: aws.String("nope"),
				})

				return err
			},
			wantCode: "ConfigurationSetDoesNotExist",
			wantMsg:  "Configuration set <nope> does not exist.",
		},
		{
			name: "config set bad name",
			call: func(c *sessdk.Client) error {
				_, err := c.CreateConfigurationSet(t.Context(), &sessdk.CreateConfigurationSetInput{
					ConfigurationSet: &types.ConfigurationSet{Name: aws.String("bad name!")},
				})

				return err
			},
			wantCode: "InvalidConfigurationSet",
			wantMsg:  "name must be 1-64",
		},
		{
			name: "config set too long",
			call: func(c *sessdk.Client) error {
				_, err := c.CreateConfigurationSet(t.Context(), &sessdk.CreateConfigurationSetInput{
					ConfigurationSet: &types.ConfigurationSet{Name: aws.String(strings.Repeat("a", 65))},
				})

				return err
			},
			wantCode: "InvalidConfigurationSet",
			wantMsg:  "name must be 1-64",
		},
		{
			name: "recipient without domain",
			call: func(c *sessdk.Client) error {
				if _, err := c.VerifyEmailIdentity(t.Context(), &sessdk.VerifyEmailIdentityInput{
					EmailAddress: aws.String("a@example.com"),
				}); err != nil {
					return err
				}
				_, err := c.SendEmail(t.Context(), &sessdk.SendEmailInput{
					Source:      aws.String("a@example.com"),
					Destination: &types.Destination{ToAddresses: []string{"bad"}},
					Message: &types.Message{
						Subject: &types.Content{Data: aws.String("s")},
						Body:    &types.Body{Text: &types.Content{Data: aws.String("x")}},
					},
				})

				return err
			},
			wantCode: "InvalidParameterValue",
			wantMsg:  "Missing final '@domain'",
		},
		{
			name: "unverified sender",
			call: func(c *sessdk.Client) error {
				_, err := c.SendEmail(t.Context(), &sessdk.SendEmailInput{
					Source:      aws.String("nobody@example.com"),
					Destination: &types.Destination{ToAddresses: []string{"b@example.com"}},
					Message: &types.Message{
						Subject: &types.Content{Data: aws.String("s")},
						Body:    &types.Body{Text: &types.Content{Data: aws.String("x")}},
					},
				})

				return err
			},
			wantCode: "MessageRejected",
			wantMsg:  "Email address is not verified",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.call(newTestSESClient(t, newHandler()))
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.Contains(t, apiErr.ErrorMessage(), tt.wantMsg)
			assert.NotContains(t, apiErr.ErrorMessage(), tt.wantCode+":")
		})
	}
}
