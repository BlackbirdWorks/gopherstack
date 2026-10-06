package sesv2_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/require"
)

// TestList_HonoursPageSize pins the query-bound PageSize member of the list
// ops (e.g. api_op_ListEmailTemplates.go: "at least 1, no more than 100").
func TestList_HonoursPageSize(t *testing.T) {
	t.Parallel()

	names := []string{"a", "b", "c"}

	type listFn func(ctx context.Context, c *sesv2sdk.Client, size *int32, token *string) (int, *string, error)

	tests := []struct {
		create func(ctx context.Context, c *sesv2sdk.Client, name string) error
		list   listFn
		name   string
	}{
		{
			name: "configuration_sets",
			create: func(ctx context.Context, c *sesv2sdk.Client, n string) error {
				_, err := c.CreateConfigurationSet(
					ctx,
					&sesv2sdk.CreateConfigurationSetInput{ConfigurationSetName: aws.String(n)},
				)

				return err
			},
			list: func(ctx context.Context, c *sesv2sdk.Client, s *int32, tok *string) (int, *string, error) {
				o, err := c.ListConfigurationSets(
					ctx,
					&sesv2sdk.ListConfigurationSetsInput{PageSize: s, NextToken: tok},
				)
				if err != nil {
					return 0, nil, err
				}

				return len(o.ConfigurationSets), o.NextToken, nil
			},
		},
		{
			name: "contact_lists",
			create: func(ctx context.Context, c *sesv2sdk.Client, n string) error {
				_, err := c.CreateContactList(ctx, &sesv2sdk.CreateContactListInput{ContactListName: aws.String(n)})

				return err
			},
			list: func(ctx context.Context, c *sesv2sdk.Client, s *int32, tok *string) (int, *string, error) {
				o, err := c.ListContactLists(ctx, &sesv2sdk.ListContactListsInput{PageSize: s, NextToken: tok})
				if err != nil {
					return 0, nil, err
				}

				return len(o.ContactLists), o.NextToken, nil
			},
		},
		{
			name: "email_identities",
			create: func(ctx context.Context, c *sesv2sdk.Client, n string) error {
				_, err := c.CreateEmailIdentity(
					ctx,
					&sesv2sdk.CreateEmailIdentityInput{EmailIdentity: aws.String(n + "@example.com")},
				)

				return err
			},
			list: func(ctx context.Context, c *sesv2sdk.Client, s *int32, tok *string) (int, *string, error) {
				o, err := c.ListEmailIdentities(ctx, &sesv2sdk.ListEmailIdentitiesInput{PageSize: s, NextToken: tok})
				if err != nil {
					return 0, nil, err
				}

				return len(o.EmailIdentities), o.NextToken, nil
			},
		},
		{
			name: "email_templates",
			create: func(ctx context.Context, c *sesv2sdk.Client, n string) error {
				_, err := c.CreateEmailTemplate(ctx, &sesv2sdk.CreateEmailTemplateInput{
					TemplateName:    aws.String(n),
					TemplateContent: &types.EmailTemplateContent{Subject: aws.String("s"), Text: aws.String("t")},
				})

				return err
			},
			list: func(ctx context.Context, c *sesv2sdk.Client, s *int32, tok *string) (int, *string, error) {
				o, err := c.ListEmailTemplates(ctx, &sesv2sdk.ListEmailTemplatesInput{PageSize: s, NextToken: tok})
				if err != nil {
					return 0, nil, err
				}

				return len(o.TemplatesMetadata), o.NextToken, nil
			},
		},
		{
			name: "custom_verification_templates",
			create: func(ctx context.Context, c *sesv2sdk.Client, n string) error {
				_, err := c.CreateCustomVerificationEmailTemplate(
					ctx,
					&sesv2sdk.CreateCustomVerificationEmailTemplateInput{
						TemplateName:          aws.String(n),
						FromEmailAddress:      aws.String("from@example.com"),
						TemplateSubject:       aws.String("s"),
						TemplateContent:       aws.String("<p>hi</p>"),
						SuccessRedirectionURL: aws.String("https://example.com/ok"),
						FailureRedirectionURL: aws.String("https://example.com/fail"),
					},
				)

				return err
			},
			list: func(ctx context.Context, c *sesv2sdk.Client, s *int32, tok *string) (int, *string, error) {
				o, err := c.ListCustomVerificationEmailTemplates(
					ctx,
					&sesv2sdk.ListCustomVerificationEmailTemplatesInput{
						PageSize: s, NextToken: tok,
					},
				)
				if err != nil {
					return 0, nil, err
				}

				return len(o.CustomVerificationEmailTemplates), o.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newSESv2TestHandler(t)
			client := newSESv2SDKClient(t, h)

			for _, n := range names {
				require.NoError(t, tt.create(t.Context(), client, n))
			}

			seen, token := 0, (*string)(nil)
			pages := 0

			for {
				n, next, err := tt.list(t.Context(), client, aws.Int32(2), token)
				require.NoError(t, err)
				require.LessOrEqual(t, n, 2)

				seen += n
				pages++

				if next == nil || *next == "" {
					break
				}

				token = next
			}

			require.Equal(t, 3, seen)
			require.Equal(t, 2, pages)
		})
	}
}
