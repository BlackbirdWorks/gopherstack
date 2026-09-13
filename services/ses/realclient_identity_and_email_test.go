package ses_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sessdk "github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// newRealClient stands up a fresh backend/handler/client triple.
func newRealClient(t *testing.T) *sessdk.Client {
	t.Helper()

	backend := ses.NewInMemoryBackend()
	h := ses.NewHandler(backend)

	return newTestSESClient(t, h)
}

// TestRealClient_IdentityAndEmail drives every op the census still listed as
// uncovered before this pass (gopherstack-n3zi typed-client coverage).
//
// Found and fixed a real bug in the send_raw_email subtest: RawMessage.Data
// is a Blob member, and the real query-protocol serializer always
// base64-encodes blob members (ses@v1.37.4 serializers.go:
// awsAwsquery_serializeDocumentRawMessage -> objectKey.Base64EncodeBytes),
// but handleSendRawEmail (handler_email_sending.go) read RawMessage.Data
// straight off the form with no decode step at all -- every real client's
// SendRawEmail call handed the handler base64 text instead of a MIME
// message, so mail.ReadMessage silently failed to parse it and From/
// Subject/To were never extracted correctly. Fixed with a tolerant decode
// (try base64, fall back to the literal value) so pre-existing hand-crafted
// unit tests that POST literal MIME text directly still pass.
func TestRealClient_IdentityAndEmail(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "identity_verification_and_dkim",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newRealClient(t)

				_, err := client.VerifyEmailIdentity(ctx, &sessdk.VerifyEmailIdentityInput{
					EmailAddress: aws.String("sender@example.com"),
				})
				require.NoError(t, err)

				domainOut, err := client.VerifyDomainIdentity(ctx, &sessdk.VerifyDomainIdentityInput{
					Domain: aws.String("example.com"),
				})
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(domainOut.VerificationToken))

				dkimOut, err := client.VerifyDomainDkim(ctx, &sessdk.VerifyDomainDkimInput{
					Domain: aws.String("example.com"),
				})
				require.NoError(t, err)
				assert.NotEmpty(t, dkimOut.DkimTokens)

				_, err = client.SetIdentityDkimEnabled(ctx, &sessdk.SetIdentityDkimEnabledInput{
					Identity:    aws.String("example.com"),
					DkimEnabled: true,
				})
				require.NoError(t, err)

				dkimAttrs, err := client.GetIdentityDkimAttributes(ctx, &sessdk.GetIdentityDkimAttributesInput{
					Identities: []string{"example.com"},
				})
				require.NoError(t, err)
				require.Contains(t, dkimAttrs.DkimAttributes, "example.com")
				assert.True(t, dkimAttrs.DkimAttributes["example.com"].DkimEnabled)

				verAttrs, err := client.GetIdentityVerificationAttributes(
					ctx,
					&sessdk.GetIdentityVerificationAttributesInput{
						Identities: []string{"sender@example.com"},
					},
				)
				require.NoError(t, err)
				require.Contains(t, verAttrs.VerificationAttributes, "sender@example.com")
				assert.Equal(
					t,
					types.VerificationStatusSuccess,
					verAttrs.VerificationAttributes["sender@example.com"].VerificationStatus,
				)

				_, err = client.SetIdentityFeedbackForwardingEnabled(
					ctx,
					&sessdk.SetIdentityFeedbackForwardingEnabledInput{
						Identity:          aws.String("sender@example.com"),
						ForwardingEnabled: true,
					},
				)
				require.NoError(t, err)

				_, err = client.SetIdentityHeadersInNotificationsEnabled(
					ctx,
					&sessdk.SetIdentityHeadersInNotificationsEnabledInput{
						Identity:         aws.String("sender@example.com"),
						NotificationType: types.NotificationTypeBounce,
						Enabled:          true,
					},
				)
				require.NoError(t, err)

				_, err = client.SetIdentityMailFromDomain(ctx, &sessdk.SetIdentityMailFromDomainInput{
					Identity:            aws.String("example.com"),
					MailFromDomain:      aws.String("mail.example.com"),
					BehaviorOnMXFailure: types.BehaviorOnMXFailureUseDefaultValue,
				})
				require.NoError(t, err)

				_, err = client.SetIdentityNotificationTopic(ctx, &sessdk.SetIdentityNotificationTopicInput{
					Identity:         aws.String("sender@example.com"),
					NotificationType: types.NotificationTypeBounce,
					SnsTopic:         aws.String("arn:aws:sns:us-east-1:000000000000:topic1"),
				})
				require.NoError(t, err)

				_, err = client.DeleteIdentity(ctx, &sessdk.DeleteIdentityInput{
					Identity: aws.String("sender@example.com"),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "identity_policies",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newRealClient(t)

				const policyDoc = `{"Version":"2012-10-17","Statement":[]}`

				_, err := client.PutIdentityPolicy(ctx, &sessdk.PutIdentityPolicyInput{
					Identity:   aws.String("policy.example.com"),
					PolicyName: aws.String("AllowSend"),
					Policy:     aws.String(policyDoc),
				})
				require.NoError(t, err)

				got, err := client.GetIdentityPolicies(ctx, &sessdk.GetIdentityPoliciesInput{
					Identity:    aws.String("policy.example.com"),
					PolicyNames: []string{"AllowSend"},
				})
				require.NoError(t, err)
				require.Contains(t, got.Policies, "AllowSend")
				assert.JSONEq(t, policyDoc, got.Policies["AllowSend"])

				names, err := client.ListIdentityPolicies(ctx, &sessdk.ListIdentityPoliciesInput{
					Identity: aws.String("policy.example.com"),
				})
				require.NoError(t, err)
				assert.Equal(t, []string{"AllowSend"}, names.PolicyNames)

				_, err = client.DeleteIdentityPolicy(ctx, &sessdk.DeleteIdentityPolicyInput{
					Identity:   aws.String("policy.example.com"),
					PolicyName: aws.String("AllowSend"),
				})
				require.NoError(t, err)

				gone, err := client.GetIdentityPolicies(ctx, &sessdk.GetIdentityPoliciesInput{
					Identity:    aws.String("policy.example.com"),
					PolicyNames: []string{"AllowSend"},
				})
				require.NoError(t, err)
				assert.Empty(t, gone.Policies)
			},
		},
		{
			name: "verified_email_addresses_legacy",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newRealClient(t)

				_, err := client.VerifyEmailAddress(ctx, &sessdk.VerifyEmailAddressInput{
					EmailAddress: aws.String("legacy@example.com"),
				})
				require.NoError(t, err)

				listed, err := client.ListVerifiedEmailAddresses(ctx, &sessdk.ListVerifiedEmailAddressesInput{})
				require.NoError(t, err)
				assert.Contains(t, listed.VerifiedEmailAddresses, "legacy@example.com")

				_, err = client.DeleteVerifiedEmailAddress(ctx, &sessdk.DeleteVerifiedEmailAddressInput{
					EmailAddress: aws.String("legacy@example.com"),
				})
				require.NoError(t, err)

				afterDelete, err := client.ListVerifiedEmailAddresses(ctx, &sessdk.ListVerifiedEmailAddressesInput{})
				require.NoError(t, err)
				assert.NotContains(t, afterDelete.VerifiedEmailAddresses, "legacy@example.com")
			},
		},
		{
			name: "account_sending_and_quota",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newRealClient(t)

				enabled, err := client.GetAccountSendingEnabled(ctx, &sessdk.GetAccountSendingEnabledInput{})
				require.NoError(t, err)
				assert.True(t, enabled.Enabled)

				_, err = client.UpdateAccountSendingEnabled(ctx, &sessdk.UpdateAccountSendingEnabledInput{
					Enabled: false,
				})
				require.NoError(t, err)

				disabled, err := client.GetAccountSendingEnabled(ctx, &sessdk.GetAccountSendingEnabledInput{})
				require.NoError(t, err)
				assert.False(t, disabled.Enabled)

				quota, err := client.GetSendQuota(ctx, &sessdk.GetSendQuotaInput{})
				require.NoError(t, err)
				assert.Positive(t, quota.Max24HourSend)
				assert.Positive(t, quota.MaxSendRate)
			},
		},
		{
			name: "custom_verification_templates",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newRealClient(t)

				_, err := client.CreateCustomVerificationEmailTemplate(
					ctx,
					&sessdk.CreateCustomVerificationEmailTemplateInput{
						TemplateName:          aws.String("slice18-cvt"),
						FromEmailAddress:      aws.String("verify@example.com"),
						TemplateSubject:       aws.String("Please verify"),
						TemplateContent:       aws.String("Click {{RedirectUrl}}"),
						SuccessRedirectionURL: aws.String("https://example.com/success"),
						FailureRedirectionURL: aws.String("https://example.com/failure"),
					},
				)
				require.NoError(t, err)

				got, err := client.GetCustomVerificationEmailTemplate(
					ctx,
					&sessdk.GetCustomVerificationEmailTemplateInput{
						TemplateName: aws.String("slice18-cvt"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "verify@example.com", aws.ToString(got.FromEmailAddress))

				_, err = client.UpdateCustomVerificationEmailTemplate(
					ctx,
					&sessdk.UpdateCustomVerificationEmailTemplateInput{
						TemplateName:    aws.String("slice18-cvt"),
						TemplateSubject: aws.String("Please verify (updated)"),
					},
				)
				require.NoError(t, err)

				updated, err := client.GetCustomVerificationEmailTemplate(
					ctx,
					&sessdk.GetCustomVerificationEmailTemplateInput{
						TemplateName: aws.String("slice18-cvt"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, "Please verify (updated)", aws.ToString(updated.TemplateSubject))

				sent, err := client.SendCustomVerificationEmail(ctx, &sessdk.SendCustomVerificationEmailInput{
					EmailAddress: aws.String("recipient@example.com"),
					TemplateName: aws.String("slice18-cvt"),
				})
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(sent.MessageId))
			},
		},
		{
			name: "templates_and_render",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newRealClient(t)

				_, err := client.CreateTemplate(ctx, &sessdk.CreateTemplateInput{
					Template: &types.Template{
						TemplateName: aws.String("slice18-template"),
						SubjectPart:  aws.String("Hi {{name}}"),
						TextPart:     aws.String("Hello {{name}}, welcome."),
					},
				})
				require.NoError(t, err)

				got, err := client.GetTemplate(ctx, &sessdk.GetTemplateInput{
					TemplateName: aws.String("slice18-template"),
				})
				require.NoError(t, err)
				require.NotNil(t, got.Template)
				assert.Equal(t, "Hi {{name}}", aws.ToString(got.Template.SubjectPart))

				rendered, err := client.TestRenderTemplate(ctx, &sessdk.TestRenderTemplateInput{
					TemplateName: aws.String("slice18-template"),
					TemplateData: aws.String(`{"name":"World"}`),
				})
				require.NoError(t, err)
				assert.Contains(t, aws.ToString(rendered.RenderedTemplate), "Hello World")
			},
		},
		{
			name: "configuration_sets",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newRealClient(t)

				_, err := client.CreateConfigurationSet(ctx, &sessdk.CreateConfigurationSetInput{
					ConfigurationSet: &types.ConfigurationSet{Name: aws.String("slice18-cs")},
				})
				require.NoError(t, err)

				_, err = client.PutConfigurationSetDeliveryOptions(ctx, &sessdk.PutConfigurationSetDeliveryOptionsInput{
					ConfigurationSetName: aws.String("slice18-cs"),
					DeliveryOptions:      &types.DeliveryOptions{TlsPolicy: types.TlsPolicyRequire},
				})
				require.NoError(t, err)

				_, err = client.CreateConfigurationSetEventDestination(
					ctx,
					&sessdk.CreateConfigurationSetEventDestinationInput{
						ConfigurationSetName: aws.String("slice18-cs"),
						EventDestination: &types.EventDestination{
							Name:               aws.String("slice18-dest"),
							Enabled:            true,
							MatchingEventTypes: []types.EventType{types.EventTypeSend},
							SNSDestination: &types.SNSDestination{
								TopicARN: aws.String("arn:aws:sns:us-east-1:000000000000:topic1"),
							},
						},
					},
				)
				require.NoError(t, err)

				_, err = client.UpdateConfigurationSetEventDestination(
					ctx,
					&sessdk.UpdateConfigurationSetEventDestinationInput{
						ConfigurationSetName: aws.String("slice18-cs"),
						EventDestination: &types.EventDestination{
							Name:               aws.String("slice18-dest"),
							Enabled:            false,
							MatchingEventTypes: []types.EventType{types.EventTypeBounce},
							SNSDestination: &types.SNSDestination{
								TopicARN: aws.String("arn:aws:sns:us-east-1:000000000000:topic2"),
							},
						},
					},
				)
				require.NoError(t, err)

				_, err = client.UpdateConfigurationSetReputationMetricsEnabled(
					ctx,
					&sessdk.UpdateConfigurationSetReputationMetricsEnabledInput{
						ConfigurationSetName: aws.String("slice18-cs"),
						Enabled:              true,
					},
				)
				require.NoError(t, err)

				_, err = client.UpdateConfigurationSetSendingEnabled(
					ctx,
					&sessdk.UpdateConfigurationSetSendingEnabledInput{
						ConfigurationSetName: aws.String("slice18-cs"),
						Enabled:              false,
					},
				)
				require.NoError(t, err)

				_, err = client.CreateConfigurationSetTrackingOptions(
					ctx,
					&sessdk.CreateConfigurationSetTrackingOptionsInput{
						ConfigurationSetName: aws.String("slice18-cs"),
						TrackingOptions: &types.TrackingOptions{
							CustomRedirectDomain: aws.String("track.example.com"),
						},
					},
				)
				require.NoError(t, err)

				_, err = client.UpdateConfigurationSetTrackingOptions(
					ctx,
					&sessdk.UpdateConfigurationSetTrackingOptionsInput{
						ConfigurationSetName: aws.String("slice18-cs"),
						TrackingOptions: &types.TrackingOptions{
							CustomRedirectDomain: aws.String("track2.example.com"),
						},
					},
				)
				require.NoError(t, err)

				desc, err := client.DescribeConfigurationSet(ctx, &sessdk.DescribeConfigurationSetInput{
					ConfigurationSetName: aws.String("slice18-cs"),
					ConfigurationSetAttributeNames: []types.ConfigurationSetAttribute{
						types.ConfigurationSetAttributeTrackingOptions,
					},
				})
				require.NoError(t, err)
				require.NotNil(t, desc.TrackingOptions)
				assert.Equal(t, "track2.example.com", aws.ToString(desc.TrackingOptions.CustomRedirectDomain))
			},
		},
		{
			name: "receipt_rule_sets_and_rules",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newRealClient(t)

				_, err := client.CreateReceiptRuleSet(ctx, &sessdk.CreateReceiptRuleSetInput{
					RuleSetName: aws.String("slice18-rs"),
				})
				require.NoError(t, err)

				_, err = client.CreateReceiptRule(ctx, &sessdk.CreateReceiptRuleInput{
					RuleSetName: aws.String("slice18-rs"),
					Rule: &types.ReceiptRule{
						Name:       aws.String("rule-a"),
						Enabled:    true,
						Recipients: []string{"a@example.com"},
						Actions: []types.ReceiptAction{
							{StopAction: &types.StopAction{Scope: types.StopScopeRuleSet}},
						},
					},
				})
				require.NoError(t, err)

				_, err = client.CreateReceiptRule(ctx, &sessdk.CreateReceiptRuleInput{
					RuleSetName: aws.String("slice18-rs"),
					Rule: &types.ReceiptRule{
						Name:       aws.String("rule-b"),
						Enabled:    true,
						Recipients: []string{"b@example.com"},
						Actions: []types.ReceiptAction{
							{StopAction: &types.StopAction{Scope: types.StopScopeRuleSet}},
						},
					},
				})
				require.NoError(t, err)

				descRule, err := client.DescribeReceiptRule(ctx, &sessdk.DescribeReceiptRuleInput{
					RuleSetName: aws.String("slice18-rs"),
					RuleName:    aws.String("rule-a"),
				})
				require.NoError(t, err)
				require.NotNil(t, descRule.Rule)
				assert.Equal(t, []string{"a@example.com"}, descRule.Rule.Recipients)

				_, err = client.UpdateReceiptRule(ctx, &sessdk.UpdateReceiptRuleInput{
					RuleSetName: aws.String("slice18-rs"),
					Rule: &types.ReceiptRule{
						Name:       aws.String("rule-a"),
						Enabled:    true,
						Recipients: []string{"a@example.com", "a2@example.com"},
						Actions: []types.ReceiptAction{
							{StopAction: &types.StopAction{Scope: types.StopScopeRuleSet}},
						},
					},
				})
				require.NoError(t, err)

				reUpdated, err := client.DescribeReceiptRule(ctx, &sessdk.DescribeReceiptRuleInput{
					RuleSetName: aws.String("slice18-rs"),
					RuleName:    aws.String("rule-a"),
				})
				require.NoError(t, err)
				assert.ElementsMatch(t, []string{"a@example.com", "a2@example.com"}, reUpdated.Rule.Recipients)

				_, err = client.ReorderReceiptRuleSet(ctx, &sessdk.ReorderReceiptRuleSetInput{
					RuleSetName: aws.String("slice18-rs"),
					RuleNames:   []string{"rule-b", "rule-a"},
				})
				require.NoError(t, err)

				descSet, err := client.DescribeReceiptRuleSet(ctx, &sessdk.DescribeReceiptRuleSetInput{
					RuleSetName: aws.String("slice18-rs"),
				})
				require.NoError(t, err)
				require.Len(t, descSet.Rules, 2)
				assert.Equal(t, "rule-b", aws.ToString(descSet.Rules[0].Name))
				assert.Equal(t, "rule-a", aws.ToString(descSet.Rules[1].Name))

				_, err = client.SetReceiptRulePosition(ctx, &sessdk.SetReceiptRulePositionInput{
					RuleSetName: aws.String("slice18-rs"),
					RuleName:    aws.String("rule-a"),
					After:       aws.String(""),
				})
				require.NoError(t, err)

				reordered, err := client.DescribeReceiptRuleSet(ctx, &sessdk.DescribeReceiptRuleSetInput{
					RuleSetName: aws.String("slice18-rs"),
				})
				require.NoError(t, err)
				require.Len(t, reordered.Rules, 2)
				assert.Equal(t, "rule-a", aws.ToString(reordered.Rules[0].Name))

				_, err = client.SetActiveReceiptRuleSet(ctx, &sessdk.SetActiveReceiptRuleSetInput{
					RuleSetName: aws.String("slice18-rs"),
				})
				require.NoError(t, err)

				active, err := client.DescribeActiveReceiptRuleSet(ctx, &sessdk.DescribeActiveReceiptRuleSetInput{})
				require.NoError(t, err)
				require.NotNil(t, active.Metadata)
				assert.Equal(t, "slice18-rs", aws.ToString(active.Metadata.Name))

				_, err = client.CreateReceiptFilter(ctx, &sessdk.CreateReceiptFilterInput{
					Filter: &types.ReceiptFilter{
						Name: aws.String("slice18-filter"),
						IpFilter: &types.ReceiptIpFilter{
							Policy: types.ReceiptFilterPolicyAllow,
							Cidr:   aws.String("10.0.0.0/24"),
						},
					},
				})
				require.NoError(t, err)

				filters, err := client.ListReceiptFilters(ctx, &sessdk.ListReceiptFiltersInput{})
				require.NoError(t, err)
				require.Len(t, filters.Filters, 1)
				assert.Equal(t, "slice18-filter", aws.ToString(filters.Filters[0].Name))
			},
		},
		{
			name: "send_email_family",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newRealClient(t)

				_, err := client.VerifyEmailIdentity(ctx, &sessdk.VerifyEmailIdentityInput{
					EmailAddress: aws.String("sender@example.com"),
				})
				require.NoError(t, err)

				sent, err := client.SendEmail(ctx, &sessdk.SendEmailInput{
					Source: aws.String("sender@example.com"),
					Destination: &types.Destination{
						ToAddresses: []string{"success@simulator.amazonses.com"},
					},
					Message: &types.Message{
						Subject: &types.Content{Data: aws.String("Slice18 subject")},
						Body: &types.Body{
							Text: &types.Content{Data: aws.String("Slice18 body")},
						},
					},
				})
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(sent.MessageId))

				rawMsg := "From: sender@example.com\r\n" +
					"To: raw-dest@example.com\r\n" +
					"Subject: Slice18 raw subject\r\n" +
					"\r\n" +
					"Slice18 raw body\r\n"

				rawSent, err := client.SendRawEmail(ctx, &sessdk.SendRawEmailInput{
					RawMessage: &types.RawMessage{Data: []byte(rawMsg)},
				})
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(rawSent.MessageId))

				_, err = client.CreateTemplate(ctx, &sessdk.CreateTemplateInput{
					Template: &types.Template{
						TemplateName: aws.String("slice18-send-template"),
						SubjectPart:  aws.String("Hi {{name}}"),
						TextPart:     aws.String("Hello {{name}}"),
					},
				})
				require.NoError(t, err)

				templatedSent, err := client.SendTemplatedEmail(ctx, &sessdk.SendTemplatedEmailInput{
					Source: aws.String("sender@example.com"),
					Destination: &types.Destination{
						ToAddresses: []string{"success@simulator.amazonses.com"},
					},
					Template:     aws.String("slice18-send-template"),
					TemplateData: aws.String(`{"name":"World"}`),
				})
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(templatedSent.MessageId))

				bulkSent, err := client.SendBulkTemplatedEmail(ctx, &sessdk.SendBulkTemplatedEmailInput{
					Source:              aws.String("sender@example.com"),
					Template:            aws.String("slice18-send-template"),
					DefaultTemplateData: aws.String(`{"name":"Default"}`),
					Destinations: []types.BulkEmailDestination{
						{
							Destination: &types.Destination{
								ToAddresses: []string{"success@simulator.amazonses.com"},
							},
						},
					},
				})
				require.NoError(t, err)
				require.Len(t, bulkSent.Status, 1)
				assert.NotEmpty(t, aws.ToString(bulkSent.Status[0].MessageId))

				bounced, err := client.SendBounce(ctx, &sessdk.SendBounceInput{
					OriginalMessageId: aws.String(aws.ToString(sent.MessageId)),
					BounceSender:      aws.String("sender@example.com"),
					BouncedRecipientInfoList: []types.BouncedRecipientInfo{
						{
							Recipient:  aws.String("success@simulator.amazonses.com"),
							BounceType: types.BounceTypeContentRejected,
						},
					},
				})
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(bounced.MessageId))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
