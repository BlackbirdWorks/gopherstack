package sesv2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTypedSlice11RealClient drives sesv2's remaining typed-client-blind ops
// through the real aws-sdk-go-v2 sesv2 client (gopherstack-n3zi typed slice
// 11).
func TestTypedSlice11RealClient(t *testing.T) {
	t.Parallel()

	t.Run("configuration set event destinations", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.CreateConfigurationSet(ctx, &sesv2sdk.CreateConfigurationSetInput{
			ConfigurationSetName: aws.String("s11-cfgset"),
		})
		require.NoError(t, err)

		_, err = client.CreateConfigurationSetEventDestination(
			ctx, &sesv2sdk.CreateConfigurationSetEventDestinationInput{
				ConfigurationSetName: aws.String("s11-cfgset"),
				EventDestinationName: aws.String("dest-1"),
				EventDestination: &sesv2types.EventDestinationDefinition{
					Enabled:            true,
					MatchingEventTypes: []sesv2types.EventType{sesv2types.EventTypeSend},
					EventBridgeDestination: &sesv2types.EventBridgeDestination{
						EventBusArn: aws.String("arn:aws:events:us-east-1:000000000000:event-bus/default"),
					},
				},
			})
		require.NoError(t, err)

		getOut, err := client.GetConfigurationSetEventDestinations(
			ctx, &sesv2sdk.GetConfigurationSetEventDestinationsInput{
				ConfigurationSetName: aws.String("s11-cfgset"),
			})
		require.NoError(t, err)
		require.Len(t, getOut.EventDestinations, 1)
		assert.Equal(t, "dest-1", aws.ToString(getOut.EventDestinations[0].Name))
		assert.True(t, getOut.EventDestinations[0].Enabled)

		_, err = client.UpdateConfigurationSetEventDestination(
			ctx, &sesv2sdk.UpdateConfigurationSetEventDestinationInput{
				ConfigurationSetName: aws.String("s11-cfgset"),
				EventDestinationName: aws.String("dest-1"),
				EventDestination: &sesv2types.EventDestinationDefinition{
					Enabled:            false,
					MatchingEventTypes: []sesv2types.EventType{sesv2types.EventTypeBounce},
					EventBridgeDestination: &sesv2types.EventBridgeDestination{
						EventBusArn: aws.String("arn:aws:events:us-east-1:000000000000:event-bus/default"),
					},
				},
			})
		require.NoError(t, err)

		afterOut, err := client.GetConfigurationSetEventDestinations(
			ctx, &sesv2sdk.GetConfigurationSetEventDestinationsInput{
				ConfigurationSetName: aws.String("s11-cfgset"),
			})
		require.NoError(t, err)
		require.Len(t, afterOut.EventDestinations, 1)
		assert.False(t, afterOut.EventDestinations[0].Enabled)

		_, err = client.DeleteConfigurationSetEventDestination(
			ctx, &sesv2sdk.DeleteConfigurationSetEventDestinationInput{
				ConfigurationSetName: aws.String("s11-cfgset"),
				EventDestinationName: aws.String("dest-1"),
			})
		require.NoError(t, err)

		finalOut, err := client.GetConfigurationSetEventDestinations(
			ctx, &sesv2sdk.GetConfigurationSetEventDestinationsInput{
				ConfigurationSetName: aws.String("s11-cfgset"),
			})
		require.NoError(t, err)
		assert.Empty(t, finalOut.EventDestinations)
	})

	t.Run("configuration set attribute puts and list", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.CreateConfigurationSet(ctx, &sesv2sdk.CreateConfigurationSetInput{
			ConfigurationSetName: aws.String("s11-cfgattrs"),
		})
		require.NoError(t, err)

		_, err = client.PutConfigurationSetDeliveryOptions(ctx, &sesv2sdk.PutConfigurationSetDeliveryOptionsInput{
			ConfigurationSetName: aws.String("s11-cfgattrs"),
			TlsPolicy:            sesv2types.TlsPolicyRequire,
		})
		require.NoError(t, err)

		_, err = client.PutConfigurationSetReputationOptions(ctx, &sesv2sdk.PutConfigurationSetReputationOptionsInput{
			ConfigurationSetName:     aws.String("s11-cfgattrs"),
			ReputationMetricsEnabled: true,
		})
		require.NoError(t, err)

		_, err = client.PutConfigurationSetSendingOptions(ctx, &sesv2sdk.PutConfigurationSetSendingOptionsInput{
			ConfigurationSetName: aws.String("s11-cfgattrs"),
			SendingEnabled:       false,
		})
		require.NoError(t, err)

		_, err = client.PutConfigurationSetSuppressionOptions(ctx, &sesv2sdk.PutConfigurationSetSuppressionOptionsInput{
			ConfigurationSetName: aws.String("s11-cfgattrs"),
			SuppressedReasons:    []sesv2types.SuppressionListReason{sesv2types.SuppressionListReasonBounce},
		})
		require.NoError(t, err)

		getOut, err := client.GetConfigurationSet(ctx, &sesv2sdk.GetConfigurationSetInput{
			ConfigurationSetName: aws.String("s11-cfgattrs"),
		})
		require.NoError(t, err)
		require.NotNil(t, getOut.DeliveryOptions)
		assert.Equal(t, sesv2types.TlsPolicyRequire, getOut.DeliveryOptions.TlsPolicy)
		require.NotNil(t, getOut.ReputationOptions)
		assert.True(t, getOut.ReputationOptions.ReputationMetricsEnabled)
		require.NotNil(t, getOut.SendingOptions)
		assert.False(t, getOut.SendingOptions.SendingEnabled)
		require.NotNil(t, getOut.SuppressionOptions)
		assert.Equal(t, []sesv2types.SuppressionListReason{sesv2types.SuppressionListReasonBounce},
			getOut.SuppressionOptions.SuppressedReasons)

		listOut, err := client.ListConfigurationSets(ctx, &sesv2sdk.ListConfigurationSetsInput{})
		require.NoError(t, err)
		assert.Contains(t, listOut.ConfigurationSets, "s11-cfgattrs")
	})

	t.Run("custom verification email templates", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.CreateEmailIdentity(ctx, &sesv2sdk.CreateEmailIdentityInput{
			EmailIdentity: aws.String("verify-from@example.com"),
		})
		require.NoError(t, err)

		_, err = client.CreateCustomVerificationEmailTemplate(ctx, &sesv2sdk.CreateCustomVerificationEmailTemplateInput{
			TemplateName:          aws.String("s11-cvt"),
			FromEmailAddress:      aws.String("verify-from@example.com"),
			TemplateSubject:       aws.String("Please verify"),
			TemplateContent:       aws.String("<p>verify {{Link}}</p>"),
			SuccessRedirectionURL: aws.String("https://example.com/success"),
			FailureRedirectionURL: aws.String("https://example.com/failure"),
		})
		require.NoError(t, err)

		getOut, err := client.GetCustomVerificationEmailTemplate(
			ctx, &sesv2sdk.GetCustomVerificationEmailTemplateInput{TemplateName: aws.String("s11-cvt")})
		require.NoError(t, err)
		assert.Equal(t, "Please verify", aws.ToString(getOut.TemplateSubject))

		_, err = client.UpdateCustomVerificationEmailTemplate(ctx, &sesv2sdk.UpdateCustomVerificationEmailTemplateInput{
			TemplateName:          aws.String("s11-cvt"),
			FromEmailAddress:      aws.String("verify-from@example.com"),
			TemplateSubject:       aws.String("Please verify (updated)"),
			TemplateContent:       aws.String("<p>verify {{Link}}</p>"),
			SuccessRedirectionURL: aws.String("https://example.com/success"),
			FailureRedirectionURL: aws.String("https://example.com/failure"),
		})
		require.NoError(t, err)

		afterOut, err := client.GetCustomVerificationEmailTemplate(
			ctx, &sesv2sdk.GetCustomVerificationEmailTemplateInput{TemplateName: aws.String("s11-cvt")})
		require.NoError(t, err)
		assert.Equal(t, "Please verify (updated)", aws.ToString(afterOut.TemplateSubject))

		listOut, err := client.ListCustomVerificationEmailTemplates(
			ctx, &sesv2sdk.ListCustomVerificationEmailTemplatesInput{})
		require.NoError(t, err)
		require.Len(t, listOut.CustomVerificationEmailTemplates, 1)

		_, err = client.SendCustomVerificationEmail(ctx, &sesv2sdk.SendCustomVerificationEmailInput{
			EmailAddress: aws.String("newuser@example.com"),
			TemplateName: aws.String("s11-cvt"),
		})
		require.NoError(t, err)

		_, err = client.DeleteCustomVerificationEmailTemplate(
			ctx, &sesv2sdk.DeleteCustomVerificationEmailTemplateInput{TemplateName: aws.String("s11-cvt")})
		require.NoError(t, err)

		afterDeleteList, err := client.ListCustomVerificationEmailTemplates(
			ctx, &sesv2sdk.ListCustomVerificationEmailTemplatesInput{})
		require.NoError(t, err)
		assert.Empty(t, afterDeleteList.CustomVerificationEmailTemplates)
	})

	t.Run("deliverability", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.CreateEmailIdentity(ctx, &sesv2sdk.CreateEmailIdentityInput{
			EmailIdentity: aws.String("dtest@example.com"),
		})
		require.NoError(t, err)

		createOut, err := client.CreateDeliverabilityTestReport(ctx, &sesv2sdk.CreateDeliverabilityTestReportInput{
			FromEmailAddress: aws.String("dtest@example.com"),
			ReportName:       aws.String("s11-report"),
			Content: &sesv2types.EmailContent{
				Simple: &sesv2types.Message{
					Subject: &sesv2types.Content{Data: aws.String("s")},
					Body:    &sesv2types.Body{Text: &sesv2types.Content{Data: aws.String("b")}},
				},
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(createOut.ReportId))

		getOut, err := client.GetDeliverabilityTestReport(ctx, &sesv2sdk.GetDeliverabilityTestReportInput{
			ReportId: createOut.ReportId,
		})
		require.NoError(t, err)
		require.NotNil(t, getOut.DeliverabilityTestReport)
		assert.Equal(t, "s11-report", aws.ToString(getOut.DeliverabilityTestReport.ReportName))

		listOut, err := client.ListDeliverabilityTestReports(ctx, &sesv2sdk.ListDeliverabilityTestReportsInput{})
		require.NoError(t, err)
		require.Len(t, listOut.DeliverabilityTestReports, 1)

		dashOut, err := client.GetDeliverabilityDashboardOptions(
			ctx, &sesv2sdk.GetDeliverabilityDashboardOptionsInput{})
		require.NoError(t, err)
		assert.False(t, dashOut.DashboardEnabled, "dashboard is disabled by default")

		_, err = client.PutDeliverabilityDashboardOption(ctx, &sesv2sdk.PutDeliverabilityDashboardOptionInput{
			DashboardEnabled: true,
		})
		require.NoError(t, err)

		afterDash, err := client.GetDeliverabilityDashboardOptions(
			ctx, &sesv2sdk.GetDeliverabilityDashboardOptionsInput{})
		require.NoError(t, err)
		assert.True(t, afterDash.DashboardEnabled)

		blOut, err := client.GetBlacklistReports(ctx, &sesv2sdk.GetBlacklistReportsInput{
			BlacklistItemNames: []string{"10.0.0.1"},
		})
		require.NoError(t, err)
		assert.NotNil(t, blOut.BlacklistReport)
	})

	t.Run("email identity policies", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.CreateEmailIdentity(ctx, &sesv2sdk.CreateEmailIdentityInput{
			EmailIdentity: aws.String("policy-id@example.com"),
		})
		require.NoError(t, err)

		policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*",` +
			`"Action":"ses:SendEmail","Resource":"*"}]}`
		_, err = client.CreateEmailIdentityPolicy(ctx, &sesv2sdk.CreateEmailIdentityPolicyInput{
			EmailIdentity: aws.String("policy-id@example.com"),
			PolicyName:    aws.String("s11-policy"),
			Policy:        aws.String(policyDoc),
		})
		require.NoError(t, err)

		getOut, err := client.GetEmailIdentityPolicies(ctx, &sesv2sdk.GetEmailIdentityPoliciesInput{
			EmailIdentity: aws.String("policy-id@example.com"),
		})
		require.NoError(t, err)
		require.Contains(t, getOut.Policies, "s11-policy")

		updatedDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*",` +
			`"Action":"ses:SendEmail","Resource":"*"}]}`
		_, err = client.UpdateEmailIdentityPolicy(ctx, &sesv2sdk.UpdateEmailIdentityPolicyInput{
			EmailIdentity: aws.String("policy-id@example.com"),
			PolicyName:    aws.String("s11-policy"),
			Policy:        aws.String(updatedDoc),
		})
		require.NoError(t, err)

		afterOut, err := client.GetEmailIdentityPolicies(ctx, &sesv2sdk.GetEmailIdentityPoliciesInput{
			EmailIdentity: aws.String("policy-id@example.com"),
		})
		require.NoError(t, err)
		assert.Contains(t, afterOut.Policies["s11-policy"], "Deny")

		_, err = client.DeleteEmailIdentityPolicy(ctx, &sesv2sdk.DeleteEmailIdentityPolicyInput{
			EmailIdentity: aws.String("policy-id@example.com"),
			PolicyName:    aws.String("s11-policy"),
		})
		require.NoError(t, err)

		finalOut, err := client.GetEmailIdentityPolicies(ctx, &sesv2sdk.GetEmailIdentityPoliciesInput{
			EmailIdentity: aws.String("policy-id@example.com"),
		})
		require.NoError(t, err)
		assert.NotContains(t, finalOut.Policies, "s11-policy")
	})

	t.Run("email identity attributes and delete", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.CreateConfigurationSet(ctx, &sesv2sdk.CreateConfigurationSetInput{
			ConfigurationSetName: aws.String("s11-identity-cfgset"),
		})
		require.NoError(t, err)

		_, err = client.CreateEmailIdentity(ctx, &sesv2sdk.CreateEmailIdentityInput{
			EmailIdentity: aws.String("attrs@example.com"),
		})
		require.NoError(t, err)

		_, err = client.PutEmailIdentityConfigurationSetAttributes(
			ctx, &sesv2sdk.PutEmailIdentityConfigurationSetAttributesInput{
				EmailIdentity:        aws.String("attrs@example.com"),
				ConfigurationSetName: aws.String("s11-identity-cfgset"),
			})
		require.NoError(t, err)

		_, err = client.PutEmailIdentityDkimSigningAttributes(
			ctx, &sesv2sdk.PutEmailIdentityDkimSigningAttributesInput{
				EmailIdentity:           aws.String("attrs@example.com"),
				SigningAttributesOrigin: sesv2types.DkimSigningAttributesOriginAwsSes,
			})
		require.NoError(t, err)

		_, err = client.PutEmailIdentityFeedbackAttributes(
			ctx, &sesv2sdk.PutEmailIdentityFeedbackAttributesInput{
				EmailIdentity:          aws.String("attrs@example.com"),
				EmailForwardingEnabled: true,
			})
		require.NoError(t, err)

		getOut, err := client.GetEmailIdentity(ctx, &sesv2sdk.GetEmailIdentityInput{
			EmailIdentity: aws.String("attrs@example.com"),
		})
		require.NoError(t, err)
		assert.Equal(t, "s11-identity-cfgset", aws.ToString(getOut.ConfigurationSetName))
		assert.True(t, getOut.FeedbackForwardingStatus)

		_, err = client.DeleteEmailIdentity(ctx, &sesv2sdk.DeleteEmailIdentityInput{
			EmailIdentity: aws.String("attrs@example.com"),
		})
		require.NoError(t, err)

		_, err = client.GetEmailIdentity(ctx, &sesv2sdk.GetEmailIdentityInput{
			EmailIdentity: aws.String("attrs@example.com"),
		})
		require.Error(t, err, "GetEmailIdentity after delete must fail")
	})

	t.Run("email templates", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.CreateEmailTemplate(ctx, &sesv2sdk.CreateEmailTemplateInput{
			TemplateName: aws.String("s11-template"),
			TemplateContent: &sesv2types.EmailTemplateContent{
				Subject: aws.String("Hi {{name}}"),
				Text:    aws.String("Hello {{name}}"),
			},
		})
		require.NoError(t, err)

		listOut, err := client.ListEmailTemplates(ctx, &sesv2sdk.ListEmailTemplatesInput{})
		require.NoError(t, err)
		require.Len(t, listOut.TemplatesMetadata, 1)
		assert.Equal(t, "s11-template", aws.ToString(listOut.TemplatesMetadata[0].TemplateName))

		renderOut, err := client.TestRenderEmailTemplate(ctx, &sesv2sdk.TestRenderEmailTemplateInput{
			TemplateName: aws.String("s11-template"),
			TemplateData: aws.String(`{"name":"World"}`),
		})
		require.NoError(t, err)
		assert.Contains(t, aws.ToString(renderOut.RenderedTemplate), "Hello World")

		_, err = client.UpdateEmailTemplate(ctx, &sesv2sdk.UpdateEmailTemplateInput{
			TemplateName: aws.String("s11-template"),
			TemplateContent: &sesv2types.EmailTemplateContent{
				Subject: aws.String("Updated {{name}}"),
				Text:    aws.String("Updated hello {{name}}"),
			},
		})
		require.NoError(t, err)

		getOut, err := client.GetEmailTemplate(ctx, &sesv2sdk.GetEmailTemplateInput{
			TemplateName: aws.String("s11-template"),
		})
		require.NoError(t, err)
		require.NotNil(t, getOut.TemplateContent)
		assert.Equal(t, "Updated {{name}}", aws.ToString(getOut.TemplateContent.Subject))

		_, err = client.DeleteEmailTemplate(ctx, &sesv2sdk.DeleteEmailTemplateInput{
			TemplateName: aws.String("s11-template"),
		})
		require.NoError(t, err)

		afterList, err := client.ListEmailTemplates(ctx, &sesv2sdk.ListEmailTemplatesInput{})
		require.NoError(t, err)
		assert.Empty(t, afterList.TemplatesMetadata)
	})

	t.Run("contacts and contact lists", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.CreateContactList(ctx, &sesv2sdk.CreateContactListInput{
			ContactListName: aws.String("s11-list"),
		})
		require.NoError(t, err)

		listOut, err := client.ListContactLists(ctx, &sesv2sdk.ListContactListsInput{})
		require.NoError(t, err)
		require.Len(t, listOut.ContactLists, 1)

		_, err = client.UpdateContactList(ctx, &sesv2sdk.UpdateContactListInput{
			ContactListName: aws.String("s11-list"),
			Description:     aws.String("updated description"),
		})
		require.NoError(t, err)

		getListOut, err := client.GetContactList(ctx, &sesv2sdk.GetContactListInput{
			ContactListName: aws.String("s11-list"),
		})
		require.NoError(t, err)
		assert.Equal(t, "updated description", aws.ToString(getListOut.Description))

		_, err = client.CreateContact(ctx, &sesv2sdk.CreateContactInput{
			ContactListName: aws.String("s11-list"),
			EmailAddress:    aws.String("contact@example.com"),
		})
		require.NoError(t, err)

		getOut, err := client.GetContact(ctx, &sesv2sdk.GetContactInput{
			ContactListName: aws.String("s11-list"),
			EmailAddress:    aws.String("contact@example.com"),
		})
		require.NoError(t, err)
		assert.Equal(t, "contact@example.com", aws.ToString(getOut.EmailAddress))

		_, err = client.UpdateContact(ctx, &sesv2sdk.UpdateContactInput{
			ContactListName: aws.String("s11-list"),
			EmailAddress:    aws.String("contact@example.com"),
			UnsubscribeAll:  true,
		})
		require.NoError(t, err)

		afterUpdateOut, err := client.GetContact(ctx, &sesv2sdk.GetContactInput{
			ContactListName: aws.String("s11-list"),
			EmailAddress:    aws.String("contact@example.com"),
		})
		require.NoError(t, err)
		assert.True(t, afterUpdateOut.UnsubscribeAll)

		_, err = client.DeleteContact(ctx, &sesv2sdk.DeleteContactInput{
			ContactListName: aws.String("s11-list"),
			EmailAddress:    aws.String("contact@example.com"),
		})
		require.NoError(t, err)

		_, err = client.GetContact(ctx, &sesv2sdk.GetContactInput{
			ContactListName: aws.String("s11-list"),
			EmailAddress:    aws.String("contact@example.com"),
		})
		require.Error(t, err, "GetContact after delete must fail")

		_, err = client.DeleteContactList(ctx, &sesv2sdk.DeleteContactListInput{
			ContactListName: aws.String("s11-list"),
		})
		require.NoError(t, err)

		afterDeleteListOut, err := client.ListContactLists(ctx, &sesv2sdk.ListContactListsInput{})
		require.NoError(t, err)
		assert.Empty(t, afterDeleteListOut.ContactLists)
	})

	t.Run("export jobs", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		createOut, err := client.CreateExportJob(ctx, &sesv2sdk.CreateExportJobInput{
			ExportDataSource: &sesv2types.ExportDataSource{
				MetricsDataSource: &sesv2types.MetricsDataSource{
					Dimensions: map[string][]string{"CONFIGURATION_SET": {"*"}},
					StartDate:  aws.Time(time.Now().Add(-24 * time.Hour)),
					EndDate:    aws.Time(time.Now()),
					Namespace:  sesv2types.MetricNamespaceVdm,
					Metrics: []sesv2types.ExportMetric{
						{Name: sesv2types.MetricSend, Aggregation: sesv2types.MetricAggregationVolume},
					},
				},
			},
			ExportDestination: &sesv2types.ExportDestination{DataFormat: sesv2types.DataFormatCsv},
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(createOut.JobId))

		getOut, err := client.GetExportJob(ctx, &sesv2sdk.GetExportJobInput{JobId: createOut.JobId})
		require.NoError(t, err)
		assert.NotEmpty(t, getOut.JobStatus)

		_, err = client.CancelExportJob(ctx, &sesv2sdk.CancelExportJobInput{JobId: createOut.JobId})
		require.NoError(t, err)

		afterCancel, err := client.GetExportJob(ctx, &sesv2sdk.GetExportJobInput{JobId: createOut.JobId})
		require.NoError(t, err)
		assert.Equal(t, sesv2types.JobStatusCancelled, afterCancel.JobStatus)
	})

	t.Run("import jobs", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		createOut, err := client.CreateImportJob(ctx, &sesv2sdk.CreateImportJobInput{
			ImportDataSource: &sesv2types.ImportDataSource{
				DataFormat: sesv2types.DataFormatCsv,
				S3Url:      aws.String("s3://s11-import-bucket/data.csv"),
			},
			ImportDestination: &sesv2types.ImportDestination{
				SuppressionListDestination: &sesv2types.SuppressionListDestination{
					SuppressionListImportAction: sesv2types.SuppressionListImportActionPut,
				},
			},
		})
		require.NoError(t, err)
		require.NotEmpty(t, aws.ToString(createOut.JobId))

		getOut, err := client.GetImportJob(ctx, &sesv2sdk.GetImportJobInput{JobId: createOut.JobId})
		require.NoError(t, err)
		require.NotNil(t, getOut.ImportDestination)
		require.NotNil(t, getOut.ImportDestination.SuppressionListDestination)
		assert.Equal(t, sesv2types.SuppressionListImportActionPut,
			getOut.ImportDestination.SuppressionListDestination.SuppressionListImportAction)
	})

	t.Run("dedicated ip pools and account warmup", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.CreateDedicatedIpPool(ctx, &sesv2sdk.CreateDedicatedIpPoolInput{
			PoolName:    aws.String("s11-pool"),
			ScalingMode: sesv2types.ScalingModeStandard,
		})
		require.NoError(t, err)

		getOut, err := client.GetDedicatedIpPool(ctx, &sesv2sdk.GetDedicatedIpPoolInput{
			PoolName: aws.String("s11-pool"),
		})
		require.NoError(t, err)
		require.NotNil(t, getOut.DedicatedIpPool)
		assert.Equal(t, sesv2types.ScalingModeStandard, getOut.DedicatedIpPool.ScalingMode)

		listOut, err := client.ListDedicatedIpPools(ctx, &sesv2sdk.ListDedicatedIpPoolsInput{})
		require.NoError(t, err)
		assert.Contains(t, listOut.DedicatedIpPools, "s11-pool")

		_, err = client.PutDedicatedIpPoolScalingAttributes(ctx, &sesv2sdk.PutDedicatedIpPoolScalingAttributesInput{
			PoolName:    aws.String("s11-pool"),
			ScalingMode: sesv2types.ScalingModeManaged,
		})
		require.NoError(t, err)

		afterOut, err := client.GetDedicatedIpPool(ctx, &sesv2sdk.GetDedicatedIpPoolInput{
			PoolName: aws.String("s11-pool"),
		})
		require.NoError(t, err)
		require.NotNil(t, afterOut.DedicatedIpPool)
		assert.Equal(t, sesv2types.ScalingModeManaged, afterOut.DedicatedIpPool.ScalingMode)

		_, err = client.PutAccountDedicatedIpWarmupAttributes(ctx, &sesv2sdk.PutAccountDedicatedIpWarmupAttributesInput{
			AutoWarmupEnabled: true,
		})
		require.NoError(t, err)
	})

	t.Run("account sending, suppression and vdm attributes", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.PutAccountSendingAttributes(ctx, &sesv2sdk.PutAccountSendingAttributesInput{
			SendingEnabled: false,
		})
		require.NoError(t, err)

		_, err = client.PutAccountSuppressionAttributes(ctx, &sesv2sdk.PutAccountSuppressionAttributesInput{
			SuppressedReasons: []sesv2types.SuppressionListReason{sesv2types.SuppressionListReasonComplaint},
		})
		require.NoError(t, err)

		_, err = client.PutAccountVdmAttributes(ctx, &sesv2sdk.PutAccountVdmAttributesInput{
			VdmAttributes: &sesv2types.VdmAttributes{VdmEnabled: sesv2types.FeatureStatusEnabled},
		})
		require.NoError(t, err)

		getOut, err := client.GetAccount(ctx, &sesv2sdk.GetAccountInput{})
		require.NoError(t, err)
		assert.False(t, getOut.SendingEnabled)
		require.NotNil(t, getOut.SuppressionAttributes)
		assert.Equal(t, []sesv2types.SuppressionListReason{sesv2types.SuppressionListReasonComplaint},
			getOut.SuppressionAttributes.SuppressedReasons)
		require.NotNil(t, getOut.VdmAttributes)
		assert.Equal(t, sesv2types.FeatureStatusEnabled, getOut.VdmAttributes.VdmEnabled)
	})

	t.Run("suppressed destinations", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.PutSuppressedDestination(ctx, &sesv2sdk.PutSuppressedDestinationInput{
			EmailAddress: aws.String("suppressed@example.com"),
			Reason:       sesv2types.SuppressionListReasonBounce,
		})
		require.NoError(t, err)

		getOut, err := client.GetSuppressedDestination(ctx, &sesv2sdk.GetSuppressedDestinationInput{
			EmailAddress: aws.String("suppressed@example.com"),
		})
		require.NoError(t, err)
		require.NotNil(t, getOut.SuppressedDestination)
		assert.Equal(t, sesv2types.SuppressionListReasonBounce, getOut.SuppressedDestination.Reason)

		_, err = client.DeleteSuppressedDestination(ctx, &sesv2sdk.DeleteSuppressedDestinationInput{
			EmailAddress: aws.String("suppressed@example.com"),
		})
		require.NoError(t, err)

		_, err = client.GetSuppressedDestination(ctx, &sesv2sdk.GetSuppressedDestinationInput{
			EmailAddress: aws.String("suppressed@example.com"),
		})
		require.Error(t, err, "GetSuppressedDestination after delete must fail")
	})

	t.Run("multi region endpoint delete", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.CreateMultiRegionEndpoint(ctx, &sesv2sdk.CreateMultiRegionEndpointInput{
			EndpointName: aws.String("s11-mre"),
			Details: &sesv2types.Details{
				RoutesDetails: []sesv2types.RouteDetails{{Region: aws.String("us-west-2")}},
			},
		})
		require.NoError(t, err)

		_, err = client.DeleteMultiRegionEndpoint(ctx, &sesv2sdk.DeleteMultiRegionEndpointInput{
			EndpointName: aws.String("s11-mre"),
		})
		require.NoError(t, err)

		_, err = client.GetMultiRegionEndpoint(ctx, &sesv2sdk.GetMultiRegionEndpointInput{
			EndpointName: aws.String("s11-mre"),
		})
		require.Error(t, err, "GetMultiRegionEndpoint after delete must fail")
	})

	t.Run("tenants and tenant resource associations", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.CreateTenant(ctx, &sesv2sdk.CreateTenantInput{
			TenantName: aws.String("s11-tenant"),
		})
		require.NoError(t, err)

		_, err = client.CreateConfigurationSet(ctx, &sesv2sdk.CreateConfigurationSetInput{
			ConfigurationSetName: aws.String("s11-tenant-cfgset"),
		})
		require.NoError(t, err)

		resourceArn := "arn:aws:ses:us-east-1:000000000000:configuration-set/s11-tenant-cfgset"
		_, err = client.CreateTenantResourceAssociation(ctx, &sesv2sdk.CreateTenantResourceAssociationInput{
			TenantName:  aws.String("s11-tenant"),
			ResourceArn: aws.String(resourceArn),
		})
		require.NoError(t, err)

		listResourcesOut, err := client.ListTenantResources(ctx, &sesv2sdk.ListTenantResourcesInput{
			TenantName: aws.String("s11-tenant"),
		})
		require.NoError(t, err)
		require.Len(t, listResourcesOut.TenantResources, 1)
		assert.Equal(t, resourceArn, aws.ToString(listResourcesOut.TenantResources[0].ResourceArn))

		listTenantsOut, err := client.ListResourceTenants(ctx, &sesv2sdk.ListResourceTenantsInput{
			ResourceArn: aws.String(resourceArn),
		})
		require.NoError(t, err)
		require.Len(t, listTenantsOut.ResourceTenants, 1)
		assert.Equal(t, "s11-tenant", aws.ToString(listTenantsOut.ResourceTenants[0].TenantName))

		_, err = client.DeleteTenantResourceAssociation(ctx, &sesv2sdk.DeleteTenantResourceAssociationInput{
			TenantName:  aws.String("s11-tenant"),
			ResourceArn: aws.String(resourceArn),
		})
		require.NoError(t, err)

		afterOut, err := client.ListTenantResources(ctx, &sesv2sdk.ListTenantResourcesInput{
			TenantName: aws.String("s11-tenant"),
		})
		require.NoError(t, err)
		assert.Empty(t, afterOut.TenantResources)

		_, err = client.DeleteTenant(ctx, &sesv2sdk.DeleteTenantInput{TenantName: aws.String("s11-tenant")})
		require.NoError(t, err)

		_, err = client.GetTenant(ctx, &sesv2sdk.GetTenantInput{TenantName: aws.String("s11-tenant")})
		require.Error(t, err, "GetTenant after delete must fail")
	})

	t.Run("email address insights, message insights and tags", func(t *testing.T) {
		t.Parallel()

		h, _ := newSESv2TestHandler(t)
		client := newSESv2SDKClient(t, h)
		ctx := t.Context()

		_, err := client.CreateEmailIdentity(ctx, &sesv2sdk.CreateEmailIdentityInput{
			EmailIdentity: aws.String("insights@example.com"),
			Tags:          []sesv2types.Tag{{Key: aws.String("team"), Value: aws.String("growth")}},
		})
		require.NoError(t, err)

		insightsOut, err := client.GetEmailAddressInsights(ctx, &sesv2sdk.GetEmailAddressInsightsInput{
			EmailAddress: aws.String("insights@example.com"),
		})
		require.NoError(t, err)
		require.NotNil(t, insightsOut.MailboxValidation, "GetEmailAddressInsights must return a validation result")

		sendOut, err := client.SendEmail(ctx, &sesv2sdk.SendEmailInput{
			FromEmailAddress: aws.String("insights@example.com"),
			Destination:      &sesv2types.Destination{ToAddresses: []string{"rcpt@example.com"}},
			Content: &sesv2types.EmailContent{
				Simple: &sesv2types.Message{
					Subject: &sesv2types.Content{Data: aws.String("s")},
					Body:    &sesv2types.Body{Text: &sesv2types.Content{Data: aws.String("b")}},
				},
			},
		})
		require.NoError(t, err)

		msgOut, err := client.GetMessageInsights(ctx, &sesv2sdk.GetMessageInsightsInput{
			MessageId: sendOut.MessageId,
		})
		require.NoError(t, err)
		assert.Equal(t, aws.ToString(sendOut.MessageId), aws.ToString(msgOut.MessageId))

		tagsOut, err := client.ListTagsForResource(ctx, &sesv2sdk.ListTagsForResourceInput{
			ResourceArn: aws.String("arn:aws:ses:us-east-1:000000000000:identity/insights@example.com"),
		})
		require.NoError(t, err)
		require.Len(t, tagsOut.Tags, 1)
		assert.Equal(t, "team", aws.ToString(tagsOut.Tags[0].Key))
	})
}
