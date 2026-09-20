package terraform_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsvc "github.com/aws/aws-sdk-go-v2/service/iot"
	sessvc "github.com/aws/aws-sdk-go-v2/service/ses"
	sestypes "github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch22 provisions IoT (thing/thing type/thing group +
// membership, certificate + principal attachment, CA certificate, policy +
// attachment, role alias, logging options, billing group, indexing
// configuration, event configurations, authorizer, provisioning template,
// topic rule + destination, domain configuration) and SES (domain identity +
// verification + DKIM + mail-from, configuration set + event destination,
// identity notification topic, identity policy, receipt rule set + active
// rule set + rule, receipt filter, template) resources via Terraform and
// verifies each through its own SDK client's Get/Describe path.
func TestTerraform_MegaBatch22(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-22",
			setup: func(t *testing.T, dir string) map[string]any {
				t.Helper()

				functionZip := filepath.Join(dir, "mega-batch-22-authorizer.zip")
				writeZipFixture(t, functionZip, "index.py",
					"def handler(event, context):\n    return {'isAuthenticated': True}\n")

				return map[string]any{
					"FunctionZip": functionZip,
				}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch22IoT(ctx, t)
				verifyMegaBatch22SES(ctx, t)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}

func verifyMegaBatch22IoT(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := iotsvc.NewFromConfig(cfg, func(o *iotsvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	thingOut, err := client.DescribeThing(ctx, &iotsvc.DescribeThingInput{
		ThingName: aws.String("mega-batch-22-thing"),
	})
	require.NoError(t, err, "DescribeThing should succeed")
	assert.Equal(t, "mega-batch-22-thing-type", aws.ToString(thingOut.ThingTypeName))

	typeOut, err := client.DescribeThingType(ctx, &iotsvc.DescribeThingTypeInput{
		ThingTypeName: aws.String("mega-batch-22-thing-type"),
	})
	require.NoError(t, err, "DescribeThingType should succeed")
	assert.Equal(t, "mega-batch-22-thing-type", aws.ToString(typeOut.ThingTypeName))

	groupOut, err := client.DescribeThingGroup(ctx, &iotsvc.DescribeThingGroupInput{
		ThingGroupName: aws.String("mega-batch-22-thing-group"),
	})
	require.NoError(t, err, "DescribeThingGroup should succeed")
	assert.Equal(t, "mega-batch-22-thing-group", aws.ToString(groupOut.ThingGroupName))

	groupsForThing, err := client.ListThingGroupsForThing(ctx, &iotsvc.ListThingGroupsForThingInput{
		ThingName: aws.String("mega-batch-22-thing"),
	})
	require.NoError(t, err, "ListThingGroupsForThing should succeed")
	require.Len(t, groupsForThing.ThingGroups, 1, "thing group membership should be recorded")

	principals, err := client.ListThingPrincipals(ctx, &iotsvc.ListThingPrincipalsInput{
		ThingName: aws.String("mega-batch-22-thing"),
	})
	require.NoError(t, err, "ListThingPrincipals should succeed")
	require.Len(t, principals.Principals, 1, "certificate should be attached to the thing")

	certARN := principals.Principals[0]
	certID := certARN[strings.LastIndex(certARN, "/")+1:]

	certOut, err := client.DescribeCertificate(ctx, &iotsvc.DescribeCertificateInput{
		CertificateId: aws.String(certID),
	})
	require.NoError(t, err, "DescribeCertificate should succeed")
	assert.Equal(t, "ACTIVE", string(certOut.CertificateDescription.Status))

	caOut, err := client.ListCACertificates(ctx, &iotsvc.ListCACertificatesInput{})
	require.NoError(t, err, "ListCACertificates should succeed")
	require.Len(t, caOut.Certificates, 1, "CA certificate should be registered")

	describeCAOut, err := client.DescribeCACertificate(ctx, &iotsvc.DescribeCACertificateInput{
		CertificateId: caOut.Certificates[0].CertificateId,
	})
	require.NoError(t, err, "DescribeCACertificate should succeed")
	require.NotNil(t, describeCAOut.CertificateDescription)
	assert.Equal(t, "ACTIVE", string(describeCAOut.CertificateDescription.Status))

	policyOut, err := client.GetPolicy(ctx, &iotsvc.GetPolicyInput{
		PolicyName: aws.String("mega-batch-22-policy"),
	})
	require.NoError(t, err, "GetPolicy should succeed")
	assert.Equal(t, "mega-batch-22-policy", aws.ToString(policyOut.PolicyName))

	targetsOut, err := client.ListTargetsForPolicy(ctx, &iotsvc.ListTargetsForPolicyInput{
		PolicyName: aws.String("mega-batch-22-policy"),
	})
	require.NoError(t, err, "ListTargetsForPolicy should succeed")
	assert.Contains(t, targetsOut.Targets, certARN)

	aliasOut, err := client.DescribeRoleAlias(ctx, &iotsvc.DescribeRoleAliasInput{
		RoleAlias: aws.String("mega-batch-22-role-alias"),
	})
	require.NoError(t, err, "DescribeRoleAlias should succeed")
	assert.Equal(t, "mega-batch-22-role-alias", aws.ToString(aliasOut.RoleAliasDescription.RoleAlias))

	loggingOut, err := client.GetV2LoggingOptions(ctx, &iotsvc.GetV2LoggingOptionsInput{})
	require.NoError(t, err, "GetV2LoggingOptions should succeed")
	assert.Equal(t, "WARN", string(loggingOut.DefaultLogLevel))

	billingOut, err := client.DescribeBillingGroup(ctx, &iotsvc.DescribeBillingGroupInput{
		BillingGroupName: aws.String("mega-batch-22-billing-group"),
	})
	require.NoError(t, err, "DescribeBillingGroup should succeed")
	assert.Equal(t, "mega-batch-22-billing-group", aws.ToString(billingOut.BillingGroupName))

	indexOut, err := client.GetIndexingConfiguration(ctx, &iotsvc.GetIndexingConfigurationInput{})
	require.NoError(t, err, "GetIndexingConfiguration should succeed")
	require.NotNil(t, indexOut.ThingIndexingConfiguration)
	assert.Equal(t, "REGISTRY", string(indexOut.ThingIndexingConfiguration.ThingIndexingMode))

	eventsOut, err := client.DescribeEventConfigurations(ctx, &iotsvc.DescribeEventConfigurationsInput{})
	require.NoError(t, err, "DescribeEventConfigurations should succeed")
	assert.True(t, eventsOut.EventConfigurations["THING"].Enabled)
	assert.True(t, eventsOut.EventConfigurations["CERTIFICATE"].Enabled)

	authOut, err := client.DescribeAuthorizer(ctx, &iotsvc.DescribeAuthorizerInput{
		AuthorizerName: aws.String("mega-batch-22-authorizer"),
	})
	require.NoError(t, err, "DescribeAuthorizer should succeed")
	assert.Equal(t, "ACTIVE", string(authOut.AuthorizerDescription.Status))

	templateOut, err := client.DescribeProvisioningTemplate(ctx, &iotsvc.DescribeProvisioningTemplateInput{
		TemplateName: aws.String("mega-batch-22-provisioning-template"),
	})
	require.NoError(t, err, "DescribeProvisioningTemplate should succeed")
	assert.True(t, aws.ToBool(templateOut.Enabled))

	ruleOut, err := client.GetTopicRule(ctx, &iotsvc.GetTopicRuleInput{
		RuleName: aws.String("mega_batch_22_rule"),
	})
	require.NoError(t, err, "GetTopicRule should succeed")
	require.NotNil(t, ruleOut.Rule)
	require.Len(t, ruleOut.Rule.Actions, 1)
	require.NotNil(t, ruleOut.Rule.Actions[0].Sns)

	destOut, err := client.ListTopicRuleDestinations(ctx, &iotsvc.ListTopicRuleDestinationsInput{})
	require.NoError(t, err, "ListTopicRuleDestinations should succeed")
	require.Len(t, destOut.DestinationSummaries, 1)
	require.NotNil(t, destOut.DestinationSummaries[0].VpcDestinationSummary)

	domainOut, err := client.DescribeDomainConfiguration(ctx, &iotsvc.DescribeDomainConfigurationInput{
		DomainConfigurationName: aws.String("mega-batch-22-domain-config"),
	})
	require.NoError(t, err, "DescribeDomainConfiguration should succeed")
	assert.Equal(t, "DATA", string(domainOut.ServiceType))
}

func verifyMegaBatch22SES(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := sessvc.NewFromConfig(cfg, func(o *sessvc.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	domain := "mega-batch-22.example.com"

	verifyOut, err := client.GetIdentityVerificationAttributes(ctx, &sessvc.GetIdentityVerificationAttributesInput{
		Identities: []string{domain},
	})
	require.NoError(t, err, "GetIdentityVerificationAttributes should succeed")
	require.Contains(t, verifyOut.VerificationAttributes, domain)
	assert.Equal(t, sestypes.VerificationStatusSuccess, verifyOut.VerificationAttributes[domain].VerificationStatus)

	dkimOut, err := client.GetIdentityDkimAttributes(ctx, &sessvc.GetIdentityDkimAttributesInput{
		Identities: []string{domain},
	})
	require.NoError(t, err, "GetIdentityDkimAttributes should succeed")
	require.Contains(t, dkimOut.DkimAttributes, domain)
	assert.True(t, dkimOut.DkimAttributes[domain].DkimEnabled)

	mailFromOut, err := client.GetIdentityMailFromDomainAttributes(
		ctx,
		&sessvc.GetIdentityMailFromDomainAttributesInput{
			Identities: []string{domain},
		},
	)
	require.NoError(t, err, "GetIdentityMailFromDomainAttributes should succeed")
	require.Contains(t, mailFromOut.MailFromDomainAttributes, domain)
	assert.Equal(
		t,
		"bounce.mega-batch-22.example.com",
		aws.ToString(mailFromOut.MailFromDomainAttributes[domain].MailFromDomain),
	)

	configOut, err := client.DescribeConfigurationSet(ctx, &sessvc.DescribeConfigurationSetInput{
		ConfigurationSetName: aws.String("mega-batch-22-config-set"),
		ConfigurationSetAttributeNames: []sestypes.ConfigurationSetAttribute{
			sestypes.ConfigurationSetAttributeEventDestinations,
		},
	})
	require.NoError(t, err, "DescribeConfigurationSet should succeed")
	require.Len(t, configOut.EventDestinations, 1)
	assert.Equal(t, "mega-batch-22-event-dest", aws.ToString(configOut.EventDestinations[0].Name))

	notifOut, err := client.GetIdentityNotificationAttributes(ctx, &sessvc.GetIdentityNotificationAttributesInput{
		Identities: []string{domain},
	})
	require.NoError(t, err, "GetIdentityNotificationAttributes should succeed")
	require.Contains(t, notifOut.NotificationAttributes, domain)
	assert.NotEmpty(t, aws.ToString(notifOut.NotificationAttributes[domain].BounceTopic))

	policiesOut, err := client.GetIdentityPolicies(ctx, &sessvc.GetIdentityPoliciesInput{
		Identity:    aws.String(domain),
		PolicyNames: []string{"mega-batch-22-identity-policy"},
	})
	require.NoError(t, err, "GetIdentityPolicies should succeed")
	assert.Contains(t, policiesOut.Policies, "mega-batch-22-identity-policy")

	activeOut, err := client.DescribeActiveReceiptRuleSet(ctx, &sessvc.DescribeActiveReceiptRuleSetInput{})
	require.NoError(t, err, "DescribeActiveReceiptRuleSet should succeed")
	require.NotNil(t, activeOut.Metadata)
	assert.Equal(t, "mega-batch-22-rule-set", aws.ToString(activeOut.Metadata.Name))

	ruleOut, err := client.DescribeReceiptRule(ctx, &sessvc.DescribeReceiptRuleInput{
		RuleSetName: aws.String("mega-batch-22-rule-set"),
		RuleName:    aws.String("mega-batch-22-receipt-rule"),
	})
	require.NoError(t, err, "DescribeReceiptRule should succeed")
	require.NotNil(t, ruleOut.Rule)
	require.Len(t, ruleOut.Rule.Actions, 2)

	filtersOut, err := client.ListReceiptFilters(ctx, &sessvc.ListReceiptFiltersInput{})
	require.NoError(t, err, "ListReceiptFilters should succeed")

	var foundFilter bool

	for _, f := range filtersOut.Filters {
		if aws.ToString(f.Name) == "mega-batch-22-receipt-filter" {
			foundFilter = true
		}
	}

	assert.True(t, foundFilter, "receipt filter should be listed")

	templateOut, err := client.GetTemplate(ctx, &sessvc.GetTemplateInput{
		TemplateName: aws.String("mega-batch-22-template"),
	})
	require.NoError(t, err, "GetTemplate should succeed")
	assert.Equal(t, "Mega Batch 22", aws.ToString(templateOut.Template.SubjectPart))
}
