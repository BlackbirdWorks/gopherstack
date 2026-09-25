package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2svc "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	pinpointsvc "github.com/aws/aws-sdk-go-v2/service/pinpoint"
	route53resolversvc "github.com/aws/aws-sdk-go-v2/service/route53resolver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_PinpointAndRoute53resolver provisions Pinpoint (app, ADM/APNS/APNS sandbox/APNS
// VoIP/APNS VoIP sandbox/Baidu/email/GCM/SMS channels, email template, event
// stream) and Route53Resolver (config, DNSSEC config, firewall config, firewall
// domain list, firewall rule + rule group + rule group association, query log
// config + association, rule + rule association) resources via Terraform and
// verifies each through its own SDK client's Get/Describe path.
func TestTerraform_PinpointAndRoute53resolver(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "pinpoint-and-route53resolver",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyPinpointAndRoute53resolverPinpoint(ctx, t)
				verifyPinpointAndRoute53resolverRoute53Resolver(ctx, t)
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

func verifyPinpointAndRoute53resolverPinpoint(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createPinpointClient(t)

	appsOut, err := client.GetApps(ctx, &pinpointsvc.GetAppsInput{})
	require.NoError(t, err, "GetApps should succeed")

	appID := ""

	for _, app := range appsOut.ApplicationsResponse.Item {
		if aws.ToString(app.Name) == "pnrr-app" {
			appID = aws.ToString(app.Id)
		}
	}

	require.NotEmpty(t, appID, "pnrr-app not found in GetApps")

	admOut, err := client.GetAdmChannel(ctx, &pinpointsvc.GetAdmChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err, "GetAdmChannel should succeed")
	assert.True(t, aws.ToBool(admOut.ADMChannelResponse.Enabled))

	apnsOut, err := client.GetApnsChannel(ctx, &pinpointsvc.GetApnsChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err, "GetApnsChannel should succeed")
	assert.True(t, aws.ToBool(apnsOut.APNSChannelResponse.Enabled))

	apnsSandboxOut, err := client.GetApnsSandboxChannel(ctx,
		&pinpointsvc.GetApnsSandboxChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err, "GetApnsSandboxChannel should succeed")
	assert.True(t, aws.ToBool(apnsSandboxOut.APNSSandboxChannelResponse.Enabled))

	apnsVoipOut, err := client.GetApnsVoipChannel(ctx,
		&pinpointsvc.GetApnsVoipChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err, "GetApnsVoipChannel should succeed")
	assert.True(t, aws.ToBool(apnsVoipOut.APNSVoipChannelResponse.Enabled))

	apnsVoipSandboxOut, err := client.GetApnsVoipSandboxChannel(ctx,
		&pinpointsvc.GetApnsVoipSandboxChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err, "GetApnsVoipSandboxChannel should succeed")
	assert.True(t, aws.ToBool(apnsVoipSandboxOut.APNSVoipSandboxChannelResponse.Enabled))

	baiduOut, err := client.GetBaiduChannel(ctx, &pinpointsvc.GetBaiduChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err, "GetBaiduChannel should succeed")
	assert.True(t, aws.ToBool(baiduOut.BaiduChannelResponse.Enabled))
	assert.Equal(t, "pnrr-baidu-api-key", aws.ToString(baiduOut.BaiduChannelResponse.Credential))

	emailOut, err := client.GetEmailChannel(ctx, &pinpointsvc.GetEmailChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err, "GetEmailChannel should succeed")
	assert.Equal(t, "test@pnrr.example.com", aws.ToString(emailOut.EmailChannelResponse.FromAddress))
	assert.Contains(t, aws.ToString(emailOut.EmailChannelResponse.Identity), "pnrr.example.com")

	templateOut, err := client.GetEmailTemplate(ctx, &pinpointsvc.GetEmailTemplateInput{
		TemplateName: aws.String("pnrr-email-template"),
	})
	require.NoError(t, err, "GetEmailTemplate should succeed")
	assert.Equal(t, "pnrr-email-template", aws.ToString(templateOut.EmailTemplateResponse.TemplateName))

	eventStreamOut, err := client.GetEventStream(
		ctx,
		&pinpointsvc.GetEventStreamInput{ApplicationId: aws.String(appID)},
	)
	require.NoError(t, err, "GetEventStream should succeed")
	assert.Contains(t, aws.ToString(eventStreamOut.EventStream.DestinationStreamArn), "pnrr-event-stream")

	gcmOut, err := client.GetGcmChannel(ctx, &pinpointsvc.GetGcmChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err, "GetGcmChannel should succeed")
	assert.Equal(t, "pnrr-gcm-api-key", aws.ToString(gcmOut.GCMChannelResponse.Credential))

	smsOut, err := client.GetSmsChannel(ctx, &pinpointsvc.GetSmsChannelInput{ApplicationId: aws.String(appID)})
	require.NoError(t, err, "GetSmsChannel should succeed")
	assert.True(t, aws.ToBool(smsOut.SMSChannelResponse.Enabled))
	assert.Equal(t, "MEGA28", aws.ToString(smsOut.SMSChannelResponse.SenderId))
}

func verifyPinpointAndRoute53resolverRoute53Resolver(ctx context.Context, t *testing.T) {
	t.Helper()

	client := createRoute53ResolverClient(t)
	ec2Client := createEC2Client(t)

	vpcsOut, err := ec2Client.DescribeVpcs(ctx, &ec2svc.DescribeVpcsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("cidr-block"), Values: []string{"10.170.0.0/16"}},
		},
	})
	require.NoError(t, err, "DescribeVpcs should succeed")
	require.Len(t, vpcsOut.Vpcs, 1)
	vpcID := aws.ToString(vpcsOut.Vpcs[0].VpcId)

	configOut, err := client.GetResolverConfig(ctx,
		&route53resolversvc.GetResolverConfigInput{ResourceId: aws.String(vpcID)})
	require.NoError(t, err, "GetResolverConfig should succeed")
	assert.Equal(t, "DISABLED", string(configOut.ResolverConfig.AutodefinedReverse))

	dnssecOut, err := client.GetResolverDnssecConfig(ctx,
		&route53resolversvc.GetResolverDnssecConfigInput{ResourceId: aws.String(vpcID)})
	require.NoError(t, err, "GetResolverDnssecConfig should succeed")
	assert.Equal(t, vpcID, aws.ToString(dnssecOut.ResolverDNSSECConfig.ResourceId))

	firewallConfigOut, err := client.GetFirewallConfig(ctx,
		&route53resolversvc.GetFirewallConfigInput{ResourceId: aws.String(vpcID)})
	require.NoError(t, err, "GetFirewallConfig should succeed")
	assert.Equal(t, "ENABLED", string(firewallConfigOut.FirewallConfig.FirewallFailOpen))

	domainListsOut, err := client.ListFirewallDomainLists(ctx, &route53resolversvc.ListFirewallDomainListsInput{})
	require.NoError(t, err, "ListFirewallDomainLists should succeed")

	domainListID := ""

	for _, l := range domainListsOut.FirewallDomainLists {
		if aws.ToString(l.Name) == "pnrr-firewall-domain-list" {
			domainListID = aws.ToString(l.Id)
		}
	}

	require.NotEmpty(t, domainListID, "pnrr-firewall-domain-list not found")

	domainListOut, err := client.GetFirewallDomainList(ctx,
		&route53resolversvc.GetFirewallDomainListInput{FirewallDomainListId: aws.String(domainListID)})
	require.NoError(t, err, "GetFirewallDomainList should succeed")
	assert.Equal(t, "pnrr-firewall-domain-list", aws.ToString(domainListOut.FirewallDomainList.Name))

	ruleGroupsOut, err := client.ListFirewallRuleGroups(ctx, &route53resolversvc.ListFirewallRuleGroupsInput{})
	require.NoError(t, err, "ListFirewallRuleGroups should succeed")

	ruleGroupID := ""

	for _, g := range ruleGroupsOut.FirewallRuleGroups {
		if aws.ToString(g.Name) == "pnrr-firewall-rule-group" {
			ruleGroupID = aws.ToString(g.Id)
		}
	}

	require.NotEmpty(t, ruleGroupID, "pnrr-firewall-rule-group not found")

	rulesOut, err := client.ListFirewallRules(ctx,
		&route53resolversvc.ListFirewallRulesInput{FirewallRuleGroupId: aws.String(ruleGroupID)})
	require.NoError(t, err, "ListFirewallRules should succeed")
	require.Len(t, rulesOut.FirewallRules, 1)
	assert.Equal(t, "pnrr-firewall-rule", aws.ToString(rulesOut.FirewallRules[0].Name))
	assert.Equal(t, "BLOCK", string(rulesOut.FirewallRules[0].Action))

	assocOut, err := client.ListFirewallRuleGroupAssociations(ctx,
		&route53resolversvc.ListFirewallRuleGroupAssociationsInput{})
	require.NoError(t, err, "ListFirewallRuleGroupAssociations should succeed")

	found := false

	for _, a := range assocOut.FirewallRuleGroupAssociations {
		if aws.ToString(a.Name) == "pnrr-firewall-rule-group-association" {
			found = true

			assert.Equal(t, vpcID, aws.ToString(a.VpcId))
		}
	}

	assert.True(t, found, "pnrr-firewall-rule-group-association not found")

	queryLogConfigsOut, err := client.ListResolverQueryLogConfigs(ctx,
		&route53resolversvc.ListResolverQueryLogConfigsInput{})
	require.NoError(t, err, "ListResolverQueryLogConfigs should succeed")

	queryLogConfigID := ""

	for _, c := range queryLogConfigsOut.ResolverQueryLogConfigs {
		if aws.ToString(c.Name) == "pnrr-query-log-config" {
			queryLogConfigID = aws.ToString(c.Id)
		}
	}

	require.NotEmpty(t, queryLogConfigID, "pnrr-query-log-config not found")

	queryLogConfigOut, err := client.GetResolverQueryLogConfig(ctx,
		&route53resolversvc.GetResolverQueryLogConfigInput{ResolverQueryLogConfigId: aws.String(queryLogConfigID)})
	require.NoError(t, err, "GetResolverQueryLogConfig should succeed")
	assert.Contains(
		t,
		aws.ToString(queryLogConfigOut.ResolverQueryLogConfig.DestinationArn),
		"pnrr",
	)

	queryLogAssocsOut, err := client.ListResolverQueryLogConfigAssociations(ctx,
		&route53resolversvc.ListResolverQueryLogConfigAssociationsInput{})
	require.NoError(t, err, "ListResolverQueryLogConfigAssociations should succeed")

	assocFound := false

	for _, a := range queryLogAssocsOut.ResolverQueryLogConfigAssociations {
		if aws.ToString(a.ResourceId) == vpcID && aws.ToString(a.ResolverQueryLogConfigId) == queryLogConfigID {
			assocFound = true
		}
	}

	assert.True(t, assocFound, "resolver query log config association not found")

	rulesListOut, err := client.ListResolverRules(ctx, &route53resolversvc.ListResolverRulesInput{})
	require.NoError(t, err, "ListResolverRules should succeed")

	ruleID := ""

	for _, r := range rulesListOut.ResolverRules {
		if aws.ToString(r.DomainName) == "pnrr.example.com." ||
			aws.ToString(r.DomainName) == "pnrr.example.com" {
			ruleID = aws.ToString(r.Id)
		}
	}

	require.NotEmpty(t, ruleID, "pnrr resolver rule not found")

	ruleAssocsOut, err := client.ListResolverRuleAssociations(
		ctx,
		&route53resolversvc.ListResolverRuleAssociationsInput{},
	)
	require.NoError(t, err, "ListResolverRuleAssociations should succeed")

	ruleAssocFound := false

	for _, a := range ruleAssocsOut.ResolverRuleAssociations {
		if aws.ToString(a.ResolverRuleId) == ruleID && aws.ToString(a.VPCId) == vpcID {
			ruleAssocFound = true
		}
	}

	assert.True(t, ruleAssocFound, "resolver rule association not found")
}
