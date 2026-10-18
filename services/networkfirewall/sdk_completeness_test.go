package networkfirewall_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	nf "github.com/blackbirdworks/gopherstack/services/networkfirewall"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := nf.NewHandler(nf.NewInMemoryBackend(testAccount, testRegion))
	notImplemented := []string{
		"AcceptNetworkFirewallTransitGatewayAttachment",
		"AssociateAvailabilityZones",
		"AttachRuleGroupsToProxyConfiguration",
		"CreateContainerAssociation",
		"CreateProxy",
		"CreateProxyConfiguration",
		"CreateProxyRuleGroup",
		"CreateProxyRules",
		"CreateVpcEndpointAssociation",
		"DeleteContainerAssociation",
		"DeleteNetworkFirewallTransitGatewayAttachment",
		"DeleteProxy",
		"DeleteProxyConfiguration",
		"DeleteProxyRuleGroup",
		"DeleteProxyRules",
		"DeleteVpcEndpointAssociation",
		"DescribeContainerAssociation",
		"DescribeFlowOperation",
		"DescribeProxy",
		"DescribeProxyConfiguration",
		"DescribeProxyRule",
		"DescribeProxyRuleGroup",
		"DescribeRuleGroupSummary",
		"DescribeVpcEndpointAssociation",
		"DetachRuleGroupsFromProxyConfiguration",
		"DisassociateAvailabilityZones",
		"GetAnalysisReportResults",
		"ListAnalysisReports",
		"ListContainerAssociations",
		"ListFlowOperationResults",
		"ListFlowOperations",
		"ListProxies",
		"ListProxyConfigurations",
		"ListProxyRuleGroups",
		"ListVpcEndpointAssociations",
		"RejectNetworkFirewallTransitGatewayAttachment",
		"StartAnalysisReport",
		"StartFlowCapture",
		"StartFlowFlush",
		"UpdateAvailabilityZoneChangeProtection",
		"UpdateContainerAssociation",
		"UpdateFirewallAnalysisSettings",
		"UpdateProxy",
		"UpdateProxyConfiguration",
		"UpdateProxyRule",
		"UpdateProxyRuleGroupPriorities",
		"UpdateProxyRulePriorities",
		"UpdateProxySettings",
	}
	sdkcheck.CheckCompleteness(t, &sdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
