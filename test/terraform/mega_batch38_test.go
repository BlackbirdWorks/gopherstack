package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsvc38 "github.com/aws/aws-sdk-go-v2/service/opensearch"
	shieldsvc38 "github.com/aws/aws-sdk-go-v2/service/shield"
	shieldtypes38 "github.com/aws/aws-sdk-go-v2/service/shield/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_MegaBatch38 provisions an OpenSearch VPC domain with a
// domain policy, SAML options, a custom package association, an authorized
// VPC endpoint, and an outbound/inbound cross-cluster connection to a
// second domain, plus Shield subscription/protection/protection group,
// DRT role and log-bucket associations, proactive engagement, and an
// application layer automatic response, via Terraform, verifying each
// through its own SDK client.
func TestTerraform_MegaBatch38(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "mega-batch-38",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return nil
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				verifyMegaBatch38OpenSearch(ctx, t)
				verifyMegaBatch38Shield(ctx, t)
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

func verifyMegaBatch38OpenSearch(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := opensearchsvc38.NewFromConfig(cfg, func(o *opensearchsvc38.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	domOut, err := client.DescribeDomain(ctx, &opensearchsvc38.DescribeDomainInput{
		DomainName: aws.String("mb38-domain"),
	})
	require.NoError(t, err, "DescribeDomain should succeed")
	require.NotNil(t, domOut.DomainStatus.VPCOptions, "domain should be VPC-based")
	assert.Contains(t, aws.ToString(domOut.DomainStatus.AccessPolicies), "es:*", "domain policy should be applied")

	cfgOut, err := client.DescribeDomainConfig(ctx, &opensearchsvc38.DescribeDomainConfigInput{
		DomainName: aws.String("mb38-domain"),
	})
	require.NoError(t, err, "DescribeDomainConfig should succeed")
	require.NotNil(t, cfgOut.DomainConfig.AdvancedSecurityOptions)
	require.NotNil(t, cfgOut.DomainConfig.AdvancedSecurityOptions.Options)
	samlOpts := cfgOut.DomainConfig.AdvancedSecurityOptions.Options.SAMLOptions
	require.NotNil(t, samlOpts, "SAML options should be set")
	require.NotNil(t, samlOpts.Idp)
	assert.Contains(t, aws.ToString(samlOpts.Idp.EntityId), "mega-batch-38-idp")

	pkgsOut, err := client.ListPackagesForDomain(ctx, &opensearchsvc38.ListPackagesForDomainInput{
		DomainName: aws.String("mb38-domain"),
	})
	require.NoError(t, err, "ListPackagesForDomain should succeed")
	require.NotEmpty(t, pkgsOut.DomainPackageDetailsList)

	vpcEpsOut, err := client.ListVpcEndpointsForDomain(ctx, &opensearchsvc38.ListVpcEndpointsForDomainInput{
		DomainName: aws.String("mb38-domain"),
	})
	require.NoError(t, err, "ListVpcEndpointsForDomain should succeed")
	require.NotEmpty(t, vpcEpsOut.VpcEndpointSummaryList)

	accessOut, err := client.ListVpcEndpointAccess(ctx, &opensearchsvc38.ListVpcEndpointAccessInput{
		DomainName: aws.String("mb38-domain"),
	})
	require.NoError(t, err, "ListVpcEndpointAccess should succeed")
	assert.NotEmpty(t, accessOut.AuthorizedPrincipalList, "account should be authorized for VPC endpoint access")

	outConnOut, err := client.DescribeOutboundConnections(ctx, &opensearchsvc38.DescribeOutboundConnectionsInput{})
	require.NoError(t, err, "DescribeOutboundConnections should succeed")

	var foundOutbound bool

	for _, c := range outConnOut.Connections {
		if aws.ToString(c.ConnectionAlias) == "mega-batch-38-connection" {
			foundOutbound = true
		}
	}

	assert.True(t, foundOutbound, "outbound connection should be listed")

	inConnOut, err := client.DescribeInboundConnections(ctx, &opensearchsvc38.DescribeInboundConnectionsInput{})
	require.NoError(t, err, "DescribeInboundConnections should succeed")
	assert.NotEmpty(t, inConnOut.Connections, "inbound connection should be listed after accept")
}

func verifyMegaBatch38Shield(ctx context.Context, t *testing.T) {
	t.Helper()
	cfg := megaConfig(t)

	client := shieldsvc38.NewFromConfig(cfg, func(o *shieldsvc38.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})

	subOut, err := client.DescribeSubscription(ctx, &shieldsvc38.DescribeSubscriptionInput{})
	require.NoError(t, err, "DescribeSubscription should succeed")
	require.NotNil(t, subOut.Subscription)
	assert.Equal(
		t,
		shieldtypes38.ProactiveEngagementStatusEnabled,
		subOut.Subscription.ProactiveEngagementStatus,
		"proactive engagement should be enabled",
	)

	contactsOut, err := client.DescribeEmergencyContactSettings(
		ctx, &shieldsvc38.DescribeEmergencyContactSettingsInput{},
	)
	require.NoError(t, err, "DescribeEmergencyContactSettings should succeed")
	require.NotEmpty(t, contactsOut.EmergencyContactList)
	assert.Contains(t, aws.ToString(contactsOut.EmergencyContactList[0].EmailAddress), "mega-batch-38-oncall")

	protsOut, err := client.ListProtections(ctx, &shieldsvc38.ListProtectionsInput{})
	require.NoError(t, err, "ListProtections should succeed")

	var protectionID, resourceArn string

	for _, p := range protsOut.Protections {
		if aws.ToString(p.Name) == "mega-batch-38-protection" {
			protectionID = aws.ToString(p.Id)
			resourceArn = aws.ToString(p.ResourceArn)
		}
	}

	require.NotEmpty(t, protectionID, "mega-batch-38 protection should be listed")

	protOut, err := client.DescribeProtection(ctx, &shieldsvc38.DescribeProtectionInput{
		ProtectionId: aws.String(protectionID),
	})
	require.NoError(t, err, "DescribeProtection should succeed")
	require.NotNil(t, protOut.Protection.ApplicationLayerAutomaticResponseConfiguration,
		"application layer automatic response should be enabled")

	groupsOut, err := client.ListProtectionGroups(ctx, &shieldsvc38.ListProtectionGroupsInput{})
	require.NoError(t, err, "ListProtectionGroups should succeed")

	var foundGroup bool

	for _, g := range groupsOut.ProtectionGroups {
		if aws.ToString(g.ProtectionGroupId) == "mega-batch-38-protection-group" {
			foundGroup = true
			assert.Contains(t, g.Members, resourceArn)
		}
	}

	assert.True(t, foundGroup, "protection group should be listed")

	drtOut, err := client.DescribeDRTAccess(ctx, &shieldsvc38.DescribeDRTAccessInput{})
	require.NoError(t, err, "DescribeDRTAccess should succeed")
	assert.Contains(t, aws.ToString(drtOut.RoleArn), "mega-batch-38-drt-role")
	assert.Contains(t, drtOut.LogBucketList, "mega-batch-38-drt-log-bucket")
}
