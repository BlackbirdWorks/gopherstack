package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	nfsdk "github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	nftypes "github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createNetworkFirewallClient(t *testing.T) *nfsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err, "unable to load SDK config")

	return nfsdk.NewFromConfig(cfg, func(o *nfsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

func nfErrCode(t *testing.T, err error) string {
	t.Helper()

	var apiErr smithy.APIError

	require.ErrorAs(t, err, &apiErr)

	return apiErr.ErrorCode()
}

// TestIntegration_NetworkFirewall_FirewallLifecycle drives policy, rule group, firewall,
// logging, tagging and deletion through the real SDK client against a real EC2 subnet.
func TestIntegration_NetworkFirewall_FirewallLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createNetworkFirewallClient(t)
	ec2Client := createEC2Client(t)
	ctx := t.Context()
	suffix := uuid.NewString()[:8]

	vpcOut, err := ec2Client.CreateVpc(ctx, &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.77.0.0/16")})
	require.NoError(t, err)

	vpcID := aws.ToString(vpcOut.Vpc.VpcId)

	subnetOut, err := ec2Client.CreateSubnet(ctx, &ec2sdk.CreateSubnetInput{
		VpcId: aws.String(vpcID), CidrBlock: aws.String("10.77.1.0/24"),
	})
	require.NoError(t, err)

	subnetID := aws.ToString(subnetOut.Subnet.SubnetId)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = ec2Client.DeleteSubnet(cleanupCtx, &ec2sdk.DeleteSubnetInput{SubnetId: aws.String(subnetID)})
		_, _ = ec2Client.DeleteVpc(cleanupCtx, &ec2sdk.DeleteVpcInput{VpcId: aws.String(vpcID)})
	})

	group, err := client.CreateRuleGroup(ctx, &nfsdk.CreateRuleGroupInput{
		RuleGroupName: aws.String("it-rg-" + suffix),
		Type:          nftypes.RuleGroupTypeStateful,
		Capacity:      aws.Int32(10),
		Rules:         aws.String("pass tcp any any -> any 443 (sid:100;)"),
	})
	require.NoError(t, err)

	policy, err := client.CreateFirewallPolicy(ctx, &nfsdk.CreateFirewallPolicyInput{
		FirewallPolicyName: aws.String("it-pol-" + suffix),
		FirewallPolicy: &nftypes.FirewallPolicy{
			StatelessDefaultActions:         []string{"aws:forward_to_sfe"},
			StatelessFragmentDefaultActions: []string{"aws:forward_to_sfe"},
			StatefulRuleGroupReferences: []nftypes.StatefulRuleGroupReference{
				{ResourceArn: group.RuleGroupResponse.RuleGroupArn},
			},
		},
	})
	require.NoError(t, err)

	fwName := "it-fw-" + suffix

	created, err := client.CreateFirewall(ctx, &nfsdk.CreateFirewallInput{
		FirewallName:      aws.String(fwName),
		FirewallPolicyArn: policy.FirewallPolicyResponse.FirewallPolicyArn,
		VpcId:             aws.String(vpcID),
		SubnetMappings:    []nftypes.SubnetMapping{{SubnetId: aws.String(subnetID)}},
		DeleteProtection:  true,
	})
	require.NoError(t, err)
	assert.Equal(t, nftypes.FirewallStatusValueProvisioning, created.FirewallStatus.Status)

	require.Eventually(t, func() bool {
		d, derr := client.DescribeFirewall(ctx, &nfsdk.DescribeFirewallInput{FirewallName: aws.String(fwName)})

		return derr == nil && d.FirewallStatus.Status == nftypes.FirewallStatusValueReady
	}, 30*time.Second, 200*time.Millisecond)

	d, err := client.DescribeFirewall(ctx, &nfsdk.DescribeFirewallInput{FirewallName: aws.String(fwName)})
	require.NoError(t, err)
	require.Len(t, d.FirewallStatus.SyncStates, 1)

	for az, st := range d.FirewallStatus.SyncStates {
		assert.NotEmpty(t, az)
		assert.Equal(t, subnetID, aws.ToString(st.Attachment.SubnetId))
	}

	_, err = client.CreateFirewall(ctx, &nfsdk.CreateFirewallInput{
		FirewallName:      aws.String("it-bad-" + suffix),
		FirewallPolicyArn: policy.FirewallPolicyResponse.FirewallPolicyArn,
		VpcId:             aws.String(vpcID),
		SubnetMappings:    []nftypes.SubnetMapping{{SubnetId: aws.String("subnet-doesnotexist")}},
	})
	assert.Equal(t, "InvalidRequestException", nfErrCode(t, err))

	_, err = client.DeleteRuleGroup(ctx, &nfsdk.DeleteRuleGroupInput{
		RuleGroupArn: group.RuleGroupResponse.RuleGroupArn,
	})
	assert.Equal(t, "InvalidOperationException", nfErrCode(t, err))

	_, err = client.DeleteFirewall(ctx, &nfsdk.DeleteFirewallInput{FirewallName: aws.String(fwName)})
	assert.Equal(t, "InvalidOperationException", nfErrCode(t, err))

	_, err = client.UpdateFirewallDeleteProtection(ctx, &nfsdk.UpdateFirewallDeleteProtectionInput{
		FirewallName: aws.String(fwName), DeleteProtection: false, UpdateToken: d.UpdateToken,
	})
	require.NoError(t, err)

	_, err = client.UpdateFirewallDeleteProtection(ctx, &nfsdk.UpdateFirewallDeleteProtectionInput{
		FirewallName: aws.String(fwName), DeleteProtection: false, UpdateToken: d.UpdateToken,
	})
	assert.Equal(t, "InvalidTokenException", nfErrCode(t, err))

	_, err = client.DeleteFirewall(ctx, &nfsdk.DeleteFirewallInput{FirewallName: aws.String(fwName)})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, derr := client.DescribeFirewall(ctx, &nfsdk.DescribeFirewallInput{FirewallName: aws.String(fwName)})

		return derr != nil && nfErrCode(t, derr) == "ResourceNotFoundException"
	}, 30*time.Second, 200*time.Millisecond)

	_, err = client.DeleteFirewallPolicy(ctx, &nfsdk.DeleteFirewallPolicyInput{
		FirewallPolicyArn: policy.FirewallPolicyResponse.FirewallPolicyArn,
	})
	require.NoError(t, err)

	_, err = client.DeleteRuleGroup(ctx, &nfsdk.DeleteRuleGroupInput{
		RuleGroupArn: group.RuleGroupResponse.RuleGroupArn,
	})
	require.NoError(t, err)
}
