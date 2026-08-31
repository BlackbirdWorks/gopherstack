package ec2_test

// uox6-sgname: AuthorizeSecurityGroupIngress, RevokeSecurityGroupIngress,
// DeleteSecurityGroup, UpdateSecurityGroupRuleDescriptionsIngress and
// UpdateSecurityGroupRuleDescriptionsEgress all declare GroupName as an
// alternative to GroupId ("[Default VPC] The name of the security group.
// You can specify either the security group name or the security group
// ID." -- api_op_DeleteSecurityGroup.go etc, ec2@v1.319.1), but the handler
// only ever read GroupId and rejected a request that supplied only
// GroupName. DescribeSecurityGroups already resolves names to IDs for its
// own filtering, so the same resolution is reused here rather than
// invented fresh.

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthorizeSecurityGroupIngress_ByGroupName(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("uox6-web-sg", "web sg", vpc.ID)
	require.NoError(t, err)

	_, err = client.AuthorizeSecurityGroupIngress(
		t.Context(),
		&ec2sdk.AuthorizeSecurityGroupIngressInput{
			GroupName: aws.String("uox6-web-sg"),
			IpPermissions: []types.IpPermission{{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(22),
				ToPort:     aws.Int32(22),
				IpRanges:   []types.IpRange{{CidrIp: aws.String("0.0.0.0/0")}},
			}},
		},
	)
	require.NoError(t, err)

	out, err := client.DescribeSecurityGroups(t.Context(), &ec2sdk.DescribeSecurityGroupsInput{
		GroupIds: []string{sg.ID},
	})
	require.NoError(t, err)
	require.Len(t, out.SecurityGroups, 1)
	require.Len(t, out.SecurityGroups[0].IpPermissions, 1)
	assert.Equal(t, int32(22), aws.ToInt32(out.SecurityGroups[0].IpPermissions[0].FromPort))
}

func TestRevokeSecurityGroupIngress_ByGroupName(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("uox6-db-sg", "db sg", vpc.ID)
	require.NoError(t, err)

	rule := ec2sdk.AuthorizeSecurityGroupIngressInput{
		GroupId: aws.String(sg.ID),
		IpPermissions: []types.IpPermission{{
			IpProtocol: aws.String("tcp"),
			FromPort:   aws.Int32(3306),
			ToPort:     aws.Int32(3306),
			IpRanges:   []types.IpRange{{CidrIp: aws.String("10.0.0.0/16")}},
		}},
	}
	_, err = client.AuthorizeSecurityGroupIngress(t.Context(), &rule)
	require.NoError(t, err)

	_, err = client.RevokeSecurityGroupIngress(t.Context(), &ec2sdk.RevokeSecurityGroupIngressInput{
		GroupName:     aws.String("uox6-db-sg"),
		IpPermissions: rule.IpPermissions,
	})
	require.NoError(t, err)

	out, err := client.DescribeSecurityGroups(t.Context(), &ec2sdk.DescribeSecurityGroupsInput{
		GroupIds: []string{sg.ID},
	})
	require.NoError(t, err)
	require.Len(t, out.SecurityGroups, 1)
	assert.Empty(t, out.SecurityGroups[0].IpPermissions)
}

func TestDeleteSecurityGroup_ByGroupName(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	_, err = b.CreateSecurityGroup("uox6-del-sg", "delete me", vpc.ID)
	require.NoError(t, err)

	_, err = client.DeleteSecurityGroup(t.Context(), &ec2sdk.DeleteSecurityGroupInput{
		GroupName: aws.String("uox6-del-sg"),
	})
	require.NoError(t, err)

	out, err := client.DescribeSecurityGroups(t.Context(), &ec2sdk.DescribeSecurityGroupsInput{
		Filters: []types.Filter{{Name: aws.String("group-name"), Values: []string{"uox6-del-sg"}}},
	})
	require.NoError(t, err)
	assert.Empty(t, out.SecurityGroups)
}

func TestUpdateSecurityGroupRuleDescriptions_ByGroupName(t *testing.T) {
	t.Parallel()

	b, client := newTestBackendAndClient(t)

	vpc, err := b.CreateVpc("10.0.0.0/16")
	require.NoError(t, err)

	sg, err := b.CreateSecurityGroup("uox6-desc-sg", "desc sg", vpc.ID)
	require.NoError(t, err)

	perm := types.IpPermission{
		IpProtocol: aws.String("tcp"),
		FromPort:   aws.Int32(443),
		ToPort:     aws.Int32(443),
		IpRanges:   []types.IpRange{{CidrIp: aws.String("0.0.0.0/0")}},
	}

	_, err = client.AuthorizeSecurityGroupIngress(
		t.Context(),
		&ec2sdk.AuthorizeSecurityGroupIngressInput{
			GroupId:       aws.String(sg.ID),
			IpPermissions: []types.IpPermission{perm},
		},
	)
	require.NoError(t, err)

	_, err = client.AuthorizeSecurityGroupEgress(
		t.Context(),
		&ec2sdk.AuthorizeSecurityGroupEgressInput{
			GroupId:       aws.String(sg.ID),
			IpPermissions: []types.IpPermission{perm},
		},
	)
	require.NoError(t, err)

	describedPerm := perm
	describedPerm.IpRanges = []types.IpRange{
		{CidrIp: aws.String("0.0.0.0/0"), Description: aws.String("https ingress")},
	}

	_, err = client.UpdateSecurityGroupRuleDescriptionsIngress(t.Context(),
		&ec2sdk.UpdateSecurityGroupRuleDescriptionsIngressInput{
			GroupName:     aws.String("uox6-desc-sg"),
			IpPermissions: []types.IpPermission{describedPerm},
		})
	require.NoError(t, err)

	_, err = client.UpdateSecurityGroupRuleDescriptionsEgress(t.Context(),
		&ec2sdk.UpdateSecurityGroupRuleDescriptionsEgressInput{
			GroupName:     aws.String("uox6-desc-sg"),
			IpPermissions: []types.IpPermission{describedPerm},
		})
	require.NoError(t, err)

	out, err := client.DescribeSecurityGroups(t.Context(), &ec2sdk.DescribeSecurityGroupsInput{
		GroupIds: []string{sg.ID},
	})
	require.NoError(t, err)
	require.Len(t, out.SecurityGroups, 1)
	require.Len(t, out.SecurityGroups[0].IpPermissions, 1)
	require.Len(t, out.SecurityGroups[0].IpPermissions[0].IpRanges, 1)
	assert.Equal(
		t,
		"https ingress",
		aws.ToString(out.SecurityGroups[0].IpPermissions[0].IpRanges[0].Description),
	)

	// Every new security group also carries AWS's default allow-all egress
	// rule (security_groups.go's CreateSecurityGroup), so the 443/tcp rule
	// added above is the SECOND entry here, not the only one.
	var egress443 *types.IpPermission

	for i, p := range out.SecurityGroups[0].IpPermissionsEgress {
		if aws.ToInt32(p.FromPort) == 443 {
			egress443 = &out.SecurityGroups[0].IpPermissionsEgress[i]
		}
	}

	require.NotNil(t, egress443, "expected the 443/tcp egress rule to be present")
	require.Len(t, egress443.IpRanges, 1)
	assert.Equal(t, "https ingress", aws.ToString(egress443.IpRanges[0].Description))
}

func TestAuthorizeSecurityGroupIngress_NoIdentifier(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	_, err := client.AuthorizeSecurityGroupIngress(
		t.Context(),
		&ec2sdk.AuthorizeSecurityGroupIngressInput{
			IpPermissions: []types.IpPermission{{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(22),
				ToPort:     aws.Int32(22),
				IpRanges:   []types.IpRange{{CidrIp: aws.String("0.0.0.0/0")}},
			}},
		},
	)
	require.Error(t, err)
}
