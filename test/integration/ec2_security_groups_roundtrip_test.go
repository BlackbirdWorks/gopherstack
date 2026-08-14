package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_EC2_SecurityGroup_IngressRuleRoundTrip verifies that a rule added
// through AuthorizeSecurityGroupIngress is visible, with real field values, through
// both DescribeSecurityGroups and DescribeSecurityGroupRules, and disappears from both
// after RevokeSecurityGroupIngress.
func TestIntegration_EC2_SecurityGroup_IngressRuleRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	name := "sg-roundtrip-" + uuid.NewString()[:8]

	createOut, err := client.CreateSecurityGroup(ctx, &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String(name),
		Description: aws.String("integration test sg"),
	})
	require.NoError(t, err)
	groupID := aws.ToString(createOut.GroupId)
	require.NotEmpty(t, groupID)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteSecurityGroup(cleanupCtx, &ec2sdk.DeleteSecurityGroupInput{GroupId: aws.String(groupID)})
	})

	_, err = client.AuthorizeSecurityGroupIngress(ctx, &ec2sdk.AuthorizeSecurityGroupIngressInput{
		GroupId: aws.String(groupID),
		IpPermissions: []ec2types.IpPermission{
			{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(443),
				ToPort:     aws.Int32(443),
				IpRanges: []ec2types.IpRange{
					{CidrIp: aws.String("203.0.113.0/24"), Description: aws.String("https-in")},
				},
			},
		},
	})
	require.NoError(t, err)

	// DescribeSecurityGroups: the rule's real protocol/ports/CIDR must round-trip,
	// not just a non-empty IpPermissions list.
	descOut, err := client.DescribeSecurityGroups(ctx, &ec2sdk.DescribeSecurityGroupsInput{
		GroupIds: []string{groupID},
	})
	require.NoError(t, err)
	require.Len(t, descOut.SecurityGroups, 1)
	require.Len(t, descOut.SecurityGroups[0].IpPermissions, 1)

	perm := descOut.SecurityGroups[0].IpPermissions[0]
	assert.Equal(t, "tcp", aws.ToString(perm.IpProtocol))
	assert.Equal(t, int32(443), aws.ToInt32(perm.FromPort))
	assert.Equal(t, int32(443), aws.ToInt32(perm.ToPort))
	require.Len(t, perm.IpRanges, 1)
	assert.Equal(t, "203.0.113.0/24", aws.ToString(perm.IpRanges[0].CidrIp))
	assert.Equal(t, "https-in", aws.ToString(perm.IpRanges[0].Description))

	// DescribeSecurityGroupRules: a distinct read op keyed off the group, should
	// show the same rule with a real per-rule ID.
	rulesOut, err := client.DescribeSecurityGroupRules(ctx, &ec2sdk.DescribeSecurityGroupRulesInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("group-id"), Values: []string{groupID}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, rulesOut.SecurityGroupRules, "DescribeSecurityGroupRules must not be empty after Authorize")

	var found bool

	for _, r := range rulesOut.SecurityGroupRules {
		if aws.ToString(r.CidrIpv4) == "203.0.113.0/24" {
			found = true

			assert.NotEmpty(t, aws.ToString(r.SecurityGroupRuleId))
			assert.Equal(t, groupID, aws.ToString(r.GroupId))
			assert.Equal(t, "tcp", aws.ToString(r.IpProtocol))
			assert.Equal(t, int32(443), aws.ToInt32(r.FromPort))
			assert.False(t, aws.ToBool(r.IsEgress))
		}
	}

	assert.True(t, found, "ingress rule not found via DescribeSecurityGroupRules")

	_, err = client.RevokeSecurityGroupIngress(ctx, &ec2sdk.RevokeSecurityGroupIngressInput{
		GroupId: aws.String(groupID),
		IpPermissions: []ec2types.IpPermission{
			{
				IpProtocol: aws.String("tcp"),
				FromPort:   aws.Int32(443),
				ToPort:     aws.Int32(443),
				IpRanges: []ec2types.IpRange{
					{CidrIp: aws.String("203.0.113.0/24")},
				},
			},
		},
	})
	require.NoError(t, err)

	descOut2, err := client.DescribeSecurityGroups(ctx, &ec2sdk.DescribeSecurityGroupsInput{
		GroupIds: []string{groupID},
	})
	require.NoError(t, err)
	require.Len(t, descOut2.SecurityGroups, 1)
	assert.Empty(t, descOut2.SecurityGroups[0].IpPermissions, "rule should be gone after Revoke")
}

// TestIntegration_EC2_SecurityGroup_TagFilterRoundTrip verifies that a tag written on
// a security group via CreateTags is both readable and usable as a DescribeSecurityGroups
// filter — a caller must be able to filter by a tag it can read, not merely see it listed.
func TestIntegration_EC2_SecurityGroup_TagFilterRoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createEC2Client(t)
	ctx := t.Context()

	tagVal := "roundtrip-" + uuid.NewString()[:8]

	taggedName := "sg-tagged-" + uuid.NewString()[:8]
	taggedOut, err := client.CreateSecurityGroup(ctx, &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String(taggedName),
		Description: aws.String("tagged sg"),
	})
	require.NoError(t, err)
	taggedID := aws.ToString(taggedOut.GroupId)

	plainName := "sg-plain-" + uuid.NewString()[:8]
	plainOut, err := client.CreateSecurityGroup(ctx, &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String(plainName),
		Description: aws.String("untagged sg"),
	})
	require.NoError(t, err)
	plainID := aws.ToString(plainOut.GroupId)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteSecurityGroup(cleanupCtx, &ec2sdk.DeleteSecurityGroupInput{GroupId: aws.String(taggedID)})
		_, _ = client.DeleteSecurityGroup(cleanupCtx, &ec2sdk.DeleteSecurityGroupInput{GroupId: aws.String(plainID)})
	})

	_, err = client.CreateTags(ctx, &ec2sdk.CreateTagsInput{
		Resources: []string{taggedID},
		Tags:      []ec2types.Tag{{Key: aws.String("Owner"), Value: aws.String(tagVal)}},
	})
	require.NoError(t, err)

	descOut, err := client.DescribeSecurityGroups(ctx, &ec2sdk.DescribeSecurityGroupsInput{
		Filters: []ec2types.Filter{
			{Name: aws.String("tag:Owner"), Values: []string{tagVal}},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, descOut.SecurityGroups, "tag: filter must return the group carrying that tag")

	var sawTagged, sawPlain bool

	for _, sg := range descOut.SecurityGroups {
		switch aws.ToString(sg.GroupId) {
		case taggedID:
			sawTagged = true
		case plainID:
			sawPlain = true
		}
	}

	assert.True(t, sawTagged, "group with the matching tag must be returned")
	assert.False(t, sawPlain, "group without the tag must be filtered out, not passed through leniently")
}
