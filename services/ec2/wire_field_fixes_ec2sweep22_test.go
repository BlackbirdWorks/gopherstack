package ec2_test

// Fixes for the handler_security_groups.go census follow-up: seven ops in
// that file had never been named by any prior audit pass.
//
//   - RevokeSecurityGroupEgress mirrored RevokeSecurityGroupIngress's
//     pre-fix bug exactly (real RevokeSecurityGroupEgressOutput has
//     RevokedSecurityGroupRuleSet/UnknownIpPermissionSet alongside Return --
//     ec2@v1.319.1 deserializers.go,
//     awsEc2query_deserializeOpDocumentRevokeSecurityGroupEgressOutput), but
//     unlike its sibling was never given those fields when Ingress was fixed.
//   - DescribeSecurityGroups accepted GroupName.N on the wire (the real
//     DescribeSecurityGroupsInput's GroupNames member,
//     awsEc2query_serializeOpDocumentDescribeSecurityGroupsInput) but the
//     handler only ever read GroupId.N, silently dropping it.
//   - DescribeSecurityGroups, DescribeSecurityGroupRules,
//     DescribeSecurityGroupVpcAssociations, and GetSecurityGroupsForVpc all
//     declare real MaxResults/NextToken on their Input/Output (confirmed
//     against each api_op_*.go) but the handlers returned every item in one
//     unbounded page, the same "unbounded single page" shape ec2sweep11 fixed
//     elsewhere in this package.

import (
	"slices"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRevokeSecurityGroupEgress_SurfacesRevokedRules_RealClient covers
// handleRevokeSecurityGroupEgress, which pre-fix returned only Return. See
// TestRevokeSecurityGroupIngress_SurfacesRevokedAndUnknown_RealClient
// (wire_field_fixes_ec2sweep14_test.go) for the sibling op fixed in the same
// prior pass -- Egress was left behind.
func TestRevokeSecurityGroupEgress_SurfacesRevokedRules_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	sg, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("sweep22-revoke-egress-sg"),
		Description: aws.String("sweep22 revoke egress test sg"),
	})
	require.NoError(t, err)

	_, err = client.AuthorizeSecurityGroupEgress(t.Context(), &ec2sdk.AuthorizeSecurityGroupEgressInput{
		GroupId: sg.GroupId,
		IpPermissions: []types.IpPermission{{
			IpProtocol: aws.String("tcp"),
			FromPort:   aws.Int32(8443),
			ToPort:     aws.Int32(8443),
			IpRanges:   []types.IpRange{{CidrIp: aws.String("203.0.113.9/32")}},
		}},
	})
	require.NoError(t, err)

	out, err := client.RevokeSecurityGroupEgress(t.Context(), &ec2sdk.RevokeSecurityGroupEgressInput{
		GroupId: sg.GroupId,
		IpPermissions: []types.IpPermission{{
			IpProtocol: aws.String("tcp"),
			FromPort:   aws.Int32(8443),
			ToPort:     aws.Int32(8443),
			IpRanges:   []types.IpRange{{CidrIp: aws.String("203.0.113.9/32")}},
		}},
	})
	require.NoError(t, err)
	require.Len(
		t, out.RevokedSecurityGroupRules, 1,
		"pre-fix this field was never rendered, only a bare Return bool",
	)
	assert.Equal(t, "203.0.113.9/32", aws.ToString(out.RevokedSecurityGroupRules[0].CidrIpv4))
	assert.True(t, aws.ToBool(out.RevokedSecurityGroupRules[0].IsEgress))
}

// TestDescribeSecurityGroups_GroupNameFilter_RealClient covers
// handleDescribeSecurityGroups, which pre-fix read only GroupId.N and
// silently dropped GroupName.N -- a real client requesting security groups
// by name (DescribeSecurityGroupsInput.GroupNames,
// api_op_DescribeSecurityGroups.go) got every security group back instead of
// the one it named.
func TestDescribeSecurityGroups_GroupNameFilter_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	_, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("sweep22-name-filter-other"),
		Description: aws.String("should not be returned"),
	})
	require.NoError(t, err)

	target, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("sweep22-name-filter-target"),
		Description: aws.String("should be returned"),
	})
	require.NoError(t, err)

	out, err := client.DescribeSecurityGroups(t.Context(), &ec2sdk.DescribeSecurityGroupsInput{
		GroupNames: []string{"sweep22-name-filter-target"},
	})
	require.NoError(t, err)
	require.Len(t, out.SecurityGroups, 1, "pre-fix GroupName.N was silently dropped, returning every group")
	assert.Equal(t, aws.ToString(target.GroupId), aws.ToString(out.SecurityGroups[0].GroupId))
}

// TestDescribeSecurityGroupRules_Pagination_RealClient covers
// handleDescribeSecurityGroupRules, which pre-fix ignored MaxResults/NextToken
// entirely (real bounds "between 5 and 1000", api_op_DescribeSecurityGroupRules.go).
func TestDescribeSecurityGroupRules_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	sg, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
		GroupName:   aws.String("sweep22-rules-page-sg"),
		Description: aws.String("sweep22 rules pagination test sg"),
	})
	require.NoError(t, err)

	const ruleCount = 6

	perms := make([]types.IpPermission, 0, ruleCount)
	for i := range ruleCount {
		perms = append(perms, types.IpPermission{
			IpProtocol: aws.String("tcp"),
			FromPort:   aws.Int32(int32(20000 + i)),
			ToPort:     aws.Int32(int32(20000 + i)),
			IpRanges:   []types.IpRange{{CidrIp: aws.String("203.0.113.10/32")}},
		})
	}

	_, err = client.AuthorizeSecurityGroupIngress(t.Context(), &ec2sdk.AuthorizeSecurityGroupIngressInput{
		GroupId:       sg.GroupId,
		IpPermissions: perms,
	})
	require.NoError(t, err)

	paginator := ec2sdk.NewDescribeSecurityGroupRulesPaginator(
		client,
		&ec2sdk.DescribeSecurityGroupRulesInput{
			Filters: []types.Filter{{Name: aws.String("group-id"), Values: []string{aws.ToString(sg.GroupId)}}},
		},
		func(o *ec2sdk.DescribeSecurityGroupRulesPaginatorOptions) { o.Limit = ec2sweep11MaxResults + 3 },
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		page, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(page.SecurityGroupRules))
		for _, r := range page.SecurityGroupRules {
			ids = append(ids, aws.ToString(r.SecurityGroupRuleId))
		}
		pages = append(pages, ids)
	}

	// +1: CreateSecurityGroup seeds a default allow-all egress rule.
	assertDisjointPages(t, pages, ruleCount+1)
}

// TestDescribeSecurityGroupVpcAssociations_Pagination_RealClient covers
// handleDescribeSecurityGroupVpcAssociations, which pre-fix ignored
// MaxResults/NextToken entirely.
func TestDescribeSecurityGroupVpcAssociations_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	const sgCount = 5

	groupIDs := make([]string, 0, sgCount)
	for i := range sgCount {
		vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{
			CidrBlock: aws.String("10.60." + strconv.Itoa(i) + ".0/24"),
		})
		require.NoError(t, err)

		sg, err := client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
			GroupName:   aws.String("sweep22-assoc-page-sg-" + aws.ToString(vpc.Vpc.VpcId)),
			Description: aws.String("sweep22 assoc pagination test sg"),
			VpcId:       vpc.Vpc.VpcId,
		})
		require.NoError(t, err)

		_, err = client.AssociateSecurityGroupVpc(t.Context(), &ec2sdk.AssociateSecurityGroupVpcInput{
			GroupId: sg.GroupId,
			VpcId:   vpc.Vpc.VpcId,
		})
		require.NoError(t, err)

		groupIDs = append(groupIDs, aws.ToString(sg.GroupId))
	}

	paginator := ec2sdk.NewDescribeSecurityGroupVpcAssociationsPaginator(
		client,
		&ec2sdk.DescribeSecurityGroupVpcAssociationsInput{},
		func(o *ec2sdk.DescribeSecurityGroupVpcAssociationsPaginatorOptions) {
			o.Limit = ec2sweep11MaxResults
		},
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		page, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		var ids []string
		for _, a := range page.SecurityGroupVpcAssociations {
			if id := aws.ToString(a.GroupId); slices.Contains(groupIDs, id) {
				ids = append(ids, id)
			}
		}
		pages = append(pages, ids)
	}

	assertDisjointPages(t, pages, sgCount)
}

// TestGetSecurityGroupsForVpc_Pagination_RealClient covers
// handleGetSecurityGroupsForVpc, which pre-fix ignored MaxResults/NextToken
// entirely.
func TestGetSecurityGroupsForVpc_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	vpc, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.61.0.0/16")})
	require.NoError(t, err)

	const sgCount = 5

	for i := range sgCount {
		_, err = client.CreateSecurityGroup(t.Context(), &ec2sdk.CreateSecurityGroupInput{
			GroupName:   aws.String("sweep22-getvpc-page-sg-" + aws.ToString(vpc.Vpc.VpcId) + "-" + strconv.Itoa(i)),
			Description: aws.String("sweep22 getvpc pagination test sg"),
			VpcId:       vpc.Vpc.VpcId,
		})
		require.NoError(t, err)
	}

	paginator := ec2sdk.NewGetSecurityGroupsForVpcPaginator(
		client,
		&ec2sdk.GetSecurityGroupsForVpcInput{VpcId: vpc.Vpc.VpcId},
		func(o *ec2sdk.GetSecurityGroupsForVpcPaginatorOptions) { o.Limit = ec2sweep11MaxResults },
	)

	var pages [][]string
	for pageNum := 0; paginator.HasMorePages() && pageNum < ec2sweep11LoopGuard; pageNum++ {
		page, pageErr := paginator.NextPage(t.Context())
		require.NoError(t, pageErr)

		ids := make([]string, 0, len(page.SecurityGroupForVpcs))
		for _, sg := range page.SecurityGroupForVpcs {
			ids = append(ids, aws.ToString(sg.GroupId))
		}
		pages = append(pages, ids)
	}

	// +1: CreateVpc seeds the VPC's own default security group.
	assertDisjointPages(t, pages, sgCount+1)
}
