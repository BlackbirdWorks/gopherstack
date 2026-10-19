package replicator

import (
	"context"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

// planVPC returns the VPC config to apply, or nil with a skipped result when the target lacks
// the subnets or security groups.
func (r *run) planVPC(ctx context.Context, name string, c *lambdatypes.FunctionConfiguration) *lambdatypes.VpcConfig {
	v := c.VpcConfig
	if v == nil || len(v.SubnetIds) == 0 {
		return nil
	}

	if missing := r.missingNetwork(ctx, v); missing != "" {
		r.add(TypeLambda, name+":vpc", ActionSkipped, missing)

		return nil
	}

	return vpcCopy(v)
}

// vpcFor is planVPC without recording a result, for historical versions.
func (r *run) vpcFor(ctx context.Context, c *lambdatypes.FunctionConfiguration) *lambdatypes.VpcConfig {
	v := c.VpcConfig
	if v == nil || len(v.SubnetIds) == 0 || r.missingNetwork(ctx, v) != "" {
		return nil
	}

	return vpcCopy(v)
}

func (r *run) missingNetwork(ctx context.Context, v *lambdatypes.VpcConfigResponse) string {
	subnets, err := r.dst.ec2.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
		Filters: []ec2types.Filter{{Name: aws.String("subnet-id"), Values: v.SubnetIds}},
	})
	if err != nil {
		return "VPC config skipped: cannot verify target subnets: " + errCode(err)
	}

	have := make([]string, 0, len(subnets.Subnets))
	for _, s := range subnets.Subnets {
		have = append(have, aws.ToString(s.SubnetId))
	}

	if m := notIn(v.SubnetIds, have); len(m) > 0 {
		return "VPC config skipped: target subnets not found: " + strings.Join(m, ",")
	}

	if len(v.SecurityGroupIds) == 0 {
		return ""
	}

	sgs, err := r.dst.ec2.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{
		Filters: []ec2types.Filter{{Name: aws.String("group-id"), Values: v.SecurityGroupIds}},
	})
	if err != nil {
		return "VPC config skipped: cannot verify target security groups: " + errCode(err)
	}

	have = have[:0]
	for _, g := range sgs.SecurityGroups {
		have = append(have, aws.ToString(g.GroupId))
	}

	if m := notIn(v.SecurityGroupIds, have); len(m) > 0 {
		return "VPC config skipped: target security groups not found: " + strings.Join(m, ",")
	}

	return ""
}

func notIn(want, have []string) []string {
	var out []string

	for _, w := range want {
		if !slices.Contains(have, w) {
			out = append(out, w)
		}
	}

	return out
}

func vpcCopy(v *lambdatypes.VpcConfigResponse) *lambdatypes.VpcConfig {
	return &lambdatypes.VpcConfig{
		SubnetIds: v.SubnetIds, SecurityGroupIds: v.SecurityGroupIds,
		Ipv6AllowedForDualStack: v.Ipv6AllowedForDualStack,
	}
}
