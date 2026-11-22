package app

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

func limVPC(ctx context.Context, c *ec2.Client) (*string, error) {
	out, err := c.DescribeVpcs(ctx, &ec2.DescribeVpcsInput{Filters: ec2ByTag()})
	if err != nil || len(out.Vpcs) == 0 {
		return nil, errors.Join(err, errNoQuotaTarget)
	}

	return out.Vpcs[0].VpcId, nil
}

func prepareLimVPC(ctx context.Context, e limitEnv) error {
	_, err := ec2Client(e).CreateVpc(ctx, &ec2.CreateVpcInput{
		CidrBlock: aws.String("10.88.0.0/16"), TagSpecifications: ec2NameTag(ec2types.ResourceTypeVpc),
	})

	return err
}

func instanceCase(name, quota string, limit int, spot bool) limitCase {
	code := "VcpuLimitExceeded"
	if spot {
		code = "MaxSpotInstanceCountExceeded"
	}

	return limitCase{
		name: name, svc: "ec2", quota: quota, code: code, limit: limit, handler: ec2Handler,
		create: func(ctx context.Context, e limitEnv, _ int) error {
			c := ec2Client(e)

			if spot {
				_, err := c.RequestSpotInstances(ctx, &ec2.RequestSpotInstancesInput{
					InstanceCount: aws.Int32(1),
					LaunchSpecification: &ec2types.RequestSpotLaunchSpecification{
						ImageId: aws.String("ami-12345678"), InstanceType: ec2types.InstanceTypeT2Micro,
					},
				})

				return err
			}

			_, err := c.RunInstances(ctx, &ec2.RunInstancesInput{
				ImageId: aws.String("ami-12345678"), InstanceType: ec2types.InstanceTypeT2Micro,
				MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
			})

			return err
		},
		remove: func(ctx context.Context, e limitEnv, _ int) error {
			c := ec2Client(e)

			if spot {
				out, err := c.DescribeSpotInstanceRequests(ctx, &ec2.DescribeSpotInstanceRequestsInput{})
				if err != nil || len(out.SpotInstanceRequests) == 0 {
					return errors.Join(err, errNoQuotaTarget)
				}

				_, err = c.CancelSpotInstanceRequests(ctx, &ec2.CancelSpotInstanceRequestsInput{
					SpotInstanceRequestIds: []string{*out.SpotInstanceRequests[0].SpotInstanceRequestId},
				})

				return err
			}

			out, err := c.DescribeInstances(ctx, &ec2.DescribeInstancesInput{})
			if err != nil || len(out.Reservations) == 0 {
				return errors.Join(err, errNoQuotaTarget)
			}

			_, err = c.TerminateInstances(ctx, &ec2.TerminateInstancesInput{
				InstanceIds: []string{*out.Reservations[0].Instances[0].InstanceId},
			})

			return err
		},
	}
}

func ec2ConfirmedCodeCases() []limitCase {
	return []limitCase{
		{
			name: "subnets per vpc", svc: "vpc", quota: "L-407747CB", code: "SubnetLimitExceeded", limit: 2,
			handler: ec2Handler, prepare: prepareLimVPC,
			create: func(ctx context.Context, e limitEnv, i int) error {
				c := ec2Client(e)

				id, err := limVPC(ctx, c)
				if err != nil {
					return err
				}

				_, err = c.CreateSubnet(ctx, &ec2.CreateSubnetInput{
					VpcId: id, CidrBlock: aws.String("10.88." + qnum(i) + ".0/24"),
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				c := ec2Client(e)

				id, err := limVPC(ctx, c)
				if err != nil {
					return err
				}

				out, err := c.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{
					Filters: []ec2types.Filter{{Name: aws.String("vpc-id"), Values: []string{*id}}},
				})
				if err != nil || len(out.Subnets) == 0 {
					return errors.Join(err, errNoQuotaTarget)
				}

				_, err = c.DeleteSubnet(ctx, &ec2.DeleteSubnetInput{SubnetId: out.Subnets[0].SubnetId})

				return err
			},
		},
		{
			name: "route tables per vpc", svc: "vpc", quota: "L-589F43AA", code: "RouteTableLimitExceeded",
			limit: 3, existing: 1, handler: ec2Handler, prepare: prepareLimVPC,
			create: func(ctx context.Context, e limitEnv, _ int) error {
				c := ec2Client(e)

				id, err := limVPC(ctx, c)
				if err != nil {
					return err
				}

				_, err = c.CreateRouteTable(ctx, &ec2.CreateRouteTableInput{VpcId: id})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				c := ec2Client(e)

				out, err := c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{})
				if err != nil {
					return err
				}

				for _, rt := range out.RouteTables {
					if len(rt.Associations) == 0 && rt.VpcId != nil {
						if id, _ := limVPC(ctx, c); id != nil && *id == *rt.VpcId {
							_, derr := c.DeleteRouteTable(ctx, &ec2.DeleteRouteTableInput{
								RouteTableId: rt.RouteTableId,
							})

							return derr
						}
					}
				}

				return errNoQuotaTarget
			},
		},
		{
			name: "internet gateways", svc: "vpc", quota: "L-A4707A72", code: "InternetGatewayLimitExceeded",
			limit: 3, handler: ec2Handler,
			create: func(ctx context.Context, e limitEnv, _ int) error {
				_, err := ec2Client(e).CreateInternetGateway(ctx, &ec2.CreateInternetGatewayInput{})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				c := ec2Client(e)

				out, err := c.DescribeInternetGateways(ctx, &ec2.DescribeInternetGatewaysInput{})
				if err != nil || len(out.InternetGateways) == 0 {
					return errors.Join(err, errNoQuotaTarget)
				}

				for _, g := range out.InternetGateways {
					if len(g.Attachments) == 0 {
						_, derr := c.DeleteInternetGateway(ctx, &ec2.DeleteInternetGatewayInput{
							InternetGatewayId: g.InternetGatewayId,
						})

						return derr
					}
				}

				return errNoQuotaTarget
			},
		},
		{
			name: "security groups", svc: "vpc", quota: "L-E79EC296", code: "SecurityGroupLimitExceeded",
			limit: 4, existing: 1, handler: ec2Handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := ec2Client(e).CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
					GroupName: aws.String("sg" + qnum(i)), Description: aws.String("d"),
				})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				_, err := ec2Client(e).DeleteSecurityGroup(ctx, &ec2.DeleteSecurityGroupInput{
					GroupName: aws.String("sg" + qnum(0)),
				})

				return err
			},
		},
		{
			name: "rules per security group", svc: "vpc", quota: "L-0EA8095F",
			code: "RulesPerSecurityGroupLimitExceeded", limit: 3, handler: ec2Handler,
			prepare: func(ctx context.Context, e limitEnv) error {
				_, err := ec2Client(e).CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
					GroupName: aws.String("rules"), Description: aws.String("d"),
					TagSpecifications: ec2NameTag(ec2types.ResourceTypeSecurityGroup),
				})

				return err
			},
			create: func(ctx context.Context, e limitEnv, i int) error {
				c := ec2Client(e)

				out, err := c.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{Filters: ec2ByTag()})
				if err != nil || len(out.SecurityGroups) == 0 {
					return errors.Join(err, errNoQuotaTarget)
				}

				_, err = c.AuthorizeSecurityGroupIngress(ctx, &ec2.AuthorizeSecurityGroupIngressInput{
					GroupId: out.SecurityGroups[0].GroupId,
					IpPermissions: []ec2types.IpPermission{{
						IpProtocol: aws.String("tcp"),
						FromPort:   aws.Int32(int32(1000 + i)), ToPort: aws.Int32(int32(1000 + i)),
						IpRanges: []ec2types.IpRange{{CidrIp: aws.String("10.0.0.0/8")}},
					}},
				})

				return err
			},
		},
		instanceCase("on-demand standard vcpus", "L-1216C47A", 3, false),
		instanceCase("spot standard vcpus", "L-34B43A08", 3, true),
	}
}

func TestQuotaLimitsEC2ConfirmedCodes(t *testing.T) {
	t.Parallel()

	runLimitCases(t, ec2ConfirmedCodeCases())
}
