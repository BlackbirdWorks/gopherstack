package app

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

func ec2Handler(*testing.T) service.Registerable {
	return ec2backend.NewHandler(ec2backend.NewInMemoryBackend(quotaTestAccount, quotaTestRegion))
}

func ec2Client(e limitEnv) *ec2.Client {
	return ec2.NewFromConfig(e.cfg, func(o *ec2.Options) { o.BaseEndpoint = e.endpoint() })
}

func defaultSubnet(ctx context.Context, c *ec2.Client) (*ec2types.Subnet, error) {
	out, err := c.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{})
	if err != nil || len(out.Subnets) == 0 {
		return nil, errors.Join(err, errNoQuotaTarget)
	}

	return &out.Subnets[0], nil
}

func ec2ByTag() []ec2types.Filter {
	return []ec2types.Filter{{Name: aws.String("tag:Name"), Values: []string{"lim"}}}
}

func ec2NameTag(rt ec2types.ResourceType) []ec2types.TagSpecification {
	return []ec2types.TagSpecification{{
		ResourceType: rt, Tags: []ec2types.Tag{{Key: aws.String("Name"), Value: aws.String("lim")}},
	}}
}

func routeCase() limitCase {
	return limitCase{
		name: "routes per table", svc: "vpc", quota: "L-93826ACB",
		code: "ResourceCountLimitExceeded", limit: 2,
		handler: ec2Handler,
		prepare: func(ctx context.Context, e limitEnv) error {
			c := ec2Client(e)

			vpc, err := c.CreateVpc(ctx, &ec2.CreateVpcInput{
				CidrBlock: aws.String("10.77.0.0/16"), TagSpecifications: ec2NameTag(ec2types.ResourceTypeVpc),
			})
			if err != nil {
				return err
			}

			if _, err = c.CreateRouteTable(ctx, &ec2.CreateRouteTableInput{
				VpcId: vpc.Vpc.VpcId, TagSpecifications: ec2NameTag(ec2types.ResourceTypeRouteTable),
			}); err != nil {
				return err
			}

			igw, err := c.CreateInternetGateway(ctx, &ec2.CreateInternetGatewayInput{
				TagSpecifications: ec2NameTag(ec2types.ResourceTypeInternetGateway),
			})
			if err != nil {
				return err
			}

			_, err = c.AttachInternetGateway(ctx, &ec2.AttachInternetGatewayInput{
				InternetGatewayId: igw.InternetGateway.InternetGatewayId, VpcId: vpc.Vpc.VpcId,
			})

			return err
		},
		create: func(ctx context.Context, e limitEnv, i int) error {
			c := ec2Client(e)

			rt, err := c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{Filters: ec2ByTag()})
			if err != nil || len(rt.RouteTables) == 0 {
				return errors.Join(err, errNoQuotaTarget)
			}

			igw, err := c.DescribeInternetGateways(ctx, &ec2.DescribeInternetGatewaysInput{Filters: ec2ByTag()})
			if err != nil || len(igw.InternetGateways) == 0 {
				return errors.Join(err, errNoQuotaTarget)
			}

			_, err = c.CreateRoute(ctx, &ec2.CreateRouteInput{
				RouteTableId:         rt.RouteTables[0].RouteTableId,
				DestinationCidrBlock: aws.String("192.168." + qnum(i) + ".0/24"),
				GatewayId:            igw.InternetGateways[0].InternetGatewayId,
			})

			return err
		},
		remove: func(ctx context.Context, e limitEnv, _ int) error {
			c := ec2Client(e)

			rt, err := c.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{Filters: ec2ByTag()})
			if err != nil || len(rt.RouteTables) == 0 {
				return errors.Join(err, errNoQuotaTarget)
			}

			_, err = c.DeleteRoute(ctx, &ec2.DeleteRouteInput{
				RouteTableId: rt.RouteTables[0].RouteTableId, DestinationCidrBlock: aws.String("192.168.0.0/24"),
			})

			return err
		},
	}
}

func ec2LimitCases() []limitCase {
	return []limitCase{
		{
			name: "network interfaces", svc: "vpc", quota: "L-DF5E4CA3",
			code: "NetworkInterfaceLimitExceeded", limit: 2,
			handler: ec2Handler,
			create: func(ctx context.Context, e limitEnv, _ int) error {
				c := ec2Client(e)

				sub, err := defaultSubnet(ctx, c)
				if err != nil {
					return err
				}

				_, err = c.CreateNetworkInterface(ctx, &ec2.CreateNetworkInterfaceInput{SubnetId: sub.SubnetId})

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				c := ec2Client(e)

				out, err := c.DescribeNetworkInterfaces(ctx, &ec2.DescribeNetworkInterfacesInput{})
				if err != nil || len(out.NetworkInterfaces) == 0 {
					return errors.Join(err, errNoQuotaTarget)
				}

				_, err = c.DeleteNetworkInterface(ctx, &ec2.DeleteNetworkInterfaceInput{
					NetworkInterfaceId: out.NetworkInterfaces[0].NetworkInterfaceId,
				})

				return err
			},
		},
		{
			name: "nat gateways per zone", svc: "vpc", quota: "L-FE5A380F",
			code: "NatGatewayLimitExceeded", limit: 2,
			handler: ec2Handler,
			create: func(ctx context.Context, e limitEnv, _ int) error {
				c := ec2Client(e)

				sub, err := defaultSubnet(ctx, c)
				if err != nil {
					return err
				}

				eip, err := c.AllocateAddress(ctx, &ec2.AllocateAddressInput{Domain: ec2types.DomainTypeVpc})
				if err != nil {
					return err
				}

				_, err = c.CreateNatGateway(
					ctx,
					&ec2.CreateNatGatewayInput{SubnetId: sub.SubnetId, AllocationId: eip.AllocationId},
				)

				return err
			},
			remove: func(ctx context.Context, e limitEnv, _ int) error {
				c := ec2Client(e)

				out, err := c.DescribeNatGateways(ctx, &ec2.DescribeNatGatewaysInput{})
				if err != nil || len(out.NatGateways) == 0 {
					return errors.Join(err, errNoQuotaTarget)
				}

				_, err = c.DeleteNatGateway(
					ctx,
					&ec2.DeleteNatGatewayInput{NatGatewayId: out.NatGateways[0].NatGatewayId},
				)

				return err
			},
		},
		routeCase(),
	}
}

func ec2CountCases() []countCase {
	return []countCase{
		{
			name: "subnets per vpc", svc: "vpc", quota: "L-407747CB", n: 1, handler: ec2Handler,
			create: func(ctx context.Context, e limitEnv, _ int) error {
				c := ec2Client(e)

				sub, err := defaultSubnet(ctx, c)
				if err != nil {
					return err
				}

				_, err = c.CreateSubnet(
					ctx,
					&ec2.CreateSubnetInput{VpcId: sub.VpcId, CidrBlock: aws.String("172.31.200.0/24")},
				)

				return err
			},
		},
		{
			name: "internet gateways", svc: "vpc", quota: "L-A4707A72", n: 3, handler: ec2Handler,
			create: func(ctx context.Context, e limitEnv, _ int) error {
				_, err := ec2Client(e).CreateInternetGateway(ctx, &ec2.CreateInternetGatewayInput{})

				return err
			},
		},
		{
			name: "security groups", svc: "vpc", quota: "L-E79EC296", n: 3, handler: ec2Handler,
			create: func(ctx context.Context, e limitEnv, i int) error {
				_, err := ec2Client(e).CreateSecurityGroup(ctx, &ec2.CreateSecurityGroupInput{
					GroupName: aws.String("sg" + qnum(i)), Description: aws.String("d"),
				})

				return err
			},
		},
		{
			name: "on-demand standard vcpus", svc: "ec2", quota: "L-1216C47A", n: 2, step: 2, handler: ec2Handler,
			create: func(ctx context.Context, e limitEnv, _ int) error {
				_, err := ec2Client(e).RunInstances(ctx, &ec2.RunInstancesInput{
					ImageId: aws.String("ami-12345678"), InstanceType: ec2types.InstanceTypeM5Large,
					MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
				})

				return err
			},
		},
	}
}

func TestQuotaLimitsEC2(t *testing.T) {
	t.Parallel()

	runLimitCases(t, ec2LimitCases())
}

func TestQuotaCountsEC2(t *testing.T) {
	t.Parallel()

	runCountCases(t, ec2CountCases())
}
