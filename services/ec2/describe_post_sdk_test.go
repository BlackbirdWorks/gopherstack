package ec2_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func wireFilter(name string, values ...string) types.Filter {
	return types.Filter{Name: aws.String(name), Values: values}
}

func envTag(resourceType types.ResourceType, env string) []types.TagSpecification {
	return []types.TagSpecification{{
		ResourceType: resourceType,
		Tags:         []types.Tag{{Key: aws.String("env"), Value: aws.String(env)}},
	}}
}

func TestDescribeFilters_WireFields(t *testing.T) {
	t.Parallel()

	type seedFn func(ctx context.Context, t *testing.T, c *ec2sdk.Client)

	type listFn func(ctx context.Context, c *ec2sdk.Client, f []types.Filter) ([]string, error)

	ipamSeed := func(ctx context.Context, t *testing.T, c *ec2sdk.Client) {
		t.Helper()

		for _, s := range []struct{ desc, env string }{{"alpha", "prod"}, {"alpha-two", "dev"}, {"beta", "prod"}} {
			_, err := c.CreateIpam(ctx, &ec2sdk.CreateIpamInput{
				Description:       aws.String(s.desc),
				TagSpecifications: envTag(types.ResourceTypeIpam, s.env),
			})
			require.NoError(t, err)
		}
	}
	ipamList := func(ctx context.Context, c *ec2sdk.Client, f []types.Filter) ([]string, error) {
		out, err := c.DescribeIpams(ctx, &ec2sdk.DescribeIpamsInput{Filters: f})
		if err != nil {
			return nil, err
		}

		var got []string
		for _, i := range out.Ipams {
			got = append(got, aws.ToString(i.Description))
		}

		return got, nil
	}
	vaiSeed := func(ctx context.Context, t *testing.T, c *ec2sdk.Client) {
		t.Helper()

		for _, s := range []struct{ desc, env string }{{"alpha", "prod"}, {"alpha-two", "dev"}, {"beta", "prod"}} {
			_, err := c.CreateVerifiedAccessInstance(ctx, &ec2sdk.CreateVerifiedAccessInstanceInput{
				Description:       aws.String(s.desc),
				TagSpecifications: envTag(types.ResourceTypeVerifiedAccessInstance, s.env),
			})
			require.NoError(t, err)
		}
	}
	vaiList := func(ctx context.Context, c *ec2sdk.Client, f []types.Filter) ([]string, error) {
		out, err := c.DescribeVerifiedAccessInstances(ctx, &ec2sdk.DescribeVerifiedAccessInstancesInput{Filters: f})
		if err != nil {
			return nil, err
		}

		var got []string
		for _, i := range out.VerifiedAccessInstances {
			got = append(got, aws.ToString(i.Description))
		}

		return got, nil
	}

	tests := []struct {
		seed    seedFn
		list    listFn
		name    string
		wantErr string
		filters []types.Filter
		want    []string
	}{
		{
			name:    "ipam_exact",
			seed:    ipamSeed,
			list:    ipamList,
			filters: []types.Filter{wireFilter("description", "beta")},
			want:    []string{"beta"},
		},
		{
			name:    "ipam_wildcard",
			seed:    ipamSeed,
			list:    ipamList,
			filters: []types.Filter{wireFilter("description", "alpha*")},
			want:    []string{"alpha", "alpha-two"},
		},
		{
			name:    "ipam_question",
			seed:    ipamSeed,
			list:    ipamList,
			filters: []types.Filter{wireFilter("description", "alph?")},
			want:    []string{"alpha"},
		},
		{
			name:    "ipam_case_sensitive",
			seed:    ipamSeed,
			list:    ipamList,
			filters: []types.Filter{wireFilter("description", "ALPHA")},
			want:    nil,
		},
		{
			name:    "ipam_or_values",
			seed:    ipamSeed,
			list:    ipamList,
			filters: []types.Filter{wireFilter("description", "alpha", "beta")},
			want:    []string{"alpha", "beta"},
		},
		{
			name: "ipam_and_filters", seed: ipamSeed, list: ipamList, want: []string{"alpha"},
			filters: []types.Filter{wireFilter("description", "alpha*"), wireFilter("tag:env", "prod")},
		},
		{
			name:    "ipam_tag_key",
			seed:    ipamSeed,
			list:    ipamList,
			filters: []types.Filter{wireFilter("tag-key", "env")},
			want:    []string{"alpha", "alpha-two", "beta"},
		},
		{
			name:    "ipam_tag_value",
			seed:    ipamSeed,
			list:    ipamList,
			filters: []types.Filter{wireFilter("tag-value", "dev")},
			want:    []string{"alpha-two"},
		},
		{
			name:    "ipam_invalid_name",
			seed:    ipamSeed,
			list:    ipamList,
			filters: []types.Filter{wireFilter("no-such-field", "x")},
			wantErr: "InvalidParameterValue",
		},
		{
			name:    "vai_exact",
			seed:    vaiSeed,
			list:    vaiList,
			filters: []types.Filter{wireFilter("description", "beta")},
			want:    []string{"beta"},
		},
		{
			name:    "vai_wildcard_and_tag",
			seed:    vaiSeed,
			list:    vaiList,
			want:    []string{"alpha-two"},
			filters: []types.Filter{wireFilter("description", "alpha*"), wireFilter("tag:env", "dev")},
		},
		{
			name:    "vai_invalid_name",
			seed:    vaiSeed,
			list:    vaiList,
			filters: []types.Filter{wireFilter("nope", "x")},
			wantErr: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestBackendAndClient(t)
			tt.seed(t.Context(), t, client)

			got, err := tt.list(t.Context(), client, tt.filters)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestDescribePagination_Families(t *testing.T) {
	t.Parallel()

	const seeded = 3

	type pageFn func(ctx context.Context, c *ec2sdk.Client, limit *int32, token *string) ([]string, *string, error)

	tests := []struct {
		seed func(ctx context.Context, t *testing.T, c *ec2sdk.Client)
		page pageFn
		name string
	}{
		{
			name: "vpcs",
			seed: func(ctx context.Context, t *testing.T, c *ec2sdk.Client) {
				t.Helper()
				for i := range seeded {
					_, err := c.CreateVpc(
						ctx,
						&ec2sdk.CreateVpcInput{CidrBlock: aws.String(fmt.Sprintf("10.%d.0.0/16", i+10))},
					)
					require.NoError(t, err)
				}
			},
			page: func(ctx context.Context, c *ec2sdk.Client, m *int32, tok *string) ([]string, *string, error) {
				out, err := c.DescribeVpcs(ctx, &ec2sdk.DescribeVpcsInput{MaxResults: m, NextToken: tok})
				if err != nil {
					return nil, nil, err
				}

				var ids []string
				for _, v := range out.Vpcs {
					ids = append(ids, aws.ToString(v.VpcId))
				}

				return ids, out.NextToken, nil
			},
		},
		{
			name: "ipams",
			seed: func(ctx context.Context, t *testing.T, c *ec2sdk.Client) {
				t.Helper()
				for range seeded {
					_, err := c.CreateIpam(ctx, &ec2sdk.CreateIpamInput{})
					require.NoError(t, err)
				}
			},
			page: func(ctx context.Context, c *ec2sdk.Client, m *int32, tok *string) ([]string, *string, error) {
				out, err := c.DescribeIpams(ctx, &ec2sdk.DescribeIpamsInput{MaxResults: m, NextToken: tok})
				if err != nil {
					return nil, nil, err
				}

				var ids []string
				for _, v := range out.Ipams {
					ids = append(ids, aws.ToString(v.IpamId))
				}

				return ids, out.NextToken, nil
			},
		},
		{
			name: "verified_access_instances",
			seed: func(ctx context.Context, t *testing.T, c *ec2sdk.Client) {
				t.Helper()
				for range seeded {
					_, err := c.CreateVerifiedAccessInstance(ctx, &ec2sdk.CreateVerifiedAccessInstanceInput{})
					require.NoError(t, err)
				}
			},
			page: func(ctx context.Context, c *ec2sdk.Client, m *int32, tok *string) ([]string, *string, error) {
				out, err := c.DescribeVerifiedAccessInstances(
					ctx,
					&ec2sdk.DescribeVerifiedAccessInstancesInput{MaxResults: m, NextToken: tok},
				)
				if err != nil {
					return nil, nil, err
				}

				var ids []string
				for _, v := range out.VerifiedAccessInstances {
					ids = append(ids, aws.ToString(v.VerifiedAccessInstanceId))
				}

				return ids, out.NextToken, nil
			},
		},
		{
			name: "route_servers",
			seed: func(ctx context.Context, t *testing.T, c *ec2sdk.Client) {
				t.Helper()
				for i := range seeded {
					_, err := c.CreateRouteServer(
						ctx,
						&ec2sdk.CreateRouteServerInput{AmazonSideAsn: aws.Int64(int64(64512 + i))},
					)
					require.NoError(t, err)
				}
			},
			page: func(ctx context.Context, c *ec2sdk.Client, m *int32, tok *string) ([]string, *string, error) {
				out, err := c.DescribeRouteServers(
					ctx,
					&ec2sdk.DescribeRouteServersInput{MaxResults: m, NextToken: tok},
				)
				if err != nil {
					return nil, nil, err
				}

				var ids []string
				for _, v := range out.RouteServers {
					ids = append(ids, aws.ToString(v.RouteServerId))
				}

				return ids, out.NextToken, nil
			},
		},
		{
			name: "transit_gateways",
			seed: func(ctx context.Context, t *testing.T, c *ec2sdk.Client) {
				t.Helper()
				for range seeded {
					_, err := c.CreateTransitGateway(ctx, &ec2sdk.CreateTransitGatewayInput{})
					require.NoError(t, err)
				}
			},
			page: func(ctx context.Context, c *ec2sdk.Client, m *int32, tok *string) ([]string, *string, error) {
				out, err := c.DescribeTransitGateways(
					ctx,
					&ec2sdk.DescribeTransitGatewaysInput{MaxResults: m, NextToken: tok},
				)
				if err != nil {
					return nil, nil, err
				}

				var ids []string
				for _, v := range out.TransitGateways {
					ids = append(ids, aws.ToString(v.TransitGatewayId))
				}

				return ids, out.NextToken, nil
			},
		},
		{
			name: "dhcp_options",
			seed: func(ctx context.Context, t *testing.T, c *ec2sdk.Client) {
				t.Helper()
				for range seeded {
					_, err := c.CreateDhcpOptions(
						ctx,
						&ec2sdk.CreateDhcpOptionsInput{DhcpConfigurations: []types.NewDhcpConfiguration{
							{Key: aws.String("domain-name"), Values: []string{"example.com"}},
						}},
					)
					require.NoError(t, err)
				}
			},
			page: func(ctx context.Context, c *ec2sdk.Client, m *int32, tok *string) ([]string, *string, error) {
				out, err := c.DescribeDhcpOptions(ctx, &ec2sdk.DescribeDhcpOptionsInput{MaxResults: m, NextToken: tok})
				if err != nil {
					return nil, nil, err
				}

				var ids []string
				for _, v := range out.DhcpOptions {
					ids = append(ids, aws.ToString(v.DhcpOptionsId))
				}

				return ids, out.NextToken, nil
			},
		},
		{
			name: "volumes",
			seed: func(ctx context.Context, t *testing.T, c *ec2sdk.Client) {
				t.Helper()
				for range seeded {
					_, err := c.CreateVolume(
						ctx,
						&ec2sdk.CreateVolumeInput{AvailabilityZone: aws.String("us-east-1a"), Size: aws.Int32(8)},
					)
					require.NoError(t, err)
				}
			},
			page: func(ctx context.Context, c *ec2sdk.Client, m *int32, tok *string) ([]string, *string, error) {
				out, err := c.DescribeVolumes(ctx, &ec2sdk.DescribeVolumesInput{MaxResults: m, NextToken: tok})
				if err != nil {
					return nil, nil, err
				}

				var ids []string
				for _, v := range out.Volumes {
					ids = append(ids, aws.ToString(v.VolumeId))
				}

				return ids, out.NextToken, nil
			},
		},
		{
			name: "launch_templates",
			seed: func(ctx context.Context, t *testing.T, c *ec2sdk.Client) {
				t.Helper()
				for i := range seeded {
					_, err := c.CreateLaunchTemplate(ctx, &ec2sdk.CreateLaunchTemplateInput{
						LaunchTemplateName: aws.String(fmt.Sprintf("lt-%d", i)),
						LaunchTemplateData: &types.RequestLaunchTemplateData{
							ImageId:      aws.String("ami-12345678"),
							InstanceType: types.InstanceTypeT3Micro,
						},
					})
					require.NoError(t, err)
				}
			},
			page: func(ctx context.Context, c *ec2sdk.Client, m *int32, tok *string) ([]string, *string, error) {
				out, err := c.DescribeLaunchTemplates(
					ctx,
					&ec2sdk.DescribeLaunchTemplatesInput{MaxResults: m, NextToken: tok},
				)
				if err != nil {
					return nil, nil, err
				}

				var ids []string
				for _, v := range out.LaunchTemplates {
					ids = append(ids, aws.ToString(v.LaunchTemplateId))
				}

				return ids, out.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestBackendAndClient(t)
			tt.seed(t.Context(), t, client)

			var (
				pages [][]string
				token *string
			)

			for range ec2sweep11LoopGuard {
				ids, next, err := tt.page(t.Context(), client, aws.Int32(2), token)
				require.NoError(t, err)

				pages = append(pages, ids)

				if next == nil {
					break
				}

				token = next
			}

			require.NotEmpty(t, pages)

			total := 0
			for _, p := range pages {
				total += len(p)
			}

			assertDisjointPages(t, pages, total)
			assert.GreaterOrEqual(t, total, seeded)
			assert.LessOrEqual(t, len(pages[0]), 2)
		})
	}
}

func TestDescribePagination_Bounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call    func(ctx context.Context, c *ec2sdk.Client) error
		name    string
		wantErr string
	}{
		{
			name: "hosts_below_min",
			call: func(ctx context.Context, c *ec2sdk.Client) error {
				_, err := c.DescribeHosts(ctx, &ec2sdk.DescribeHostsInput{MaxResults: aws.Int32(4)})

				return err
			},
			wantErr: "InvalidParameterValue",
		},
		{
			name: "hosts_ids_with_max_results",
			call: func(ctx context.Context, c *ec2sdk.Client) error {
				_, err := c.DescribeHosts(
					ctx,
					&ec2sdk.DescribeHostsInput{MaxResults: aws.Int32(5), HostIds: []string{"h-0123456789abcdef0"}},
				)

				return err
			},
			wantErr: "InvalidParameterCombination",
		},
		{
			name: "instance_status_ids_with_max_results",
			call: func(ctx context.Context, c *ec2sdk.Client) error {
				_, err := c.DescribeInstanceStatus(
					ctx,
					&ec2sdk.DescribeInstanceStatusInput{
						MaxResults:  aws.Int32(5),
						InstanceIds: []string{"i-0123456789abcdef0"},
					},
				)

				return err
			},
			wantErr: "InvalidParameterCombination",
		},
		{
			name: "network_interfaces_ids_with_max_results",
			call: func(ctx context.Context, c *ec2sdk.Client) error {
				_, err := c.DescribeNetworkInterfaces(
					ctx,
					&ec2sdk.DescribeNetworkInterfacesInput{
						MaxResults:          aws.Int32(5),
						NetworkInterfaceIds: []string{"eni-0123456789abcdef0"},
					},
				)

				return err
			},
			wantErr: "InvalidParameterCombination",
		},
		{
			name: "launch_templates_above_max",
			call: func(ctx context.Context, c *ec2sdk.Client) error {
				_, err := c.DescribeLaunchTemplates(
					ctx,
					&ec2sdk.DescribeLaunchTemplatesInput{MaxResults: aws.Int32(201)},
				)

				return err
			},
			wantErr: "InvalidParameterValue",
		},
		{
			name: "tags_below_min",
			call: func(ctx context.Context, c *ec2sdk.Client) error {
				_, err := c.DescribeTags(ctx, &ec2sdk.DescribeTagsInput{MaxResults: aws.Int32(4)})

				return err
			},
			wantErr: "InvalidParameterValue",
		},
		{
			name: "vpc_endpoints_clamped",
			call: func(ctx context.Context, c *ec2sdk.Client) error {
				_, err := c.DescribeVpcEndpoints(ctx, &ec2sdk.DescribeVpcEndpointsInput{MaxResults: aws.Int32(5000)})

				return err
			},
		},
		{
			name: "vpcs_bad_token",
			call: func(ctx context.Context, c *ec2sdk.Client) error {
				_, err := c.DescribeVpcs(ctx, &ec2sdk.DescribeVpcsInput{NextToken: aws.String("bogus")})

				return err
			},
			wantErr: "InvalidPaginationToken",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ec2.NewHandler(ec2.NewInMemoryBackend("123456789012", "us-east-1"))
			err := tt.call(t.Context(), newTestEC2Client(t, h))

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestDescribeRequestBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(ctx context.Context, t *testing.T, c *ec2sdk.Client)
		name string
	}{
		{
			name: "spot_price_history_end_time_in_past_drops_recent",
			run: func(ctx context.Context, t *testing.T, c *ec2sdk.Client) {
				t.Helper()

				all, err := c.DescribeSpotPriceHistory(ctx, &ec2sdk.DescribeSpotPriceHistoryInput{
					InstanceTypes: []types.InstanceType{types.InstanceTypeT3Micro},
				})
				require.NoError(t, err)

				end := time.Now().Add(-12 * time.Hour)
				bounded, err := c.DescribeSpotPriceHistory(ctx, &ec2sdk.DescribeSpotPriceHistoryInput{
					InstanceTypes: []types.InstanceType{types.InstanceTypeT3Micro},
					EndTime:       aws.Time(end),
				})
				require.NoError(t, err)

				for _, p := range bounded.SpotPriceHistory {
					assert.False(t, aws.ToTime(p.Timestamp).After(end))
				}

				assert.Less(t, len(bounded.SpotPriceHistory), len(all.SpotPriceHistory))
			},
		},
		{
			name: "traffic_mirror_rule_ids_select_rules",
			run: func(ctx context.Context, t *testing.T, c *ec2sdk.Client) {
				t.Helper()

				f, err := c.CreateTrafficMirrorFilter(ctx, &ec2sdk.CreateTrafficMirrorFilterInput{})
				require.NoError(t, err)

				ruleIDs := make([]string, 0, 3)

				for i := range 3 {
					r, ruleErr := c.CreateTrafficMirrorFilterRule(ctx, &ec2sdk.CreateTrafficMirrorFilterRuleInput{
						TrafficMirrorFilterId: f.TrafficMirrorFilter.TrafficMirrorFilterId,
						TrafficDirection:      types.TrafficDirectionIngress,
						RuleNumber:            aws.Int32(int32(100 + i)),
						RuleAction:            types.TrafficMirrorRuleActionAccept,
						DestinationCidrBlock:  aws.String("10.0.0.0/16"),
						SourceCidrBlock:       aws.String("10.1.0.0/16"),
					})
					require.NoError(t, ruleErr)

					ruleIDs = append(ruleIDs, aws.ToString(r.TrafficMirrorFilterRule.TrafficMirrorFilterRuleId))
				}

				out, err := c.DescribeTrafficMirrorFilterRules(ctx, &ec2sdk.DescribeTrafficMirrorFilterRulesInput{
					TrafficMirrorFilterRuleIds: ruleIDs[:2],
				})
				require.NoError(t, err)

				assert.Len(t, out.TrafficMirrorFilterRules, 2)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestBackendAndClient(t)
			tt.run(t.Context(), t, client)
		})
	}
}

func TestDescribeFilters_SharedEngineWildcards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(ctx context.Context, t *testing.T, c *ec2sdk.Client) []string
		name string
		want []string
	}{
		{
			name: "vpc_tag_wildcard",
			want: []string{"10.50.0.0/16", "10.51.0.0/16"},
			run: func(ctx context.Context, t *testing.T, c *ec2sdk.Client) []string {
				t.Helper()

				vpcs := map[string]string{"10.50.0.0/16": "web-1", "10.51.0.0/16": "web-2", "10.52.0.0/16": "db-1"}
				for cidr, name := range vpcs {
					_, err := c.CreateVpc(ctx, &ec2sdk.CreateVpcInput{
						CidrBlock: aws.String(cidr),
						TagSpecifications: []types.TagSpecification{{
							ResourceType: types.ResourceTypeVpc,
							Tags:         []types.Tag{{Key: aws.String("Name"), Value: aws.String(name)}},
						}},
					})
					require.NoError(t, err)
				}

				out, err := c.DescribeVpcs(
					ctx,
					&ec2sdk.DescribeVpcsInput{Filters: []types.Filter{wireFilter("tag:Name", "web-*")}},
				)
				require.NoError(t, err)

				got := make([]string, 0, len(out.Vpcs))
				for _, v := range out.Vpcs {
					got = append(got, aws.ToString(v.CidrBlock))
				}

				return got
			},
		},
		{
			name: "instance_type_wildcard",
			want: []string{"t3.micro"},
			run: func(ctx context.Context, t *testing.T, c *ec2sdk.Client) []string {
				t.Helper()

				for _, it := range []types.InstanceType{types.InstanceTypeT3Micro, types.InstanceTypeM5Large} {
					_, err := c.RunInstances(ctx, &ec2sdk.RunInstancesInput{
						ImageId: aws.String(
							"ami-12345678",
						),
						InstanceType: it,
						MinCount:     aws.Int32(1),
						MaxCount:     aws.Int32(1),
					})
					require.NoError(t, err)
				}

				out, err := c.DescribeInstances(
					ctx,
					&ec2sdk.DescribeInstancesInput{Filters: []types.Filter{wireFilter("instance-type", "t3.*")}},
				)
				require.NoError(t, err)

				var got []string
				for _, r := range out.Reservations {
					for _, i := range r.Instances {
						got = append(got, string(i.InstanceType))
					}
				}

				return got
			},
		},
		{
			name: "security_group_name_question",
			want: []string{"web-a", "web-b"},
			run: func(ctx context.Context, t *testing.T, c *ec2sdk.Client) []string {
				t.Helper()

				for _, n := range []string{"web-a", "web-b", "db-a"} {
					_, err := c.CreateSecurityGroup(
						ctx,
						&ec2sdk.CreateSecurityGroupInput{GroupName: aws.String(n), Description: aws.String("d")},
					)
					require.NoError(t, err)
				}

				out, err := c.DescribeSecurityGroups(
					ctx,
					&ec2sdk.DescribeSecurityGroupsInput{Filters: []types.Filter{wireFilter("group-name", "web-?")}},
				)
				require.NoError(t, err)

				got := make([]string, 0, len(out.SecurityGroups))
				for _, g := range out.SecurityGroups {
					got = append(got, aws.ToString(g.GroupName))
				}

				return got
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, client := newTestBackendAndClient(t)
			assert.ElementsMatch(t, tt.want, tt.run(t.Context(), t, client))
		})
	}
}
