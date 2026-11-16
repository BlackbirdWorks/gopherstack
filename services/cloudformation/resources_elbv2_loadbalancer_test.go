package cloudformation_test

import (
	"maps"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
)

type elbv2EC2Resolver struct{ ec2 ec2backend.Backend }

func (r elbv2EC2Resolver) SecurityGroupExists(id string) bool {
	return len(r.ec2.DescribeSecurityGroups([]string{id})) > 0
}

func (r elbv2EC2Resolver) SubnetExists(id string) bool {
	return len(r.ec2.DescribeSubnets([]string{id})) > 0
}

func (r elbv2EC2Resolver) SubnetVPC(id string) string {
	if s := r.ec2.DescribeSubnets([]string{id}); len(s) > 0 {
		return s[0].VPCID
	}

	return ""
}

func (r elbv2EC2Resolver) SubnetAZ(id string) string {
	if s := r.ec2.DescribeSubnets([]string{id}); len(s) > 0 {
		return s[0].AvailabilityZone
	}

	return ""
}

type elbv2StackEnv struct {
	cfn *cfnsdk.Client
	elb elbv2backend.StorageBackend
}

func (e elbv2StackEnv) createStack(t *testing.T, name, template string) {
	t.Helper()

	_, err := e.cfn.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
		StackName: aws.String(name), TemplateBody: aws.String(template),
	})
	require.NoError(t, err)

	desc, err := e.cfn.DescribeStacks(t.Context(), &cfnsdk.DescribeStacksInput{StackName: aws.String(name)})
	require.NoError(t, err)
	require.Len(t, desc.Stacks, 1)
	require.Equal(t, cfntypes.StackStatusCreateComplete, desc.Stacks[0].StackStatus,
		aws.ToString(desc.Stacks[0].StackStatusReason))
}

func newELBv2StackEnv(t *testing.T) elbv2StackEnv {
	t.Helper()

	backends := newMoreTypesServiceBackends(t)
	elb := backends.ELBv2.Backend
	mem, ok := elb.(*elbv2backend.InMemoryBackend)
	require.True(t, ok)
	mem.SetEC2Resolver(elbv2EC2Resolver{ec2: backends.EC2.Backend})

	creator := cloudformation.NewResourceCreator(backends)
	backend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)

	return elbv2StackEnv{cfn: newTestClientForBackend(t, backend), elb: elb}
}

func elbv2NetTemplate(lb map[string]any) map[string]any {
	return map[string]any{
		"Vpc": cfnRes("AWS::EC2::VPC", map[string]any{"CidrBlock": "10.40.0.0/16"}),
		"SubnetA": cfnRes("AWS::EC2::Subnet", map[string]any{
			"VpcId": cfnRef("Vpc"), "CidrBlock": "10.40.1.0/24",
		}),
		"SubnetB": cfnRes("AWS::EC2::Subnet", map[string]any{
			"VpcId": cfnRef("Vpc"), "CidrBlock": "10.40.2.0/24",
		}),
		"Sg": cfnRes("AWS::EC2::SecurityGroup", map[string]any{
			"GroupDescription": "lb", "VpcId": cfnRef("Vpc"),
		}),
		"Lb": cfnRes("AWS::ElasticLoadBalancingV2::LoadBalancer", lb),
	}
}

func TestCFN_ELBv2LoadBalancer_PropertiesReachBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lb         map[string]any
		name       string
		wantScheme string
		wantType   string
		wantSGs    int
		wantZones  int
	}{
		{
			name: "subnets_sgs_tags_attrs",
			lb: map[string]any{
				"Name":           "cfn-alb",
				"Scheme":         "internal",
				"Subnets":        []any{cfnRef("SubnetA"), cfnRef("SubnetB")},
				"SecurityGroups": []any{cfnRef("Sg")},
				"Tags":           cfnTags(map[string]string{"env": "test"}),
				"LoadBalancerAttributes": []any{
					map[string]any{"Key": "idle_timeout.timeout_seconds", "Value": "90"},
				},
			},
			wantScheme: "internal", wantType: "application", wantSGs: 1, wantZones: 2,
		},
		{
			name: "subnet_mappings",
			lb: map[string]any{
				"Name": "cfn-alb-map",
				"SubnetMappings": []any{
					map[string]any{"SubnetId": cfnRef("SubnetA")},
					map[string]any{"SubnetId": cfnRef("SubnetB")},
				},
			},
			wantScheme: "internet-facing", wantType: "application", wantZones: 2,
		},
		{
			name: "network_lb_single_zone",
			lb: map[string]any{
				"Name": "cfn-nlb", "Type": "network",
				"Subnets": []any{cfnRef("SubnetA")},
			},
			wantScheme: "internet-facing", wantType: "network", wantZones: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newELBv2StackEnv(t)
			env.createStack(t, "s", stackTemplate(t, elbv2NetTemplate(tt.lb), map[string]any{"Arn": cfnRef("Lb")}))

			lbs, err := env.elb.DescribeLoadBalancers(nil, nil)
			require.NoError(t, err)
			require.Len(t, lbs, 1)

			got := lbs[0]
			assert.Equal(t, tt.wantScheme, got.Scheme)
			assert.Equal(t, tt.wantType, got.Type)
			assert.Len(t, got.SecurityGroups, tt.wantSGs)
			assert.Len(t, got.AvailabilityZones, tt.wantZones)
			assert.NotEmpty(t, got.VpcID)

			if tt.wantSGs > 0 {
				assert.Equal(t, "90", got.Attributes["idle_timeout.timeout_seconds"])
				tags, terr := env.elb.DescribeTags([]string{got.LoadBalancerArn})
				require.NoError(t, terr)
				assert.Len(t, tags[got.LoadBalancerArn], 1)
			}
		})
	}
}

func TestCFN_ELBv2LoadBalancer_SingleZoneALBFailsStack(t *testing.T) {
	t.Parallel()

	tests := []struct {
		lb   map[string]any
		name string
	}{
		{name: "subnets", lb: map[string]any{"Name": "one-zone", "Subnets": []any{cfnRef("SubnetA")}}},
		{name: "subnet_mappings", lb: map[string]any{
			"Name": "one-zone", "SubnetMappings": []any{map[string]any{"SubnetId": cfnRef("SubnetA")}},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newELBv2StackEnv(t)
			tmpl := stackTemplate(t, elbv2NetTemplate(tt.lb), nil)

			_, err := env.cfn.CreateStack(t.Context(), &cfnsdk.CreateStackInput{
				StackName: aws.String("s"), TemplateBody: aws.String(tmpl), DisableRollback: aws.Bool(true),
			})
			require.NoError(t, err)

			ev, err := env.cfn.DescribeStackEvents(t.Context(), &cfnsdk.DescribeStackEventsInput{
				StackName: aws.String("s"),
			})
			require.NoError(t, err)

			var reason string

			for _, e := range ev.StackEvents {
				failed := e.ResourceStatus == cfntypes.ResourceStatusCreateFailed
				if aws.ToString(e.LogicalResourceId) == "Lb" && failed {
					reason = aws.ToString(e.ResourceStatusReason)
				}
			}

			assert.Contains(t, reason, "At least two subnets in two different Availability Zones")

			lbs, lerr := env.elb.DescribeLoadBalancers(nil, nil)
			require.NoError(t, lerr)
			assert.Empty(t, lbs)
		})
	}
}

func TestCFN_ELBv2LoadBalancer_UpdateInPlace(t *testing.T) {
	t.Parallel()

	base := map[string]any{
		"Name": "upd-alb", "Subnets": []any{cfnRef("SubnetA"), cfnRef("SubnetB")},
		"Tags": cfnTags(map[string]string{"a": "1"}),
	}

	tests := []struct {
		change map[string]any
		check  func(t *testing.T, env elbv2StackEnv, old, cur elbv2backend.LoadBalancer)
		name   string
	}{
		{
			name: "attributes_and_tags",
			change: map[string]any{
				"Tags": cfnTags(map[string]string{"b": "2"}),
				"LoadBalancerAttributes": []any{
					map[string]any{"Key": "idle_timeout.timeout_seconds", "Value": "120"},
				},
			},
			check: func(t *testing.T, env elbv2StackEnv, old, cur elbv2backend.LoadBalancer) {
				t.Helper()
				assert.Equal(t, old.LoadBalancerArn, cur.LoadBalancerArn)
				assert.Equal(t, "120", cur.Attributes["idle_timeout.timeout_seconds"])

				tags, err := env.elb.DescribeTags([]string{cur.LoadBalancerArn})
				require.NoError(t, err)
				require.Len(t, tags[cur.LoadBalancerArn], 1)
				assert.Equal(t, "b", tags[cur.LoadBalancerArn][0].Key)
			},
		},
		{
			name:   "security_groups",
			change: map[string]any{"SecurityGroups": []any{cfnRef("Sg")}},
			check: func(t *testing.T, _ elbv2StackEnv, old, cur elbv2backend.LoadBalancer) {
				t.Helper()
				assert.Equal(t, old.LoadBalancerArn, cur.LoadBalancerArn)
				assert.Len(t, cur.SecurityGroups, 1)
			},
		},
		{
			name:   "subnets_to_one_zone_rejected_keeps_old",
			change: map[string]any{"Subnets": []any{cfnRef("SubnetA")}},
			check: func(t *testing.T, _ elbv2StackEnv, old, cur elbv2backend.LoadBalancer) {
				t.Helper()
				assert.Equal(t, old.LoadBalancerArn, cur.LoadBalancerArn)
				assert.Len(t, cur.AvailabilityZones, 2)
			},
		},
		{
			name:   "scheme_replaces",
			change: map[string]any{"Name": "upd-alb-2", "Scheme": "internal"},
			check: func(t *testing.T, _ elbv2StackEnv, old, cur elbv2backend.LoadBalancer) {
				t.Helper()
				assert.NotEqual(t, old.LoadBalancerArn, cur.LoadBalancerArn)
				assert.Equal(t, "internal", cur.Scheme)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			env := newELBv2StackEnv(t)
			env.createStack(t, "s", stackTemplate(t, elbv2NetTemplate(base), nil))

			before, err := env.elb.DescribeLoadBalancers(nil, nil)
			require.NoError(t, err)
			require.Len(t, before, 1)

			next := maps.Clone(base)
			maps.Copy(next, tt.change)

			_, err = env.cfn.UpdateStack(t.Context(), &cfnsdk.UpdateStackInput{
				StackName:    aws.String("s"),
				TemplateBody: aws.String(stackTemplate(t, elbv2NetTemplate(next), nil)),
			})
			require.NoError(t, err)

			after, err := env.elb.DescribeLoadBalancers(nil, nil)
			require.NoError(t, err)
			require.Len(t, after, 1)

			tt.check(t, env, before[0], after[0])
		})
	}
}
