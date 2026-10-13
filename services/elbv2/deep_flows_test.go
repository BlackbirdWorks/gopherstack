package elbv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

type placementResolver struct {
	instanceState map[string]string
	subnetAZ      map[string]string
	subnetVPC     map[string]string
}

func (r *placementResolver) SecurityGroupExists(string) bool { return true }

func (r *placementResolver) SubnetExists(id string) bool {
	_, ok := r.subnetAZ[id]

	return ok
}

func (r *placementResolver) SubnetVPC(id string) string { return r.subnetVPC[id] }
func (r *placementResolver) SubnetAZ(id string) string  { return r.subnetAZ[id] }

func (r *placementResolver) InstanceStateName(id string) (string, bool) {
	s, ok := r.instanceState[id]

	return s, ok
}

func newResolverBackedClient(t *testing.T, r elbv2.EC2Resolver) *elbv2sdk.Client {
	t.Helper()

	backend := elbv2.NewInMemoryBackend("123456789012", config.DefaultRegion)
	t.Cleanup(backend.Close)
	backend.SetEC2Resolver(r)

	return newTestELBv2Client(t, elbv2.NewHandler(backend))
}

func TestTargetGroupMatcherHttpCodeWire(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		matcher *elbv2types.Matcher
		want    string
	}{
		{name: "custom range", matcher: &elbv2types.Matcher{HttpCode: aws.String("200-399")}, want: "200-399"},
		{name: "default", matcher: nil, want: "200"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newResolverBackedClient(t, &placementResolver{})

			created, err := client.CreateTargetGroup(t.Context(), &elbv2sdk.CreateTargetGroupInput{
				Name: aws.String("tg-matcher"), Protocol: elbv2types.ProtocolEnumHttp,
				Port: aws.Int32(80), VpcId: aws.String("vpc-1"), Matcher: tt.matcher,
			})
			require.NoError(t, err)
			require.Len(t, created.TargetGroups, 1)
			assert.Equal(t, tt.want, aws.ToString(created.TargetGroups[0].Matcher.HttpCode))

			desc, err := client.DescribeTargetGroups(t.Context(), &elbv2sdk.DescribeTargetGroupsInput{
				Names: []string{"tg-matcher"},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, aws.ToString(desc.TargetGroups[0].Matcher.HttpCode))
		})
	}
}

func TestLoadBalancerSubnetPlacement(t *testing.T) {
	t.Parallel()

	client := newResolverBackedClient(t, &placementResolver{
		subnetAZ:  map[string]string{"subnet-a": "us-east-1c", "subnet-b": "us-east-1f"},
		subnetVPC: map[string]string{"subnet-a": "vpc-real", "subnet-b": "vpc-real"},
	})

	out, err := client.CreateLoadBalancer(t.Context(), &elbv2sdk.CreateLoadBalancerInput{
		Name: aws.String("placed"), Subnets: []string{"subnet-a", "subnet-b"},
	})
	require.NoError(t, err)

	lb := out.LoadBalancers[0]
	assert.Equal(t, "vpc-real", aws.ToString(lb.VpcId))
	require.Len(t, lb.AvailabilityZones, 2)
	assert.Equal(t, "us-east-1c", aws.ToString(lb.AvailabilityZones[0].ZoneName))
	assert.Equal(t, "us-east-1f", aws.ToString(lb.AvailabilityZones[1].ZoneName))
}

func TestTargetHealthFollowsInstanceState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		state      string
		wantState  elbv2types.TargetHealthStateEnum
		wantReason elbv2types.TargetHealthReasonEnum
		wantDesc   string
	}{
		{name: "stopped", state: "stopped", wantState: elbv2types.TargetHealthStateEnumUnused,
			wantReason: elbv2types.TargetHealthReasonEnumInvalidState, wantDesc: "Target is in the stopped state"},
		{name: "terminated", state: "terminated", wantState: elbv2types.TargetHealthStateEnumUnused,
			wantReason: elbv2types.TargetHealthReasonEnumInvalidState, wantDesc: "Target is in the terminated state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newResolverBackedClient(t, &placementResolver{instanceState: map[string]string{"i-1": tt.state}})

			tg, err := client.CreateTargetGroup(t.Context(), &elbv2sdk.CreateTargetGroupInput{
				Name: aws.String("tg-inst"), Protocol: elbv2types.ProtocolEnumHttp, Port: aws.Int32(80),
				VpcId: aws.String("vpc-1"), TargetType: elbv2types.TargetTypeEnumInstance,
			})
			require.NoError(t, err)

			tgArn := tg.TargetGroups[0].TargetGroupArn
			_, err = client.RegisterTargets(t.Context(), &elbv2sdk.RegisterTargetsInput{
				TargetGroupArn: tgArn,
				Targets:        []elbv2types.TargetDescription{{Id: aws.String("i-1")}},
			})
			require.NoError(t, err)

			health, err := client.DescribeTargetHealth(t.Context(), &elbv2sdk.DescribeTargetHealthInput{
				TargetGroupArn: tgArn,
			})
			require.NoError(t, err)
			require.Len(t, health.TargetHealthDescriptions, 1)

			th := health.TargetHealthDescriptions[0].TargetHealth
			assert.Equal(t, tt.wantState, th.State)
			assert.Equal(t, tt.wantReason, th.Reason)
			assert.Equal(t, tt.wantDesc, aws.ToString(th.Description))
		})
	}
}

func TestCreateRuleConditionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cond    elbv2types.RuleCondition
		wantErr bool
	}{
		{
			name: "valid path pattern",
			cond: elbv2types.RuleCondition{Field: aws.String("path-pattern"), Values: []string{"/api/*"}},
		},
		{
			name:    "path pattern with space",
			cond:    elbv2types.RuleCondition{Field: aws.String("path-pattern"), Values: []string{"/a b"}},
			wantErr: true,
		},
		{
			name:    "unknown field",
			cond:    elbv2types.RuleCondition{Field: aws.String("bogus"), Values: []string{"x"}},
			wantErr: true,
		},
		{
			name:    "host header without values",
			cond:    elbv2types.RuleCondition{Field: aws.String("host-header")},
			wantErr: true,
		},
		{
			name: "http header without name",
			cond: elbv2types.RuleCondition{
				Field:            aws.String("http-header"),
				HttpHeaderConfig: &elbv2types.HttpHeaderConditionConfig{Values: []string{"x"}},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newResolverBackedClient(t, &placementResolver{})

			lb, err := client.CreateLoadBalancer(
				t.Context(),
				&elbv2sdk.CreateLoadBalancerInput{Name: aws.String("rules-lb")},
			)
			require.NoError(t, err)

			tg, err := client.CreateTargetGroup(t.Context(), &elbv2sdk.CreateTargetGroupInput{
				Name: aws.String("rules-tg"), Protocol: elbv2types.ProtocolEnumHttp,
				Port: aws.Int32(80), VpcId: aws.String("vpc-1"),
			})
			require.NoError(t, err)

			action := elbv2types.Action{
				Type:           elbv2types.ActionTypeEnumForward,
				TargetGroupArn: tg.TargetGroups[0].TargetGroupArn,
			}
			l, err := client.CreateListener(t.Context(), &elbv2sdk.CreateListenerInput{
				LoadBalancerArn: lb.LoadBalancers[0].LoadBalancerArn, Protocol: elbv2types.ProtocolEnumHttp,
				Port: aws.Int32(80), DefaultActions: []elbv2types.Action{action},
			})
			require.NoError(t, err)

			_, err = client.CreateRule(t.Context(), &elbv2sdk.CreateRuleInput{
				ListenerArn: l.Listeners[0].ListenerArn, Priority: aws.Int32(1),
				Conditions: []elbv2types.RuleCondition{tt.cond}, Actions: []elbv2types.Action{action},
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationError")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestModifyLoadBalancerAttributeValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		key     string
		value   string
		wantErr bool
	}{
		{name: "idle timeout ok", key: "idle_timeout.timeout_seconds", value: "120"},
		{name: "idle timeout too large", key: "idle_timeout.timeout_seconds", value: "9999", wantErr: true},
		{name: "idle timeout not a number", key: "idle_timeout.timeout_seconds", value: "abc", wantErr: true},
		{name: "bool ok", key: "deletion_protection.enabled", value: "true"},
		{name: "bool invalid", key: "deletion_protection.enabled", value: "yes", wantErr: true},
		{name: "unknown key accepted", key: "future.setting", value: "x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newResolverBackedClient(t, &placementResolver{})

			lb, err := client.CreateLoadBalancer(
				t.Context(),
				&elbv2sdk.CreateLoadBalancerInput{Name: aws.String("attr-lb")},
			)
			require.NoError(t, err)

			_, err = client.ModifyLoadBalancerAttributes(t.Context(), &elbv2sdk.ModifyLoadBalancerAttributesInput{
				LoadBalancerArn: lb.LoadBalancers[0].LoadBalancerArn,
				Attributes: []elbv2types.LoadBalancerAttribute{
					{Key: aws.String(tt.key), Value: aws.String(tt.value)},
				},
			})
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationError")

				return
			}

			require.NoError(t, err)
		})
	}
}
