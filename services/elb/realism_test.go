package elb_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbsdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

func lbInput(name string) *elbsdk.CreateLoadBalancerInput {
	return &elbsdk.CreateLoadBalancerInput{
		LoadBalancerName:  aws.String(name),
		AvailabilityZones: []string{"us-east-1a"},
		Listeners: []types.Listener{
			{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
		},
	}
}

func TestRealism_CreateLoadBalancerIdempotentAndValidated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate   func(in *elbsdk.CreateLoadBalancerInput)
		name     string
		wantCode string
	}{
		{name: "identical is idempotent", mutate: func(*elbsdk.CreateLoadBalancerInput) {}},
		{
			name: "different listener port conflicts",
			mutate: func(in *elbsdk.CreateLoadBalancerInput) {
				in.Listeners[0].InstancePort = aws.Int32(9090)
			},
			wantCode: "DuplicateLoadBalancerName",
		},
		{
			name: "zone from another region",
			mutate: func(in *elbsdk.CreateLoadBalancerInput) {
				in.LoadBalancerName = aws.String("other")
				in.AvailabilityZones = []string{"eu-west-1a"}
			},
			wantCode: "ValidationError",
		},
		{
			name: "duplicate listener ports",
			mutate: func(in *elbsdk.CreateLoadBalancerInput) {
				in.LoadBalancerName = aws.String("dup-ports")
				in.Listeners = append(in.Listeners, types.Listener{
					Protocol: aws.String("TCP"), LoadBalancerPort: 80, InstancePort: aws.Int32(81),
				})
			},
			wantCode: "DuplicateListener",
		},
		{
			name: "duplicate tag keys leave nothing behind",
			mutate: func(in *elbsdk.CreateLoadBalancerInput) {
				in.LoadBalancerName = aws.String("bad-tags")
				in.Tags = []types.Tag{
					{Key: aws.String("a"), Value: aws.String("1")},
					{Key: aws.String("a"), Value: aws.String("2")},
				}
			},
			wantCode: "DuplicateTagKeys",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestELBClient(t, elb.NewHandler(elb.NewInMemoryBackend("000000000000", rtTestRegion)))
			ctx := t.Context()

			_, err := client.CreateLoadBalancer(ctx, lbInput("base"))
			require.NoError(t, err)

			in := lbInput("base")
			tt.mutate(in)

			_, err = client.CreateLoadBalancer(ctx, in)
			if tt.wantCode == "" {
				require.NoError(t, err)

				return
			}

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), tt.wantCode)

			out, derr := client.DescribeLoadBalancers(ctx, &elbsdk.DescribeLoadBalancersInput{})
			require.NoError(t, derr)
			assert.Len(t, out.LoadBalancerDescriptions, 1)
		})
	}
}

func TestRealism_ErrorWording(t *testing.T) {
	t.Parallel()

	client := newTestELBClient(t, elb.NewHandler(elb.NewInMemoryBackend("000000000000", rtTestRegion)))
	ctx := t.Context()

	_, err := client.CreateLoadBalancer(ctx, lbInput("lb"))
	require.NoError(t, err)

	_, err = client.DescribeLoadBalancers(ctx, &elbsdk.DescribeLoadBalancersInput{LoadBalancerNames: []string{"nope"}})

	var nf *types.AccessPointNotFoundException
	require.ErrorAs(t, err, &nf)
	assert.Equal(t, "There is no ACTIVE Load Balancer named 'nope'", aws.ToString(nf.Message))

	_, err = client.DescribeLoadBalancerPolicies(ctx, &elbsdk.DescribeLoadBalancerPoliciesInput{
		LoadBalancerName: aws.String("lb"),
		PolicyNames:      []string{"missing"},
	})

	var pnf *types.PolicyNotFoundException
	require.ErrorAs(t, err, &pnf)

	_, err = client.DescribeLoadBalancers(ctx, &elbsdk.DescribeLoadBalancersInput{PageSize: aws.Int32(401)})

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ValidationError", apiErr.ErrorCode())

	_, err = client.DescribeLoadBalancers(ctx, &elbsdk.DescribeLoadBalancersInput{Marker: aws.String("!!bad")})
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ValidationError", apiErr.ErrorCode())
}
