package elbv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestDeleteLoadBalancer_DeletionProtectionRoundTrip proves ModifyLoadBalancerAttributes'
// deletion_protection.enabled has an effect on DeleteLoadBalancer, not just on what
// DescribeLoadBalancerAttributes echoes back. Real AWS's DeleteLoadBalancer deserializer
// (elasticloadbalancingv2@v1.58.5 deserializers.go:1329) models "OperationNotPermitted" as
// a typed error for this op, and the op's own doc says "You can't delete a load balancer
// if deletion protection is enabled" -- before the fix, gopherstack stored the attribute
// on ModifyLoadBalancerAttributes and never read it back anywhere, so DeleteLoadBalancer
// always succeeded regardless.
func TestDeleteLoadBalancer_DeletionProtectionRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		lbName    string
		protected bool
		wantErr   bool
	}{
		{"protected blocks delete", "dp-rt-protected", true, true},
		{"unprotected allows delete", "dp-rt-unprotected", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elbv2.NewInMemoryBackend("000000000000", "us-east-1")
			h := elbv2.NewHandler(backend)
			client := newTestELBv2Client(t, h)
			ctx := t.Context()

			created, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
				Name:    aws.String(tt.lbName),
				Subnets: []string{"subnet-11111111", "subnet-22222222"},
			})
			require.NoError(t, err)
			lbArn := created.LoadBalancers[0].LoadBalancerArn

			_, err = client.ModifyLoadBalancerAttributes(ctx, &elbv2sdk.ModifyLoadBalancerAttributesInput{
				LoadBalancerArn: lbArn,
				Attributes: []types.LoadBalancerAttribute{
					{Key: aws.String("deletion_protection.enabled"), Value: aws.String(boolStr(tt.protected))},
				},
			})
			require.NoError(t, err)

			_, err = client.DeleteLoadBalancer(ctx, &elbv2sdk.DeleteLoadBalancerInput{
				LoadBalancerArn: lbArn,
			})

			if tt.wantErr {
				require.Error(t, err)

				var opNotPermitted *types.OperationNotPermittedException
				require.ErrorAs(t, err, &opNotPermitted,
					"expected a typed OperationNotPermittedException, got %v", err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}

	return "false"
}
