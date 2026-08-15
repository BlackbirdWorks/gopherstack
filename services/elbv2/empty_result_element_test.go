package elbv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

// TestEmptyResultElement_RealClient covers ops whose real ELBv2 output shape has zero
// members but whose deserializer still calls decoder.GetElement("<Op>Result")
// (elasticloadbalancingv2@v1.58.5 deserializers.go, confirmed per-op: e.g.
// RemoveListenerCertificates at deserializers.go:5386,
// RemoveTrustStoreRevocations at deserializers.go:5628). gopherstack omitted the
// element entirely for these two, so every real SDK client failed deserialization
// with "deserialization failed: failed to decode response body ... node not found"
// even though the backend mutation succeeded. The assertion is exactly that the call
// deserializes without error -- there is nothing else to check on an empty output.
func TestEmptyResultElement_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T, client *elbv2sdk.Client, lbArn, listenerArn string) error
		name string
	}{
		{
			name: "removelistenercertificates",
			call: func(t *testing.T, client *elbv2sdk.Client, _, listenerArn string) error {
				t.Helper()

				_, err := client.RemoveListenerCertificates(t.Context(), &elbv2sdk.RemoveListenerCertificatesInput{
					ListenerArn: aws.String(listenerArn),
					Certificates: []types.Certificate{
						{CertificateArn: aws.String("arn:aws:acm:us-east-1:123456789012:certificate/nonexistent")},
					},
				})

				return err
			},
		},
		{
			name: "removetruststorerevocations",
			call: func(t *testing.T, client *elbv2sdk.Client, _, _ string) error {
				t.Helper()

				tsOut, err := client.CreateTrustStore(t.Context(), &elbv2sdk.CreateTrustStoreInput{
					Name:                         aws.String("empty-result-ts"),
					CaCertificatesBundleS3Bucket: aws.String("test-bucket"),
					CaCertificatesBundleS3Key:    aws.String("test-key.pem"),
				})
				require.NoError(t, err)

				_, err = client.RemoveTrustStoreRevocations(t.Context(), &elbv2sdk.RemoveTrustStoreRevocationsInput{
					TrustStoreArn: tsOut.TrustStores[0].TrustStoreArn,
					RevocationIds: []int64{1},
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
			h := elbv2.NewHandler(backend)
			client := newTestELBv2Client(t, h)
			ctx := t.Context()

			lbOut, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
				Name:    aws.String("empty-result-lb"),
				Subnets: []string{"subnet-11111111", "subnet-22222222"},
			})
			require.NoError(t, err)
			lbArn := aws.ToString(lbOut.LoadBalancers[0].LoadBalancerArn)

			lsOut, err := client.CreateListener(ctx, &elbv2sdk.CreateListenerInput{
				LoadBalancerArn: aws.String(lbArn),
				Protocol:        types.ProtocolEnumHttp,
				Port:            aws.Int32(80),
				DefaultActions: []types.Action{
					{
						Type: types.ActionTypeEnumFixedResponse,
						FixedResponseConfig: &types.FixedResponseActionConfig{
							StatusCode: aws.String("200"),
						},
					},
				},
			})
			require.NoError(t, err)
			listenerArn := aws.ToString(lsOut.Listeners[0].ListenerArn)

			require.NoError(t, tt.call(t, client, lbArn, listenerArn))
		})
	}
}
