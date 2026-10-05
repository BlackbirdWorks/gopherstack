package elbv2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListenerMutualAuthentication_RealClient: the SDK sends IgnoreClientCertificateExpiry and
// AdvertiseTrustStoreCaNames (elbv2 v1.58.5 serializers.go:4197); both used to be dropped.
func TestListenerMutualAuthentication_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		advertise  elbv2types.AdvertiseTrustStoreCaNamesEnum
		wantStatus elbv2types.TrustStoreAssociationStatusEnum
		ignoreExp  bool
	}{
		{
			name: "advertise_on_ignore_expiry", advertise: elbv2types.AdvertiseTrustStoreCaNamesEnumOn,
			ignoreExp: true, wantStatus: elbv2types.TrustStoreAssociationStatusEnumActive,
		},
		{
			name: "advertise_off", advertise: elbv2types.AdvertiseTrustStoreCaNamesEnumOff,
			wantStatus: elbv2types.TrustStoreAssociationStatusEnumActive,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newTestELBv2Backend(t)

			lb, err := client.CreateLoadBalancer(ctx, &elbv2sdk.CreateLoadBalancerInput{
				Name: aws.String("mtls-lb"), Subnets: []string{"subnet-11111111", "subnet-22222222"},
			})
			require.NoError(t, err)

			tg, err := client.CreateTargetGroup(ctx, &elbv2sdk.CreateTargetGroupInput{
				Name: aws.String("mtls-tg"), Protocol: elbv2types.ProtocolEnumHttps, Port: aws.Int32(443),
			})
			require.NoError(t, err)

			ts, err := client.CreateTrustStore(ctx, &elbv2sdk.CreateTrustStoreInput{
				Name:                         aws.String("mtls-ts"),
				CaCertificatesBundleS3Bucket: aws.String("b"),
				CaCertificatesBundleS3Key:    aws.String("k.pem"),
			})
			require.NoError(t, err)

			created, err := client.CreateListener(ctx, &elbv2sdk.CreateListenerInput{
				LoadBalancerArn: lb.LoadBalancers[0].LoadBalancerArn,
				Protocol:        elbv2types.ProtocolEnumHttps,
				Port:            aws.Int32(443),
				Certificates: []elbv2types.Certificate{
					{CertificateArn: aws.String("arn:aws:acm:us-east-1:123456789012:certificate/c")},
				},
				DefaultActions: []elbv2types.Action{{
					Type: elbv2types.ActionTypeEnumForward, TargetGroupArn: tg.TargetGroups[0].TargetGroupArn,
				}},
				MutualAuthentication: &elbv2types.MutualAuthenticationAttributes{
					Mode:                          aws.String("verify"),
					TrustStoreArn:                 ts.TrustStores[0].TrustStoreArn,
					AdvertiseTrustStoreCaNames:    tt.advertise,
					IgnoreClientCertificateExpiry: aws.Bool(tt.ignoreExp),
				},
			})
			require.NoError(t, err)

			desc, err := client.DescribeListeners(ctx, &elbv2sdk.DescribeListenersInput{
				ListenerArns: []string{aws.ToString(created.Listeners[0].ListenerArn)},
			})
			require.NoError(t, err)

			for _, l := range []elbv2types.Listener{created.Listeners[0], desc.Listeners[0]} {
				require.NotNil(t, l.MutualAuthentication)
				assert.Equal(t, tt.advertise, l.MutualAuthentication.AdvertiseTrustStoreCaNames)
				assert.Equal(t, tt.wantStatus, l.MutualAuthentication.TrustStoreAssociationStatus)
				assert.Equal(t, tt.ignoreExp, aws.ToBool(l.MutualAuthentication.IgnoreClientCertificateExpiry))
			}
		})
	}
}
