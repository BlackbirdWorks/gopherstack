package vpclattice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	"github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTargetGroup_DocumentedDefaults(t *testing.T) {
	t.Parallel()

	cases := []struct {
		cfg      *types.TargetGroupConfig
		name     string
		tgType   types.TargetGroupType
		wantIP   types.IpAddressType
		wantLmbd types.LambdaEventStructureVersion
		wantPort int32
	}{
		{
			name: "ip https", tgType: types.TargetGroupTypeIp, wantIP: types.IpAddressTypeIpv4, wantPort: 443,
			cfg: &types.TargetGroupConfig{
				Protocol: types.TargetGroupProtocolHttps, VpcIdentifier: aws.String("vpc-1"),
				HealthCheck: &types.HealthCheckConfig{},
			},
		},
		{
			name: "lambda", tgType: types.TargetGroupTypeLambda, wantLmbd: types.LambdaEventStructureVersionV1,
			cfg: &types.TargetGroupConfig{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestVPCLatticeClient(t, newTestHandler(t))
			created, err := c.CreateTargetGroup(t.Context(), &vpclatticesdk.CreateTargetGroupInput{
				Name: aws.String("tg"), Type: tc.tgType, Config: tc.cfg,
			})
			require.NoError(t, err)

			got, err := c.GetTargetGroup(
				t.Context(),
				&vpclatticesdk.GetTargetGroupInput{TargetGroupIdentifier: created.Id},
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantIP, got.Config.IpAddressType)
			assert.Equal(t, tc.wantLmbd, got.Config.LambdaEventStructureVersion)
			assert.Equal(t, tc.wantPort, aws.ToInt32(got.Config.Port))

			if tc.tgType == types.TargetGroupTypeIp {
				assert.Equal(t, types.TargetGroupProtocolVersionHttp1, got.Config.ProtocolVersion)
				hc := got.Config.HealthCheck
				require.NotNil(t, hc)
				assert.Equal(t, int32(30), aws.ToInt32(hc.HealthCheckIntervalSeconds))
				assert.Equal(t, int32(5), aws.ToInt32(hc.HealthCheckTimeoutSeconds))
				assert.Equal(t, int32(5), aws.ToInt32(hc.HealthyThresholdCount))
				assert.Equal(t, int32(2), aws.ToInt32(hc.UnhealthyThresholdCount))
				assert.Equal(t, "/", aws.ToString(hc.Path))
				assert.Equal(t, types.TargetGroupProtocolHttp, hc.Protocol)
			}
		})
	}
}

func TestTargetGroup_UpdateHealthCheckAppliesDefaults(t *testing.T) {
	t.Parallel()

	c := newTestVPCLatticeClient(t, newTestHandler(t))
	created, err := c.CreateTargetGroup(t.Context(), &vpclatticesdk.CreateTargetGroupInput{
		Name: aws.String("tg"), Type: types.TargetGroupTypeIp,
		Config: &types.TargetGroupConfig{
			Protocol: types.TargetGroupProtocolHttp, VpcIdentifier: aws.String("vpc-1"), Port: aws.Int32(8080),
		},
	})
	require.NoError(t, err)

	_, err = c.UpdateTargetGroup(t.Context(), &vpclatticesdk.UpdateTargetGroupInput{
		TargetGroupIdentifier: created.Id,
		HealthCheck:           &types.HealthCheckConfig{HealthyThresholdCount: aws.Int32(3)},
	})
	require.NoError(t, err)

	got, err := c.GetTargetGroup(t.Context(), &vpclatticesdk.GetTargetGroupInput{TargetGroupIdentifier: created.Id})
	require.NoError(t, err)

	hc := got.Config.HealthCheck
	require.NotNil(t, hc)
	assert.Equal(t, int32(3), aws.ToInt32(hc.HealthyThresholdCount))
	assert.Equal(t, int32(30), aws.ToInt32(hc.HealthCheckIntervalSeconds))
	assert.Equal(t, int32(8080), aws.ToInt32(hc.Port))
}

func TestService_UpdateWithoutCertificateKeepsIt(t *testing.T) {
	t.Parallel()

	c := newTestVPCLatticeClient(t, newTestHandler(t))
	created, err := c.CreateService(t.Context(), &vpclatticesdk.CreateServiceInput{
		Name: aws.String("svc"), CertificateArn: aws.String("arn:aws:acm:us-east-1:123456789012:certificate/c1"),
	})
	require.NoError(t, err)

	_, err = c.UpdateService(t.Context(), &vpclatticesdk.UpdateServiceInput{
		ServiceIdentifier: created.Id, AuthType: types.AuthTypeAwsIam,
	})
	require.NoError(t, err)

	got, err := c.GetService(t.Context(), &vpclatticesdk.GetServiceInput{ServiceIdentifier: created.Id})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:acm:us-east-1:123456789012:certificate/c1", aws.ToString(got.CertificateArn))
	assert.Equal(t, types.AuthTypeAwsIam, got.AuthType)
}
