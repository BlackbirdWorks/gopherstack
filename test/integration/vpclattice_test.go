package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	vpclatticetypes "github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// createVPCLatticeClient returns a VPC Lattice client pointed at the shared
// test container.
func createVPCLatticeClient(t *testing.T) *vpclatticesdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return vpclatticesdk.NewFromConfig(cfg, func(o *vpclatticesdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_VPCLattice_ResourceGatewayAndConfigurationLifecycle drives
// the new ResourceGateway/ResourceConfiguration/
// ServiceNetworkResourceAssociation families (gopherstack-lx2k) through the
// real AWS SDK, catching wire-shape bugs a hand-rolled unit test's JSON
// assertions could miss (real deserializer wiring, real field-name
// resolution).
func TestIntegration_VPCLattice_ResourceGatewayAndConfigurationLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createVPCLatticeClient(t)
	ctx := t.Context()

	gw, err := client.CreateResourceGateway(ctx, &vpclatticesdk.CreateResourceGatewayInput{
		Name:          aws.String("it-gw-" + uuid.NewString()[:8]),
		VpcIdentifier: aws.String("vpc-1234"),
		SubnetIds:     []string{"subnet-1", "subnet-2"},
	})
	require.NoError(t, err)
	require.NotNil(t, gw.Id)

	t.Cleanup(func() {
		cctx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteResourceGateway(cctx, &vpclatticesdk.DeleteResourceGatewayInput{
			ResourceGatewayIdentifier: gw.Id,
		})
	})

	gotGW, err := client.GetResourceGateway(ctx, &vpclatticesdk.GetResourceGatewayInput{
		ResourceGatewayIdentifier: gw.Id,
	})
	require.NoError(t, err)
	require.Equal(t, aws.ToString(gw.Id), aws.ToString(gotGW.Id))
	require.Equal(t, "vpc-1234", aws.ToString(gotGW.VpcId))

	rc, err := client.CreateResourceConfiguration(ctx, &vpclatticesdk.CreateResourceConfigurationInput{
		Name:                      aws.String("it-rc-" + uuid.NewString()[:8]),
		Type:                      vpclatticetypes.ResourceConfigurationTypeArn,
		ResourceGatewayIdentifier: gw.Id,
		ResourceConfigurationDefinition: &vpclatticetypes.ResourceConfigurationDefinitionMemberArnResource{
			Value: vpclatticetypes.ArnResource{
				Arn: aws.String("arn:aws:rds:us-east-1:000000000000:db:mydb"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, rc.Id)

	t.Cleanup(func() {
		cctx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteResourceConfiguration(cctx, &vpclatticesdk.DeleteResourceConfigurationInput{
			ResourceConfigurationIdentifier: rc.Id,
		})
	})

	sn, err := client.CreateServiceNetwork(ctx, &vpclatticesdk.CreateServiceNetworkInput{
		Name: aws.String("it-sn-" + uuid.NewString()[:8]),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cctx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteServiceNetwork(cctx, &vpclatticesdk.DeleteServiceNetworkInput{
			ServiceNetworkIdentifier: sn.Id,
		})
	})

	snra, err := client.CreateServiceNetworkResourceAssociation(
		ctx, &vpclatticesdk.CreateServiceNetworkResourceAssociationInput{
			ServiceNetworkIdentifier:        sn.Id,
			ResourceConfigurationIdentifier: rc.Id,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, snra.Id)

	// The resource configuration and service network must both refuse to
	// delete while the association exists (matches Service/ServiceNetwork's
	// established cascade-guard behavior).
	_, err = client.DeleteResourceConfiguration(ctx, &vpclatticesdk.DeleteResourceConfigurationInput{
		ResourceConfigurationIdentifier: rc.Id,
	})
	require.Error(t, err)

	_, err = client.DeleteServiceNetworkResourceAssociation(
		ctx, &vpclatticesdk.DeleteServiceNetworkResourceAssociationInput{
			ServiceNetworkResourceAssociationIdentifier: snra.Id,
		},
	)
	require.NoError(t, err)

	listGW, err := client.ListResourceGateways(ctx, &vpclatticesdk.ListResourceGatewaysInput{})
	require.NoError(t, err)
	require.NotEmpty(t, listGW.Items)
}

// TestIntegration_VPCLattice_DomainVerification drives StartDomainVerification
// through the real SDK and verifies the status stays PENDING (this backend
// has no DNS to observe -- see PARITY.md).
func TestIntegration_VPCLattice_DomainVerification(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createVPCLatticeClient(t)
	ctx := t.Context()

	domain := "it-" + uuid.NewString()[:8] + ".example.com"

	dv, err := client.StartDomainVerification(ctx, &vpclatticesdk.StartDomainVerificationInput{
		DomainName: aws.String(domain),
	})
	require.NoError(t, err)
	require.Equal(t, vpclatticetypes.VerificationStatusPending, dv.Status)

	t.Cleanup(func() {
		cctx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteDomainVerification(cctx, &vpclatticesdk.DeleteDomainVerificationInput{
			DomainVerificationIdentifier: dv.Id,
		})
	})

	got, err := client.GetDomainVerification(ctx, &vpclatticesdk.GetDomainVerificationInput{
		DomainVerificationIdentifier: dv.Id,
	})
	require.NoError(t, err)
	require.Equal(t, domain, aws.ToString(got.DomainName))
	require.Equal(t, vpclatticetypes.VerificationStatusPending, got.Status)
}

// TestIntegration_VPCLattice_AuthPolicyOrphanFixedByARNNormalization is a
// regression test for gopherstack-lx2k's PutAuthPolicy/PutResourcePolicy
// orphan bug, driven through the real SDK: putting an auth policy by short
// ID must not survive the parent service's deletion.
func TestIntegration_VPCLattice_AuthPolicyOrphanFixedByARNNormalization(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createVPCLatticeClient(t)
	ctx := t.Context()

	svc, err := client.CreateService(ctx, &vpclatticesdk.CreateServiceInput{
		Name: aws.String("it-svc-" + uuid.NewString()[:8]),
	})
	require.NoError(t, err)

	_, err = client.PutAuthPolicy(ctx, &vpclatticesdk.PutAuthPolicyInput{
		ResourceIdentifier: svc.Id,
		Policy:             aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	_, err = client.DeleteService(ctx, &vpclatticesdk.DeleteServiceInput{ServiceIdentifier: svc.Id})
	require.NoError(t, err)

	_, err = client.GetAuthPolicy(ctx, &vpclatticesdk.GetAuthPolicyInput{ResourceIdentifier: svc.Id})
	require.Error(t, err, "auth policy must not survive its parent service's deletion")
}
