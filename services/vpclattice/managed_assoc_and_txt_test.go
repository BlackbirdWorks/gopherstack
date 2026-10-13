package vpclattice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

func TestServiceNetworkResourceAssociation_IsManagedAssociation_RealClient(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	ctx := t.Context()

	rc, err := client.CreateResourceConfiguration(ctx, &vpclatticesdk.CreateResourceConfigurationInput{
		Name: aws.String("rc-managed"),
		Type: "GROUP",
	})
	require.NoError(t, err)

	sn, err := client.CreateServiceNetwork(
		ctx,
		&vpclatticesdk.CreateServiceNetworkInput{Name: aws.String("sn-managed")},
	)
	require.NoError(t, err)

	created, err := client.CreateServiceNetworkResourceAssociation(
		ctx, &vpclatticesdk.CreateServiceNetworkResourceAssociationInput{
			ServiceNetworkIdentifier:        sn.Id,
			ResourceConfigurationIdentifier: rc.Id,
		},
	)
	require.NoError(t, err)

	got, err := client.GetServiceNetworkResourceAssociation(
		ctx,
		&vpclatticesdk.GetServiceNetworkResourceAssociationInput{
			ServiceNetworkResourceAssociationIdentifier: created.Id,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, got.IsManagedAssociation)
	assert.False(t, *got.IsManagedAssociation)

	list, err := client.ListServiceNetworkResourceAssociations(
		ctx, &vpclatticesdk.ListServiceNetworkResourceAssociationsInput{ServiceNetworkIdentifier: sn.Id},
	)
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	require.NotNil(t, list.Items[0].IsManagedAssociation)
	assert.False(t, *list.Items[0].IsManagedAssociation)
}

func TestDomainVerification_TxtMethodConfig_RealClient(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	ctx := t.Context()

	started, err := client.StartDomainVerification(ctx, &vpclatticesdk.StartDomainVerificationInput{
		DomainName: aws.String("example.com"),
	})
	require.NoError(t, err)
	require.NotNil(t, started.TxtMethodConfig)
	assert.NotEmpty(t, aws.ToString(started.TxtMethodConfig.Name))
	assert.NotEmpty(t, aws.ToString(started.TxtMethodConfig.Value))

	got, err := client.GetDomainVerification(ctx, &vpclatticesdk.GetDomainVerificationInput{
		DomainVerificationIdentifier: started.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, got.TxtMethodConfig)
	assert.Equal(t, started.TxtMethodConfig, got.TxtMethodConfig)

	list, err := client.ListDomainVerifications(ctx, &vpclatticesdk.ListDomainVerificationsInput{})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	require.NotNil(t, list.Items[0].TxtMethodConfig)
	assert.Equal(t, started.TxtMethodConfig.Value, list.Items[0].TxtMethodConfig.Value)
}
