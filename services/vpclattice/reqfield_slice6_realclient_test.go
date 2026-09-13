package vpclattice_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"
	vpclatticetypes "github.com/aws/aws-sdk-go-v2/service/vpclattice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/vpclattice"
)

// Test_Slice6_ServiceIdleTimeoutSeconds proves CreateService/UpdateService's
// IdleTimeoutSeconds (reqfielddiff slice 6, gopherstack-xhu2t; dropped:
// undeclared) is validated to the documented 60-600 range, defaults to 60,
// and round-trips through both ops.
func Test_Slice6_ServiceIdleTimeoutSeconds(t *testing.T) {
	t.Parallel()

	newClient := func(t *testing.T) *vpclatticesdk.Client {
		t.Helper()

		backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")

		return newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	}

	t.Run("defaults_to_60", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()

		out, err := client.CreateService(ctx, &vpclatticesdk.CreateServiceInput{
			Name: aws.String("svc-idle-default"),
		})
		require.NoError(t, err)
		assert.Equal(t, int32(60), aws.ToInt32(out.IdleTimeoutSeconds))
	})

	t.Run("honors_requested_value_on_create_and_update", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()

		created, err := client.CreateService(ctx, &vpclatticesdk.CreateServiceInput{
			Name:               aws.String("svc-idle-custom"),
			IdleTimeoutSeconds: aws.Int32(300),
		})
		require.NoError(t, err)
		assert.Equal(t, int32(300), aws.ToInt32(created.IdleTimeoutSeconds))

		updated, err := client.UpdateService(ctx, &vpclatticesdk.UpdateServiceInput{
			ServiceIdentifier:  created.Id,
			IdleTimeoutSeconds: aws.Int32(600),
		})
		require.NoError(t, err)
		assert.Equal(t, int32(600), aws.ToInt32(updated.IdleTimeoutSeconds))
	})

	t.Run("rejects_out_of_range_value", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)
		ctx := t.Context()

		_, err := client.CreateService(ctx, &vpclatticesdk.CreateServiceInput{
			Name:               aws.String("svc-idle-bad"),
			IdleTimeoutSeconds: aws.Int32(30),
		})
		require.Error(t, err)
	})
}

// Test_Slice6_ListServiceNetworkResourceAssociationsIncludeChildren proves
// IncludeChildren (reqfielddiff slice 6, gopherstack-xhu2t; dropped:
// undeclared) controls whether associations of a CHILD resource
// configuration are returned when listing by its GROUP parent.
func Test_Slice6_ListServiceNetworkResourceAssociationsIncludeChildren(t *testing.T) {
	t.Parallel()

	backend := vpclattice.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestVPCLatticeClient(t, vpclattice.NewHandler(backend))
	ctx := t.Context()

	group, err := client.CreateResourceConfiguration(ctx, &vpclatticesdk.CreateResourceConfigurationInput{
		Name: aws.String("rc-group"),
		Type: vpclatticetypes.ResourceConfigurationTypeGroup,
	})
	require.NoError(t, err)

	child, err := client.CreateResourceConfiguration(ctx, &vpclatticesdk.CreateResourceConfigurationInput{
		Name:                                 aws.String("rc-child"),
		Type:                                 vpclatticetypes.ResourceConfigurationTypeChild,
		ResourceConfigurationGroupIdentifier: group.Id,
	})
	require.NoError(t, err)

	sn, err := client.CreateServiceNetwork(ctx, &vpclatticesdk.CreateServiceNetworkInput{
		Name: aws.String("sn-includechildren"),
	})
	require.NoError(t, err)

	_, err = client.CreateServiceNetworkResourceAssociation(
		ctx, &vpclatticesdk.CreateServiceNetworkResourceAssociationInput{
			ServiceNetworkIdentifier:        sn.Id,
			ResourceConfigurationIdentifier: child.Id,
		},
	)
	require.NoError(t, err)

	withoutChildren, err := client.ListServiceNetworkResourceAssociations(
		ctx, &vpclatticesdk.ListServiceNetworkResourceAssociationsInput{
			ResourceConfigurationIdentifier: group.Id,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, withoutChildren.Items)

	withChildren, err := client.ListServiceNetworkResourceAssociations(
		ctx, &vpclatticesdk.ListServiceNetworkResourceAssociationsInput{
			ResourceConfigurationIdentifier: group.Id,
			IncludeChildren:                 aws.Bool(true),
		},
	)
	require.NoError(t, err)
	require.Len(t, withChildren.Items, 1)
	assert.Equal(t, aws.ToString(child.Id), aws.ToString(withChildren.Items[0].ResourceConfigurationId))
}
