package sagemaker_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

const smHyperpodRegion = "us-east-1"

// TestClusterSchedulerConfigLifecycle_RealClient drives the full
// Create/Describe/List/Update/Delete lifecycle through a real
// aws-sdk-go-v2 client. sagemaker@v1.263.2 api_op_DescribeClusterSchedulerConfig.go,
// api_op_UpdateClusterSchedulerConfig.go, and api_op_DeleteClusterSchedulerConfig.go
// all key ClusterSchedulerConfigId (not Name) -- a hand-built request body
// sending ClusterSchedulerConfigName would pass the old handler's decode
// silently, which is exactly how gopherstack-ihxk survived; the real client's
// own serializer can't be fooled that way.
func TestClusterSchedulerConfigLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smHyperpodRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	created, err := client.CreateClusterSchedulerConfig(t.Context(), &sagemakersdk.CreateClusterSchedulerConfigInput{
		Name:            aws.String("hp-config"),
		ClusterArn:      aws.String("arn:aws:sagemaker:us-east-1:000000000000:cluster/hp-cluster"),
		SchedulerConfig: &smtypes.SchedulerConfig{},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.ClusterSchedulerConfigId))

	id := aws.ToString(created.ClusterSchedulerConfigId)

	desc, err := client.DescribeClusterSchedulerConfig(
		t.Context(),
		&sagemakersdk.DescribeClusterSchedulerConfigInput{ClusterSchedulerConfigId: aws.String(id)},
	)
	require.NoError(t, err)
	assert.Equal(t, "hp-config", aws.ToString(desc.Name))
	assert.Equal(t, smtypes.SchedulerResourceStatusCreating, desc.Status)
	assert.Equal(t, int32(1), aws.ToInt32(desc.ClusterSchedulerConfigVersion))

	listOut, err := client.ListClusterSchedulerConfigs(t.Context(), &sagemakersdk.ListClusterSchedulerConfigsInput{})
	require.NoError(t, err)
	require.Len(t, listOut.ClusterSchedulerConfigSummaries, 1)
	assert.Equal(t, id, aws.ToString(listOut.ClusterSchedulerConfigSummaries[0].ClusterSchedulerConfigId))

	updated, err := client.UpdateClusterSchedulerConfig(t.Context(), &sagemakersdk.UpdateClusterSchedulerConfigInput{
		ClusterSchedulerConfigId: aws.String(id),
		TargetVersion:            aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), aws.ToInt32(updated.ClusterSchedulerConfigVersion))

	// A stale TargetVersion (the pre-update value) must be rejected.
	_, err = client.UpdateClusterSchedulerConfig(t.Context(), &sagemakersdk.UpdateClusterSchedulerConfigInput{
		ClusterSchedulerConfigId: aws.String(id),
		TargetVersion:            aws.Int32(1),
	})
	require.Error(t, err)

	var conflict *smtypes.ConflictException
	require.ErrorAs(t, err, &conflict)

	_, err = client.DeleteClusterSchedulerConfig(
		t.Context(),
		&sagemakersdk.DeleteClusterSchedulerConfigInput{ClusterSchedulerConfigId: aws.String(id)},
	)
	require.NoError(t, err)

	_, err = client.DescribeClusterSchedulerConfig(
		t.Context(),
		&sagemakersdk.DescribeClusterSchedulerConfigInput{ClusterSchedulerConfigId: aws.String(id)},
	)
	require.Error(t, err)

	var notFound *smtypes.ResourceNotFound
	require.ErrorAs(t, err, &notFound)
}

// TestClusterSchedulerConfigCreate_DuplicateName_RealClient mirrors
// TestClusterSchedulerConfigLifecycle_RealClient's duplicate-name case
// through the real client, asserting the wire error is ConflictException
// (sagemaker@v1.263.2 api_op_CreateClusterSchedulerConfig.go's error list),
// not the service's generic ValidationException.
func TestClusterSchedulerConfigCreate_DuplicateName_RealClient(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smHyperpodRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	in := &sagemakersdk.CreateClusterSchedulerConfigInput{
		Name:            aws.String("dup-config"),
		ClusterArn:      aws.String("arn:aws:sagemaker:us-east-1:000000000000:cluster/hp-cluster"),
		SchedulerConfig: &smtypes.SchedulerConfig{},
	}

	_, err := client.CreateClusterSchedulerConfig(t.Context(), in)
	require.NoError(t, err)

	_, err = client.CreateClusterSchedulerConfig(t.Context(), in)
	require.Error(t, err)

	var conflict *smtypes.ConflictException
	require.ErrorAs(t, err, &conflict)
}

// TestComputeQuotaLifecycle_RealClient is ClusterSchedulerConfig's lifecycle
// test mirrored for ComputeQuota — sagemaker@v1.263.2
// api_op_DescribeComputeQuota.go, api_op_UpdateComputeQuota.go, and
// api_op_DeleteComputeQuota.go all key ComputeQuotaId (not Name).
func TestComputeQuotaLifecycle_RealClient(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smHyperpodRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	created, err := client.CreateComputeQuota(t.Context(), &sagemakersdk.CreateComputeQuotaInput{
		Name:               aws.String("hp-quota"),
		ClusterArn:         aws.String("arn:aws:sagemaker:us-east-1:000000000000:cluster/hp-cluster"),
		ComputeQuotaConfig: &smtypes.ComputeQuotaConfig{},
		ComputeQuotaTarget: &smtypes.ComputeQuotaTarget{TeamName: aws.String("team-1")},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.ComputeQuotaId))

	id := aws.ToString(created.ComputeQuotaId)

	desc, err := client.DescribeComputeQuota(
		t.Context(),
		&sagemakersdk.DescribeComputeQuotaInput{ComputeQuotaId: aws.String(id)},
	)
	require.NoError(t, err)
	assert.Equal(t, "hp-quota", aws.ToString(desc.Name))
	assert.Equal(t, smtypes.SchedulerResourceStatusCreated, desc.Status)
	assert.Equal(t, int32(1), aws.ToInt32(desc.ComputeQuotaVersion))

	listOut, err := client.ListComputeQuotas(t.Context(), &sagemakersdk.ListComputeQuotasInput{})
	require.NoError(t, err)
	require.Len(t, listOut.ComputeQuotaSummaries, 1)
	assert.Equal(t, id, aws.ToString(listOut.ComputeQuotaSummaries[0].ComputeQuotaId))

	updated, err := client.UpdateComputeQuota(t.Context(), &sagemakersdk.UpdateComputeQuotaInput{
		ComputeQuotaId: aws.String(id),
		TargetVersion:  aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2), aws.ToInt32(updated.ComputeQuotaVersion))

	_, err = client.DeleteComputeQuota(
		t.Context(),
		&sagemakersdk.DeleteComputeQuotaInput{ComputeQuotaId: aws.String(id)},
	)
	require.NoError(t, err)

	_, err = client.DescribeComputeQuota(
		t.Context(),
		&sagemakersdk.DescribeComputeQuotaInput{ComputeQuotaId: aws.String(id)},
	)
	require.Error(t, err)

	var notFound *smtypes.ResourceNotFound
	require.ErrorAs(t, err, &notFound)
}

func TestComputeQuotaListPagination_RealClient(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smHyperpodRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	for i := range 3 {
		_, err := client.CreateComputeQuota(t.Context(), &sagemakersdk.CreateComputeQuotaInput{
			Name:               aws.String("hp-quota-" + string(rune('a'+i))),
			ClusterArn:         aws.String("arn:aws:sagemaker:us-east-1:000000000000:cluster/hp-cluster"),
			ComputeQuotaConfig: &smtypes.ComputeQuotaConfig{},
			ComputeQuotaTarget: &smtypes.ComputeQuotaTarget{TeamName: aws.String("team-1")},
		})
		require.NoError(t, err)
	}

	out, err := client.ListComputeQuotas(t.Context(), &sagemakersdk.ListComputeQuotasInput{})
	require.NoError(t, err)
	assert.Len(t, out.ComputeQuotaSummaries, 3)
}

// TestDescribeNotFound_RealClient asserts the not-found wire type for both
// resources is ResourceNotFound (sagemaker@v1.263.2's Describe/Update/Delete
// error lists), not the service's generic ValidationException that the old
// ErrClusterSchedulerConfigNotFound / ErrComputeQuotaNotFound sentinels used
// to wrap.
func TestDescribeNotFound_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		describe func(t *testing.T, client *sagemakersdk.Client) error
		name     string
	}{
		{
			name: "cluster scheduler config",
			describe: func(t *testing.T, client *sagemakersdk.Client) error {
				t.Helper()

				_, err := client.DescribeClusterSchedulerConfig(
					t.Context(),
					&sagemakersdk.DescribeClusterSchedulerConfigInput{
						ClusterSchedulerConfigId: aws.String("abcdef012345"),
					},
				)

				return err
			},
		},
		{
			name: "compute quota",
			describe: func(t *testing.T, client *sagemakersdk.Client) error {
				t.Helper()

				_, err := client.DescribeComputeQuota(
					t.Context(),
					&sagemakersdk.DescribeComputeQuotaInput{ComputeQuotaId: aws.String("abcdef012345")},
				)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sagemaker.NewInMemoryBackend("000000000000", smHyperpodRegion)
			client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

			err := tt.describe(t, client)
			require.Error(t, err)

			var notFound *smtypes.ResourceNotFound
			require.ErrorAs(t, err, &notFound)
		})
	}
}
