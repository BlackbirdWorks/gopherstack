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
	require.Len(t, out.ComputeQuotaSummaries, 3)
	// ComputeQuotaTarget is a required member of ComputeQuotaSummary --
	// sagemaker@v1.263.2 types/types.go:6153.
	assert.Equal(t, "team-1", aws.ToString(out.ComputeQuotaSummaries[0].ComputeQuotaTarget.TeamName))
}

// TestClusterSchedulerConfigCreate_StoresSchedulerConfigAndDescription_RealClient
// drives Create through a real client with a populated SchedulerConfig and
// Description and asserts Describe echoes them back -- gopherstack-kbxx: both
// were accepted on the wire and silently dropped.
func TestClusterSchedulerConfigCreate_StoresSchedulerConfigAndDescription_RealClient(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smHyperpodRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	created, err := client.CreateClusterSchedulerConfig(t.Context(), &sagemakersdk.CreateClusterSchedulerConfigInput{
		Name:        aws.String("hp-config-full"),
		ClusterArn:  aws.String("arn:aws:sagemaker:us-east-1:000000000000:cluster/hp-cluster"),
		Description: aws.String("my scheduler policy"),
		SchedulerConfig: &smtypes.SchedulerConfig{
			FairShare:           smtypes.FairShareEnabled,
			IdleResourceSharing: smtypes.IdleResourceSharingEnabled,
			PriorityClasses: []smtypes.PriorityClass{
				{Name: aws.String("team-a"), Weight: aws.Int32(50)},
			},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeClusterSchedulerConfig(t.Context(), &sagemakersdk.DescribeClusterSchedulerConfigInput{
		ClusterSchedulerConfigId: created.ClusterSchedulerConfigId,
	})
	require.NoError(t, err)

	assert.Equal(t, "my scheduler policy", aws.ToString(desc.Description))
	require.NotNil(t, desc.SchedulerConfig)
	assert.Equal(t, smtypes.FairShareEnabled, desc.SchedulerConfig.FairShare)
	assert.Equal(t, smtypes.IdleResourceSharingEnabled, desc.SchedulerConfig.IdleResourceSharing)
	require.Len(t, desc.SchedulerConfig.PriorityClasses, 1)
	assert.Equal(t, "team-a", aws.ToString(desc.SchedulerConfig.PriorityClasses[0].Name))
	assert.Equal(t, int32(50), aws.ToInt32(desc.SchedulerConfig.PriorityClasses[0].Weight))
}

// TestClusterSchedulerConfigUpdate_ReplacesSchedulerConfigAndDescription_RealClient
// asserts Update's SchedulerConfig and Description members (both accepted and
// dropped pre-fix) actually replace the stored values.
func TestClusterSchedulerConfigUpdate_ReplacesSchedulerConfigAndDescription_RealClient(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smHyperpodRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	created, err := client.CreateClusterSchedulerConfig(t.Context(), &sagemakersdk.CreateClusterSchedulerConfigInput{
		Name:            aws.String("hp-config-upd"),
		ClusterArn:      aws.String("arn:aws:sagemaker:us-east-1:000000000000:cluster/hp-cluster"),
		Description:     aws.String("original"),
		SchedulerConfig: &smtypes.SchedulerConfig{FairShare: smtypes.FairShareEnabled},
	})
	require.NoError(t, err)

	_, err = client.UpdateClusterSchedulerConfig(t.Context(), &sagemakersdk.UpdateClusterSchedulerConfigInput{
		ClusterSchedulerConfigId: created.ClusterSchedulerConfigId,
		TargetVersion:            aws.Int32(1),
		Description:              aws.String("updated"),
		SchedulerConfig: &smtypes.SchedulerConfig{
			FairShare:           smtypes.FairShareDisabled,
			IdleResourceSharing: smtypes.IdleResourceSharingEnabled,
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeClusterSchedulerConfig(t.Context(), &sagemakersdk.DescribeClusterSchedulerConfigInput{
		ClusterSchedulerConfigId: created.ClusterSchedulerConfigId,
	})
	require.NoError(t, err)

	assert.Equal(t, "updated", aws.ToString(desc.Description))
	require.NotNil(t, desc.SchedulerConfig)
	assert.Equal(t, smtypes.FairShareDisabled, desc.SchedulerConfig.FairShare)
	assert.Equal(t, smtypes.IdleResourceSharingEnabled, desc.SchedulerConfig.IdleResourceSharing)
}

// TestComputeQuotaCreate_StoresConfigTargetAndActivationState_RealClient drives
// Create through a real client with a populated ComputeQuotaConfig,
// ComputeQuotaTarget, Description and ActivationState and asserts Describe
// echoes them back -- gopherstack-kbxx: all four were accepted on the wire and
// silently dropped, and ComputeQuotaConfig/ComputeQuotaTarget are required by
// the real API (sagemaker@v1.263.2 api_op_CreateComputeQuota.go).
func TestComputeQuotaCreate_StoresConfigTargetAndActivationState_RealClient(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smHyperpodRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	created, err := client.CreateComputeQuota(t.Context(), &sagemakersdk.CreateComputeQuotaInput{
		Name:            aws.String("hp-quota-full"),
		ClusterArn:      aws.String("arn:aws:sagemaker:us-east-1:000000000000:cluster/hp-cluster"),
		Description:     aws.String("my compute quota"),
		ActivationState: smtypes.ActivationStateDisabled,
		ComputeQuotaTarget: &smtypes.ComputeQuotaTarget{
			TeamName:        aws.String("team-a"),
			FairShareWeight: aws.Int32(25),
		},
		ComputeQuotaConfig: &smtypes.ComputeQuotaConfig{
			PreemptTeamTasks: smtypes.PreemptTeamTasksNever,
			ComputeQuotaResources: []smtypes.ComputeQuotaResourceConfig{
				{InstanceType: smtypes.ClusterInstanceTypeMlC5Xlarge, Count: aws.Int32(2)},
			},
			ResourceSharingConfig: &smtypes.ResourceSharingConfig{
				Strategy:    smtypes.ResourceSharingStrategyLend,
				BorrowLimit: aws.Int32(75),
			},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeComputeQuota(t.Context(), &sagemakersdk.DescribeComputeQuotaInput{
		ComputeQuotaId: created.ComputeQuotaId,
	})
	require.NoError(t, err)

	assert.Equal(t, "my compute quota", aws.ToString(desc.Description))
	assert.Equal(t, smtypes.ActivationStateDisabled, desc.ActivationState)
	require.NotNil(t, desc.ComputeQuotaTarget)
	assert.Equal(t, "team-a", aws.ToString(desc.ComputeQuotaTarget.TeamName))
	assert.Equal(t, int32(25), aws.ToInt32(desc.ComputeQuotaTarget.FairShareWeight))
	require.NotNil(t, desc.ComputeQuotaConfig)
	assert.Equal(t, smtypes.PreemptTeamTasksNever, desc.ComputeQuotaConfig.PreemptTeamTasks)
	require.Len(t, desc.ComputeQuotaConfig.ComputeQuotaResources, 1)
	resource := desc.ComputeQuotaConfig.ComputeQuotaResources[0]
	assert.Equal(t, smtypes.ClusterInstanceTypeMlC5Xlarge, resource.InstanceType)
	assert.Equal(t, int32(2), aws.ToInt32(resource.Count))
	require.NotNil(t, desc.ComputeQuotaConfig.ResourceSharingConfig)
	assert.Equal(t, smtypes.ResourceSharingStrategyLend, desc.ComputeQuotaConfig.ResourceSharingConfig.Strategy)
	assert.Equal(t, int32(75), aws.ToInt32(desc.ComputeQuotaConfig.ResourceSharingConfig.BorrowLimit))
}

// TestComputeQuotaUpdate_ReplacesConfigTargetActivationDescription_RealClient
// asserts Update's ComputeQuotaConfig, ComputeQuotaTarget, ActivationState and
// Description members actually replace the stored values.
func TestComputeQuotaUpdate_ReplacesConfigTargetActivationDescription_RealClient(t *testing.T) {
	t.Parallel()

	backend := sagemaker.NewInMemoryBackend("000000000000", smHyperpodRegion)
	client := newTestSageMakerClient(t, sagemaker.NewHandler(backend))

	created, err := client.CreateComputeQuota(t.Context(), &sagemakersdk.CreateComputeQuotaInput{
		Name:               aws.String("hp-quota-upd"),
		ClusterArn:         aws.String("arn:aws:sagemaker:us-east-1:000000000000:cluster/hp-cluster"),
		ComputeQuotaConfig: &smtypes.ComputeQuotaConfig{},
		ComputeQuotaTarget: &smtypes.ComputeQuotaTarget{TeamName: aws.String("team-1")},
	})
	require.NoError(t, err)

	_, err = client.UpdateComputeQuota(t.Context(), &sagemakersdk.UpdateComputeQuotaInput{
		ComputeQuotaId:  created.ComputeQuotaId,
		TargetVersion:   aws.Int32(1),
		Description:     aws.String("updated quota"),
		ActivationState: smtypes.ActivationStateDisabled,
		ComputeQuotaTarget: &smtypes.ComputeQuotaTarget{
			TeamName:        aws.String("team-2"),
			FairShareWeight: aws.Int32(90),
		},
		ComputeQuotaConfig: &smtypes.ComputeQuotaConfig{
			PreemptTeamTasks: smtypes.PreemptTeamTasksLowerpriority,
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeComputeQuota(t.Context(), &sagemakersdk.DescribeComputeQuotaInput{
		ComputeQuotaId: created.ComputeQuotaId,
	})
	require.NoError(t, err)

	assert.Equal(t, "updated quota", aws.ToString(desc.Description))
	assert.Equal(t, smtypes.ActivationStateDisabled, desc.ActivationState)
	require.NotNil(t, desc.ComputeQuotaTarget)
	assert.Equal(t, "team-2", aws.ToString(desc.ComputeQuotaTarget.TeamName))
	assert.Equal(t, int32(90), aws.ToInt32(desc.ComputeQuotaTarget.FairShareWeight))
	require.NotNil(t, desc.ComputeQuotaConfig)
	assert.Equal(t, smtypes.PreemptTeamTasksLowerpriority, desc.ComputeQuotaConfig.PreemptTeamTasks)
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
