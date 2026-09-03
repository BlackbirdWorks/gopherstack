package batch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

// Test_SDKRoundTrip_ComputeEnvironment_UnmanagedvCpus proves
// CreateComputeEnvironmentInput.UnmanagedvCpus/UpdateComputeEnvironmentInput.UnmanagedvCpus
// (batch@v1.68.4 api_op_CreateComputeEnvironment.go/api_op_UpdateComputeEnvironment.go --
// "the maximum number of vCPUs expected to be used for an unmanaged compute
// environment... only used for fair-share scheduling to reserve vCPU
// capacity for new share identifiers") were real request members this
// backend parsed nowhere at all -- grep for UnmanagedvCpus across
// services/batch/*.go returned zero hits before this fix, so a real
// client's value was silently dropped on both Create and Update and never
// echoed by DescribeComputeEnvironments' ComputeEnvironmentDetail.UnmanagedvCpus.
func Test_SDKRoundTrip_ComputeEnvironment_UnmanagedvCpus(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	ceName := "unmanaged-vcpus-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeUnmanaged,
		UnmanagedvCpus:         aws.Int32(16),
	})
	require.NoError(t, err)

	desc, err := client.DescribeComputeEnvironments(ctx, &batchsdk.DescribeComputeEnvironmentsInput{
		ComputeEnvironments: []string{ceName},
	})
	require.NoError(t, err)
	require.Len(t, desc.ComputeEnvironments, 1)
	assert.Equal(t, int32(16), aws.ToInt32(desc.ComputeEnvironments[0].UnmanagedvCpus),
		"UnmanagedvCpus was silently dropped by CreateComputeEnvironment before this backend had a field for it")

	_, err = client.UpdateComputeEnvironment(ctx, &batchsdk.UpdateComputeEnvironmentInput{
		ComputeEnvironment: aws.String(ceName),
		UnmanagedvCpus:     aws.Int32(32),
	})
	require.NoError(t, err)

	desc2, err := client.DescribeComputeEnvironments(ctx, &batchsdk.DescribeComputeEnvironmentsInput{
		ComputeEnvironments: []string{ceName},
	})
	require.NoError(t, err)
	require.Len(t, desc2.ComputeEnvironments, 1)
	assert.Equal(t, int32(32), aws.ToInt32(desc2.ComputeEnvironments[0].UnmanagedvCpus),
		"UnmanagedvCpus was silently dropped by UpdateComputeEnvironment before this backend had a field for it")
}

// Test_SDKRoundTrip_ComputeEnvironment_ContainerOrchestrationTypeAndUuid
// proves two more real ComputeEnvironmentDetail members
// (batch@v1.68.4 types/types.go) were entirely unmodeled:
//
//   - ContainerOrchestrationType ("The orchestration type of the compute
//     environment. The valid values are ECS (default) or EKS"), deterministic
//     from whether EksConfiguration was set at creation -- this backend
//     already tracks that.
//   - Uuid ("Unique identifier for the compute environment"), an opaque
//     AWS-generated identifier this backend never modeled or generated,
//     unlike every other resource's Id/Arn in this service (which all use
//     github.com/google/uuid, already an existing dependency here).
func Test_SDKRoundTrip_ComputeEnvironment_ContainerOrchestrationTypeAndUuid(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	ecsCEName := "orch-ecs-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ecsCEName),
		Type:                   types.CETypeManaged,
		ComputeResources: &types.ComputeResource{
			Type:     types.CRTypeFargate,
			MaxvCpus: aws.Int32(4),
			Subnets:  []string{"subnet-1"},
		},
	})
	require.NoError(t, err)

	eksCEName := "orch-eks-ce-" + uuid.NewString()[:8]
	_, err = client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(eksCEName),
		Type:                   types.CETypeManaged,
		EksConfiguration: &types.EksConfiguration{
			EksClusterArn:       aws.String("arn:aws:eks:us-east-1:000000000000:cluster/demo"),
			KubernetesNamespace: aws.String("batch"),
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeComputeEnvironments(ctx, &batchsdk.DescribeComputeEnvironmentsInput{
		ComputeEnvironments: []string{ecsCEName, eksCEName},
	})
	require.NoError(t, err)
	require.Len(t, desc.ComputeEnvironments, 2)

	byName := make(map[string]types.ComputeEnvironmentDetail, 2)
	for _, ce := range desc.ComputeEnvironments {
		byName[aws.ToString(ce.ComputeEnvironmentName)] = ce
	}

	ecsCE := byName[ecsCEName]
	assert.Equal(t, types.OrchestrationTypeEcs, ecsCE.ContainerOrchestrationType,
		"ContainerOrchestrationType was never derived/emitted for a non-EKS compute environment")
	assert.NotEmpty(t, aws.ToString(ecsCE.Uuid), "Uuid was never generated/emitted by CreateComputeEnvironment")

	eksCE := byName[eksCEName]
	assert.Equal(t, types.OrchestrationTypeEks, eksCE.ContainerOrchestrationType,
		"ContainerOrchestrationType was never derived/emitted for an EKS compute environment")
	assert.NotEmpty(t, aws.ToString(eksCE.Uuid), "Uuid was never generated/emitted by CreateComputeEnvironment")
	assert.NotEqual(
		t,
		aws.ToString(ecsCE.Uuid),
		aws.ToString(eksCE.Uuid),
		"Uuid must be unique per compute environment",
	)
}
