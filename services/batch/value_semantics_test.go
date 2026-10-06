package batch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	batchsdk "github.com/aws/aws-sdk-go-v2/service/batch"
	"github.com/aws/aws-sdk-go-v2/service/batch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/batch"
)

func TestUpdateComputeEnvironment_ComputeResourcesMerge(t *testing.T) {
	t.Parallel()

	cases := []struct {
		update   *types.ComputeResourceUpdate
		name     string
		wantMax  int32
		wantMin  int32
		wantSubs int
	}{
		{
			name:    "min only keeps max and members",
			update:  &types.ComputeResourceUpdate{MinvCpus: aws.Int32(0)},
			wantMax: 16, wantMin: 0, wantSubs: 2,
		},
		{
			name:    "max only keeps min",
			update:  &types.ComputeResourceUpdate{MaxvCpus: aws.Int32(32)},
			wantMax: 32, wantMin: 4, wantSubs: 2,
		},
		{
			name:    "subnets replaced",
			update:  &types.ComputeResourceUpdate{Subnets: []string{"subnet-c"}},
			wantMax: 16, wantMin: 4, wantSubs: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestBatchClient(t, batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion)))
			ctx := t.Context()

			_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
				ComputeEnvironmentName: aws.String("ce-merge"),
				Type:                   types.CETypeManaged,
				ComputeResources: &types.ComputeResource{
					Type:          types.CRTypeEc2,
					MinvCpus:      aws.Int32(4),
					MaxvCpus:      aws.Int32(16),
					InstanceRole:  aws.String("ecsInstanceRole"),
					InstanceTypes: []string{"optimal"},
					Subnets:       []string{"subnet-a", "subnet-b"},
					Tags:          map[string]string{"k": "v"},
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateComputeEnvironment(ctx, &batchsdk.UpdateComputeEnvironmentInput{
				ComputeEnvironment: aws.String("ce-merge"), ComputeResources: tc.update,
			})
			require.NoError(t, err)

			out, err := client.DescribeComputeEnvironments(ctx, &batchsdk.DescribeComputeEnvironmentsInput{
				ComputeEnvironments: []string{"ce-merge"},
			})
			require.NoError(t, err)
			require.Len(t, out.ComputeEnvironments, 1)

			cr := out.ComputeEnvironments[0].ComputeResources
			require.NotNil(t, cr)
			assert.Equal(t, types.CRTypeEc2, cr.Type)
			assert.Equal(t, "ecsInstanceRole", aws.ToString(cr.InstanceRole))
			assert.Equal(t, []string{"optimal"}, cr.InstanceTypes)
			assert.Equal(t, map[string]string{"k": "v"}, cr.Tags)
			assert.Equal(t, tc.wantMax, aws.ToInt32(cr.MaxvCpus))
			assert.Equal(t, tc.wantMin, aws.ToInt32(cr.MinvCpus))
			assert.Len(t, cr.Subnets, tc.wantSubs)
		})
	}
}
