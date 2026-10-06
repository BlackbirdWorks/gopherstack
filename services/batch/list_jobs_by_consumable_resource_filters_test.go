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

func TestListJobsByConsumableResource_Filters(t *testing.T) {
	t.Parallel()

	h := batch.NewHandler(batch.NewInMemoryBackend("000000000000", rtTestRegion))
	client := newTestBatchClient(t, h)
	ctx := t.Context()

	ceName := "f-ce-" + uuid.NewString()[:8]
	_, err := client.CreateComputeEnvironment(ctx, &batchsdk.CreateComputeEnvironmentInput{
		ComputeEnvironmentName: aws.String(ceName),
		Type:                   types.CETypeManaged,
	})
	require.NoError(t, err)

	qName := "f-q-" + uuid.NewString()[:8]
	_, err = client.CreateJobQueue(ctx, &batchsdk.CreateJobQueueInput{
		JobQueueName: aws.String(qName),
		Priority:     aws.Int32(1),
		ComputeEnvironmentOrder: []types.ComputeEnvironmentOrder{
			{Order: aws.Int32(1), ComputeEnvironment: aws.String(ceName)},
		},
	})
	require.NoError(t, err)

	jdName := "f-jd-" + uuid.NewString()[:8]
	_, err = client.RegisterJobDefinition(ctx, &batchsdk.RegisterJobDefinitionInput{
		JobDefinitionName:   aws.String(jdName),
		Type:                types.JobDefinitionTypeContainer,
		ContainerProperties: &types.ContainerProperties{Image: aws.String("busybox")},
	})
	require.NoError(t, err)

	resName := "f-cr-" + uuid.NewString()[:8]
	_, err = client.CreateConsumableResource(ctx, &batchsdk.CreateConsumableResourceInput{
		ConsumableResourceName: aws.String(resName),
		TotalQuantity:          aws.Int64(50),
	})
	require.NoError(t, err)

	for _, name := range []string{"alpha-1", "alpha-2", "beta-1"} {
		_, err = client.SubmitJob(ctx, &batchsdk.SubmitJobInput{
			JobName:       aws.String(name),
			JobQueue:      aws.String(qName),
			JobDefinition: aws.String(jdName),
			ConsumableResourcePropertiesOverride: &types.ConsumableResourceProperties{
				ConsumableResourceList: []types.ConsumableResourceRequirement{
					{ConsumableResource: aws.String(resName), Quantity: aws.Int64(1)},
				},
			},
		})
		require.NoError(t, err)
	}

	all, err := client.ListJobsByConsumableResource(ctx, &batchsdk.ListJobsByConsumableResourceInput{
		ConsumableResource: aws.String(resName),
	})
	require.NoError(t, err)
	require.Len(t, all.Jobs, 3)

	status := aws.ToString(all.Jobs[0].JobStatus)

	tests := []struct {
		name    string
		filters []types.KeyValuesPair
		want    int
	}{
		{name: "none", want: 3},
		{
			name:    "name prefix",
			filters: []types.KeyValuesPair{{Name: aws.String("JOB_NAME"), Values: []string{"alpha*"}}},
			want:    2,
		},
		{
			name:    "name case insensitive",
			filters: []types.KeyValuesPair{{Name: aws.String("JOB_NAME"), Values: []string{"BETA-1"}}},
			want:    1,
		},
		{
			name:    "name values or",
			filters: []types.KeyValuesPair{{Name: aws.String("JOB_NAME"), Values: []string{"alpha-1", "beta-1"}}},
			want:    2,
		},
		{
			name:    "status match",
			filters: []types.KeyValuesPair{{Name: aws.String("JOB_STATUS"), Values: []string{status}}},
			want:    3,
		},
		{
			name:    "status miss",
			filters: []types.KeyValuesPair{{Name: aws.String("JOB_STATUS"), Values: []string{"FAILED"}}},
			want:    0,
		},
		{
			name: "entries and",
			filters: []types.KeyValuesPair{
				{Name: aws.String("JOB_NAME"), Values: []string{"alpha*"}},
				{Name: aws.String("JOB_STATUS"), Values: []string{"FAILED"}},
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, lErr := client.ListJobsByConsumableResource(ctx, &batchsdk.ListJobsByConsumableResourceInput{
				ConsumableResource: aws.String(resName),
				Filters:            tt.filters,
			})
			require.NoError(t, lErr)
			assert.Len(t, out.Jobs, tt.want)
		})
	}
}
