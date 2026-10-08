package datasync_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	datasyncsdk "github.com/aws/aws-sdk-go-v2/service/datasync"
	"github.com/aws/aws-sdk-go-v2/service/datasync/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeTask_ScheduleDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		updates      []types.ScheduleStatus
		wantDetails  bool
		wantDisabled bool
	}{
		{name: "never_updated"},
		{
			name:         "disabled",
			updates:      []types.ScheduleStatus{types.ScheduleStatusDisabled},
			wantDetails:  true,
			wantDisabled: true,
		},
		{
			name:        "reenabled",
			updates:     []types.ScheduleStatus{types.ScheduleStatusDisabled, types.ScheduleStatusEnabled},
			wantDetails: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			src, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
				ServerHostname: aws.String("src.example.com"), BucketName: aws.String("src"),
			})
			require.NoError(t, err)

			dst, err := client.CreateLocationObjectStorage(ctx, &datasyncsdk.CreateLocationObjectStorageInput{
				ServerHostname: aws.String("dst.example.com"), BucketName: aws.String("dst"),
			})
			require.NoError(t, err)

			task, err := client.CreateTask(ctx, &datasyncsdk.CreateTaskInput{
				SourceLocationArn:      src.LocationArn,
				DestinationLocationArn: dst.LocationArn,
				Schedule: &types.TaskSchedule{
					ScheduleExpression: aws.String("rate(12 hours)"),
					Status:             types.ScheduleStatusEnabled,
				},
			})
			require.NoError(t, err)

			for _, st := range tt.updates {
				_, err = client.UpdateTask(ctx, &datasyncsdk.UpdateTaskInput{
					TaskArn: task.TaskArn,
					Schedule: &types.TaskSchedule{
						ScheduleExpression: aws.String("rate(12 hours)"),
						Status:             st,
					},
				})
				require.NoError(t, err)
			}

			got, err := client.DescribeTask(ctx, &datasyncsdk.DescribeTaskInput{TaskArn: task.TaskArn})
			require.NoError(t, err)

			if !tt.wantDetails {
				assert.Nil(t, got.ScheduleDetails)

				return
			}

			require.NotNil(t, got.ScheduleDetails)
			assert.NotNil(t, got.ScheduleDetails.StatusUpdateTime)

			if tt.wantDisabled {
				assert.Equal(t, types.ScheduleDisabledByUser, got.ScheduleDetails.DisabledBy)
				assert.Equal(t, "Manually disabled by user.", aws.ToString(got.ScheduleDetails.DisabledReason))
			} else {
				assert.Empty(t, got.ScheduleDetails.DisabledBy)
				assert.Nil(t, got.ScheduleDetails.DisabledReason)
			}
		})
	}
}
