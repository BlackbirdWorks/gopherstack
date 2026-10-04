package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	"github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DeleteOTAUpdateInput.ForceDeleteAWSJob: a non-terminal job blocks the delete unless forced.
func TestDeleteOTAUpdate_ForceDeleteAWSJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		force      bool
		cancelJob  bool
		wantErr    bool
		wantJobGon bool
	}{
		{name: "in-progress-no-force", wantErr: true},
		{name: "in-progress-force", force: true, wantJobGon: true},
		{name: "canceled-job-no-force", cancelJob: true, wantJobGon: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newIoTTestClient(t)
			ctx := t.Context()

			_, err := client.CreateThing(ctx, &iotsdk.CreateThingInput{ThingName: aws.String("ota-thing")})
			require.NoError(t, err)

			created, err := client.CreateOTAUpdate(ctx, &iotsdk.CreateOTAUpdateInput{
				OtaUpdateId: aws.String("ota-1"),
				RoleArn:     aws.String("arn:aws:iam::000000000000:role/ota"),
				Targets:     []string{"arn:aws:iot:us-east-1:000000000000:thing/ota-thing"},
				Files:       []types.OTAUpdateFile{{FileName: aws.String("fw.bin")}},
			})
			require.NoError(t, err)

			job, err := client.DescribeJob(ctx, &iotsdk.DescribeJobInput{JobId: created.AwsIotJobId})
			require.NoError(t, err, "the OTA update must own a real job")
			assert.Equal(t, types.JobStatusInProgress, job.Job.Status)

			if tt.cancelJob {
				_, err = client.CancelJob(ctx, &iotsdk.CancelJobInput{JobId: created.AwsIotJobId})
				require.NoError(t, err)
			}

			_, err = client.DeleteOTAUpdate(ctx, &iotsdk.DeleteOTAUpdateInput{
				OtaUpdateId: aws.String("ota-1"), ForceDeleteAWSJob: tt.force,
			})

			_, getErr := client.GetOTAUpdate(ctx, &iotsdk.GetOTAUpdateInput{OtaUpdateId: aws.String("ota-1")})
			_, jobErr := client.DescribeJob(ctx, &iotsdk.DescribeJobInput{JobId: created.AwsIotJobId})

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "InvalidRequestException")
				require.NoError(t, getErr, "a rejected delete must keep the OTA update")
				require.NoError(t, jobErr)

				return
			}

			require.NoError(t, err)
			require.Error(t, getErr)
			assert.Equal(t, tt.wantJobGon, jobErr != nil)
		})
	}
}
