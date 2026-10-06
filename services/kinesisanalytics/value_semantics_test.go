package kinesisanalytics_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kasdk "github.com/aws/aws-sdk-go-v2/service/kinesisanalytics"
	katypes "github.com/aws/aws-sdk-go-v2/service/kinesisanalytics/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateApplication_OutputUpdateKeepsOmittedARN(t *testing.T) {
	t.Parallel()

	const (
		oldStream = "arn:aws:kinesis:us-east-1:000000000000:stream/old"
		newStream = "arn:aws:kinesis:us-east-1:000000000000:stream/new"
		oldRole   = "arn:aws:iam::000000000000:role/old"
		newRole   = "arn:aws:iam::000000000000:role/new"
	)

	cases := []struct {
		update   katypes.KinesisStreamsOutputUpdate
		name     string
		wantARN  string
		wantRole string
	}{
		{
			name:    "stream only keeps role",
			update:  katypes.KinesisStreamsOutputUpdate{ResourceARNUpdate: aws.String(newStream)},
			wantARN: newStream, wantRole: oldRole,
		},
		{
			name:    "role only keeps stream",
			update:  katypes.KinesisStreamsOutputUpdate{RoleARNUpdate: aws.String(newRole)},
			wantARN: oldStream, wantRole: newRole,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandlerWithBackend(t)
			client := newTestKASDKClient(t, h)
			ctx := t.Context()

			_, err := client.CreateApplication(ctx, &kasdk.CreateApplicationInput{ApplicationName: aws.String("app")})
			require.NoError(t, err)

			_, err = client.AddApplicationOutput(ctx, &kasdk.AddApplicationOutputInput{
				ApplicationName: aws.String("app"), CurrentApplicationVersionId: aws.Int64(1),
				Output: &katypes.Output{
					Name:              aws.String("DEST"),
					DestinationSchema: &katypes.DestinationSchema{RecordFormatType: katypes.RecordFormatTypeJson},
					KinesisStreamsOutput: &katypes.KinesisStreamsOutput{
						ResourceARN: aws.String(oldStream), RoleARN: aws.String(oldRole),
					},
				},
			})
			require.NoError(t, err)

			desc, err := client.DescribeApplication(
				ctx,
				&kasdk.DescribeApplicationInput{ApplicationName: aws.String("app")},
			)
			require.NoError(t, err)
			require.Len(t, desc.ApplicationDetail.OutputDescriptions, 1)

			_, err = client.UpdateApplication(ctx, &kasdk.UpdateApplicationInput{
				ApplicationName:             aws.String("app"),
				CurrentApplicationVersionId: desc.ApplicationDetail.ApplicationVersionId,
				ApplicationUpdate: &katypes.ApplicationUpdate{
					OutputUpdates: []katypes.OutputUpdate{{
						OutputId:                   desc.ApplicationDetail.OutputDescriptions[0].OutputId,
						KinesisStreamsOutputUpdate: &tc.update,
					}},
				},
			})
			require.NoError(t, err)

			got, err := client.DescribeApplication(
				ctx,
				&kasdk.DescribeApplicationInput{ApplicationName: aws.String("app")},
			)
			require.NoError(t, err)

			out := got.ApplicationDetail.OutputDescriptions[0]
			require.NotNil(t, out.KinesisStreamsOutputDescription)
			assert.Equal(t, tc.wantARN, aws.ToString(out.KinesisStreamsOutputDescription.ResourceARN))
			assert.Equal(t, tc.wantRole, aws.ToString(out.KinesisStreamsOutputDescription.RoleARN))
			assert.Equal(t, "DEST", aws.ToString(out.Name))
		})
	}
}
