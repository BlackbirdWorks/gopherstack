package appstream_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	appstreamsdk "github.com/aws/aws-sdk-go-v2/service/appstream"
	"github.com/aws/aws-sdk-go-v2/service/appstream/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appstream"
)

func createTestAppBlock(t *testing.T, c *appstreamsdk.Client, name string) string {
	t.Helper()

	out, err := c.CreateAppBlock(t.Context(), &appstreamsdk.CreateAppBlockInput{
		Name: aws.String(name),
		SourceS3Location: &types.S3Location{
			S3Bucket: aws.String("blocks"),
			S3Key:    aws.String(name + ".zip"),
		},
	})
	require.NoError(t, err)

	return aws.ToString(out.AppBlock.Arn)
}

func TestApplication_AppBlockArnMustExist_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		update bool
	}{
		{name: "create"},
		{name: "update", update: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestAppStreamClient(
				t,
				appstream.NewHandler(appstream.NewInMemoryBackend("000000000000", "us-east-1")),
			)
			missing := "arn:aws:appstream:us-east-1:000000000000:app-block/missing"
			blockArn := createTestAppBlock(t, c, "real-block")

			in := &appstreamsdk.CreateApplicationInput{
				Name:             aws.String("app"),
				LaunchPath:       aws.String("C:\\app.exe"),
				AppBlockArn:      aws.String(blockArn),
				Platforms:        []types.PlatformType{types.PlatformTypeWindows},
				InstanceFamilies: []string{"GENERAL_PURPOSE"},
				IconS3Location:   &types.S3Location{S3Bucket: aws.String("icons"), S3Key: aws.String("a.png")},
			}

			if !tt.update {
				in.AppBlockArn = aws.String(missing)
				_, err := c.CreateApplication(t.Context(), in)
				var nf *types.ResourceNotFoundException
				require.ErrorAs(t, err, &nf)

				return
			}

			_, err := c.CreateApplication(t.Context(), in)
			require.NoError(t, err)

			_, err = c.UpdateApplication(t.Context(), &appstreamsdk.UpdateApplicationInput{
				Name:        aws.String("app"),
				AppBlockArn: aws.String(missing),
			})
			var nf *types.ResourceNotFoundException
			require.ErrorAs(t, err, &nf)

			got, err := c.DescribeApplications(t.Context(), &appstreamsdk.DescribeApplicationsInput{})
			require.NoError(t, err)
			require.Len(t, got.Applications, 1)
			assert.Equal(t, blockArn, aws.ToString(got.Applications[0].AppBlockArn))
		})
	}
}
