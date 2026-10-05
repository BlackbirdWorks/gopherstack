package cloudtrail_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

func TestCreateDashboard_AppliesTagsList(t *testing.T) {
	t.Parallel()

	client := newTestCloudTrailClient(
		t,
		cloudtrail.NewHandler(cloudtrail.NewInMemoryBackend("123456789012", "us-east-1")),
	)

	out, err := client.CreateDashboard(t.Context(), &cloudtrailsdk.CreateDashboardInput{
		Name:     aws.String("tagged-dash"),
		TagsList: []cttypes.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)
	require.Len(t, out.TagsList, 1)
	assert.Equal(t, "env", aws.ToString(out.TagsList[0].Key))

	tags, err := client.ListTags(t.Context(), &cloudtrailsdk.ListTagsInput{
		ResourceIdList: []string{aws.ToString(out.DashboardArn)},
	})
	require.NoError(t, err)
	require.Len(t, tags.ResourceTagList, 1)
	require.Len(t, tags.ResourceTagList[0].TagsList, 1)
	assert.Equal(t, "prod", aws.ToString(tags.ResourceTagList[0].TagsList[0].Value))
}

func TestStartImport_RetryByImportID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		importID   *string
		extra      func(in *cloudtrailsdk.StartImportInput)
		name       string
		wantStatus cttypes.ImportStatus
		wantCode   string
		stop       bool
	}{
		{name: "retry stopped", stop: true, wantStatus: cttypes.ImportStatusInitializing},
		{name: "retry running is rejected", stop: false, wantCode: "InvalidParameterException"},
		{name: "unknown id", stop: true, importID: aws.String("import-999999"), wantCode: "ImportNotFoundException"},
		{
			name: "id with destinations", stop: true, wantCode: "InvalidParameterCombinationException",
			extra: func(in *cloudtrailsdk.StartImportInput) {
				in.Destinations = []string{"arn:aws:cloudtrail:us-east-1:1:eventdatastore/x"}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestCloudTrailClient(
				t,
				cloudtrail.NewHandler(cloudtrail.NewInMemoryBackend("123456789012", "us-east-1")),
			)
			start := time.Unix(1_700_000_000, 0)

			started, err := client.StartImport(t.Context(), &cloudtrailsdk.StartImportInput{
				Destinations: []string{"arn:aws:cloudtrail:us-east-1:123456789012:eventdatastore/abc"},
				ImportSource: &cttypes.ImportSource{S3: &cttypes.S3ImportSource{
					S3LocationUri:         aws.String("s3://bucket/prefix"),
					S3BucketRegion:        aws.String("us-east-1"),
					S3BucketAccessRoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
				}},
				StartEventTime: &start,
				EndEventTime:   aws.Time(start.Add(time.Hour)),
			})
			require.NoError(t, err)

			if tt.stop {
				_, err = client.StopImport(t.Context(), &cloudtrailsdk.StopImportInput{ImportId: started.ImportId})
				require.NoError(t, err)
			}

			in := &cloudtrailsdk.StartImportInput{ImportId: started.ImportId}
			if tt.importID != nil {
				in.ImportId = tt.importID
			}

			if tt.extra != nil {
				tt.extra(in)
			}

			out, err := client.StartImport(t.Context(), in)

			if tt.wantCode != "" {
				var apiErr smithy.APIError

				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, aws.ToString(started.ImportId), aws.ToString(out.ImportId))
			assert.Equal(t, tt.wantStatus, out.ImportStatus)
		})
	}
}
