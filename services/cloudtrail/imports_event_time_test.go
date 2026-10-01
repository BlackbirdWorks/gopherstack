package cloudtrail_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cloudtrailsdk "github.com/aws/aws-sdk-go-v2/service/cloudtrail"
	cttypes "github.com/aws/aws-sdk-go-v2/service/cloudtrail/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

func TestImport_EventTimeBoundsRoundTrip(t *testing.T) {
	t.Parallel()

	start := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	end := time.Date(2024, 2, 3, 4, 5, 6, 0, time.UTC)

	cases := []struct {
		start *time.Time
		end   *time.Time
		name  string
	}{
		{name: "both bounds", start: &start, end: &end},
		{name: "start only", start: &start},
		{name: "no bounds"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := cloudtrail.NewInMemoryBackend("123456789012", ctTagsRTRegion)
			client := newTestCloudTrailClient(t, cloudtrail.NewHandler(backend))

			eds, err := client.CreateEventDataStore(t.Context(), &cloudtrailsdk.CreateEventDataStoreInput{
				Name: aws.String("imp-eds"),
			})
			require.NoError(t, err)

			startOut, err := client.StartImport(t.Context(), &cloudtrailsdk.StartImportInput{
				Destinations:   []string{aws.ToString(eds.EventDataStoreArn)},
				StartEventTime: tc.start,
				EndEventTime:   tc.end,
				ImportSource: &cttypes.ImportSource{S3: &cttypes.S3ImportSource{
					S3LocationUri:         aws.String("s3://b/p"),
					S3BucketRegion:        aws.String(ctTagsRTRegion),
					S3BucketAccessRoleArn: aws.String("arn:aws:iam::123456789012:role/r"),
				}},
			})
			require.NoError(t, err)

			id := startOut.ImportId
			getOut, err := client.GetImport(t.Context(), &cloudtrailsdk.GetImportInput{ImportId: id})
			require.NoError(t, err)
			stopOut, err := client.StopImport(t.Context(), &cloudtrailsdk.StopImportInput{ImportId: id})
			require.NoError(t, err)

			type bounds struct{ start, end *time.Time }

			for _, got := range []bounds{
				{startOut.StartEventTime, startOut.EndEventTime},
				{getOut.StartEventTime, getOut.EndEventTime},
				{stopOut.StartEventTime, stopOut.EndEventTime},
			} {
				assert.Equal(t, tc.start == nil, got.start == nil)
				assert.Equal(t, tc.end == nil, got.end == nil)

				if tc.start != nil {
					assert.True(t, tc.start.Equal(*got.start))
				}

				if tc.end != nil {
					assert.True(t, tc.end.Equal(*got.end))
				}
			}
		})
	}
}
