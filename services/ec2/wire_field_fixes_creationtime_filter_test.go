package ec2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// DescribeImageUsageReportEntries' creation-time filter
// (api_op_DescribeImageUsageReportEntries.go: "The time when the report was
// created ... You can use a wildcard (*) ... which matches an entire day.")
// is documented to support an exact ISO 8601 timestamp match in addition to
// the day-wildcard. handler_filters.go's usageReportEntryMatchesFilter
// formats the entry's ReportCreationTime with time.RFC3339Nano for
// comparison, but handler_image_ops.go's toImageUsageReportEntryItem
// formats the SAME field with time.RFC3339 (no fractional seconds) when
// putting it on the wire. Since the underlying time.Time almost always
// carries a nonzero nanosecond component, the two formats disagree, so an
// exact-match creation-time filter built from the timestamp the API itself
// just returned never matches its own record.
func TestDescribeImageUsageReportEntries_CreationTimeExactFilter_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	img, err := client.RegisterImage(t.Context(), &ec2sdk.RegisterImageInput{Name: aws.String("creationtime-image")})
	require.NoError(t, err)

	_, err = client.RunInstances(t.Context(), &ec2sdk.RunInstancesInput{
		ImageId: img.ImageId, InstanceType: types.InstanceTypeT3Micro,
		MinCount: aws.Int32(1), MaxCount: aws.Int32(1),
	})
	require.NoError(t, err)

	report, err := client.CreateImageUsageReport(t.Context(), &ec2sdk.CreateImageUsageReportInput{
		ImageId: img.ImageId,
		ResourceTypes: []types.ImageUsageResourceTypeRequest{
			{ResourceType: aws.String("ec2:Instance")},
		},
	})
	require.NoError(t, err)

	unfiltered, err := client.DescribeImageUsageReportEntries(t.Context(), &ec2sdk.DescribeImageUsageReportEntriesInput{
		ReportIds: []string{aws.ToString(report.ReportId)},
	})
	require.NoError(t, err)
	require.Len(t, unfiltered.ImageUsageReportEntries, 1)
	creationTime := unfiltered.ImageUsageReportEntries[0].ReportCreationTime
	require.NotNil(t, creationTime)

	// The exact wire-format string this same server just emitted for this
	// field (see toImageUsageReportEntryItem, handler_image_ops.go).
	wireFormatted := creationTime.UTC().Format(time.RFC3339)

	filtered, err := client.DescribeImageUsageReportEntries(t.Context(), &ec2sdk.DescribeImageUsageReportEntriesInput{
		ReportIds: []string{aws.ToString(report.ReportId)},
		Filters:   []types.Filter{{Name: aws.String("creation-time"), Values: []string{wireFormatted}}},
	})
	require.NoError(t, err)
	require.Len(t, filtered.ImageUsageReportEntries, 1,
		"exact creation-time filter built from the API's own wire-format timestamp must match its own record")
	assert.Equal(t, "ec2:Instance", aws.ToString(filtered.ImageUsageReportEntries[0].ResourceType))

	// Sanity: an unrelated exact timestamp must still not match.
	other, err := client.DescribeImageUsageReportEntries(t.Context(), &ec2sdk.DescribeImageUsageReportEntriesInput{
		ReportIds: []string{aws.ToString(report.ReportId)},
		Filters: []types.Filter{
			{Name: aws.String("creation-time"), Values: []string{"1999-01-01T00:00:00Z"}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, other.ImageUsageReportEntries)
}
