package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// TestDescribeSpotPriceHistory_AvailabilityZoneFilter_RealClient covers
// handleDescribeSpotPriceHistory. DescribeSpotPriceHistoryInput.AvailabilityZone
// is a scalar *string serialized as a bare "AvailabilityZone" key
// (ec2@v1.319.1 serializers.go:81147,
// awsEc2query_serializeOpDocumentDescribeSpotPriceHistoryInput), not an
// indexed list. The handler read it via parseMemberList(vals,
// "AvailabilityZone") (handler_spot_instances.go), which looks for
// "AvailabilityZone.1", "AvailabilityZone.2", ... -- a key a real client's
// scalar AZ filter never sends -- so the filter was always silently ignored
// and every call returned all 3 default AZs instead of narrowing to one.
func TestDescribeSpotPriceHistory_AvailabilityZoneFilter_RealClient(t *testing.T) {
	t.Parallel()

	client := newTestEC2Client(t, ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1")))
	ctx := t.Context()

	out, err := client.DescribeSpotPriceHistory(ctx, &ec2sdk.DescribeSpotPriceHistoryInput{
		AvailabilityZone: aws.String("us-east-1a"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.SpotPriceHistory,
		"expected spot price history for the requested AZ")

	for _, rec := range out.SpotPriceHistory {
		require.Equal(t, "us-east-1a", aws.ToString(rec.AvailabilityZone),
			"AvailabilityZone filter ignored - got a record from a different AZ")
	}
}
