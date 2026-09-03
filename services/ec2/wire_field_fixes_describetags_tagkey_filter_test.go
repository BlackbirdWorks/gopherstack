package ec2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

// DescribeTagsInput.Filters documents "tag:<key>" as a real filter name
// (api_op_DescribeTags.go: `tag : - The key/value combination of the tag.
// For example, specify "tag:Owner" for the filter name and "TeamA" for the
// filter value...`), alongside the literal names key/resource-id/
// resource-type/value. handleDescribeTags's validDescribeTagsFilters
// (handler_tags.go) is an exact-match set containing only the four literal
// names, so any "tag:<key>" filter name -- a legitimate, documented pattern
// -- was rejected outright as "unknown filter name" instead of being
// evaluated.
func TestDescribeTags_TagKeyFilter_RealClient(t *testing.T) {
	t.Parallel()

	h := ec2.NewHandler(ec2.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestEC2Client(t, h)

	vpc1, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.0.0.0/16")})
	require.NoError(t, err)
	vpc2, err := client.CreateVpc(t.Context(), &ec2sdk.CreateVpcInput{CidrBlock: aws.String("10.1.0.0/16")})
	require.NoError(t, err)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{aws.ToString(vpc1.Vpc.VpcId)},
		Tags:      []types.Tag{{Key: aws.String("Owner"), Value: aws.String("TeamA")}},
	})
	require.NoError(t, err)
	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{aws.ToString(vpc2.Vpc.VpcId)},
		Tags:      []types.Tag{{Key: aws.String("Owner"), Value: aws.String("TeamB")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeTags(t.Context(), &ec2sdk.DescribeTagsInput{
		Filters: []types.Filter{
			{Name: aws.String("tag:Owner"), Values: []string{"TeamA"}},
		},
	})
	require.NoError(t, err, "tag:<key> is a documented DescribeTags filter name and must not be rejected as unknown")
	require.Len(t, out.Tags, 1)
	assert.Equal(t, aws.ToString(vpc1.Vpc.VpcId), aws.ToString(out.Tags[0].ResourceId))
	assert.Equal(t, "TeamA", aws.ToString(out.Tags[0].Value))
}
