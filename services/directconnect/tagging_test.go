package directconnect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	directconnectsdk "github.com/aws/aws-sdk-go-v2/service/directconnect"
	"github.com/aws/aws-sdk-go-v2/service/directconnect/types"
	"github.com/stretchr/testify/require"
)

// TestTagResource_DuplicateTagKeys verifies DuplicateTagKeysException fires
// when the caller supplies the same tag key twice in one call.
func TestTagResource_DuplicateTagKeys(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	conn := createTestConnection(t, client)
	connARN := "arn:aws:directconnect:us-east-1:000000000000:dxcon/" + aws.ToString(conn.ConnectionId)

	_, err := client.TagResource(ctx, &directconnectsdk.TagResourceInput{
		ResourceArn: aws.String(connARN),
		Tags: []types.Tag{
			{Key: aws.String("dup"), Value: aws.String("1")},
			{Key: aws.String("dup"), Value: aws.String("2")},
		},
	})
	require.Error(t, err)

	var dupErr *types.DuplicateTagKeysException
	require.ErrorAs(t, err, &dupErr)
}

// TestCreateConnection_TooManyTags verifies TooManyTagsException fires when
// the caller exceeds this backend's documented (non-authoritative) 50-tag
// cap.
func TestCreateConnection_TooManyTags(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	const tooMany = 51

	tags := make([]types.Tag, 0, tooMany)
	for i := range tooMany {
		tags = append(
			tags,
			types.Tag{Key: aws.String(string(rune('a'+i%26)) + string(rune(i))), Value: aws.String("v")},
		)
	}

	_, err := client.CreateConnection(ctx, &directconnectsdk.CreateConnectionInput{
		Bandwidth:      aws.String("1Gbps"),
		ConnectionName: aws.String("too-many-tags-conn"),
		Location:       aws.String("EqDC2"),
		Tags:           tags,
	})
	require.Error(t, err)

	var tooManyErr *types.TooManyTagsException
	require.ErrorAs(t, err, &tooManyErr)
}

// TestDescribeTags_MultipleResourceKindsInOneBatch verifies DescribeTags'
// batch shape (ResourceArns []string, unlike the single-ARN
// ListTagsForResource pattern) across two different taggable resource
// kinds (Connection and Lag) in a single call.
func TestDescribeTags_MultipleResourceKindsInOneBatch(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)
	ctx := t.Context()

	conn := createTestConnection(t, client)
	connARN := "arn:aws:directconnect:us-east-1:000000000000:dxcon/" + aws.ToString(conn.ConnectionId)

	lag, err := client.CreateLag(ctx, &directconnectsdk.CreateLagInput{
		ConnectionsBandwidth: aws.String("1Gbps"),
		LagName:              aws.String("tag-batch-lag"),
		Location:             aws.String("EqDC2"),
		NumberOfConnections:  1,
	})
	require.NoError(t, err)
	lagARN := "arn:aws:directconnect:us-east-1:000000000000:dxlag/" + aws.ToString(lag.LagId)

	_, err = client.TagResource(ctx, &directconnectsdk.TagResourceInput{
		ResourceArn: aws.String(connARN),
		Tags:        []types.Tag{{Key: aws.String("k"), Value: aws.String("conn")}},
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &directconnectsdk.TagResourceInput{
		ResourceArn: aws.String(lagARN),
		Tags:        []types.Tag{{Key: aws.String("k"), Value: aws.String("lag")}},
	})
	require.NoError(t, err)

	out, err := client.DescribeTags(ctx, &directconnectsdk.DescribeTagsInput{
		ResourceArns: []string{connARN, lagARN},
	})
	require.NoError(t, err)
	require.Len(t, out.ResourceTags, 2)
}
