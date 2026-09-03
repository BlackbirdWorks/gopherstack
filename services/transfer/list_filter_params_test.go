package transfer_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	transfersdk "github.com/aws/aws-sdk-go-v2/service/transfer"
	"github.com/aws/aws-sdk-go-v2/service/transfer/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestListTagsForResource_SDKRoundTrip_Pagination drives the real SDK client across two pages
// of ListTagsForResource and asserts the pages are disjoint and the marker round-trips. Before
// the fix, handleListTagsForResource ignored MaxResults/NextToken (both real
// ListTagsForResourceInput members) and always returned every tag in one unbounded page.
func TestListTagsForResource_SDKRoundTrip_Pagination(t *testing.T) {
	t.Parallel()

	backend := transfer.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	client := newTestTransferClient(t, transfer.NewHandler(backend))
	ctx := t.Context()

	conn, err := client.CreateConnector(ctx, &transfersdk.CreateConnectorInput{
		Url:        aws.String("sftp://example.com"),
		AccessRole: aws.String("arn:aws:iam::123456789012:role/transfer"),
		Tags: []types.Tag{
			{Key: aws.String("k1"), Value: aws.String("v1")},
			{Key: aws.String("k2"), Value: aws.String("v2")},
			{Key: aws.String("k3"), Value: aws.String("v3")},
			{Key: aws.String("k4"), Value: aws.String("v4")},
		},
	})
	require.NoError(t, err)

	connectorARN := fmt.Sprintf("arn:aws:transfer:us-east-1:123456789012:connector/%s", aws.ToString(conn.ConnectorId))

	page1, err := client.ListTagsForResource(ctx, &transfersdk.ListTagsForResourceInput{
		Arn:        aws.String(connectorARN),
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, page1.Tags, 2)
	require.NotNil(t, page1.NextToken)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.ListTagsForResource(ctx, &transfersdk.ListTagsForResourceInput{
		Arn:        aws.String(connectorARN),
		MaxResults: aws.Int32(2),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Tags, 2)

	seen := make(map[string]bool, 4)
	for _, tag := range page1.Tags {
		seen[aws.ToString(tag.Key)] = true
	}

	for _, tag := range page2.Tags {
		require.False(t, seen[aws.ToString(tag.Key)], "page 2 repeated tag %s from page 1", aws.ToString(tag.Key))
		seen[aws.ToString(tag.Key)] = true
	}

	require.Len(t, seen, 4)
}
