package elasticsearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	essdk "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

// DescribeReservedElasticsearchInstancesInput.MaxResults (and its
// NextToken/DescribeReservedElasticsearchInstanceOfferingsInput sibling,
// elasticsearchservice@v1.45.4: "Set this value to limit the number of
// results returned. If not specified, defaults to 100.") were parsed nowhere
// -- every call returned the entire unbounded list.
func TestDescribeReservedElasticsearchInstances_Pagination_RealClient(t *testing.T) {
	t.Parallel()

	backend := elasticsearch.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestElasticsearchClient(t, elasticsearch.NewHandler(backend))
	ctx := t.Context()

	for range 3 {
		_, err := client.PurchaseReservedElasticsearchInstanceOffering(
			ctx, &essdk.PurchaseReservedElasticsearchInstanceOfferingInput{
				ReservedElasticsearchInstanceOfferingId: aws.String("offer-t3-small-1y"),
				ReservationName:                         aws.String("res"),
			},
		)
		require.NoError(t, err)
	}

	firstPage, err := client.DescribeReservedElasticsearchInstances(
		ctx, &essdk.DescribeReservedElasticsearchInstancesInput{MaxResults: 2},
	)
	require.NoError(t, err)
	require.Len(t, firstPage.ReservedElasticsearchInstances, 2, "MaxResults must bound the page size")
	require.NotNil(t, firstPage.NextToken, "more instances remain, so NextToken must be set")

	secondPage, err := client.DescribeReservedElasticsearchInstances(
		ctx, &essdk.DescribeReservedElasticsearchInstancesInput{
			MaxResults: 2,
			NextToken:  firstPage.NextToken,
		},
	)
	require.NoError(t, err)
	require.Len(t, secondPage.ReservedElasticsearchInstances, 1, "the last page holds the remaining instance")

	seen := map[string]bool{}
	for _, i := range firstPage.ReservedElasticsearchInstances {
		seen[aws.ToString(i.ReservedElasticsearchInstanceId)] = true
	}

	for _, i := range secondPage.ReservedElasticsearchInstances {
		require.False(t, seen[aws.ToString(i.ReservedElasticsearchInstanceId)],
			"second page must not repeat a first-page item")
	}
}
