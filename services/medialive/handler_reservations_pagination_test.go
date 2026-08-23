package medialive_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	medialivesdk "github.com/aws/aws-sdk-go-v2/service/medialive"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestListReservations_RealClientPaginator_AdvancesThroughAllPages drives a
// real aws-sdk-go-v2 paginator (which resends whatever NextToken the server
// last returned -- api_op_ListReservations.go's NextPage) through more than
// one page of reservations. Every List* handler in this service used to
// hardcode maxResults=0/nextToken="" regardless of the query params a real
// client sent (verified against
// awsRestjson1_serializeOpHttpBindingsListReservationsInput in
// aws-sdk-go-v2/service/medialive@v1.101.4's serializers.go, which binds
// both as httpQuery), so every subsequent page was identical to the first
// and the paginator's NextToken never changed -- an unbounded client loop
// (ListReservationsPaginator.HasMorePages never goes false without
// StopOnDuplicateToken, which defaults false) would never terminate.
func TestListReservations_RealClientPaginator_AdvancesThroughAllPages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestMediaLiveClient(t, h)

	const totalReservations = 25 // > defaultMaxResults (20), forces a second page

	for range totalReservations {
		_, err := client.PurchaseOffering(t.Context(), &medialivesdk.PurchaseOfferingInput{
			OfferingId: aws.String("87654321"),
			Count:      aws.Int32(1),
			Name:       aws.String("pagination-test"),
		})
		require.NoError(t, err)
	}

	paginator := medialivesdk.NewListReservationsPaginator(client, &medialivesdk.ListReservationsInput{})

	var pages [][]string
	for page := 0; page < totalReservations && paginator.HasMorePages(); page++ {
		out, err := paginator.NextPage(t.Context())
		require.NoError(t, err)

		ids := make([]string, len(out.Reservations))
		for i, r := range out.Reservations {
			ids[i] = *r.ReservationId
		}
		pages = append(pages, ids)

		if len(pages) >= 2 {
			break
		}
	}

	require.Len(t, pages, 2, "expected the paginator to reach a second page for %d reservations", totalReservations)
	assert.NotEqual(t, pages[0], pages[1],
		"second page returned the exact same reservations as the first -- "+
			"the server ignored the client's nextToken and restarted from page 0")

	for _, id := range pages[1] {
		_, wasOnFirstPage := indexOf(pages[0], id)
		assert.False(t, wasOnFirstPage, "reservation %s appeared on both page 1 and page 2", id)
	}
}

func indexOf(haystack []string, needle string) (int, bool) {
	for i, s := range haystack {
		if s == needle {
			return i, true
		}
	}

	return -1, false
}
