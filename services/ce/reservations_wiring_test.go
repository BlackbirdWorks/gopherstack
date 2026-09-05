package ce_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	costexplorersdk "github.com/aws/aws-sdk-go-v2/service/costexplorer"
	cetypes "github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ce"
)

// TestGetReservationCoverage_Pagination_PreservesSortOrder_RealClient proves
// NextPageToken pagination over CoveragesByTime is real and, critically,
// does not undo SortBy=Time DESCENDING: a naive re-sort-by-cursor-key
// pagination helper would silently flip the order back to ascending. A
// 130-day DAILY range forces more than the default 100-item page size.
func TestGetReservationCoverage_Pagination_PreservesSortOrder_RealClient(t *testing.T) {
	t.Parallel()

	h := ce.NewHandler(ce.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCEClient(t, h)

	end := time.Now().UTC().Truncate(24 * time.Hour)
	start := end.AddDate(0, 0, -130)
	period := &cetypes.DateInterval{
		Start: aws.String(start.Format("2006-01-02")),
		End:   aws.String(end.Format("2006-01-02")),
	}

	var (
		token    *string
		allStart []string
		pages    int
	)

	for {
		out, err := client.GetReservationCoverage(t.Context(), &costexplorersdk.GetReservationCoverageInput{
			TimePeriod:    period,
			Granularity:   cetypes.GranularityDaily,
			SortBy:        &cetypes.SortDefinition{Key: aws.String("Time"), SortOrder: cetypes.SortOrderDescending},
			NextPageToken: token,
		})
		require.NoError(t, err)

		pages++

		for _, c := range out.CoveragesByTime {
			allStart = append(allStart, aws.ToString(c.TimePeriod.Start))
		}

		if aws.ToString(out.NextPageToken) == "" {
			break
		}

		token = out.NextPageToken

		require.Less(t, pages, 10, "runaway pagination loop")
	}

	require.Greater(t, pages, 1, "130 daily buckets must force multiple pages")

	wantBuckets := int(end.Sub(start).Hours() / 24)

	seen := make(map[string]bool, len(allStart))
	for i, s := range allStart {
		assert.False(t, seen[s], "duplicate bucket %s across pages", s)
		seen[s] = true

		if i > 0 {
			assert.GreaterOrEqual(t, allStart[i-1], s,
				"DESCENDING order must be preserved across the page boundary, not re-sorted ascending")
		}
	}

	assert.Len(t, seen, wantBuckets,
		"every bucket must appear exactly once across the page walk -- a cursor off-by-one silently "+
			"drops the first record of every resumed page without ever duplicating one")
}
