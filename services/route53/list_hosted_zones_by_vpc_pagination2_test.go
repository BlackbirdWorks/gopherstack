package route53_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// TestListHostedZonesByVPC_PaginationStableAcrossDuplicateNames proves that
// ListHostedZonesByVPC's pagination is reproducible when several private
// hosted zones associated with the same VPC share a Name (real Route53
// allows duplicate zone names). The source (b.vpcAssociations) is a plain
// map keyed by zone ID (unspecified Go map order), and the result is sorted
// only by Name, which is not unique across zones sharing a name, so paging
// in small windows can drop or duplicate a zone at a page boundary.
func TestListHostedZonesByVPC_PaginationStableAcrossDuplicateNames(t *testing.T) {
	t.Parallel()

	const numZones = 8

	for iter := range 30 {
		b := route53.NewInMemoryBackend()

		wantIDs := make(map[string]bool, numZones)

		for i := range numZones {
			hz, err := b.CreateHostedZone(
				"tied.example.com.",
				fmt.Sprintf("caller-ref-%d-%d", iter, i),
				"",
				true,
				"",
				"vpc-shared",
				"us-east-1",
			)
			require.NoError(t, err)
			wantIDs[hz.ID] = true
		}

		got := make(map[string]int, numZones)

		var token string

		for {
			page, err := b.ListHostedZonesByVPC("vpc-shared", "us-east-1", token, 3)
			require.NoError(t, err)

			for _, z := range page.Data {
				got[z.ID]++
			}

			if page.Next == "" {
				break
			}

			token = page.Next
		}

		require.Lenf(t, got, numZones, "iter %d: distinct zone IDs across pages: %v", iter, got)

		for id, count := range got {
			require.Equalf(t, 1, count, "iter %d: zone %q appeared %d times across pages", iter, id, count)
		}

		for id := range wantIDs {
			require.Equalf(t, 1, got[id], "iter %d: zone %q missing from paginated results", iter, id)
		}
	}
}
