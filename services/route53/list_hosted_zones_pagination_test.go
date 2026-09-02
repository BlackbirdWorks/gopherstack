package route53_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// TestListHostedZones_PaginationStableAcrossDuplicateNames proves that
// ListHostedZones's pagination is reproducible when several hosted zones
// share the same Name -- which real Route53 explicitly permits (distinct
// CallerReference, same domain name; e.g. a public + private pair, or
// several public zones for the same name). The source (b.zones) is a
// store.Table walked via All() (unspecified Go map order), and
// ListHostedZones sorts only by Name, which is not unique when zones share
// a name, so paging in small windows can drop or duplicate a zone at a page
// boundary across calls.
func TestListHostedZones_PaginationStableAcrossDuplicateNames(t *testing.T) {
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
				false,
				"",
				"",
				"",
			)
			require.NoError(t, err)
			wantIDs[hz.ID] = true
		}

		got := make(map[string]int, numZones)

		var marker string

		for {
			page, err := b.ListHostedZones(marker, 3, "", "")
			require.NoError(t, err)

			for _, z := range page.Data {
				got[z.ID]++
			}

			if page.Next == "" {
				break
			}

			marker = page.Next
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
