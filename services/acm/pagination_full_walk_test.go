package acm_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	acmsdk "github.com/aws/aws-sdk-go-v2/service/acm"
	"github.com/aws/aws-sdk-go-v2/service/acm/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/acm"
)

// TestListCertificates_FullWalk_NoDropsOrDuplicates walks ListCertificates
// (SortBy=CREATED_AT, the one non-unique sort key this op exposes) to
// completion with a page size well below the seed count, and asserts the
// union of every page is exactly the seeded set, with no duplicate or
// missing certificate ARN.
//
// ListCertificates sources its list from b.certsByRegion.Get(region), a
// pkgs/store.Index lookup filtered to a single region (see
// pkgs/store/index.go: Index.Get's order is stable across calls, unlike a
// Table.All()/Range() map walk), then re-sorts by CreatedAt when SortBy is
// CREATED_AT. CreatedAt is not a unique key, but because the pre-sort input
// is already deterministic across calls, Go's sort is a deterministic
// function of that input, so ties resolve identically on every call even
// without a tiebreaker -- this is the "sort not total, but source isn't a
// map walk" case, and this test proves it holds up across repeated runs.
func TestListCertificates_FullWalk_NoDropsOrDuplicates(t *testing.T) {
	t.Parallel()

	h := acm.NewInMemoryBackend("000000000000", wireTestRegion)
	client := newTestACMClient(t, acm.NewHandler(h))

	const seedCount = 25

	want := make(map[string]struct{}, seedCount)

	for i := range seedCount {
		out, err := client.RequestCertificate(t.Context(), &acmsdk.RequestCertificateInput{
			DomainName: aws.String(fmt.Sprintf("d%02d.example.com", i)),
		})
		require.NoError(t, err)

		want[aws.ToString(out.CertificateArn)] = struct{}{}
	}

	got := make(map[string]int, seedCount)

	var nextToken *string

	for page := 0; ; page++ {
		require.Lessf(t, page, seedCount, "walked more pages than seeded records without exhausting NextToken")

		out, err := client.ListCertificates(t.Context(), &acmsdk.ListCertificatesInput{
			MaxItems:  aws.Int32(5),
			NextToken: nextToken,
			SortBy:    types.SortByCreatedAt,
			SortOrder: types.SortOrderDescending,
		})
		require.NoError(t, err)

		for _, item := range out.CertificateSummaryList {
			got[aws.ToString(item.CertificateArn)]++
		}

		if aws.ToString(out.NextToken) == "" {
			break
		}

		nextToken = out.NextToken
	}

	require.Len(t, got, seedCount, "union of all pages must contain every seeded certificate exactly once")

	for arn, count := range got {
		_, seeded := want[arn]
		require.True(t, seeded, "page walk returned unseeded certificate arn %q", arn)
		require.Equal(t, 1, count, "certificate arn %q appeared on more than one page", arn)
	}

	for arn := range want {
		_, ok := got[arn]
		require.True(t, ok, "certificate arn %q was seeded but never appeared in the page walk", arn)
	}
}
