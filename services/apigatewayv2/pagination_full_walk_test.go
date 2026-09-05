package apigatewayv2_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigatewayv2sdk "github.com/aws/aws-sdk-go-v2/service/apigatewayv2"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
)

// TestGetDomainNames_FullWalk_NoDropsOrDuplicates walks GetDomainNames to
// completion with a page size well below the seed count and asserts the
// union of every page is exactly the seeded set, with no duplicate or
// missing domain name.
//
// GetDomainNames sources its list from Table.All() (an unspecified-order map
// walk -- see pkgs/store.Table.All's doc comment) and then sorts by
// DomainNameValue, which is also the table's own primary key (store_setup.go
// domainNameKeyFn), so the sort is total. A single-page test cannot see a
// map-order regression here; walking to completion across repeated runs can.
func TestGetDomainNames_FullWalk_NoDropsOrDuplicates(t *testing.T) {
	t.Parallel()

	h := apigatewayv2.NewInMemoryBackend()
	client := newTestAPIGatewayV2Client(t, apigatewayv2.NewHandler(h))

	const seedCount = 25

	want := make(map[string]struct{}, seedCount)

	for i := range seedCount {
		name := fmt.Sprintf("d%02d.example.com", i)
		_, err := client.CreateDomainName(t.Context(), &apigatewayv2sdk.CreateDomainNameInput{
			DomainName: aws.String(name),
		})
		require.NoError(t, err)

		want[name] = struct{}{}
	}

	got := make(map[string]int, seedCount)

	var nextToken *string

	for page := 0; ; page++ {
		require.Lessf(t, page, seedCount, "walked more pages than seeded records without exhausting NextToken")

		out, err := client.GetDomainNames(t.Context(), &apigatewayv2sdk.GetDomainNamesInput{
			MaxResults: aws.String("5"),
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		for _, item := range out.Items {
			got[aws.ToString(item.DomainName)]++
		}

		if aws.ToString(out.NextToken) == "" {
			break
		}

		nextToken = out.NextToken
	}

	require.Len(t, got, seedCount, "union of all pages must contain every seeded domain name exactly once")

	for name, count := range got {
		_, seeded := want[name]
		require.True(t, seeded, "page walk returned unseeded domain name %q", name)
		require.Equal(t, 1, count, "domain name %q appeared on more than one page", name)
	}

	for name := range want {
		_, ok := got[name]
		require.True(t, ok, "domain name %q was seeded but never appeared in the page walk", name)
	}
}
