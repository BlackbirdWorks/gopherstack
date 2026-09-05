package apprunner_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apprunnersdk "github.com/aws/aws-sdk-go-v2/service/apprunner"
	"github.com/aws/aws-sdk-go-v2/service/apprunner/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apprunner"
)

// TestListServices_FullWalk_NoDropsOrDuplicates walks ListServices to
// completion with a page size well below the seed count and asserts the
// union of every page is exactly the seeded set, with no duplicate or
// missing service ARN.
//
// ListServices sources its list from Table.Snapshot(), which -- unlike
// Table.All()/Table.Range() -- returns entries already sorted by the
// table's own primary key (pkgs/store.Table.Snapshot's doc comment), so
// its pre-pagination order is deterministic across calls without any
// additional sort in the service package. A single-page test cannot see a
// map-order regression here; walking to completion across repeated runs can.
func TestListServices_FullWalk_NoDropsOrDuplicates(t *testing.T) {
	t.Parallel()

	h := apprunner.NewInMemoryBackend("000000000000", apprunnerTagsRTRegion)
	client := newTestAppRunnerClient(t, apprunner.NewHandler(h))

	const seedCount = 25

	want := make(map[string]struct{}, seedCount)

	for i := range seedCount {
		out, err := client.CreateService(t.Context(), &apprunnersdk.CreateServiceInput{
			ServiceName: aws.String(fmt.Sprintf("svc-%02d", i)),
			SourceConfiguration: &types.SourceConfiguration{
				ImageRepository: &types.ImageRepository{
					ImageIdentifier:     aws.String("public.ecr.aws/nginx/nginx:latest"),
					ImageRepositoryType: types.ImageRepositoryTypeEcrPublic,
				},
			},
		})
		require.NoError(t, err)

		want[aws.ToString(out.Service.ServiceArn)] = struct{}{}
	}

	got := make(map[string]int, seedCount)

	var nextToken *string

	for page := 0; ; page++ {
		require.Lessf(t, page, seedCount, "walked more pages than seeded records without exhausting NextToken")

		out, err := client.ListServices(t.Context(), &apprunnersdk.ListServicesInput{
			MaxResults: aws.Int32(5),
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		for _, item := range out.ServiceSummaryList {
			got[aws.ToString(item.ServiceArn)]++
		}

		if aws.ToString(out.NextToken) == "" {
			break
		}

		nextToken = out.NextToken
	}

	require.Len(t, got, seedCount, "union of all pages must contain every seeded service exactly once")

	for arn, count := range got {
		_, seeded := want[arn]
		require.True(t, seeded, "page walk returned unseeded service arn %q", arn)
		require.Equal(t, 1, count, "service arn %q appeared on more than one page", arn)
	}

	for arn := range want {
		_, ok := got[arn]
		require.True(t, ok, "service arn %q was seeded but never appeared in the page walk", arn)
	}
}
