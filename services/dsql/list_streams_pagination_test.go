package dsql_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"
	"github.com/aws/aws-sdk-go-v2/service/dsql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListStreams_MaxResultsPaginates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		maxResults int32
		wantPages  int
	}{
		{name: "one_per_page", maxResults: 1, wantPages: 3},
		{name: "two_per_page", maxResults: 2, wantPages: 2},
		{name: "all_in_one_page", maxResults: 10, wantPages: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			ctx := t.Context()

			cluster, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{})
			require.NoError(t, err)

			const total = 3

			for range total {
				_, streamErr := client.CreateStream(ctx, &dsqlsdk.CreateStreamInput{
					ClusterIdentifier: cluster.Identifier,
					TargetDefinition:  testKinesisTarget(),
					Format:            types.StreamFormatJson,
					Ordering:          types.StreamOrderingUnordered,
				})
				require.NoError(t, streamErr)
			}

			pager := dsqlsdk.NewListStreamsPaginator(client, &dsqlsdk.ListStreamsInput{
				ClusterIdentifier: cluster.Identifier,
				MaxResults:        aws.Int32(tt.maxResults),
			})

			pages, seen := 0, 0

			for pager.HasMorePages() {
				page, pageErr := pager.NextPage(ctx)
				require.NoError(t, pageErr)
				assert.LessOrEqual(t, len(page.Streams), int(tt.maxResults))

				pages++
				seen += len(page.Streams)
			}

			assert.Equal(t, tt.wantPages, pages)
			assert.Equal(t, total, seen)
		})
	}
}
