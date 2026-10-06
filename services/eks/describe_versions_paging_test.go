package eks_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeVersions_Paging checks maxResults/nextToken on DescribeAddonVersions and DescribeClusterVersions.
func TestDescribeVersions_Paging(t *testing.T) {
	t.Parallel()

	type listFn func(ctx context.Context, c *ekssdk.Client, size *int32, token *string) (int, *string, error)

	addons := func(ctx context.Context, c *ekssdk.Client, s *int32, tok *string) (int, *string, error) {
		o, err := c.DescribeAddonVersions(ctx, &ekssdk.DescribeAddonVersionsInput{MaxResults: s, NextToken: tok})
		if err != nil {
			return 0, nil, err
		}

		return len(o.Addons), o.NextToken, nil
	}
	clusters := func(ctx context.Context, c *ekssdk.Client, s *int32, tok *string) (int, *string, error) {
		o, err := c.DescribeClusterVersions(ctx, &ekssdk.DescribeClusterVersionsInput{MaxResults: s, NextToken: tok})
		if err != nil {
			return 0, nil, err
		}

		return len(o.ClusterVersions), o.NextToken, nil
	}

	tests := []struct {
		list listFn
		name string
		size int32
	}{
		{name: "addons", list: addons, size: 3},
		{name: "cluster_versions", list: clusters, size: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEKSClient(t, newTestEKSHandler(t))

			_, _, err := tt.list(t.Context(), client, aws.Int32(101), nil)
			require.ErrorContains(t, err, "InvalidParameterException")

			_, _, err = tt.list(t.Context(), client, nil, aws.String("!!bad"))
			require.ErrorContains(t, err, "InvalidParameterException")

			all, next, err := tt.list(t.Context(), client, nil, nil)
			require.NoError(t, err)
			assert.Nil(t, next)

			var (
				token *string
				seen  int
				pages int
			)

			for range 20 {
				n, nxt, listErr := tt.list(t.Context(), client, aws.Int32(tt.size), token)
				require.NoError(t, listErr)

				seen += n
				pages++

				if token = nxt; token == nil {
					break
				}

				assert.Equal(t, int(tt.size), n)
			}

			assert.Equal(t, all, seen)
			assert.Equal(t, (all+int(tt.size)-1)/int(tt.size), pages)
		})
	}
}
