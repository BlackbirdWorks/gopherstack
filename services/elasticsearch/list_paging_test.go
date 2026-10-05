package elasticsearch_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticsearchsdk "github.com/aws/aws-sdk-go-v2/service/elasticsearchservice"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elasticsearch"
)

// TestListOps_PageAndRejectBadTokens covers maxResults/nextToken (query members, elasticsearchservice@v1.45.4).
func TestListOps_PageAndRejectBadTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		list func(ctx context.Context, c *elasticsearchsdk.Client, size int32, tok *string) (int, *string, error)
		name string
	}{
		{
			name: "versions",
			list: func(ctx context.Context, c *elasticsearchsdk.Client, sz int32, tok *string) (int, *string, error) {
				out, err := c.ListElasticsearchVersions(ctx, &elasticsearchsdk.ListElasticsearchVersionsInput{
					MaxResults: sz, NextToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.ElasticsearchVersions), out.NextToken, nil
			},
		},
		{
			name: "instance_types",
			list: func(ctx context.Context, c *elasticsearchsdk.Client, sz int32, tok *string) (int, *string, error) {
				out, err := c.ListElasticsearchInstanceTypes(ctx, &elasticsearchsdk.ListElasticsearchInstanceTypesInput{
					ElasticsearchVersion: aws.String("7.10"), MaxResults: sz, NextToken: tok,
				})
				if err != nil {
					return 0, nil, err
				}

				return len(out.ElasticsearchInstanceTypes), out.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestElasticsearchClient(
				t,
				elasticsearch.NewHandler(elasticsearch.NewInMemoryBackend("123456789012", rtTestRegion)),
			)

			total, next, err := tt.list(t.Context(), c, 0, nil)
			require.NoError(t, err)
			require.GreaterOrEqual(t, total, 2)
			assert.Nil(t, next)

			n, next, err := tt.list(t.Context(), c, 1, nil)
			require.NoError(t, err)
			assert.Equal(t, 1, n)
			require.NotNil(t, next)

			rest, _, err := tt.list(t.Context(), c, 0, next)
			require.NoError(t, err)
			assert.Equal(t, total-1, rest)

			_, _, err = tt.list(t.Context(), c, 0, aws.String("%%bogus%%"))
			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, "ValidationException", apiErr.ErrorCode())
		})
	}
}
