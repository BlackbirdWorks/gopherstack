package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateCatalog_TagsAndResourceArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{name: "with_tags", tags: map[string]string{"env": "prod"}},
		{name: "no_tags"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			ctx := t.Context()

			_, err := client.CreateCatalog(ctx, &gluesdk.CreateCatalogInput{
				Name:         aws.String("cat1"),
				CatalogInput: &types.CatalogInput{Description: aws.String("d")},
				Tags:         tt.tags,
			})
			require.NoError(t, err)

			got, err := client.GetCatalog(ctx, &gluesdk.GetCatalogInput{CatalogId: aws.String("cat1")})
			require.NoError(t, err)

			wantARN := "arn:aws:glue:" + testRegion + ":" + testAccountID + ":catalog/cat1"
			assert.Equal(t, wantARN, aws.ToString(got.Catalog.ResourceArn))

			out, err := client.GetTags(ctx, &gluesdk.GetTagsInput{ResourceArn: aws.String(wantARN)})
			require.NoError(t, err)

			if tt.tags == nil {
				assert.Empty(t, out.Tags)
			} else {
				assert.Equal(t, tt.tags, out.Tags)
			}

			_, err = client.TagResource(ctx, &gluesdk.TagResourceInput{
				ResourceArn: aws.String(wantARN), TagsToAdd: map[string]string{"k": "v"},
			})
			require.NoError(t, err)

			out, err = client.GetTags(ctx, &gluesdk.GetTagsInput{ResourceArn: aws.String(wantARN)})
			require.NoError(t, err)
			assert.Equal(t, "v", out.Tags["k"])
		})
	}
}
