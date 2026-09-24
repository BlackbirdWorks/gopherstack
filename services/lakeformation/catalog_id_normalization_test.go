package lakeformation_test

import (
	"testing"

	lakeformationsdk "github.com/aws/aws-sdk-go-v2/service/lakeformation"
	lakeformationtypes "github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAddLFTagsToResource_CatalogIDNormalization covers the fix in
// store_setup.go: normalizeLFTagCatalogID maps an empty CatalogId to the
// default account ID, the same default real Lake Formation applies. Without
// it, an LF-tag created with an explicit CatalogId (as Terraform's
// aws_lakeformation_lf_tag sends) was unreachable from a request that omits
// CatalogId (as aws_lakeformation_resource_lf_tag's AddLFTagsToResource call
// does), even though both refer to the same catalog on real AWS.
func TestAddLFTagsToResource_CatalogIDNormalization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createCatalogID  *string
		addCatalogID     *string
		name             string
		wantFailureCount int
	}{
		{
			name:             "explicit create, omitted add",
			createCatalogID:  new("000000000000"),
			addCatalogID:     nil,
			wantFailureCount: 0,
		},
		{
			name:             "omitted create, explicit add",
			createCatalogID:  nil,
			addCatalogID:     new("000000000000"),
			wantFailureCount: 0,
		},
		{
			name:             "explicit create, explicit add",
			createCatalogID:  new("000000000000"),
			addCatalogID:     new("000000000000"),
			wantFailureCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestHandlerAndClient(t)

			tagKey := "env"

			_, err := client.CreateLFTag(t.Context(), &lakeformationsdk.CreateLFTagInput{
				CatalogId: tc.createCatalogID,
				TagKey:    &tagKey,
				TagValues: []string{"dev", "prod"},
			})
			require.NoError(t, err)

			dbName := "mydb"
			tagValue := "dev"

			addOut, err := client.AddLFTagsToResource(t.Context(), &lakeformationsdk.AddLFTagsToResourceInput{
				CatalogId: tc.addCatalogID,
				Resource: &lakeformationtypes.Resource{
					Database: &lakeformationtypes.DatabaseResource{Name: &dbName},
				},
				LFTags: []lakeformationtypes.LFTagPair{
					{TagKey: &tagKey, TagValues: []string{tagValue}},
				},
			})
			require.NoError(t, err)
			assert.Len(t, addOut.Failures, tc.wantFailureCount)

			getOut, err := client.GetResourceLFTags(t.Context(), &lakeformationsdk.GetResourceLFTagsInput{
				Resource: &lakeformationtypes.Resource{
					Database: &lakeformationtypes.DatabaseResource{Name: &dbName},
				},
			})
			require.NoError(t, err)
			require.NotEmpty(t, getOut.LFTagOnDatabase)
			assert.Equal(t, tagKey, *getOut.LFTagOnDatabase[0].TagKey)
		})
	}
}
