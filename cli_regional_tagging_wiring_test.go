package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	codecommitbackend "github.com/blackbirdworks/gopherstack/services/codecommit"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	rgtapibackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

func TestInitializeServices_TaggingBridgeFollowsRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	rgtH, ok := byName["ResourceGroupsTaggingAPI"].(*rgtapibackend.Handler)
	require.True(t, ok)

	ecrH, ok := byName["ECR"].(*ecrbackend.Handler)
	require.True(t, ok)

	ccH, ok := byName["CodeCommit"].(*codecommitbackend.Handler)
	require.True(t, ok)

	nested := func(out map[string]any, outer, field string) string {
		inner, _ := out[outer].(map[string]any)
		arn, _ := inner[field].(string)

		return arn
	}

	tests := []struct {
		create func(region string) string
		name   string
	}{
		{
			name: "ecr",
			create: func(region string) string {
				out := regionTargetCall(t, ecrH.Handler(), region,
					"AmazonEC2ContainerRegistry_V20150921.CreateRepository", `{"repositoryName":"tag-ecr"}`)

				return nested(out, "repository", "repositoryArn")
			},
		},
		{
			name: "codecommit",
			create: func(region string) string {
				out := regionTargetCall(t, ccH.Handler(), region,
					"CodeCommit_20150413.CreateRepository", `{"repositoryName":"tag-cc"}`)

				return nested(out, "repositoryMetadata", "Arn")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			euARN := tc.create(euRegion)
			require.Contains(t, euARN, ":"+euRegion+":")

			ctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: crossHome, Account: crossAcct})

			out, err := rgtH.Backend.TagResources(ctx, &rgtapibackend.TagResourcesInput{
				ResourceARNList: []string{euARN}, Tags: map[string]string{"Team": tc.name},
			})
			require.NoError(t, err)
			assert.Empty(t, out.FailedResourcesMap)

			list := func(region string) []string {
				rctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: region, Account: crossAcct})

				res, listErr := rgtH.Backend.GetResources(rctx, &rgtapibackend.GetResourcesInput{
					TagFilters: []rgtapibackend.TagFilter{{Key: "Team", Values: []string{tc.name}}},
				})
				require.NoError(t, listErr)

				arns := make([]string, 0, len(res.ResourceTagMappingList))
				for _, m := range res.ResourceTagMappingList {
					arns = append(arns, m.ResourceARN)
				}

				return arns
			}

			assert.Contains(t, list(euRegion), euARN)
			assert.NotContains(t, list(crossHome), euARN)

			_, err = rgtH.Backend.UntagResources(ctx, &rgtapibackend.UntagResourcesInput{
				ResourceARNList: []string{euARN}, TagKeys: []string{"Team"},
			})
			require.NoError(t, err)
			assert.NotContains(t, list(euRegion), euARN)
		})
	}
}
