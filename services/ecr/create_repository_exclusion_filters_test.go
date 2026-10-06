package ecr_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateRepository_ExclusionFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		filters []types.ImageTagMutabilityExclusionFilter
	}{
		{name: "none"},
		{
			name: "two wildcards",
			filters: []types.ImageTagMutabilityExclusionFilter{
				{Filter: aws.String("dev-*"), FilterType: types.ImageTagMutabilityExclusionFilterTypeWildcard},
				{Filter: aws.String("latest"), FilterType: types.ImageTagMutabilityExclusionFilterTypeWildcard},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestECRClient(t, newTestHandler(t))

			created, err := client.CreateRepository(t.Context(), &ecrsdk.CreateRepositoryInput{
				RepositoryName:                     aws.String("excl-repo"),
				ImageTagMutability:                 types.ImageTagMutabilityImmutable,
				ImageTagMutabilityExclusionFilters: tt.filters,
			})
			require.NoError(t, err)

			desc, err := client.DescribeRepositories(t.Context(), &ecrsdk.DescribeRepositoriesInput{})
			require.NoError(t, err)
			require.Len(t, desc.Repositories, 1)

			for _, got := range [][]types.ImageTagMutabilityExclusionFilter{
				created.Repository.ImageTagMutabilityExclusionFilters,
				desc.Repositories[0].ImageTagMutabilityExclusionFilters,
			} {
				require.Len(t, got, len(tt.filters))

				for i, f := range got {
					assert.Equal(t, aws.ToString(tt.filters[i].Filter), aws.ToString(f.Filter))
					assert.Equal(t, tt.filters[i].FilterType, f.FilterType)
				}
			}
		})
	}
}
