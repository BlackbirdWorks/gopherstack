package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrpublicsdk "github.com/aws/aws-sdk-go-v2/service/ecrpublic"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createEcrPublicClient returns an ECR Public client pointed at the shared test container.
func createEcrPublicClient(t *testing.T) *ecrpublicsdk.Client {
	t.Helper()

	return createClientWithEndpoint(t, ecrpublicsdk.NewFromConfig, endpoint)
}

// TestTerraform_EcrPublicRepositories provisions an aws_ecrpublic_repository
// and an aws_ecrpublic_repository_policy via Terraform, then verifies both
// are visible via the Amazon ECR Public SDK.
func TestTerraform_EcrPublicRepositories(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "ecr-public-repositories",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{
					"RepositoryName": "tf-ecrpublic-" + uuid.NewString()[:8],
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createEcrPublicClient(t)
				repoName := vars["RepositoryName"].(string)

				out, err := client.DescribeRepositories(ctx, &ecrpublicsdk.DescribeRepositoriesInput{
					RepositoryNames: []string{repoName},
				})
				require.NoError(t, err, "DescribeRepositories should succeed after terraform apply")
				require.Len(t, out.Repositories, 1)

				repo := out.Repositories[0]
				assert.Equal(t, repoName, aws.ToString(repo.RepositoryName))
				assert.Regexp(t, `^arn:aws:ecr-public::\d+:repository/`+repoName+`$`, aws.ToString(repo.RepositoryArn))
				assert.Regexp(t, `^public\.ecr\.aws/`, aws.ToString(repo.RepositoryUri))

				catalog, err := client.GetRepositoryCatalogData(ctx, &ecrpublicsdk.GetRepositoryCatalogDataInput{
					RepositoryName: aws.String(repoName),
				})
				require.NoError(t, err, "GetRepositoryCatalogData should succeed after terraform apply")
				assert.Equal(t, "About "+repoName, aws.ToString(catalog.CatalogData.AboutText))
				assert.Equal(t, []string{"ARM"}, catalog.CatalogData.Architectures)
				assert.Equal(t, []string{"Linux"}, catalog.CatalogData.OperatingSystems)

				tagsOut, err := client.ListTagsForResource(ctx, &ecrpublicsdk.ListTagsForResourceInput{
					ResourceArn: repo.RepositoryArn,
				})
				require.NoError(t, err, "ListTagsForResource should succeed after terraform apply")
				gotTags := make(map[string]string, len(tagsOut.Tags))
				for _, tag := range tagsOut.Tags {
					gotTags[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
				}
				assert.Equal(t, map[string]string{"Environment": "test", "Owner": "terraform"}, gotTags)

				policyOut, err := client.GetRepositoryPolicy(ctx, &ecrpublicsdk.GetRepositoryPolicyInput{
					RepositoryName: aws.String(repoName),
				})
				require.NoError(t, err, "GetRepositoryPolicy should succeed after terraform apply")
				assert.Contains(t, aws.ToString(policyOut.PolicyText), "AllowPull")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}
