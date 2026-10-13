package ecr_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForeignRegistryIDRepoOps(t *testing.T) {
	t.Parallel()

	const foreign = "111111111111"

	id := ecrtypes.ImageIdentifier{ImageTag: aws.String("latest")}

	tests := []struct {
		call    func(c *ecrsdk.Client) error
		name    string
		errCode string
	}{
		{name: "list images", errCode: "RepositoryNotFoundException", call: func(c *ecrsdk.Client) error {
			_, err := c.ListImages(t.Context(), &ecrsdk.ListImagesInput{
				RegistryId: aws.String(foreign), RepositoryName: aws.String("own"),
			})

			return err
		}},
		{name: "batch get image", errCode: "RepositoryNotFoundException", call: func(c *ecrsdk.Client) error {
			_, err := c.BatchGetImage(t.Context(), &ecrsdk.BatchGetImageInput{
				RegistryId: aws.String(foreign), RepositoryName: aws.String("own"),
				ImageIds: []ecrtypes.ImageIdentifier{id},
			})

			return err
		}},
		{name: "batch delete image", errCode: "RepositoryNotFoundException", call: func(c *ecrsdk.Client) error {
			_, err := c.BatchDeleteImage(t.Context(), &ecrsdk.BatchDeleteImageInput{
				RegistryId: aws.String(foreign), RepositoryName: aws.String("own"),
				ImageIds: []ecrtypes.ImageIdentifier{id},
			})

			return err
		}},
		{name: "get repository policy", errCode: "RepositoryNotFoundException", call: func(c *ecrsdk.Client) error {
			_, err := c.GetRepositoryPolicy(t.Context(), &ecrsdk.GetRepositoryPolicyInput{
				RegistryId: aws.String(foreign), RepositoryName: aws.String("own"),
			})

			return err
		}},
		{name: "get lifecycle policy", errCode: "RepositoryNotFoundException", call: func(c *ecrsdk.Client) error {
			_, err := c.GetLifecyclePolicy(t.Context(), &ecrsdk.GetLifecyclePolicyInput{
				RegistryId: aws.String(foreign), RepositoryName: aws.String("own"),
			})

			return err
		}},
		{name: "layer availability", errCode: "RepositoryNotFoundException", call: func(c *ecrsdk.Client) error {
			_, err := c.BatchCheckLayerAvailability(t.Context(), &ecrsdk.BatchCheckLayerAvailabilityInput{
				RegistryId: aws.String(foreign), RepositoryName: aws.String("own"),
				LayerDigests: []string{"sha256:aa"},
			})

			return err
		}},
		{name: "delete repository", errCode: "RepositoryNotFoundException", call: func(c *ecrsdk.Client) error {
			_, err := c.DeleteRepository(t.Context(), &ecrsdk.DeleteRepositoryInput{
				RegistryId: aws.String(foreign), RepositoryName: aws.String("own"),
			})

			return err
		}},
		{
			name:    "delete pull through rule",
			errCode: "PullThroughCacheRuleNotFoundException",
			call: func(c *ecrsdk.Client) error {
				_, err := c.DeletePullThroughCacheRule(t.Context(), &ecrsdk.DeletePullThroughCacheRuleInput{
					RegistryId: aws.String(foreign), EcrRepositoryPrefix: aws.String("own"),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestECRClient(t, newTestHandler(t))
			_, err := c.CreateRepository(t.Context(), &ecrsdk.CreateRepositoryInput{RepositoryName: aws.String("own")})
			require.NoError(t, err)

			err = tt.call(c)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errCode)

			_, descErr := c.DescribeRepositories(t.Context(), &ecrsdk.DescribeRepositoriesInput{
				RepositoryNames: []string{"own"},
			})
			require.NoError(t, descErr)
		})
	}
}
