package ecrpublic_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrpublicsdk "github.com/aws/aws-sdk-go-v2/service/ecrpublic"
	"github.com/aws/aws-sdk-go-v2/service/ecrpublic/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErrorRealism(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(c *ecrpublicsdk.Client) error
		name     string
		wantCode string
		wantMsg  string
		noPrefix string
	}{
		{
			name: "repository not found wording",
			call: func(c *ecrpublicsdk.Client) error {
				_, err := c.DescribeRepositories(t.Context(), &ecrpublicsdk.DescribeRepositoriesInput{
					RepositoryNames: []string{"nope"},
				})

				return err
			},
			wantCode: "RepositoryNotFoundException",
			wantMsg:  "The repository with name 'nope' does not exist in the registry with id",
			noPrefix: "RepositoryNotFoundException",
		},
		{
			name: "bad next token",
			call: func(c *ecrpublicsdk.Client) error {
				_, err := c.DescribeRepositories(t.Context(), &ecrpublicsdk.DescribeRepositoriesInput{
					NextToken: aws.String("!!garbage"),
				})

				return err
			},
			wantCode: "InvalidParameterException",
			wantMsg:  "invalid nextToken",
			noPrefix: "InvalidParameterException",
		},
		{
			name: "non json policy",
			call: func(c *ecrpublicsdk.Client) error {
				_, err := c.SetRepositoryPolicy(t.Context(), &ecrpublicsdk.SetRepositoryPolicyInput{
					RepositoryName: aws.String("rr"),
					PolicyText:     aws.String("notjson"),
				})

				return err
			},
			wantCode: "InvalidParameterException",
			wantMsg:  "PolicyText",
		},
		{
			name: "oversized catalog description",
			call: func(c *ecrpublicsdk.Client) error {
				_, err := c.PutRepositoryCatalogData(t.Context(), &ecrpublicsdk.PutRepositoryCatalogDataInput{
					RepositoryName: aws.String("rr"),
					CatalogData: &types.RepositoryCatalogDataInput{
						Description: aws.String(strings.Repeat("x", 1025)),
					},
				})

				return err
			},
			wantCode: "InvalidParameterException",
			wantMsg:  "description",
		},
		{
			name: "too many layer digests",
			call: func(c *ecrpublicsdk.Client) error {
				digests := make([]string, 101)
				for i := range digests {
					digests[i] = "sha256:abc"
				}

				_, err := c.BatchCheckLayerAvailability(t.Context(), &ecrpublicsdk.BatchCheckLayerAvailabilityInput{
					RepositoryName: aws.String("rr"),
					LayerDigests:   digests,
				})

				return err
			},
			wantCode: "InvalidParameterException",
			wantMsg:  "layerDigests",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			_, err := client.CreateRepository(
				t.Context(),
				&ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("rr")},
			)
			require.NoError(t, err)

			err = tt.call(client)
			require.Error(t, err)

			var apiErr smithy.APIError
			require.ErrorAs(t, err, &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.Contains(t, apiErr.ErrorMessage(), tt.wantMsg)

			if tt.noPrefix != "" {
				assert.NotContains(t, apiErr.ErrorMessage(), tt.noPrefix)
			}
		})
	}
}
