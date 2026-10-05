package ecrpublic_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrpublicsdk "github.com/aws/aws-sdk-go-v2/service/ecrpublic"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// DescribeRepositories: maxResults 1-1000, default 100, not combinable with
// repositoryNames (api_op_DescribeRepositories.go:30-50).
func TestDescribeRepositories_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxRes   *int32
		name     string
		wantCode string
		names    []string
		wantLen  int
		token    bool
		wantNext bool
	}{
		{name: "default 100", wantLen: 100, wantNext: true},
		{name: "explicit", maxRes: aws.Int32(30), wantLen: 30, wantNext: true},
		{name: "max 1000", maxRes: aws.Int32(1000), wantLen: 105},
		{name: "over 1000 rejected", maxRes: aws.Int32(1001), wantCode: "InvalidParameterException"},
		{
			name:     "names with maxResults rejected",
			names:    []string{"repo-000"},
			maxRes:   aws.Int32(5),
			wantCode: "InvalidParameterException",
		},
		{name: "names alone", names: []string{"repo-000", "repo-001"}, wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			ctx := t.Context()

			for i := range 105 {
				_, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{
					RepositoryName: aws.String(fmt.Sprintf("repo-%03d", i)),
				})
				require.NoError(t, err)
			}

			out, err := client.DescribeRepositories(ctx, &ecrpublicsdk.DescribeRepositoriesInput{
				MaxResults: tt.maxRes, RepositoryNames: tt.names,
			})

			if tt.wantCode != "" {
				var apiErr smithy.APIError
				require.ErrorAs(t, err, &apiErr)
				assert.Equal(t, tt.wantCode, apiErr.ErrorCode())

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Repositories, tt.wantLen)
			assert.Equal(t, tt.wantNext, out.NextToken != nil)
		})
	}
}

func TestDescribeRepositories_PagesCoverAll(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	for i := range 7 {
		_, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{
			RepositoryName: aws.String(fmt.Sprintf("repo-%d", i)),
		})
		require.NoError(t, err)
	}

	var got []string

	var token *string

	for range 10 {
		out, err := client.DescribeRepositories(ctx, &ecrpublicsdk.DescribeRepositoriesInput{
			MaxResults: aws.Int32(3), NextToken: token,
		})
		require.NoError(t, err)

		for _, r := range out.Repositories {
			got = append(got, aws.ToString(r.RepositoryName))
		}

		if token = out.NextToken; token == nil {
			break
		}
	}

	assert.Equal(t, []string{"repo-0", "repo-1", "repo-2", "repo-3", "repo-4", "repo-5", "repo-6"}, got)
}

func TestDescribeImageTags_Pagination(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("tags-repo")})
	require.NoError(t, err)

	configDigest := pushLayer(ctx, t, client, "tags-repo", []byte("config-blob"))
	layerDigestA := pushLayer(ctx, t, client, "tags-repo", []byte("layer-blob"))
	manifest := buildManifest(configDigest, layerDigestA)

	for _, tag := range []string{"a", "b", "c"} {
		_, err = client.PutImage(ctx, &ecrpublicsdk.PutImageInput{
			RepositoryName: aws.String("tags-repo"), ImageManifest: aws.String(manifest), ImageTag: aws.String(tag),
		})
		require.NoError(t, err)
	}

	page1, err := client.DescribeImageTags(ctx, &ecrpublicsdk.DescribeImageTagsInput{
		RepositoryName: aws.String("tags-repo"), MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, page1.ImageTagDetails, 2)
	require.NotNil(t, page1.NextToken)

	page2, err := client.DescribeImageTags(ctx, &ecrpublicsdk.DescribeImageTagsInput{
		RepositoryName: aws.String("tags-repo"), MaxResults: aws.Int32(2), NextToken: page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.ImageTagDetails, 1)
	assert.Equal(t, "c", aws.ToString(page2.ImageTagDetails[0].ImageTag))
	assert.Nil(t, page2.NextToken)

	imgs, err := client.DescribeImages(ctx, &ecrpublicsdk.DescribeImagesInput{
		RepositoryName: aws.String("tags-repo"), MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Len(t, imgs.ImageDetails, 1)
	assert.Nil(t, imgs.NextToken, "one image digest total, so no further page")
}
