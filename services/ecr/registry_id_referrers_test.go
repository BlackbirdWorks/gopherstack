package ecr_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrsdk "github.com/aws/aws-sdk-go-v2/service/ecr"
	ecrtypes "github.com/aws/aws-sdk-go-v2/service/ecr/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestForeignRegistryID(t *testing.T) {
	t.Parallel()

	const foreign = "111111111111"

	tests := []struct {
		call    func(c *ecrsdk.Client, repo string) error
		name    string
		errCode string
	}{
		{
			name:    "create",
			errCode: "InvalidParameterException",
			call: func(c *ecrsdk.Client, _ string) error {
				_, err := c.CreateRepository(t.Context(), &ecrsdk.CreateRepositoryInput{
					RepositoryName: aws.String("foreign-create"), RegistryId: aws.String(foreign),
				})

				return err
			},
		},
		{
			name:    "describe-repos-named",
			errCode: "RepositoryNotFoundException",
			call: func(c *ecrsdk.Client, repo string) error {
				_, err := c.DescribeRepositories(t.Context(), &ecrsdk.DescribeRepositoriesInput{
					RegistryId: aws.String(foreign), RepositoryNames: []string{repo},
				})

				return err
			},
		},
		{
			name:    "describe-images",
			errCode: "RepositoryNotFoundException",
			call: func(c *ecrsdk.Client, repo string) error {
				_, err := c.DescribeImages(t.Context(), &ecrsdk.DescribeImagesInput{
					RegistryId: aws.String(foreign), RepositoryName: aws.String(repo),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestECRClient(t, newTestHandler(t))
			_, err := c.CreateRepository(t.Context(), &ecrsdk.CreateRepositoryInput{
				RepositoryName: aws.String("own"), RegistryId: aws.String(testAccountID),
			})
			require.NoError(t, err)

			err = tt.call(c, "own")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errCode)
		})
	}

	t.Run("describe-repos-unnamed-empty", func(t *testing.T) {
		t.Parallel()

		c := newTestECRClient(t, newTestHandler(t))
		_, err := c.CreateRepository(t.Context(), &ecrsdk.CreateRepositoryInput{RepositoryName: aws.String("own")})
		require.NoError(t, err)

		out, err := c.DescribeRepositories(t.Context(), &ecrsdk.DescribeRepositoriesInput{
			RegistryId: aws.String(foreign),
		})
		require.NoError(t, err)
		assert.Empty(t, out.Repositories)

		out, err = c.DescribeRepositories(t.Context(), &ecrsdk.DescribeRepositoriesInput{
			RegistryId: aws.String(testAccountID),
		})
		require.NoError(t, err)
		assert.Len(t, out.Repositories, 1)
	})
}

func TestListImageReferrers(t *testing.T) {
	t.Parallel()

	const subject = `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",` +
		`"config":{"mediaType":"x","digest":"sha256:aa","size":1},"layers":[]}`

	tests := []struct {
		filter    *ecrtypes.ListImageReferrersFilter
		name      string
		wantTypes []string
		maxPage   int32
	}{
		{name: "default-all-active", wantTypes: []string{"app/sbom", "app/sig", "app/sig"}},
		{
			name:      "type-filter",
			filter:    &ecrtypes.ListImageReferrersFilter{ArtifactTypes: []string{"app/sig"}},
			wantTypes: []string{"app/sig", "app/sig"},
		},
		{
			name:      "archived-none",
			filter:    &ecrtypes.ListImageReferrersFilter{ArtifactStatus: ecrtypes.ArtifactStatusFilterArchived},
			wantTypes: []string{},
		},
		{name: "paged", maxPage: 2, wantTypes: []string{"app/sbom", "app/sig", "app/sig"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestECRClient(t, newTestHandler(t))
			ctx := t.Context()
			_, err := c.CreateRepository(ctx, &ecrsdk.CreateRepositoryInput{RepositoryName: aws.String("r")})
			require.NoError(t, err)

			put, err := c.PutImage(ctx, &ecrsdk.PutImageInput{
				RepositoryName: aws.String("r"), ImageManifest: aws.String(subject),
				ImageManifestMediaType: aws.String("application/vnd.oci.image.manifest.v1+json"),
			})
			require.NoError(t, err)

			subjDigest := aws.ToString(put.Image.ImageId.ImageDigest)

			for i, at := range []string{"app/sig", "app/sbom", "app/sig"} {
				m := fmt.Sprintf(
					`{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json",`+
						`"artifactType":%q,"subject":{"digest":%q},"annotations":{"n":"%d"},"layers":[]}`,
					at, subjDigest, i)
				_, err = c.PutImage(ctx, &ecrsdk.PutImageInput{
					RepositoryName: aws.String("r"), ImageManifest: aws.String(m),
					ImageManifestMediaType: aws.String("application/vnd.oci.image.manifest.v1+json"),
				})
				require.NoError(t, err)
			}

			in := &ecrsdk.ListImageReferrersInput{
				RepositoryName: aws.String("r"),
				SubjectId:      &ecrtypes.SubjectIdentifier{ImageDigest: aws.String(subjDigest)},
				Filter:         tt.filter,
			}
			if tt.maxPage > 0 {
				in.MaxResults = aws.Int32(tt.maxPage)
			}

			got := []string{}
			pages := 0

			for {
				out, listErr := c.ListImageReferrers(ctx, in)
				require.NoError(t, listErr)

				pages++

				for _, r := range out.Referrers {
					got = append(got, aws.ToString(r.ArtifactType))
					assert.Equal(t, ecrtypes.ArtifactStatusActive, r.ArtifactStatus)
					assert.NotEmpty(t, r.Annotations["n"])
				}

				if out.NextToken == nil {
					break
				}

				in.NextToken = out.NextToken
			}

			assert.ElementsMatch(t, tt.wantTypes, got)

			if tt.maxPage > 0 {
				assert.Equal(t, 2, pages)
			}
		})
	}
}
