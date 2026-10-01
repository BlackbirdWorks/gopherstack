package codeartifact_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPackageVersionOrigin_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		originType types.PackageVersionOriginType
		wantCount  int
	}{
		{name: "no_filter", wantCount: 2},
		{name: "internal", originType: types.PackageVersionOriginTypeInternal, wantCount: 1},
		{name: "unknown", originType: types.PackageVersionOriginTypeUnknown, wantCount: 1},
		{name: "external", originType: types.PackageVersionOriginTypeExternal, wantCount: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupDomain(t, h, "o-domain")
			setupRepo(t, h, "o-domain", "o-repo")
			setupRepo(t, h, "o-domain", "o-copy")
			client := newTestCodeArtifactClient(t, h)
			ctx := t.Context()

			_, err := client.PublishPackageVersion(ctx, &casdk.PublishPackageVersionInput{
				Domain: aws.String("o-domain"), Repository: aws.String("o-repo"),
				Format: types.PackageFormatGeneric, Package: aws.String("lib"),
				PackageVersion: aws.String("1.0.0"), AssetName: aws.String("lib.bin"),
				AssetSHA256:  aws.String(sha256Hex("x")),
				AssetContent: strings.NewReader("x"),
			})
			require.NoError(t, err)

			// A version seeded through Describe has no recorded origin.
			_, err = client.DescribePackageVersion(ctx, &casdk.DescribePackageVersionInput{
				Domain: aws.String("o-domain"), Repository: aws.String("o-repo"),
				Format: types.PackageFormatGeneric, Package: aws.String("lib"),
				PackageVersion: aws.String("2.0.0"),
			})
			require.NoError(t, err)

			out, err := client.ListPackageVersions(ctx, &casdk.ListPackageVersionsInput{
				Domain: aws.String("o-domain"), Repository: aws.String("o-repo"),
				Format: types.PackageFormatGeneric, Package: aws.String("lib"),
				OriginType: tc.originType,
			})
			require.NoError(t, err)
			assert.Len(t, out.Versions, tc.wantCount)

			if tc.originType == "" {
				require.Len(t, out.Versions, 2)
				assert.Equal(t, types.PackageVersionOriginTypeInternal, out.Versions[0].Origin.OriginType)
				assert.Equal(t, "o-repo", aws.ToString(out.Versions[0].Origin.DomainEntryPoint.RepositoryName))
				assert.Equal(t, types.PackageVersionOriginTypeUnknown, out.Versions[1].Origin.OriginType)
			}
		})
	}
}

func TestPackageVersionOrigin_SurvivesCopy_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupDomain(t, h, "oc-domain")
	setupRepo(t, h, "oc-domain", "oc-src")
	setupRepo(t, h, "oc-domain", "oc-dst")
	client := newTestCodeArtifactClient(t, h)
	ctx := t.Context()

	_, err := client.PublishPackageVersion(ctx, &casdk.PublishPackageVersionInput{
		Domain: aws.String("oc-domain"), Repository: aws.String("oc-src"),
		Format: types.PackageFormatGeneric, Package: aws.String("lib"),
		PackageVersion: aws.String("1.0.0"), AssetName: aws.String("lib.bin"),
		AssetSHA256:  aws.String(sha256Hex("x")),
		AssetContent: strings.NewReader("x"),
	})
	require.NoError(t, err)

	_, err = client.CopyPackageVersions(ctx, &casdk.CopyPackageVersionsInput{
		Domain: aws.String("oc-domain"), SourceRepository: aws.String("oc-src"),
		DestinationRepository: aws.String("oc-dst"), Format: types.PackageFormatGeneric,
		Package: aws.String("lib"), Versions: []string{"1.0.0"},
	})
	require.NoError(t, err)

	got, err := client.DescribePackageVersion(ctx, &casdk.DescribePackageVersionInput{
		Domain: aws.String("oc-domain"), Repository: aws.String("oc-dst"),
		Format: types.PackageFormatGeneric, Package: aws.String("lib"),
		PackageVersion: aws.String("1.0.0"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.PackageVersionOriginTypeInternal, got.PackageVersion.Origin.OriginType)
	assert.Equal(t, "oc-src", aws.ToString(got.PackageVersion.Origin.DomainEntryPoint.RepositoryName))
}
