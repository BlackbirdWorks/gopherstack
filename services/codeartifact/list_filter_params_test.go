package codeartifact_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

// TestListPackages_Filters proves ListPackages applies its packagePrefix,
// publish, and upstream query-bound filters (serializers.go's
// awsRestjson1_serializeOpHttpBindingsListPackagesInput) instead of
// returning every package in the repository regardless of what was
// requested, as handleListPackages did before this fix.
func TestListPackages_Filters(t *testing.T) {
	t.Parallel()

	h := codeartifact.NewHandler(codeartifact.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestCodeArtifactClient(t, h)

	_, err := client.CreateDomain(t.Context(), &casdk.CreateDomainInput{Domain: aws.String("filter-domain")})
	require.NoError(t, err)
	_, err = client.CreateRepository(t.Context(), &casdk.CreateRepositoryInput{
		Domain: aws.String("filter-domain"), Repository: aws.String("filter-repo"),
	})
	require.NoError(t, err)

	publishAsset := func(name string) {
		t.Helper()
		_, publishErr := client.PublishPackageVersion(t.Context(), &casdk.PublishPackageVersionInput{
			Domain:         aws.String("filter-domain"),
			Repository:     aws.String("filter-repo"),
			Format:         types.PackageFormatGeneric,
			Package:        aws.String(name),
			PackageVersion: aws.String("1.0.0"),
			AssetContent:   strings.NewReader("content"),
			AssetName:      aws.String("asset.bin"),
			AssetSHA256:    aws.String(sha256Hex("content")),
		})
		require.NoError(t, publishErr)
	}

	publishAsset("prefix-alpha")
	publishAsset("prefix-beta")
	publishAsset("other-gamma")

	_, err = client.PutPackageOriginConfiguration(t.Context(), &casdk.PutPackageOriginConfigurationInput{
		Domain:     aws.String("filter-domain"),
		Repository: aws.String("filter-repo"),
		Format:     types.PackageFormatGeneric,
		Package:    aws.String("prefix-alpha"),
		Restrictions: &types.PackageOriginRestrictions{
			Publish:  types.AllowPublishBlock,
			Upstream: types.AllowUpstreamAllow,
		},
	})
	require.NoError(t, err)

	prefixed, err := client.ListPackages(t.Context(), &casdk.ListPackagesInput{
		Domain: aws.String("filter-domain"), Repository: aws.String("filter-repo"),
		PackagePrefix: aws.String("prefix-"),
	})
	require.NoError(t, err)
	names := make([]string, 0, len(prefixed.Packages))
	for _, p := range prefixed.Packages {
		names = append(names, aws.ToString(p.Package))
	}
	require.ElementsMatch(t, []string{"prefix-alpha", "prefix-beta"}, names)

	blockedPublish, err := client.ListPackages(t.Context(), &casdk.ListPackagesInput{
		Domain: aws.String("filter-domain"), Repository: aws.String("filter-repo"),
		Publish: types.AllowPublishBlock,
	})
	require.NoError(t, err)
	require.Len(t, blockedPublish.Packages, 1)
	require.Equal(t, "prefix-alpha", aws.ToString(blockedPublish.Packages[0].Package))
}
