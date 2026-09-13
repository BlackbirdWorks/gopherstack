package codeartifact_test

import (
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

// TestRealClient_DomainRepositoryAndPackageManagement drives every
// gopherstack-n3zi uncovered codeartifact op through the real aws-sdk-go-v2
// client.
func TestRealClient_DomainRepositoryAndPackageManagement(t *testing.T) {
	t.Parallel()

	newHandler := func() *codeartifact.Handler {
		return codeartifact.NewHandler(codeartifact.NewInMemoryBackend("123456789012", "us-east-1"))
	}
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "domain_auth_and_policy", run: func(t *testing.T) {
			t.Helper()

			h := newHandler()
			client := newTestCodeArtifactClient(t, h)
			ctx := t.Context()

			_, err := client.CreateDomain(ctx, &casdk.CreateDomainInput{Domain: aws.String("dom-auth")})
			require.NoError(t, err)

			tokOut, err := client.GetAuthorizationToken(ctx, &casdk.GetAuthorizationTokenInput{
				Domain: aws.String("dom-auth"),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(tokOut.AuthorizationToken))
			assert.NotNil(t, tokOut.Expiration)

			putOut, err := client.PutDomainPermissionsPolicy(ctx, &casdk.PutDomainPermissionsPolicyInput{
				Domain:         aws.String("dom-auth"),
				PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
			})
			require.NoError(t, err)
			require.NotNil(t, putOut.Policy)

			getOut, err := client.GetDomainPermissionsPolicy(ctx, &casdk.GetDomainPermissionsPolicyInput{
				Domain: aws.String("dom-auth"),
			})
			require.NoError(t, err)
			assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, aws.ToString(getOut.Policy.Document))

			delOut, err := client.DeleteDomainPermissionsPolicy(ctx, &casdk.DeleteDomainPermissionsPolicyInput{
				Domain: aws.String("dom-auth"),
			})
			require.NoError(t, err)
			require.NotNil(t, delOut.Policy)

			_, err = client.GetDomainPermissionsPolicy(ctx, &casdk.GetDomainPermissionsPolicyInput{
				Domain: aws.String("dom-auth"),
			})
			require.Error(t, err, "policy was deleted")
		}},
		{name: "repository_endpoint_and_policy_and_external_connection", run: func(t *testing.T) {
			t.Helper()

			h := newHandler()
			client := newTestCodeArtifactClient(t, h)
			ctx := t.Context()

			_, err := client.CreateDomain(ctx, &casdk.CreateDomainInput{Domain: aws.String("dom-repo")})
			require.NoError(t, err)
			_, err = client.CreateRepository(ctx, &casdk.CreateRepositoryInput{
				Domain: aws.String("dom-repo"), Repository: aws.String("repo1"),
			})
			require.NoError(t, err)

			epOut, err := client.GetRepositoryEndpoint(ctx, &casdk.GetRepositoryEndpointInput{
				Domain:     aws.String("dom-repo"),
				Repository: aws.String("repo1"),
				Format:     types.PackageFormatNpm,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(epOut.RepositoryEndpoint))

			putOut, err := client.PutRepositoryPermissionsPolicy(ctx, &casdk.PutRepositoryPermissionsPolicyInput{
				Domain:         aws.String("dom-repo"),
				Repository:     aws.String("repo1"),
				PolicyDocument: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
			})
			require.NoError(t, err)
			require.NotNil(t, putOut.Policy)

			getOut, err := client.GetRepositoryPermissionsPolicy(ctx, &casdk.GetRepositoryPermissionsPolicyInput{
				Domain:     aws.String("dom-repo"),
				Repository: aws.String("repo1"),
			})
			require.NoError(t, err)
			assert.JSONEq(t, `{"Version":"2012-10-17","Statement":[]}`, aws.ToString(getOut.Policy.Document))

			_, err = client.DeleteRepositoryPermissionsPolicy(ctx, &casdk.DeleteRepositoryPermissionsPolicyInput{
				Domain:     aws.String("dom-repo"),
				Repository: aws.String("repo1"),
			})
			require.NoError(t, err)

			assocOut, err := client.AssociateExternalConnection(ctx, &casdk.AssociateExternalConnectionInput{
				Domain:             aws.String("dom-repo"),
				Repository:         aws.String("repo1"),
				ExternalConnection: aws.String("public:npmjs"),
			})
			require.NoError(t, err)
			require.NotNil(t, assocOut.Repository)
			require.Len(t, assocOut.Repository.ExternalConnections, 1)
			assert.Equal(
				t,
				"public:npmjs",
				aws.ToString(assocOut.Repository.ExternalConnections[0].ExternalConnectionName),
			)

			disOut, err := client.DisassociateExternalConnection(ctx, &casdk.DisassociateExternalConnectionInput{
				Domain:             aws.String("dom-repo"),
				Repository:         aws.String("repo1"),
				ExternalConnection: aws.String("public:npmjs"),
			})
			require.NoError(t, err)
			require.NotNil(t, disOut.Repository)
			assert.Empty(t, disOut.Repository.ExternalConnections)

			updOut, err := client.UpdateRepository(ctx, &casdk.UpdateRepositoryInput{
				Domain:      aws.String("dom-repo"),
				Repository:  aws.String("repo1"),
				Description: aws.String("updated description"),
			})
			require.NoError(t, err)
			assert.Equal(t, "updated description", aws.ToString(updOut.Repository.Description))
		}},
		{name: "package_group_lifecycle", run: func(t *testing.T) {
			t.Helper()

			h := newHandler()
			client := newTestCodeArtifactClient(t, h)
			ctx := t.Context()

			_, err := client.CreateDomain(ctx, &casdk.CreateDomainInput{Domain: aws.String("dom-pg")})
			require.NoError(t, err)

			createOut, err := client.CreatePackageGroup(ctx, &casdk.CreatePackageGroupInput{
				Domain:       aws.String("dom-pg"),
				PackageGroup: aws.String("/npm/*"),
			})
			require.NoError(t, err)
			require.NotNil(t, createOut.PackageGroup)

			descOut, err := client.DescribePackageGroup(ctx, &casdk.DescribePackageGroupInput{
				Domain:       aws.String("dom-pg"),
				PackageGroup: aws.String("/npm/*"),
			})
			require.NoError(t, err)
			assert.Equal(t, "/npm/*", aws.ToString(descOut.PackageGroup.Pattern))

			listOut, err := client.ListPackageGroups(ctx, &casdk.ListPackageGroupsInput{
				Domain: aws.String("dom-pg"),
			})
			require.NoError(t, err)
			require.Len(t, listOut.PackageGroups, 1)

			_, err = client.CreatePackageGroup(ctx, &casdk.CreatePackageGroupInput{
				Domain:       aws.String("dom-pg"),
				PackageGroup: aws.String("/npm/sub/*"),
			})
			require.NoError(t, err)

			subOut, err := client.ListSubPackageGroups(ctx, &casdk.ListSubPackageGroupsInput{
				Domain:       aws.String("dom-pg"),
				PackageGroup: aws.String("/npm/*"),
			})
			require.NoError(t, err)
			require.Len(t, subOut.PackageGroups, 1)
			assert.Equal(t, "/npm/sub/*", aws.ToString(subOut.PackageGroups[0].Pattern))

			updOut, err := client.UpdatePackageGroup(ctx, &casdk.UpdatePackageGroupInput{
				Domain:       aws.String("dom-pg"),
				PackageGroup: aws.String("/npm/*"),
				Description:  aws.String("updated group"),
				ContactInfo:  aws.String("team@example.com"),
			})
			require.NoError(t, err)
			assert.Equal(t, "updated group", aws.ToString(updOut.PackageGroup.Description))

			delOut, err := client.DeletePackageGroup(ctx, &casdk.DeletePackageGroupInput{
				Domain:       aws.String("dom-pg"),
				PackageGroup: aws.String("/npm/sub/*"),
			})
			require.NoError(t, err)
			assert.Equal(t, "/npm/sub/*", aws.ToString(delOut.PackageGroup.Pattern))

			listOut2, err := client.ListPackageGroups(ctx, &casdk.ListPackageGroupsInput{
				Domain: aws.String("dom-pg"),
			})
			require.NoError(t, err)
			assert.Len(t, listOut2.PackageGroups, 1)
		}},
		{name: "associated_packages", run: func(t *testing.T) {
			t.Helper()

			h := newHandler()
			client := newTestCodeArtifactClient(t, h)
			ctx := t.Context()

			_, err := client.CreateDomain(ctx, &casdk.CreateDomainInput{Domain: aws.String("dom-assoc")})
			require.NoError(t, err)
			_, err = client.CreateRepository(ctx, &casdk.CreateRepositoryInput{
				Domain: aws.String("dom-assoc"), Repository: aws.String("repo1"),
			})
			require.NoError(t, err)
			_, err = client.CreatePackageGroup(ctx, &casdk.CreatePackageGroupInput{
				Domain: aws.String("dom-assoc"), PackageGroup: aws.String("/npm/*"),
			})
			require.NoError(t, err)

			_, err = h.Backend.PublishPackageVersion(
				ctx, "dom-assoc", "repo1", "npm", "", "left-pad", "1.0.0",
				codeartifact.AssetInfo{Name: "left-pad-1.0.0.tgz", Content: []byte("data"), SHA256: "abc", Size: 4},
			)
			require.NoError(t, err)

			assocOut, err := client.ListAssociatedPackages(ctx, &casdk.ListAssociatedPackagesInput{
				Domain:       aws.String("dom-assoc"),
				PackageGroup: aws.String("/npm/*"),
			})
			require.NoError(t, err)
			require.Len(t, assocOut.Packages, 1)
			assert.Equal(t, "left-pad", aws.ToString(assocOut.Packages[0].Package))
		}},
		{name: "package_version_asset_and_readme_and_dependencies", run: func(t *testing.T) {
			t.Helper()

			h := newHandler()
			client := newTestCodeArtifactClient(t, h)
			ctx := t.Context()

			_, err := client.CreateDomain(ctx, &casdk.CreateDomainInput{Domain: aws.String("dom-pv")})
			require.NoError(t, err)
			_, err = client.CreateRepository(ctx, &casdk.CreateRepositoryInput{
				Domain: aws.String("dom-pv"), Repository: aws.String("repo1"),
			})
			require.NoError(t, err)

			_, err = h.Backend.PublishPackageVersion(
				ctx, "dom-pv", "repo1", "npm", "", "left-pad", "1.0.0",
				codeartifact.AssetInfo{
					Name:    "package.tgz",
					Content: []byte("tarball-bytes"),
					SHA256:  "deadbeef",
					Size:    int64(len("tarball-bytes")),
				},
			)
			require.NoError(t, err)

			assetOut, err := client.GetPackageVersionAsset(ctx, &casdk.GetPackageVersionAssetInput{
				Domain:         aws.String("dom-pv"),
				Repository:     aws.String("repo1"),
				Format:         types.PackageFormatNpm,
				Package:        aws.String("left-pad"),
				PackageVersion: aws.String("1.0.0"),
				Asset:          aws.String("package.tgz"),
			})
			require.NoError(t, err)
			body, err := io.ReadAll(assetOut.Asset)
			require.NoError(t, err)
			assert.Equal(t, "tarball-bytes", string(body))
			assert.Equal(t, "package.tgz", aws.ToString(assetOut.AssetName))

			listAssetsOut, err := client.ListPackageVersionAssets(ctx, &casdk.ListPackageVersionAssetsInput{
				Domain:         aws.String("dom-pv"),
				Repository:     aws.String("repo1"),
				Format:         types.PackageFormatNpm,
				Package:        aws.String("left-pad"),
				PackageVersion: aws.String("1.0.0"),
			})
			require.NoError(t, err)
			require.Len(t, listAssetsOut.Assets, 1)
			assert.Equal(t, "package.tgz", aws.ToString(listAssetsOut.Assets[0].Name))

			readmeOut, err := client.GetPackageVersionReadme(ctx, &casdk.GetPackageVersionReadmeInput{
				Domain:         aws.String("dom-pv"),
				Repository:     aws.String("repo1"),
				Format:         types.PackageFormatNpm,
				Package:        aws.String("left-pad"),
				PackageVersion: aws.String("1.0.0"),
			})
			require.NoError(
				t,
				err,
				"GetPackageVersionReadme returns an empty readme, not an error, when none was published",
			)
			assert.Empty(t, aws.ToString(readmeOut.Readme))
			assert.Equal(t, "left-pad", aws.ToString(readmeOut.Package))

			depsOut, err := client.ListPackageVersionDependencies(ctx, &casdk.ListPackageVersionDependenciesInput{
				Domain:         aws.String("dom-pv"),
				Repository:     aws.String("repo1"),
				Format:         types.PackageFormatNpm,
				Package:        aws.String("left-pad"),
				PackageVersion: aws.String("1.0.0"),
			})
			require.NoError(t, err)
			assert.Empty(t, depsOut.Dependencies)
		}},
		{name: "tags", run: func(t *testing.T) {
			t.Helper()

			h := newHandler()
			client := newTestCodeArtifactClient(t, h)
			ctx := t.Context()

			createOut, err := client.CreateDomain(ctx, &casdk.CreateDomainInput{Domain: aws.String("dom-tags")})
			require.NoError(t, err)

			_, err = client.TagResource(ctx, &casdk.TagResourceInput{
				ResourceArn: createOut.Domain.Arn,
				Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
			})
			require.NoError(t, err)

			listOut, err := client.ListTagsForResource(ctx, &casdk.ListTagsForResourceInput{
				ResourceArn: createOut.Domain.Arn,
			})
			require.NoError(t, err)
			require.Len(t, listOut.Tags, 1)

			_, err = client.UntagResource(ctx, &casdk.UntagResourceInput{
				ResourceArn: createOut.Domain.Arn,
				TagKeys:     []string{"env"},
			})
			require.NoError(t, err)

			listOut2, err := client.ListTagsForResource(ctx, &casdk.ListTagsForResourceInput{
				ResourceArn: createOut.Domain.Arn,
			})
			require.NoError(t, err)
			assert.Empty(t, listOut2.Tags)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
