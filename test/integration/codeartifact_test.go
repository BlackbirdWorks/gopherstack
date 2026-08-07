package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codeartifactsdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	codeartifacttypes "github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CodeArtifact_DomainAndRepositoryLifecycle exercises the core
// CodeArtifact workflow via the AWS SDK v2: create a domain, describe/list it,
// create a repository in that domain, describe/list repositories, then delete
// repository and domain. Primary integration coverage for the CodeArtifact
// REST handler (query-string parameter routing).
func TestIntegration_CodeArtifact_DomainAndRepositoryLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCodeArtifactClient(t)
	ctx := t.Context()

	const (
		domainName = "it-ca-domain"
		repoName   = "it-ca-repo"
	)

	// CreateDomain.
	createDomainOut, err := client.CreateDomain(ctx, &codeartifactsdk.CreateDomainInput{
		Domain: aws.String(domainName),
	})
	require.NoError(t, err)
	require.NotNil(t, createDomainOut.Domain)
	assert.Equal(t, domainName, aws.ToString(createDomainOut.Domain.Name))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteDomain(cleanupCtx, &codeartifactsdk.DeleteDomainInput{Domain: aws.String(domainName)})
	})

	// DescribeDomain.
	descDomainOut, err := client.DescribeDomain(ctx, &codeartifactsdk.DescribeDomainInput{
		Domain: aws.String(domainName),
	})
	require.NoError(t, err)
	require.NotNil(t, descDomainOut.Domain)
	assert.Equal(t, domainName, aws.ToString(descDomainOut.Domain.Name))

	// ListDomains should contain the new domain.
	listDomainsOut, err := client.ListDomains(ctx, &codeartifactsdk.ListDomainsInput{})
	require.NoError(t, err)

	foundDomain := false

	for _, d := range listDomainsOut.Domains {
		if aws.ToString(d.Name) == domainName {
			foundDomain = true

			break
		}
	}

	assert.True(t, foundDomain, "newly created domain should be listed")

	// CreateRepository.
	createRepoOut, err := client.CreateRepository(ctx, &codeartifactsdk.CreateRepositoryInput{
		Domain:      aws.String(domainName),
		Repository:  aws.String(repoName),
		Description: aws.String("integration test repository"),
	})
	require.NoError(t, err)
	require.NotNil(t, createRepoOut.Repository)
	assert.Equal(t, repoName, aws.ToString(createRepoOut.Repository.Name))
	assert.Equal(t, domainName, aws.ToString(createRepoOut.Repository.DomainName))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteRepository(cleanupCtx, &codeartifactsdk.DeleteRepositoryInput{
			Domain:     aws.String(domainName),
			Repository: aws.String(repoName),
		})
	})

	// DescribeRepository.
	descRepoOut, err := client.DescribeRepository(ctx, &codeartifactsdk.DescribeRepositoryInput{
		Domain:     aws.String(domainName),
		Repository: aws.String(repoName),
	})
	require.NoError(t, err)
	require.NotNil(t, descRepoOut.Repository)
	assert.Equal(t, repoName, aws.ToString(descRepoOut.Repository.Name))

	// ListRepositoriesInDomain should contain the new repo.
	listReposOut, err := client.ListRepositoriesInDomain(ctx, &codeartifactsdk.ListRepositoriesInDomainInput{
		Domain: aws.String(domainName),
	})
	require.NoError(t, err)

	foundRepo := false

	for _, r := range listReposOut.Repositories {
		if aws.ToString(r.Name) == repoName {
			foundRepo = true

			break
		}
	}

	assert.True(t, foundRepo, "newly created repository should be listed in domain")
}

// TestIntegration_CodeArtifact_PackageGroupWeakMatch exercises package-group
// weak-match classification (casefold + dash/dot/underscore-run
// normalization) via the real AWS SDK v2 client: a package whose name is an
// exact match for a group's pattern is STRONG; a case/separator variation of
// the same name is WEAK, per AWS's documented dependency-confusion
// protection behavior.
func TestIntegration_CodeArtifact_PackageGroupWeakMatch(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCodeArtifactClient(t)
	ctx := t.Context()

	const (
		domainName = "it-ca-weak-domain"
		repoName   = "it-ca-weak-repo"
	)

	_, err := client.CreateDomain(ctx, &codeartifactsdk.CreateDomainInput{Domain: aws.String(domainName)})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteDomain(cleanupCtx, &codeartifactsdk.DeleteDomainInput{Domain: aws.String(domainName)})
	})

	_, err = client.CreateRepository(ctx, &codeartifactsdk.CreateRepositoryInput{
		Domain:     aws.String(domainName),
		Repository: aws.String(repoName),
	})
	require.NoError(t, err)

	_, err = client.CreatePackageGroup(ctx, &codeartifactsdk.CreatePackageGroupInput{
		Domain:       aws.String(domainName),
		PackageGroup: aws.String("/npm//my-package$"),
	})
	require.NoError(t, err)

	publish := func(pkgName string) {
		t.Helper()

		content := strings.NewReader("integration test content")
		sum := sha256.Sum256([]byte("integration test content"))

		_, publishErr := client.PublishPackageVersion(ctx, &codeartifactsdk.PublishPackageVersionInput{
			Domain:         aws.String(domainName),
			Repository:     aws.String(repoName),
			Format:         codeartifacttypes.PackageFormatNpm,
			Package:        aws.String(pkgName),
			PackageVersion: aws.String("1.0.0"),
			AssetName:      aws.String("a.tgz"),
			AssetContent:   content,
			AssetSHA256:    aws.String(hex.EncodeToString(sum[:])),
		})
		require.NoError(t, publishErr)
	}

	publish("my-package")
	publish("My.Package")

	strongOut, err := client.GetAssociatedPackageGroup(ctx, &codeartifactsdk.GetAssociatedPackageGroupInput{
		Domain:  aws.String(domainName),
		Format:  codeartifacttypes.PackageFormatNpm,
		Package: aws.String("my-package"),
	})
	require.NoError(t, err)
	assert.Equal(t, codeartifacttypes.PackageGroupAssociationTypeStrong, strongOut.AssociationType)

	weakOut, err := client.GetAssociatedPackageGroup(ctx, &codeartifactsdk.GetAssociatedPackageGroupInput{
		Domain:  aws.String(domainName),
		Format:  codeartifacttypes.PackageFormatNpm,
		Package: aws.String("My.Package"),
	})
	require.NoError(t, err)
	assert.Equal(t, codeartifacttypes.PackageGroupAssociationTypeWeak, weakOut.AssociationType)
}
