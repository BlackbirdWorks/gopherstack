package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codeartifactsdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
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
