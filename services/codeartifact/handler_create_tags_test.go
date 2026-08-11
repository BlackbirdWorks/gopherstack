package codeartifact_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	casdk "github.com/aws/aws-sdk-go-v2/service/codeartifact"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codeartifact"
)

// newTestCodeArtifactClient stands up the real aws-sdk-go-v2 CodeArtifact
// client against an httptest server running this package's Handler, wired
// through the same pkgs/service registry/router used in production.
func newTestCodeArtifactClient(t *testing.T, h *codeartifact.Handler) *casdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return casdk.NewFromConfig(cfg, func(o *casdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives every CodeArtifact Create op that
// accepts Tags in the real SDK (codeartifact@v1.41.4: CreateDomain,
// CreateRepository, CreatePackageGroup) through a real SDK client and
// asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl).
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tags := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	newClient := func(t *testing.T) *casdk.Client {
		t.Helper()

		h := codeartifact.NewHandler(codeartifact.NewInMemoryBackend("123456789012", "us-east-1"))

		return newTestCodeArtifactClient(t, h)
	}

	requireTags := func(t *testing.T, client *casdk.Client, resourceARN *string) {
		t.Helper()

		out, err := client.ListTagsForResource(t.Context(), &casdk.ListTagsForResourceInput{
			ResourceArn: resourceARN,
		})
		require.NoError(t, err)
		require.Len(t, out.Tags, 1)
		assert.Equal(t, "env", aws.ToString(out.Tags[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.Tags[0].Value))
	}

	t.Run("createdomain", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		out, err := client.CreateDomain(t.Context(), &casdk.CreateDomainInput{
			Domain: aws.String("tagged-domain"),
			Tags:   tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.Domain.Arn)
	})

	t.Run("createrepository", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		_, err := client.CreateDomain(t.Context(), &casdk.CreateDomainInput{
			Domain: aws.String("tagged-repo-domain"),
		})
		require.NoError(t, err)

		out, err := client.CreateRepository(t.Context(), &casdk.CreateRepositoryInput{
			Domain:     aws.String("tagged-repo-domain"),
			Repository: aws.String("tagged-repo"),
			Tags:       tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.Repository.Arn)
	})

	t.Run("createpackagegroup", func(t *testing.T) {
		t.Parallel()

		client := newClient(t)

		_, err := client.CreateDomain(t.Context(), &casdk.CreateDomainInput{
			Domain: aws.String("tagged-pg-domain"),
		})
		require.NoError(t, err)

		out, err := client.CreatePackageGroup(t.Context(), &casdk.CreatePackageGroupInput{
			Domain:       aws.String("tagged-pg-domain"),
			PackageGroup: aws.String("/npm/tagged/*"),
			Tags:         tags,
		})
		require.NoError(t, err)
		requireTags(t, client, out.PackageGroup.Arn)
	})
}
