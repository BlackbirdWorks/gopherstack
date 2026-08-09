package mediastore_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediastoresdk "github.com/aws/aws-sdk-go-v2/service/mediastore"
	"github.com/aws/aws-sdk-go-v2/service/mediastore/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mediastore"
)

// newTestMediaStoreClient stands up the real aws-sdk-go-v2 MediaStore client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestMediaStoreClient(t *testing.T, h *mediastore.Handler) *mediastoresdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(testRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return mediastoresdk.NewFromConfig(cfg, func(o *mediastoresdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateContainer_TagsRoundTrip drives CreateContainer, whose real Input
// struct accepts Tags (mediastore@v1.32.4 api_op_CreateContainer.go:19,
// `Tags []types.Tag`), through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
func TestCreateContainer_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestMediaStoreClient(t, newTestHandler(t))

	out, err := client.CreateContainer(t.Context(), &mediastoresdk.CreateContainerInput{
		ContainerName: aws.String("tagged-container"),
		Tags:          []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	got, err := client.ListTagsForResource(t.Context(), &mediastoresdk.ListTagsForResourceInput{
		Resource: out.Container.ARN,
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(got.Tags[0].Value))
}
