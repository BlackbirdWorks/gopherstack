package opensearch_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// newTestOpenSearchClient stands up the real aws-sdk-go-v2 OpenSearch client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production.
func newTestOpenSearchClient(t *testing.T, h *opensearch.Handler) *opensearchsdk.Client {
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

	return opensearchsdk.NewFromConfig(cfg, func(o *opensearchsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateOps_TagsRoundTrip drives CreateDomain and CreateApplication
// through the real SDK client and asserts ListTags sees what was supplied at
// creation (gopherstack-2mwl). Both Input.TagList fields are []types.Tag --
// a JSON array of {Key,Value} objects, verified at opensearch@v1.75.4
// api_op_CreateDomain.go:153 and api_op_CreateApplication.go:61.
func TestCreateOps_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	tagList := []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}

	requireTags := func(t *testing.T, client *opensearchsdk.Client, resourceARN string) {
		t.Helper()
		out, err := client.ListTags(t.Context(), &opensearchsdk.ListTagsInput{ARN: aws.String(resourceARN)})
		require.NoError(t, err)
		require.Len(t, out.TagList, 1)
		assert.Equal(t, "env", aws.ToString(out.TagList[0].Key))
		assert.Equal(t, "prod", aws.ToString(out.TagList[0].Value))
	}

	// gopherstack's request struct declared TagList as map[string]string, so
	// json.Unmarshal rejected the real array shape outright and every real
	// CreateDomain call carrying tags 400'd with ValidationException before
	// the handler ever reached the backend.
	t.Run("createdomain", func(t *testing.T) {
		t.Parallel()

		h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestOpenSearchClient(t, h)

		out, err := client.CreateDomain(t.Context(), &opensearchsdk.CreateDomainInput{
			DomainName: aws.String("tagged-domain"),
			TagList:    tagList,
		})
		require.NoError(t, err)
		require.NotNil(t, out.DomainStatus)
		requireTags(t, client, aws.ToString(out.DomainStatus.ARN))
	})

	// CreateApplication's TagList was not decoded at all, and Application
	// had no Tags field or ARN index for ListTags/AddTags/RemoveTags to
	// resolve against -- a missing-case gap on top of the decode-drop.
	t.Run("createapplication", func(t *testing.T) {
		t.Parallel()

		h := opensearch.NewHandler(opensearch.NewInMemoryBackend("123456789012", "us-east-1"))
		client := newTestOpenSearchClient(t, h)

		out, err := client.CreateApplication(t.Context(), &opensearchsdk.CreateApplicationInput{
			Name:    aws.String("tagged-app"),
			TagList: tagList,
		})
		require.NoError(t, err)
		requireTags(t, client, aws.ToString(out.Arn))
	})
}
