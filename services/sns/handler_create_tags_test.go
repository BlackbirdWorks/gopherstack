package sns_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sns"
)

// newTestSNSClient stands up the real aws-sdk-go-v2 SNS client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production.
func newTestSNSClient(t *testing.T, h *sns.Handler) *snssdk.Client {
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

	return snssdk.NewFromConfig(cfg, func(o *snssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateTopic_TagsRoundTrip drives CreateTopic, the only sns Create* op
// whose real Input struct accepts Tags (sns@v1.42.4: api_op_CreateTopic.go:201
// -- CreatePlatformApplication/CreatePlatformEndpoint/CreateSMSSandboxPhoneNumber
// take no Tags field), through the real SDK client and asserts
// ListTagsForResource sees what was supplied at creation (gopherstack-2mwl).
func TestCreateTopic_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := sns.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	h := sns.NewHandler(backend)
	client := newTestSNSClient(t, h)

	out, err := client.CreateTopic(t.Context(), &snssdk.CreateTopicInput{
		Name: aws.String("tagged-topic"),
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(out.TopicArn))

	tagsOut, err := client.ListTagsForResource(t.Context(), &snssdk.ListTagsForResourceInput{
		ResourceArn: out.TopicArn,
	})
	require.NoError(t, err)

	require.Len(t, tagsOut.Tags, 1)
	assert.Equal(t, "env", aws.ToString(tagsOut.Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(tagsOut.Tags[0].Value))
}
