package cognitoidp_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cognitoidpsdk "github.com/aws/aws-sdk-go-v2/service/cognitoidentityprovider"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// newTestCognitoIDPClient stands up the real aws-sdk-go-v2 Cognito Identity
// Provider client against an httptest server running this package's
// Handler, wired through the same pkgs/service registry/router used in
// production.
func newTestCognitoIDPClient(t *testing.T, h *cognitoidp.Handler) *cognitoidpsdk.Client {
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

	return cognitoidpsdk.NewFromConfig(cfg, func(o *cognitoidpsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreateUserPool_TagsRoundTrip drives CreateUserPool through a real SDK
// client and asserts ListTagsForResource sees what was supplied at creation
// (gopherstack-2mwl). CreateUserPoolInput.UserPoolTags is map[string]string
// (verified: cognitoidentityprovider@v1.67.4 api_op_CreateUserPool.go:230),
// but gopherstack's decode struct (createUserPoolWithOptsInput) had no
// UserPoolTags field at all -- a pure decode-drop -- and the backend's
// UserPoolOptions carried no Tags field either, so there was nowhere for the
// value to land even if decoded. CreateUserPoolReplica's sibling UserPoolTags
// field was already wired correctly and is not exercised here.
func TestCreateUserPool_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := cognitoidp.NewInMemoryBackend("123456789012", "us-east-1", "http://localhost:8000")
	h := cognitoidp.NewHandler(backend, "us-east-1")
	client := newTestCognitoIDPClient(t, h)

	out, err := client.CreateUserPool(t.Context(), &cognitoidpsdk.CreateUserPoolInput{
		PoolName:     aws.String("tagged-pool"),
		UserPoolTags: map[string]string{"env": "prod"},
	})
	require.NoError(t, err)
	require.NotNil(t, out.UserPool)

	tagsOut, err := client.ListTagsForResource(t.Context(), &cognitoidpsdk.ListTagsForResourceInput{
		ResourceArn: out.UserPool.Arn,
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, tagsOut.Tags)
}
