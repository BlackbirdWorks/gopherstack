package verifiedpermissions_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	avpsdk "github.com/aws/aws-sdk-go-v2/service/verifiedpermissions"
	"github.com/aws/aws-sdk-go-v2/service/verifiedpermissions/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/verifiedpermissions"
)

const (
	rtTestRegion    = "us-east-1"
	rtTestAccountID = "000000000000"
)

// newTestHandlerAndClient stands up a fresh in-memory verifiedpermissions
// backend and a real aws-sdk-go-v2 client against an httptest server running
// its Handler, wired through the same pkgs/service registry/router used in
// production.
func newTestHandlerAndClient(t *testing.T) *avpsdk.Client {
	t.Helper()

	backend := verifiedpermissions.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	h := verifiedpermissions.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return avpsdk.NewFromConfig(cfg, func(o *avpsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestCreatePolicyStore_TagsRoundTrip drives CreatePolicyStore, the only
// verifiedpermissions Create* op whose real Input struct accepts Tags
// (verifiedpermissions@v1.36.4: api_op_CreatePolicyStore.go, `Tags
// map[string]string`; CreateIdentitySource/CreatePolicy/
// CreatePolicyStoreAlias/CreatePolicyTemplate take no Tags), through the
// real SDK client and asserts ListTagsForResource sees what was supplied at
// creation (gopherstack-2mwl).
func TestCreatePolicyStore_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestHandlerAndClient(t)

	wantTags := map[string]string{"env": "prod"}

	out, err := client.CreatePolicyStore(t.Context(), &avpsdk.CreatePolicyStoreInput{
		ValidationSettings: &types.ValidationSettings{Mode: types.ValidationModeOff},
		Tags:               wantTags,
	})
	require.NoError(t, err)

	got, err := client.ListTagsForResource(t.Context(), &avpsdk.ListTagsForResourceInput{
		ResourceArn: out.Arn,
	})
	require.NoError(t, err)
	assert.Equal(t, wantTags, got.Tags)
}
