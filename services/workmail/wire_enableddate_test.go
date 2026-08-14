package workmail_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	workmailsdk "github.com/aws/aws-sdk-go-v2/service/workmail"
	"github.com/aws/aws-sdk-go-v2/service/workmail/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// newWorkMailSDKClient stands up the real aws-sdk-go-v2 workmail client
// against an httptest server running this package's Handler through the
// same pkgs/service registry/router used in production, so responses are
// decoded by the genuine SDK deserializer rather than ad-hoc structs.
func newWorkMailSDKClient(t *testing.T, h *workmail.Handler) *workmailsdk.Client {
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

	return workmailsdk.NewFromConfig(cfg, func(o *workmailsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_ListUsers_EnabledDate proves ListUsers decodes a
// non-nil EnabledDate through the real SDK client. types.User (the
// ListUsersOutput item, aws-sdk-go-v2/service/workmail@v1.39.4/types/types.go)
// carries EnabledDate/DisabledDate exactly like DescribeUserOutput, but the
// backend's ListUsers built a UserSummary DTO that dropped both fields
// before they ever reached the handler -- a raw-JSON assertion on
// DescribeUser's shape would never have caught the List path losing them.
func Test_SDKRoundTrip_ListUsers_EnabledDate(t *testing.T) {
	t.Parallel()

	backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
	h := workmail.NewHandler(backend)
	client := newWorkMailSDKClient(t, h)
	ctx := t.Context()

	org, err := client.CreateOrganization(ctx, &workmailsdk.CreateOrganizationInput{
		Alias: aws.String("org-" + uuid.NewString()[:8]),
	})
	require.NoError(t, err)

	userName := "user-" + uuid.NewString()[:8]
	user, err := client.CreateUser(ctx, &workmailsdk.CreateUserInput{
		OrganizationId: org.OrganizationId,
		Name:           aws.String(userName),
		DisplayName:    aws.String(userName),
	})
	require.NoError(t, err)

	_, err = client.RegisterToWorkMail(ctx, &workmailsdk.RegisterToWorkMailInput{
		OrganizationId: org.OrganizationId,
		EntityId:       user.UserId,
		Email:          aws.String(userName + "@example.com"),
	})
	require.NoError(t, err)

	listed, err := client.ListUsers(ctx, &workmailsdk.ListUsersInput{OrganizationId: org.OrganizationId})
	require.NoError(t, err)
	require.Len(t, listed.Users, 1)
	require.NotNil(t, listed.Users[0].EnabledDate, "ListUsers must decode a non-nil EnabledDate")
	assert.NotZero(t, *listed.Users[0].EnabledDate)
}

// Test_SDKRoundTrip_ListGroups_EnabledDate is ListUsers' sibling check for
// ListGroups/types.Group.
func Test_SDKRoundTrip_ListGroups_EnabledDate(t *testing.T) {
	t.Parallel()

	backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
	h := workmail.NewHandler(backend)
	client := newWorkMailSDKClient(t, h)
	ctx := t.Context()

	org, err := client.CreateOrganization(ctx, &workmailsdk.CreateOrganizationInput{
		Alias: aws.String("org-" + uuid.NewString()[:8]),
	})
	require.NoError(t, err)

	groupName := "group-" + uuid.NewString()[:8]
	group, err := client.CreateGroup(ctx, &workmailsdk.CreateGroupInput{
		OrganizationId: org.OrganizationId,
		Name:           aws.String(groupName),
	})
	require.NoError(t, err)

	_, err = client.RegisterToWorkMail(ctx, &workmailsdk.RegisterToWorkMailInput{
		OrganizationId: org.OrganizationId,
		EntityId:       group.GroupId,
		Email:          aws.String(groupName + "@example.com"),
	})
	require.NoError(t, err)

	listed, err := client.ListGroups(ctx, &workmailsdk.ListGroupsInput{OrganizationId: org.OrganizationId})
	require.NoError(t, err)
	require.Len(t, listed.Groups, 1)
	require.NotNil(t, listed.Groups[0].EnabledDate, "ListGroups must decode a non-nil EnabledDate")
	assert.NotZero(t, *listed.Groups[0].EnabledDate)
}

// Test_SDKRoundTrip_ListResources_EnabledDate is ListUsers' sibling check
// for ListResources/types.Resource.
func Test_SDKRoundTrip_ListResources_EnabledDate(t *testing.T) {
	t.Parallel()

	backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
	h := workmail.NewHandler(backend)
	client := newWorkMailSDKClient(t, h)
	ctx := t.Context()

	org, err := client.CreateOrganization(ctx, &workmailsdk.CreateOrganizationInput{
		Alias: aws.String("org-" + uuid.NewString()[:8]),
	})
	require.NoError(t, err)

	resName := "res-" + uuid.NewString()[:8]
	res, err := client.CreateResource(ctx, &workmailsdk.CreateResourceInput{
		OrganizationId: org.OrganizationId,
		Name:           aws.String(resName),
		Type:           types.ResourceTypeRoom,
	})
	require.NoError(t, err)

	_, err = client.RegisterToWorkMail(ctx, &workmailsdk.RegisterToWorkMailInput{
		OrganizationId: org.OrganizationId,
		EntityId:       res.ResourceId,
		Email:          aws.String(resName + "@example.com"),
	})
	require.NoError(t, err)

	listed, err := client.ListResources(ctx, &workmailsdk.ListResourcesInput{OrganizationId: org.OrganizationId})
	require.NoError(t, err)
	require.Len(t, listed.Resources, 1)
	require.NotNil(t, listed.Resources[0].EnabledDate, "ListResources must decode a non-nil EnabledDate")
	assert.NotZero(t, *listed.Resources[0].EnabledDate)
}

// Test_SDKRoundTrip_PersonalAccessToken_DateLastUsed_OmittedWhenUnused
// proves a never-used personal access token's DateLastUsed decodes as nil
// rather than the zero time.Time's Unix() (a large negative number that a
// raw-JSON "field present" check would not catch, since it silently defeats
// the omitempty tag instead of failing to decode).
func Test_SDKRoundTrip_PersonalAccessToken_DateLastUsed_OmittedWhenUnused(t *testing.T) {
	t.Parallel()

	backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
	h := workmail.NewHandler(backend)
	client := newWorkMailSDKClient(t, h)
	ctx := t.Context()

	org, err := client.CreateOrganization(ctx, &workmailsdk.CreateOrganizationInput{
		Alias: aws.String("org-" + uuid.NewString()[:8]),
	})
	require.NoError(t, err)

	userName := "user-" + uuid.NewString()[:8]
	user, err := client.CreateUser(ctx, &workmailsdk.CreateUserInput{
		OrganizationId: org.OrganizationId,
		Name:           aws.String(userName),
		DisplayName:    aws.String(userName),
	})
	require.NoError(t, err)

	tok, err := backend.CreatePersonalAccessToken(
		*org.OrganizationId, *user.UserId, "token-"+uuid.NewString()[:8], nil,
	)
	require.NoError(t, err)

	meta, err := client.GetPersonalAccessTokenMetadata(ctx, &workmailsdk.GetPersonalAccessTokenMetadataInput{
		OrganizationId:        org.OrganizationId,
		PersonalAccessTokenId: aws.String(tok.TokenID),
	})
	require.NoError(t, err)
	require.NotNil(t, meta.DateCreated)
	assert.Nil(t, meta.DateLastUsed, "a never-used token must decode a nil DateLastUsed, not a huge negative epoch")
}
