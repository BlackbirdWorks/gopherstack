package identitystore_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	identitystoresdk "github.com/aws/aws-sdk-go-v2/service/identitystore"
	"github.com/aws/aws-sdk-go-v2/service/identitystore/document"
	identitystoretypes "github.com/aws/aws-sdk-go-v2/service/identitystore/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/identitystore"
)

// newSlice19IdentityStoreClient stands up the real aws-sdk-go-v2 identitystore
// client against an httptest server running this package's Handler through
// the same pkgs/service registry/router used in production -- this package
// had no real-client helper at all before this slice (gopherstack-n3zi).
func newSlice19IdentityStoreClient(t *testing.T) *identitystoresdk.Client {
	t.Helper()

	backend := identitystore.NewInMemoryBackend("123456789012", "us-east-1")
	h := identitystore.NewHandler(backend)

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

	return identitystoresdk.NewFromConfig(cfg, func(o *identitystoresdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestTypedSlice19_IdentityStore drives the 11 ops that a real aws-sdk-go-v2
// identitystore client had never exercised before this slice (gopherstack-n3zi):
// CreateGroupMembership, DeleteGroupMembership, DescribeGroupMembership,
// GetGroupId, GetGroupMembershipId, IsMemberInGroups, ListGroupMemberships,
// ListGroupMembershipsForMember, ListGroups, UpdateGroup, UpdateUser.
func TestTypedSlice19_IdentityStore(t *testing.T) {
	t.Parallel()

	const identityStoreID = "d-1234567890"

	client := newSlice19IdentityStoreClient(t)
	ctx := t.Context()

	createdUser, setupErr := client.CreateUser(ctx, &identitystoresdk.CreateUserInput{
		IdentityStoreId: aws.String(identityStoreID),
		UserName:        aws.String("s19-user"),
		DisplayName:     aws.String("Slice19 User"),
	})
	require.NoError(t, setupErr)
	userID := aws.ToString(createdUser.UserId)

	createdGroup, setupErr := client.CreateGroup(ctx, &identitystoresdk.CreateGroupInput{
		IdentityStoreId: aws.String(identityStoreID),
		DisplayName:     aws.String("s19-group"),
	})
	require.NoError(t, setupErr)
	groupID := aws.ToString(createdGroup.GroupId)

	t.Run("get_group_id", func(t *testing.T) {
		t.Parallel()

		out, err := client.GetGroupId(ctx, &identitystoresdk.GetGroupIdInput{
			IdentityStoreId: aws.String(identityStoreID),
			AlternateIdentifier: &identitystoretypes.AlternateIdentifierMemberUniqueAttribute{
				Value: identitystoretypes.UniqueAttribute{
					AttributePath:  aws.String("DisplayName"),
					AttributeValue: document.NewLazyDocument("s19-group"),
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, groupID, aws.ToString(out.GroupId))
	})

	t.Run("list_groups", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListGroups(ctx, &identitystoresdk.ListGroupsInput{
			IdentityStoreId: aws.String(identityStoreID),
		})
		require.NoError(t, err)
		found := false
		for _, g := range out.Groups {
			if aws.ToString(g.GroupId) == groupID {
				found = true
			}
		}
		require.True(t, found, "ListGroups must include the created group")
	})

	t.Run("update_group", func(t *testing.T) {
		t.Parallel()

		_, err := client.UpdateGroup(ctx, &identitystoresdk.UpdateGroupInput{
			IdentityStoreId: aws.String(identityStoreID),
			GroupId:         aws.String(groupID),
			Operations: []identitystoretypes.AttributeOperation{
				{
					AttributePath:  aws.String("description"),
					AttributeValue: document.NewLazyDocument("updated by slice19"),
				},
			},
		})
		require.NoError(t, err)

		desc, err := client.DescribeGroup(ctx, &identitystoresdk.DescribeGroupInput{
			IdentityStoreId: aws.String(identityStoreID),
			GroupId:         aws.String(groupID),
		})
		require.NoError(t, err)
		require.Equal(t, "updated by slice19", aws.ToString(desc.Description))
	})

	t.Run("update_user", func(t *testing.T) {
		t.Parallel()

		_, err := client.UpdateUser(ctx, &identitystoresdk.UpdateUserInput{
			IdentityStoreId: aws.String(identityStoreID),
			UserId:          aws.String(userID),
			Operations: []identitystoretypes.AttributeOperation{
				{
					AttributePath:  aws.String("title"),
					AttributeValue: document.NewLazyDocument("Staff Engineer"),
				},
			},
		})
		require.NoError(t, err)

		desc, err := client.DescribeUser(ctx, &identitystoresdk.DescribeUserInput{
			IdentityStoreId: aws.String(identityStoreID),
			UserId:          aws.String(userID),
		})
		require.NoError(t, err)
		require.Equal(t, "Staff Engineer", aws.ToString(desc.Title))
	})

	t.Run("group_membership_lifecycle", func(t *testing.T) {
		t.Parallel()

		created, err := client.CreateGroupMembership(ctx, &identitystoresdk.CreateGroupMembershipInput{
			IdentityStoreId: aws.String(identityStoreID),
			GroupId:         aws.String(groupID),
			MemberId:        &identitystoretypes.MemberIdMemberUserId{Value: userID},
		})
		require.NoError(t, err)
		membershipID := aws.ToString(created.MembershipId)
		require.NotEmpty(t, membershipID)

		// GetGroupMembershipId.
		gotID, err := client.GetGroupMembershipId(ctx, &identitystoresdk.GetGroupMembershipIdInput{
			IdentityStoreId: aws.String(identityStoreID),
			GroupId:         aws.String(groupID),
			MemberId:        &identitystoretypes.MemberIdMemberUserId{Value: userID},
		})
		require.NoError(t, err)
		require.Equal(t, membershipID, aws.ToString(gotID.MembershipId))

		// DescribeGroupMembership.
		desc, err := client.DescribeGroupMembership(ctx, &identitystoresdk.DescribeGroupMembershipInput{
			IdentityStoreId: aws.String(identityStoreID),
			MembershipId:    aws.String(membershipID),
		})
		require.NoError(t, err)
		require.Equal(t, groupID, aws.ToString(desc.GroupId))
		memberUserID, ok := desc.MemberId.(*identitystoretypes.MemberIdMemberUserId)
		require.True(t, ok, "MemberId must decode as MemberIdMemberUserId")
		require.Equal(t, userID, memberUserID.Value)

		// ListGroupMemberships.
		listed, err := client.ListGroupMemberships(ctx, &identitystoresdk.ListGroupMembershipsInput{
			IdentityStoreId: aws.String(identityStoreID),
			GroupId:         aws.String(groupID),
		})
		require.NoError(t, err)
		require.Len(t, listed.GroupMemberships, 1)
		require.Equal(t, membershipID, aws.ToString(listed.GroupMemberships[0].MembershipId))

		// ListGroupMembershipsForMember.
		listedForMember, err := client.ListGroupMembershipsForMember(
			ctx,
			&identitystoresdk.ListGroupMembershipsForMemberInput{
				IdentityStoreId: aws.String(identityStoreID),
				MemberId:        &identitystoretypes.MemberIdMemberUserId{Value: userID},
			},
		)
		require.NoError(t, err)
		require.Len(t, listedForMember.GroupMemberships, 1)
		require.Equal(t, groupID, aws.ToString(listedForMember.GroupMemberships[0].GroupId))

		// IsMemberInGroups.
		isMember, err := client.IsMemberInGroups(ctx, &identitystoresdk.IsMemberInGroupsInput{
			IdentityStoreId: aws.String(identityStoreID),
			MemberId:        &identitystoretypes.MemberIdMemberUserId{Value: userID},
			GroupIds:        []string{groupID},
		})
		require.NoError(t, err)
		require.Len(t, isMember.Results, 1)
		require.Equal(t, groupID, aws.ToString(isMember.Results[0].GroupId))
		require.True(t, isMember.Results[0].MembershipExists)

		// DeleteGroupMembership.
		_, err = client.DeleteGroupMembership(ctx, &identitystoresdk.DeleteGroupMembershipInput{
			IdentityStoreId: aws.String(identityStoreID),
			MembershipId:    aws.String(membershipID),
		})
		require.NoError(t, err)

		_, err = client.DescribeGroupMembership(ctx, &identitystoresdk.DescribeGroupMembershipInput{
			IdentityStoreId: aws.String(identityStoreID),
			MembershipId:    aws.String(membershipID),
		})
		require.Error(t, err, "membership must be gone after DeleteGroupMembership")
	})
}
