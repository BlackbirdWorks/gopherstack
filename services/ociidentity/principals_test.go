package ociidentity_test

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	oci "github.com/blackbirdworks/gopherstack/services/ociidentity"
)

func TestUsersAndGroups(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()
	u := e.mkUser(t, "alice")
	g := e.mkGroup(t, "admins")

	assert.Equal(t, identity.UserLifecycleStateActive, u.LifecycleState)
	assert.False(t, *u.IsMfaActivated)
	assert.True(t, *u.Capabilities.CanUseApiKeys)

	upd, err := e.client.UpdateUser(ctx, identity.UpdateUserRequest{
		UserId: u.Id, UpdateUserDetails: identity.UpdateUserDetails{Email: new("a@example.com"), Description: new("changed")},
	})
	require.NoError(t, err)
	assert.Equal(t, "a@example.com", *upd.Email)

	m, err := e.client.AddUserToGroup(ctx, identity.AddUserToGroupRequest{
		AddUserToGroupDetails: identity.AddUserToGroupDetails{UserId: u.Id, GroupId: g.Id},
	})
	require.NoError(t, err)
	assert.Equal(t, *u.Id, *m.UserId)

	got, err := e.client.GetUserGroupMembership(ctx, identity.GetUserGroupMembershipRequest{UserGroupMembershipId: m.Id})
	require.NoError(t, err)
	assert.Equal(t, *g.Id, *got.GroupId)

	byUser, err := e.client.ListUserGroupMemberships(ctx, identity.ListUserGroupMembershipsRequest{
		CompartmentId: new(testTenancy), UserId: u.Id,
	})
	require.NoError(t, err)
	assert.Len(t, byUser.Items, 1)

	byGroup, err := e.client.ListUserGroupMemberships(ctx, identity.ListUserGroupMembershipsRequest{
		CompartmentId: new(testTenancy), GroupId: new("ocid1.group.oc1..none"),
	})
	require.NoError(t, err)
	assert.Empty(t, byGroup.Items)

	tests := []struct {
		name     string
		call     func() error
		wantCode string
		status   int
	}{
		{"duplicate membership", func() error {
			_, err := e.client.AddUserToGroup(ctx, identity.AddUserToGroupRequest{
				AddUserToGroupDetails: identity.AddUserToGroupDetails{UserId: u.Id, GroupId: g.Id},
			})

			return err
		}, "Conflict", http.StatusConflict},
		{"delete user in group", func() error {
			_, err := e.client.DeleteUser(ctx, identity.DeleteUserRequest{UserId: u.Id})

			return err
		}, "Conflict", http.StatusConflict},
		{"delete non-empty group", func() error {
			_, err := e.client.DeleteGroup(ctx, identity.DeleteGroupRequest{GroupId: g.Id})

			return err
		}, "Conflict", http.StatusConflict},
		{"duplicate user name", func() error {
			_, err := e.client.CreateUser(ctx, identity.CreateUserRequest{CreateUserDetails: identity.CreateUserDetails{
				CompartmentId: new(testTenancy), Name: new("ALICE"), Description: new("d"),
			}})

			return err
		}, "Conflict", http.StatusConflict},
		{"user outside tenancy", func() error {
			_, err := e.client.CreateUser(ctx, identity.CreateUserRequest{CreateUserDetails: identity.CreateUserDetails{
				CompartmentId: new("ocid1.compartment.oc1..x"), Name: new("bob"), Description: new("d"),
			}})

			return err
		}, "InvalidParameter", http.StatusBadRequest},
		{"bad email", func() error {
			_, err := e.client.CreateUser(ctx, identity.CreateUserRequest{CreateUserDetails: identity.CreateUserDetails{
				CompartmentId: new(testTenancy), Name: new("bob"), Description: new("d"), Email: new("nope"),
			}})

			return err
		}, "InvalidParameter", http.StatusBadRequest},
		{"unknown user in membership", func() error {
			_, err := e.client.AddUserToGroup(ctx, identity.AddUserToGroupRequest{
				AddUserToGroupDetails: identity.AddUserToGroupDetails{UserId: new("ocid1.user.oc1..x"), GroupId: g.Id},
			})

			return err
		}, "NotAuthorizedOrNotFound", http.StatusNotFound},
		{"get unknown group", func() error {
			_, err := e.client.GetGroup(ctx, identity.GetGroupRequest{GroupId: new("ocid1.group.oc1..x")})

			return err
		}, "NotAuthorizedOrNotFound", http.StatusNotFound},
	}

	t.Run("errors", func(t *testing.T) {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				se := svcErr(t, tt.call())
				assert.Equal(t, tt.status, se.GetHTTPStatusCode())
				assert.Equal(t, tt.wantCode, se.GetCode())
			})
		}
	})

	_, err = e.client.RemoveUserFromGroup(ctx, identity.RemoveUserFromGroupRequest{UserGroupMembershipId: m.Id})
	require.NoError(t, err)

	_, err = e.client.DeleteGroup(ctx, identity.DeleteGroupRequest{GroupId: g.Id})
	require.NoError(t, err)

	_, err = e.client.DeleteUser(ctx, identity.DeleteUserRequest{UserId: u.Id})
	require.NoError(t, err)

	_, err = e.client.GetUser(ctx, identity.GetUserRequest{UserId: u.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestListUsersPaging(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	for i := range 5 {
		e.mkUser(t, "user"+strconv.Itoa(i))
	}

	var (
		names []string
		token *string
	)

	for {
		r, err := e.client.ListUsers(t.Context(), identity.ListUsersRequest{
			CompartmentId: new(testTenancy), Limit: new(2), Page: token, SortBy: identity.ListUsersSortByName,
		})
		require.NoError(t, err)

		for _, u := range r.Items {
			names = append(names, *u.Name)
		}

		if token = r.OpcNextPage; token == nil {
			break
		}
	}

	assert.Equal(t, []string{"user0", "user1", "user2", "user3", "user4"}, names)
}

func TestDynamicGroups(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)
	ctx := t.Context()

	tests := []struct {
		name    string
		rule    string
		wantErr bool
	}{
		{"any single", "ANY {instance.compartment.id = 'ocid1.compartment.oc1..a'}", false},
		{"all several", "ALL {resource.type = 'fnfunc', resource.compartment.id = 'ocid1.compartment.oc1..a'}", false},
		{"lowercase keyword", "any {tag.Ops.Cost.value = 'x'}", false},
		{"not equal", "ANY {instance.id != 'x'}", false},
		{"pattern", "ANY {tag.Ops.Name.value = /web*/}", false},
		{"no keyword", "{instance.id = 'x'}", true},
		{"unterminated", "ANY {instance.id = 'x'", true},
		{"unterminated string", "ANY {instance.id = 'x}", true},
		{"empty group", "ANY {}", true},
		{"no operator", "ANY {instance.id 'x'}", true},
		{"not a variable", "ANY {id = 'x'}", true},
		{"trailing", "ANY {instance.id = 'x'} extra", true},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r, err := e.client.CreateDynamicGroup(ctx, identity.CreateDynamicGroupRequest{
				CreateDynamicGroupDetails: identity.CreateDynamicGroupDetails{
					CompartmentId: new(testTenancy), Name: new("dg" + strconv.Itoa(i)), Description: new("d"),
					MatchingRule: new(tt.rule),
				},
			})
			if tt.wantErr {
				se := svcErr(t, err)
				assert.Equal(t, "InvalidParameter", se.GetCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.rule, *r.MatchingRule)
		})
	}

	dg := identity.CreateDynamicGroupDetails{
		CompartmentId: new(testTenancy), Name: new("main"), Description: new("d"), MatchingRule: new("ANY {instance.id = 'x'}"),
	}
	created, err := e.client.CreateDynamicGroup(ctx, identity.CreateDynamicGroupRequest{CreateDynamicGroupDetails: dg})
	require.NoError(t, err)

	_, err = e.client.CreateDynamicGroup(ctx, identity.CreateDynamicGroupRequest{CreateDynamicGroupDetails: dg})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.UpdateDynamicGroup(ctx, identity.UpdateDynamicGroupRequest{
		DynamicGroupId:            created.Id,
		UpdateDynamicGroupDetails: identity.UpdateDynamicGroupDetails{MatchingRule: new("bogus")},
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	up, err := e.client.UpdateDynamicGroup(ctx, identity.UpdateDynamicGroupRequest{
		DynamicGroupId:            created.Id,
		UpdateDynamicGroupDetails: identity.UpdateDynamicGroupDetails{MatchingRule: new("ALL {instance.id = 'y'}")},
	})
	require.NoError(t, err)
	assert.Equal(t, "ALL {instance.id = 'y'}", *up.MatchingRule)

	_, err = e.client.DeleteDynamicGroup(ctx, identity.DeleteDynamicGroupRequest{DynamicGroupId: created.Id})
	require.NoError(t, err)

	lst, err := e.client.ListDynamicGroups(ctx, identity.ListDynamicGroupsRequest{
		CompartmentId: new(testTenancy), Name: new("main"),
	})
	require.NoError(t, err)
	assert.Empty(t, lst.Items)
}

func TestDynamicGroupLimit(t *testing.T) {
	t.Parallel()

	e := newEnv(t, false)

	for i := range 50 {
		_, err := e.client.CreateDynamicGroup(t.Context(), identity.CreateDynamicGroupRequest{
			CreateDynamicGroupDetails: identity.CreateDynamicGroupDetails{
				CompartmentId: new(oci.TenancyID), Name: new("g" + strconv.Itoa(i)), Description: new("d"),
				MatchingRule: new("ANY {instance.id = 'x'}"),
			},
		})
		require.NoError(t, err)
	}

	_, err := e.client.CreateDynamicGroup(t.Context(), identity.CreateDynamicGroupRequest{
		CreateDynamicGroupDetails: identity.CreateDynamicGroupDetails{
			CompartmentId: new(oci.TenancyID), Name: new("one-more"), Description: new("d"),
			MatchingRule: new("ANY {instance.id = 'x'}"),
		},
	})
	assert.Equal(t, "LimitExceeded", svcErr(t, err).GetCode())
}
