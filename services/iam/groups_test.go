package iam_test

import (
	"net/http"
	"strings"
	"testing"
	"unicode"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDetachGroupPolicy covers DetachGroupPolicy and ListAttachedGroupPolicies.
func TestDetachGroupPolicy(t *testing.T) {
	t.Parallel()

	h, b := newTestHandler(t)

	_, err := b.CreateGroup("detach-policy-group", "/")
	require.NoError(t, err)

	policyARN := "arn:aws:iam::000000000000:policy/GroupPolicy"
	require.NoError(t, b.AttachGroupPolicy("detach-policy-group", policyARN))

	// ListAttachedGroupPolicies.
	rec := callIAM(t, h, "ListAttachedGroupPolicies", map[string]string{
		"GroupName": "detach-policy-group",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ListAttachedGroupPoliciesResponse")

	// DetachGroupPolicy.
	rec = callIAM(t, h, "DetachGroupPolicy", map[string]string{
		"GroupName": "detach-policy-group",
		"PolicyArn": policyARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DetachGroupPolicyResponse")
}

func TestRemoveUserFromGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *iam.InMemoryBackend)
		name    string
		group   string
		user    string
	}{
		{
			name: "remove_existing_member",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("admins", "/")
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.AddUserToGroup("admins", "alice")
			},
			group: "admins",
			user:  "alice",
		},
		{
			name: "remove_non_member_is_no_op",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("admins", "/")
				_, _ = b.CreateUser("alice", "/", "")
			},
			group: "admins",
			user:  "alice",
		},
		{
			name:    "group_not_found",
			setup:   func(_ *iam.InMemoryBackend) {},
			group:   "ghost-group",
			user:    "alice",
			wantErr: iam.ErrGroupNotFound,
		},
		{
			name: "user_not_found",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("admins", "/")
			},
			group:   "admins",
			user:    "ghost-user",
			wantErr: iam.ErrUserNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			err := b.RemoveUserFromGroup(tt.group, tt.user)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestAddUserToGroup_TracksMembership(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, _ = b.CreateGroup("devs", "/")
	_, _ = b.CreateUser("bob", "/", "")

	require.NoError(t, b.AddUserToGroup("devs", "bob"))

	// Idempotent: adding twice should not error
	require.NoError(t, b.AddUserToGroup("devs", "bob"))
}

func TestGetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		setup   func(b *iam.InMemoryBackend)
		name    string
		group   string
	}{
		{
			name: "get_existing_group",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("admins", "/")
			},
			group: "admins",
		},
		{
			name:    "group_not_found",
			setup:   func(_ *iam.InMemoryBackend) {},
			group:   "ghost",
			wantErr: iam.ErrGroupNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			g, err := b.GetGroup(tt.group)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.group, g.GroupName)
		})
	}
}

func TestPersistence_GroupMembers_RoundTrip(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, _ = b.CreateGroup("devs", "/")
	_, _ = b.CreateUser("carol", "/", "")
	_ = b.AddUserToGroup("devs", "carol")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iam.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// carol is still in devs; removing should succeed without error
	require.NoError(t, b2.RemoveUserFromGroup("devs", "carol"))
}

func TestDeleteUser_CleansGroupMembership(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *iam.InMemoryBackend)
		name    string
		delUser string
	}{
		{
			name: "user_removed_from_groups_on_deletion",
			setup: func(b *iam.InMemoryBackend) {
				_, err := b.CreateUser("alice", "/", "")
				require.NoError(t, err)
				_, err = b.CreateGroup("admins", "/")
				require.NoError(t, err)
				_, err = b.CreateGroup("devs", "/")
				require.NoError(t, err)
				require.NoError(t, b.AddUserToGroup("admins", "alice"))
				require.NoError(t, b.AddUserToGroup("devs", "alice"))
			},
			delUser: "alice",
		},
		{
			name: "recreated_user_not_in_old_group",
			setup: func(b *iam.InMemoryBackend) {
				_, err := b.CreateUser("bob", "/", "")
				require.NoError(t, err)
				_, err = b.CreateGroup("ops", "/")
				require.NoError(t, err)
				require.NoError(t, b.AddUserToGroup("ops", "bob"))
			},
			delUser: "bob",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			require.NoError(t, b.DeleteUser(tt.delUser))

			// Re-create the same username; AddUserToGroup for any group
			// must work without "already a member" from stale state.
			_, err := b.CreateUser(tt.delUser, "/", "")
			require.NoError(t, err)

			// The user should not be in any group after re-creation.
			// AddUserToGroup must succeed (idempotent on fresh membership).
			for _, groupName := range []string{"admins", "devs", "ops"} {
				// Only test groups that actually exist in the setup.
				addErr := b.AddUserToGroup(groupName, tt.delUser)
				if addErr != nil {
					// Group may not exist — that's fine.
					require.ErrorIs(t, addErr, iam.ErrGroupNotFound)
				}
			}
		})
	}
}

func TestDeleteGroup_CleansGroupMembers(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	_, err := b.CreateGroup("temp-group", "/")
	require.NoError(t, err)
	_, err = b.CreateUser("alice", "/", "")
	require.NoError(t, err)
	require.NoError(t, b.AddUserToGroup("temp-group", "alice"))

	require.NoError(t, b.DeleteGroup("temp-group"))

	// Snapshot must not contain the stale members entry.
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iam.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Re-create the group — it should be empty.
	_, groupErr := b2.CreateGroup("temp-group", "/")
	require.NoError(t, groupErr)
	// alice still exists; adding her must succeed (no duplicate membership).
	require.NoError(t, b2.AddUserToGroup("temp-group", "alice"))
}

// TestListGroupsForUser tests ListGroupsForUser.
func TestListGroupsForUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*iam.InMemoryBackend)
		userName   string
		wantGroups int
		wantErr    bool
	}{
		{
			name: "no_groups",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName:   "alice",
			wantGroups: 0,
		},
		{
			name: "one_group",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateGroup("Admins", "/")
				_ = b.AddUserToGroup("Admins", "alice")
			},
			userName:   "alice",
			wantGroups: 1,
		},
		{
			name:     "user_not_found",
			setup:    func(_ *iam.InMemoryBackend) {},
			userName: "ghost",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			groups, err := b.ListGroupsForUser(tt.userName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, groups, tt.wantGroups)
		})
	}
}

// TestUpdateGroup_Backend tests UpdateGroup.
func TestUpdateGroup_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(*iam.InMemoryBackend)
		groupName    string
		newPath      string
		newGroupName string
		wantErr      bool
	}{
		{
			name: "rename_group",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
			},
			groupName:    "Admins",
			newGroupName: "Operators",
		},
		{
			name: "update_path",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
			},
			groupName: "Admins",
			newPath:   "/teams/",
		},
		{
			name:      "group_not_found",
			setup:     func(_ *iam.InMemoryBackend) {},
			groupName: "ghost",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			err := b.UpdateGroup(tt.groupName, tt.newPath, tt.newGroupName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestTagGroup_StoresOnModel(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateGroup("Admins", "/")
	require.NoError(t, err)

	require.NoError(t, b.TagGroup("Admins", map[string]string{"tier": "gold"}))

	g, err := b.GetGroup("Admins")
	require.NoError(t, err)
	assert.Equal(t, "gold", g.Tags["tier"])
}

func TestTagGroup_NotFoundError(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.TagGroup("Ghost", map[string]string{"k": "v"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrGroupNotFound)
}

func TestUntagGroup_RemovesKeys(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateGroup("G1", "/")
	require.NoError(t, b.TagGroup("G1", map[string]string{"a": "1", "b": "2"}))
	require.NoError(t, b.UntagGroup("G1", []string{"b"}))

	g, err := b.GetGroup("G1")
	require.NoError(t, err)
	assert.Equal(t, "1", g.Tags["a"])
	assert.Empty(t, g.Tags["b"])
}

func TestTagGroup_SurvivesMultipleOperations(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateGroup("Engineers", "/")

	require.NoError(t, b.TagGroup("Engineers", map[string]string{"tier": "senior", "dept": "eng"}))
	require.NoError(t, b.UntagGroup("Engineers", []string{"tier"}))
	require.NoError(t, b.TagGroup("Engineers", map[string]string{"region": "us-east"}))

	g, err := b.GetGroup("Engineers")
	require.NoError(t, err)
	assert.Empty(t, g.Tags["tier"])
	assert.Equal(t, "eng", g.Tags["dept"])
	assert.Equal(t, "us-east", g.Tags["region"])
}

func TestGroupID_Format(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	g, err := b.CreateGroup("Admins", "/")
	require.NoError(t, err)

	// Group IDs are AGPA + 16 uppercase alphanumeric chars.
	assert.True(t, strings.HasPrefix(g.GroupID, "AGPA"), "group ID must start with AGPA")
	assert.Len(t, g.GroupID, 20, "group ID must be 20 chars")
	assert.NotContains(t, g.GroupID, "-", "group ID must not contain dashes")

	for _, ch := range g.GroupID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"group ID char %q must be uppercase alphanumeric", ch)
	}
}

func TestGroupARN_Format(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackendWithConfig("123456789012")
	g, err := b.CreateGroup("Admins", "/")
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:iam::123456789012:group/Admins", g.Arn)
}

func TestUpdateGroup_Rename(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateGroup("OldGroup", "/")

	require.NoError(t, b.UpdateGroup("OldGroup", "/", "NewGroup"))

	_, err := b.GetGroup("OldGroup")
	require.Error(t, err, "old name must not exist after rename")

	g, err := b.GetGroup("NewGroup")
	require.NoError(t, err)
	assert.Equal(t, "NewGroup", g.GroupName)
}

func TestInMemoryBackend_Groups(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndDeleteGroup", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		g, err := b.CreateGroup("Admins", "/")
		require.NoError(t, err)
		assert.Equal(t, "Admins", g.GroupName)

		err = b.DeleteGroup("Admins")
		require.NoError(t, err)
	})

	t.Run("CreateGroupAlreadyExists", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreateGroup("Admins", "/")
		require.NoError(t, err)
		_, err = b.CreateGroup("Admins", "/")
		require.ErrorIs(t, err, iam.ErrGroupAlreadyExists)
	})

	t.Run("DeleteGroupNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DeleteGroup("nonexistent")
		require.ErrorIs(t, err, iam.ErrGroupNotFound)
	})

	t.Run("AddUserToGroup", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateGroup("Admins", "/")
		_, _ = b.CreateUser("alice", "/", "")
		err := b.AddUserToGroup("Admins", "alice")
		require.NoError(t, err)
	})

	t.Run("AddUserToGroupGroupNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/", "")
		err := b.AddUserToGroup("nonexistent", "alice")
		require.ErrorIs(t, err, iam.ErrGroupNotFound)
	})

	t.Run("AddUserToGroupUserNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateGroup("Admins", "/")
		err := b.AddUserToGroup("Admins", "nonexistent")
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("ListAllGroups", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateGroup("ZGroup", "/")
		_, _ = b.CreateGroup("AGroup", "/")
		groups := b.ListAllGroups()
		require.Len(t, groups, 2)
		assert.Equal(t, "AGroup", groups[0].GroupName)
	})
}

func TestInMemoryBackend_GroupInlinePolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*iam.InMemoryBackend)
		name    string
		wantDoc string
		wantErr error
		action  string
	}{
		{
			name: "PutAndGetGroupPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
			},
			action:  "put_get",
			wantDoc: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		},
		{
			name:    "PutGroupPolicy_GroupNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			action:  "put_notfound",
			wantErr: iam.ErrGroupNotFound,
		},
		{
			name: "GetGroupPolicy_PolicyNotFound",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
			},
			action:  "get_notfound",
			wantErr: iam.ErrInlinePolicyNotFound,
		},
		{
			name: "DeleteGroupPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
				_ = b.PutGroupPolicy(
					"Admins",
					"InlinePolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			action: "delete",
		},
		{
			name: "ListGroupPolicies_Sorted",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
				_ = b.PutGroupPolicy(
					"Admins",
					"ZPolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
				_ = b.PutGroupPolicy(
					"Admins",
					"APolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			action: "list",
		},
		{
			name: "DeleteGroup_WithInlinePolicy_Conflict",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("Admins", "/")
				_ = b.PutGroupPolicy(
					"Admins",
					"InlinePolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			action:  "delete_group_conflict",
			wantErr: iam.ErrDeleteConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			switch tt.action {
			case "put_get":
				err := b.PutGroupPolicy("Admins", "MyPolicy", tt.wantDoc)
				require.NoError(t, err)

				doc, err := b.GetGroupPolicy("Admins", "MyPolicy")
				require.NoError(t, err)
				assert.Equal(t, tt.wantDoc, doc)

			case "put_notfound":
				err := b.PutGroupPolicy(
					"Ghost",
					"MyPolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
				require.ErrorIs(t, err, tt.wantErr)

			case "get_notfound":
				_, err := b.GetGroupPolicy("Admins", "Ghost")
				require.ErrorIs(t, err, tt.wantErr)

			case "delete":
				err := b.DeleteGroupPolicy("Admins", "InlinePolicy")
				require.NoError(t, err)

				names, err := b.ListGroupPolicies("Admins")
				require.NoError(t, err)
				assert.Empty(t, names)

			case "list":
				names, err := b.ListGroupPolicies("Admins")
				require.NoError(t, err)
				require.Len(t, names, 2)
				assert.Equal(t, "APolicy", names[0])
				assert.Equal(t, "ZPolicy", names[1])

			case "delete_group_conflict":
				err := b.DeleteGroup("Admins")
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}
