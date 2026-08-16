package iam_test

import (
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestUpdateUser_Backend tests UpdateUser.
func TestUpdateUser_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setup       func(*iam.InMemoryBackend)
		userName    string
		newPath     string
		newUserName string
		wantErr     bool
	}{
		{
			name: "rename_user",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName:    "alice",
			newUserName: "alice2",
		},
		{
			name: "update_path",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName: "alice",
			newPath:  "/team/",
		},
		{
			name:     "user_not_found",
			setup:    func(_ *iam.InMemoryBackend) {},
			userName: "ghost",
			wantErr:  true,
		},
		{
			name: "rename_to_existing",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_, _ = b.CreateUser("bob", "/", "")
			},
			userName:    "alice",
			newUserName: "bob",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			err := b.UpdateUser(tt.userName, tt.newPath, tt.newUserName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestTagUser_StoresOnModel(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("alice", "/", "")
	require.NoError(t, err)

	require.NoError(t, b.TagUser("alice", map[string]string{"env": "prod", "team": "platform"}))

	u, err := b.GetUser("alice")
	require.NoError(t, err)
	assert.Equal(t, "prod", u.Tags["env"])
	assert.Equal(t, "platform", u.Tags["team"])
}

func TestTagUser_MergesExistingTags(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("bob", "/", "")
	require.NoError(t, err)

	require.NoError(t, b.TagUser("bob", map[string]string{"a": "1"}))
	require.NoError(t, b.TagUser("bob", map[string]string{"b": "2"}))

	u, err := b.GetUser("bob")
	require.NoError(t, err)
	assert.Equal(t, "1", u.Tags["a"])
	assert.Equal(t, "2", u.Tags["b"])
}

func TestTagUser_NotFoundError(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.TagUser("nonexistent", map[string]string{"k": "v"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrUserNotFound)
}

func TestUntagUser_RemovesKeys(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("charlie", "/", "")
	require.NoError(t, err)

	require.NoError(t, b.TagUser("charlie", map[string]string{"env": "prod", "team": "ops"}))
	require.NoError(t, b.UntagUser("charlie", []string{"env"}))

	u, err := b.GetUser("charlie")
	require.NoError(t, err)
	assert.Empty(t, u.Tags["env"])
	assert.Equal(t, "ops", u.Tags["team"])
}

func TestUntagUser_NotFoundError(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	err := b.UntagUser("ghost", []string{"k"})
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrUserNotFound)
}

func TestUserID_Format(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	u, err := b.CreateUser("alice", "/", "")
	require.NoError(t, err)

	// User IDs are AIDA + 16 uppercase alphanumeric chars.
	assert.True(t, strings.HasPrefix(u.UserID, "AIDA"), "user ID must start with AIDA")
	assert.Len(t, u.UserID, 20, "user ID must be 20 chars")
	assert.NotContains(t, u.UserID, "-", "user ID must not contain dashes")

	for _, ch := range u.UserID {
		assert.True(t, unicode.IsUpper(ch) || unicode.IsDigit(ch),
			"user ID char %q must be uppercase alphanumeric", ch)
	}
}

func TestUserARN_Format(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackendWithConfig("123456789012")
	u, err := b.CreateUser("alice", "/", "")
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:iam::123456789012:user/alice", u.Arn)
}

func TestUserARN_WithPath(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackendWithConfig("123456789012")
	u, err := b.CreateUser("alice", "/division/team/", "")
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:iam::123456789012:user/division/team/alice", u.Arn)
}

func TestDeleteUser_FailsWhenAttachedPoliciesExist(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	_, _ = b.CreateUser("alice", "/", "")
	p, _ := b.CreatePolicy("P", "/", doc)
	require.NoError(t, b.AttachUserPolicy("alice", p.Arn))

	err := b.DeleteUser("alice")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrDeleteConflict)
}

func TestDeleteUser_FailsWhenInlinePoliciesExist(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")
	doc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	require.NoError(t, b.PutUserPolicy("alice", "MyPolicy", doc))

	err := b.DeleteUser("alice")
	require.Error(t, err)
	assert.ErrorIs(t, err, iam.ErrDeleteConflict)
}

func TestUpdateUser_RenameAndPath(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, _ = b.CreateUser("alice", "/", "")

	require.NoError(t, b.UpdateUser("alice", "/new/", "alicia"))

	_, err := b.GetUser("alice")
	require.Error(t, err, "old name must not exist after rename")

	u, err := b.GetUser("alicia")
	require.NoError(t, err)
	assert.Equal(t, "/new/", u.Path)
}

// TestUpdateUser_Rename_KeepsPolicyAttachmentInSync guards against a rename
// desyncing the reverse policyAttachments index: renaming a user with an
// attached managed policy must let the policy still be detached (and
// eventually deleted) under the user's NEW name. Before this was fixed, the
// reverse index kept referencing the pre-rename user name forever, so
// DetachUserPolicy(newName, ...) silently no-op'd and DeletePolicy stayed
// permanently blocked by a DeleteConflict against a name nothing pointed to
// anymore.
func TestUpdateUser_Rename_KeepsPolicyAttachmentInSync(t *testing.T) {
	t.Parallel()

	b := newBackend(t)
	_, err := b.CreateUser("alice", "/", "")
	require.NoError(t, err)
	pol, err := b.CreatePolicy(
		"RenamePolicy", "/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)
	require.NoError(t, err)
	require.NoError(t, b.AttachUserPolicy("alice", pol.Arn))

	require.NoError(t, b.UpdateUser("alice", "", "alicia"))

	attached, err := b.ListAttachedUserPolicies("alicia")
	require.NoError(t, err)
	require.Len(t, attached, 1, "attachment must follow the user under its new name")

	// Detaching under the NEW name must actually clear the attachment.
	require.NoError(t, b.DetachUserPolicy("alicia", pol.Arn))
	require.NoError(t, b.DeletePolicy(pol.Arn), "policy must be deletable once detached under the new name")
}

func TestInMemoryBackend_Users(t *testing.T) {
	t.Parallel()

	t.Run("CreateAndGetUser", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		u, err := b.CreateUser("alice", "/", "")
		require.NoError(t, err)
		assert.Equal(t, "alice", u.UserName)
		assert.Equal(t, "/", u.Path)
		assert.NotEmpty(t, u.UserID)
		assert.Contains(t, u.Arn, "alice")

		got, err := b.GetUser("alice")
		require.NoError(t, err)
		assert.Equal(t, "alice", got.UserName)
	})

	t.Run("CreateUserDefaultPath", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		u, err := b.CreateUser("bob", "", "")
		require.NoError(t, err)
		assert.Equal(t, "/", u.Path)
	})

	t.Run("CreateUserAlreadyExists", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.CreateUser("alice", "/", "")
		require.NoError(t, err)
		_, err = b.CreateUser("alice", "/", "")
		require.ErrorIs(t, err, iam.ErrUserAlreadyExists)
	})

	t.Run("GetUserNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, err := b.GetUser("nonexistent")
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("DeleteUser", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("alice", "/", "")
		err := b.DeleteUser("alice")
		require.NoError(t, err)
		_, err = b.GetUser("alice")
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("DeleteUserNotFound", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		err := b.DeleteUser("nonexistent")
		require.ErrorIs(t, err, iam.ErrUserNotFound)
	})

	t.Run("ListUsers", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("bob", "/", "")
		_, _ = b.CreateUser("alice", "/", "")
		users, err := b.ListUsers("", 0)
		require.NoError(t, err)
		require.Len(t, users.Data, 2)
		assert.Equal(t, "alice", users.Data[0].UserName) // sorted
		assert.Equal(t, "bob", users.Data[1].UserName)
	})

	t.Run("ListAllUsers", func(t *testing.T) {
		t.Parallel()
		b := iam.NewInMemoryBackend()
		_, _ = b.CreateUser("charlie", "/", "")
		_, _ = b.CreateUser("alice", "/", "")
		users := b.ListAllUsers()
		require.Len(t, users, 2)
		assert.Equal(t, "alice", users[0].UserName)
	})
}

func TestIAMBackend_ListUsers_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		marker      string
		maxItems    int
		wantLen     int
		wantHasNext bool
	}{
		{
			name:        "all users default limit",
			maxItems:    0,
			wantLen:     3,
			wantHasNext: false,
		},
		{
			name:        "first page of 2",
			maxItems:    2,
			wantLen:     2,
			wantHasNext: true,
		},
		{
			name:        "page size larger than total",
			maxItems:    10,
			wantLen:     3,
			wantHasNext: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			_, _ = b.CreateUser("alice", "/", "")
			_, _ = b.CreateUser("bob", "/", "")
			_, _ = b.CreateUser("charlie", "/", "")

			p, err := b.ListUsers(tt.marker, tt.maxItems)
			require.NoError(t, err)
			assert.Len(t, p.Data, tt.wantLen)
			assert.Equal(t, tt.wantHasNext, p.Next != "")
		})
	}
}

func TestIAMBackend_ListUsers_MarkerRoundTrip(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()

	for _, name := range []string{"alice", "bob", "charlie", "dave"} {
		_, _ = b.CreateUser(name, "/", "")
	}

	first, err := b.ListUsers("", 2)
	require.NoError(t, err)
	require.Len(t, first.Data, 2)
	require.NotEmpty(t, first.Next)

	second, err := b.ListUsers(first.Next, 2)
	require.NoError(t, err)
	require.Len(t, second.Data, 2)
	assert.Empty(t, second.Next)

	allNames := make([]string, 0, 4)
	for _, u := range first.Data {
		allNames = append(allNames, u.UserName)
	}

	for _, u := range second.Data {
		allNames = append(allNames, u.UserName)
	}

	assert.Equal(t, []string{"alice", "bob", "charlie", "dave"}, allNames)
}

func TestInMemoryBackend_UserInlinePolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*iam.InMemoryBackend)
		name    string
		wantDoc string
		wantErr error
		action  string
	}{
		{
			name: "PutAndGetUserPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			action:  "put_get",
			wantDoc: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		},
		{
			name:    "PutUserPolicy_UserNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			action:  "put_notfound",
			wantErr: iam.ErrUserNotFound,
		},
		{
			name: "GetUserPolicy_PolicyNotFound",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			action:  "get_notfound",
			wantErr: iam.ErrInlinePolicyNotFound,
		},
		{
			name: "DeleteUserPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.PutUserPolicy(
					"alice",
					"MyPolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			action: "delete",
		},
		{
			name: "DeleteUserPolicy_NotFound",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			action:  "delete_notfound",
			wantErr: iam.ErrInlinePolicyNotFound,
		},
		{
			name: "ListUserPolicies_Sorted",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.PutUserPolicy(
					"alice",
					"ZPolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
				_ = b.PutUserPolicy(
					"alice",
					"APolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			action: "list",
		},
		{
			name:    "ListUserPolicies_UserNotFound",
			setup:   func(_ *iam.InMemoryBackend) {},
			action:  "list_notfound",
			wantErr: iam.ErrUserNotFound,
		},
		{
			name: "PutUserPolicy_InvalidJSON",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			action:  "put_invalid_json",
			wantErr: iam.ErrMalformedPolicyDocument,
		},
		{
			name: "DeleteUser_WithInlinePolicy_Conflict",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
				_ = b.PutUserPolicy(
					"alice",
					"MyPolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			action:  "delete_user_conflict",
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
				err := b.PutUserPolicy("alice", "MyPolicy", tt.wantDoc)
				require.NoError(t, err)

				doc, err := b.GetUserPolicy("alice", "MyPolicy")
				require.NoError(t, err)
				assert.Equal(t, tt.wantDoc, doc)

			case "put_notfound":
				err := b.PutUserPolicy(
					"nobody",
					"MyPolicy",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
				require.ErrorIs(t, err, tt.wantErr)

			case "get_notfound":
				_, err := b.GetUserPolicy("alice", "Ghost")
				require.ErrorIs(t, err, tt.wantErr)

			case "delete":
				err := b.DeleteUserPolicy("alice", "MyPolicy")
				require.NoError(t, err)

				names, err := b.ListUserPolicies("alice")
				require.NoError(t, err)
				assert.Empty(t, names)

			case "delete_notfound":
				err := b.DeleteUserPolicy("alice", "Ghost")
				require.ErrorIs(t, err, tt.wantErr)

			case "list":
				names, err := b.ListUserPolicies("alice")
				require.NoError(t, err)
				require.Len(t, names, 2)
				assert.Equal(t, "APolicy", names[0])
				assert.Equal(t, "ZPolicy", names[1])

			case "list_notfound":
				_, err := b.ListUserPolicies("nobody")
				require.ErrorIs(t, err, tt.wantErr)

			case "put_invalid_json":
				err := b.PutUserPolicy("alice", "MyPolicy", "not-json")
				require.ErrorIs(t, err, tt.wantErr)

			case "delete_user_conflict":
				err := b.DeleteUser("alice")
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}
