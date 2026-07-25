package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateUserGroup_ValidatesUserIDs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(t *testing.T, client *elasticachesdk.Client)
		userIDs []string
		wantErr bool
	}{
		{
			name:    "no_users_succeeds",
			userIDs: nil,
		},
		{
			name:    "existing_users_succeeds",
			userIDs: []string{"ug-user-1", "ug-user-2"},
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				for _, id := range []string{"ug-user-1", "ug-user-2"} {
					_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
						UserId:             aws.String(id),
						UserName:           aws.String(id),
						Engine:             aws.String("redis"),
						AccessString:       aws.String("on ~* +@all"),
						NoPasswordRequired: aws.Bool(true),
					})
					require.NoError(t, err)
				}
			},
		},
		{
			name:    "nonexistent_user_rejected",
			userIDs: []string{"no-such-user"},
			wantErr: true,
		},
		{
			name:    "mix_existing_and_missing_rejected",
			userIDs: []string{"real-user", "ghost-user"},
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
					UserId:             aws.String("real-user"),
					UserName:           aws.String("real-user"),
					Engine:             aws.String("redis"),
					AccessString:       aws.String("on ~* +@all"),
					NoPasswordRequired: aws.Bool(true),
				})
				require.NoError(t, err)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		groupID := aws.String("grp-validate-" + tt.name)

		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
				UserGroupId: groupID,
				Engine:      aws.String("redis"),
				UserIds:     tt.userIDs,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, aws.ToString(groupID), aws.ToString(out.UserGroupId))
		})
	}
}

func TestCreateUserGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		groupID string
		wantErr bool
	}{
		{
			name:    "success",
			groupID: "group-1",
		},
		{
			name:    "already_exists",
			groupID: "dup-group",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
					UserGroupId: aws.String("dup-group"),
					Engine:      aws.String("redis"),
				})
				require.NoError(t, err)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
				UserGroupId: aws.String(tt.groupID),
				Engine:      aws.String("redis"),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupID, aws.ToString(out.UserGroupId))
		})
	}
}

// ----------------------------------------
// DeleteUserGroup
// ----------------------------------------

func TestDeleteUserGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		groupID string
		wantErr bool
	}{
		{
			name:    "success",
			groupID: "group-del-1",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
					UserGroupId: aws.String("group-del-1"),
					Engine:      aws.String("redis"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			groupID: "no-such-group",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.DeleteUserGroup(t.Context(), &elasticachesdk.DeleteUserGroupInput{
				UserGroupId: aws.String(tt.groupID),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupID, aws.ToString(out.UserGroupId))
		})
	}
}

// ----------------------------------------
// DescribeUserGroups
// ----------------------------------------

func TestDescribeUserGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		groupID   string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "all_groups",
			wantCount: 2,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				for _, id := range []string{"ug1", "ug2"} {
					_, err := client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
						UserGroupId: aws.String(id),
						Engine:      aws.String("redis"),
					})
					require.NoError(t, err)
				}
			},
		},
		{
			name:      "filter_by_id",
			groupID:   "ug3",
			wantCount: 1,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
					UserGroupId: aws.String("ug3"),
					Engine:      aws.String("redis"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			groupID: "no-such-group",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			input := &elasticachesdk.DescribeUserGroupsInput{}
			if tt.groupID != "" {
				input.UserGroupId = aws.String(tt.groupID)
			}

			out, err := client.DescribeUserGroups(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.UserGroups, tt.wantCount)
		})
	}
}

// ----------------------------------------
// ModifyUserGroup
// ----------------------------------------

func TestModifyUserGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		groupID string
		wantErr bool
	}{
		{
			name:    "success",
			groupID: "group-mod-1",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
					UserGroupId: aws.String("group-mod-1"),
					Engine:      aws.String("redis"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			groupID: "no-such-group",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.ModifyUserGroup(t.Context(), &elasticachesdk.ModifyUserGroupInput{
				UserGroupId: aws.String(tt.groupID),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupID, aws.ToString(out.UserGroupId))
		})
	}
}

// TestHandler_UserGroup_ReplicationGroupsWireShape locks the UserGroup
// response's ReplicationGroups field (types.UserGroup.ReplicationGroups) --
// the reverse of a ReplicationGroup's UserGroupIds -- which a prior pass
// left entirely unwired even though a placeholder model field existed.
// Field-diffed against aws-sdk-go-v2's deserializer for the UserGroup
// document (element name "ReplicationGroups", unlabeled <member> list).
func TestHandler_UserGroup_ReplicationGroupsWireShape(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
		UserGroupId: aws.String("rg-linked-group"),
		Engine:      aws.String("redis"),
	})
	require.NoError(t, err)

	// Freshly created, not yet attached to any replication group.
	created, err := client.DescribeUserGroups(t.Context(), &elasticachesdk.DescribeUserGroupsInput{
		UserGroupId: aws.String("rg-linked-group"),
	})
	require.NoError(t, err)
	require.Len(t, created.UserGroups, 1)
	assert.Empty(t, created.UserGroups[0].ReplicationGroups)

	_, err = client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("rg-links-to-group"),
		ReplicationGroupDescription: aws.String("d"),
		UserGroupIds:                []string{"rg-linked-group"},
	})
	require.NoError(t, err)

	out, err := client.DescribeUserGroups(t.Context(), &elasticachesdk.DescribeUserGroupsInput{
		UserGroupId: aws.String("rg-linked-group"),
	})
	require.NoError(t, err)
	require.Len(t, out.UserGroups, 1)
	assert.Equal(t, []string{"rg-links-to-group"}, out.UserGroups[0].ReplicationGroups)
}
