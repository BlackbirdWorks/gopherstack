package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DeleteUser_RejectsWhenInGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantErrMsg string
		setup      func(t *testing.T, client *elasticachesdk.Client)
		userID     string
		wantErr    bool
	}{
		{
			name:   "user_not_in_group_succeeds",
			userID: "free-user",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
					UserId:             aws.String("free-user"),
					UserName:           aws.String("free-user"),
					Engine:             aws.String("redis"),
					AccessString:       aws.String("on ~* +@all"),
					NoPasswordRequired: aws.Bool(true),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "user_in_group_rejected",
			userID:  "grouped-user",
			wantErr: true,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
					UserId:             aws.String("grouped-user"),
					UserName:           aws.String("grouped-user"),
					Engine:             aws.String("redis"),
					AccessString:       aws.String("on ~* +@all"),
					NoPasswordRequired: aws.Bool(true),
				})
				require.NoError(t, err)

				_, err = client.CreateUserGroup(t.Context(), &elasticachesdk.CreateUserGroupInput{
					UserGroupId: aws.String("owns-user"),
					Engine:      aws.String("redis"),
					UserIds:     []string{"grouped-user"},
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "user_not_found_rejected",
			userID:  "no-such-user",
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

			out, err := client.DeleteUser(t.Context(), &elasticachesdk.DeleteUserInput{
				UserId: aws.String(tt.userID),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.userID, aws.ToString(out.UserId))
		})
	}
}

// ----------------------------------------
// CreateUserGroup: validate user IDs exist (AWS accuracy)
// ----------------------------------------

func TestHandler_DescribeUsers_FilterByID(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	for _, id := range []string{"filter-u1", "filter-u2"} {
		_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
			UserId:             aws.String(id),
			UserName:           aws.String(id),
			Engine:             aws.String("redis"),
			AccessString:       aws.String("on ~* +@all"),
			NoPasswordRequired: aws.Bool(true),
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeUsers(t.Context(), &elasticachesdk.DescribeUsersInput{
		UserId: aws.String("filter-u1"),
	})
	require.NoError(t, err)
	require.Len(t, out.Users, 1)
	assert.Equal(t, "filter-u1", aws.ToString(out.Users[0].UserId))
}

// ----------------------------------------
// User — Modify AccessString
// ----------------------------------------

func TestHandler_ModifyUser_AccessString(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
		UserId:             aws.String("mod-access-user"),
		UserName:           aws.String("mod-access-user"),
		Engine:             aws.String("redis"),
		AccessString:       aws.String("on ~* +@all"),
		NoPasswordRequired: aws.Bool(true),
	})
	require.NoError(t, err)

	out, err := client.ModifyUser(t.Context(), &elasticachesdk.ModifyUserInput{
		UserId:       aws.String("mod-access-user"),
		AccessString: aws.String("on ~cache:* +get +set"),
	})
	require.NoError(t, err)
	assert.Equal(t, "on ~cache:* +get +set", aws.ToString(out.AccessString))
}

// ----------------------------------------
// ServiceUpdate — describe
// ----------------------------------------

func TestHandler_Tags_OnUser(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	out, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
		UserId:       aws.String("tag-user"),
		UserName:     aws.String("tagger"),
		AccessString: aws.String("on ~* +@all"),
		Engine:       aws.String("redis"),
	})
	require.NoError(t, err)

	arn := aws.ToString(out.ARN)
	require.NotEmpty(t, arn)

	_, err = client.AddTagsToResource(t.Context(), &elasticachesdk.AddTagsToResourceInput{
		ResourceName: aws.String(arn),
		Tags: []elasticachetypes.Tag{
			{Key: aws.String("team"), Value: aws.String("platform")},
		},
	})
	require.NoError(t, err)

	tagsOut, err := client.ListTagsForResource(t.Context(), &elasticachesdk.ListTagsForResourceInput{
		ResourceName: aws.String(arn),
	})
	require.NoError(t, err)
	require.Len(t, tagsOut.TagList, 1)
	assert.Equal(t, "team", aws.ToString(tagsOut.TagList[0].Key))
}

func TestDeleteUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		userID  string
		wantErr bool
	}{
		{
			name:   "success",
			userID: "user-del-1",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
					UserId:             aws.String("user-del-1"),
					UserName:           aws.String("user-del-1"),
					Engine:             aws.String("redis"),
					AccessString:       aws.String("on ~* +@all"),
					NoPasswordRequired: aws.Bool(true),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			userID:  "user-nonexistent",
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

			out, err := client.DeleteUser(t.Context(), &elasticachesdk.DeleteUserInput{
				UserId: aws.String(tt.userID),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.userID, aws.ToString(out.UserId))
		})
	}
}

// ----------------------------------------
// DescribeUsers
// ----------------------------------------

func TestDescribeUsers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		userID    string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "all_users",
			wantCount: 2,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				for _, id := range []string{"u1", "u2"} {
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
			name:      "filter_by_id",
			userID:    "u3",
			wantCount: 1,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
					UserId:             aws.String("u3"),
					UserName:           aws.String("u3"),
					Engine:             aws.String("redis"),
					AccessString:       aws.String("on ~* +@all"),
					NoPasswordRequired: aws.Bool(true),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			userID:  "no-such-user",
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

			input := &elasticachesdk.DescribeUsersInput{}
			if tt.userID != "" {
				input.UserId = aws.String(tt.userID)
			}

			out, err := client.DescribeUsers(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Users, tt.wantCount)
		})
	}
}

// ----------------------------------------
// ModifyUser
// ----------------------------------------

func TestModifyUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, client *elasticachesdk.Client)
		name         string
		userID       string
		accessString string
		wantErr      bool
	}{
		{
			name:         "success",
			userID:       "user-mod-1",
			accessString: "on ~* +@read",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
					UserId:             aws.String("user-mod-1"),
					UserName:           aws.String("user-mod-1"),
					Engine:             aws.String("redis"),
					AccessString:       aws.String("on ~* +@all"),
					NoPasswordRequired: aws.Bool(true),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			userID:  "no-such-user",
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

			out, err := client.ModifyUser(t.Context(), &elasticachesdk.ModifyUserInput{
				UserId:             aws.String(tt.userID),
				AccessString:       aws.String(tt.accessString),
				NoPasswordRequired: aws.Bool(true),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.userID, aws.ToString(out.UserId))
		})
	}
}

// ----------------------------------------
// CreateUserGroup
// ----------------------------------------

func TestCreateUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup              func(t *testing.T, client *elasticachesdk.Client)
		name               string
		userID             string
		userName           string
		accessString       string
		engine             string
		noPasswordRequired bool
		wantErr            bool
	}{
		{
			name:         "success",
			userID:       "user1",
			userName:     "alice",
			accessString: "on ~* +@all",
			engine:       "redis",
		},
		{
			name:               "success_no_password",
			userID:             "user2",
			userName:           "bob",
			accessString:       "on ~* +@read",
			engine:             "redis",
			noPasswordRequired: true,
		},
		{
			name:   "already_exists",
			userID: "dup-user",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
					UserId:             aws.String("dup-user"),
					UserName:           aws.String("charlie"),
					AccessString:       aws.String("on ~* +@all"),
					Engine:             aws.String("redis"),
					NoPasswordRequired: aws.Bool(false),
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

			out, err := client.CreateUser(t.Context(), &elasticachesdk.CreateUserInput{
				UserId:             aws.String(tt.userID),
				UserName:           aws.String(tt.userName),
				AccessString:       aws.String(tt.accessString),
				Engine:             aws.String(tt.engine),
				NoPasswordRequired: aws.Bool(tt.noPasswordRequired),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.userID, aws.ToString(out.UserId))
			assert.Equal(t, tt.userName, aws.ToString(out.UserName))
			assert.NotEmpty(t, aws.ToString(out.ARN))
		})
	}
}
