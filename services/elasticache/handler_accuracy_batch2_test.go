package elasticache_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------
// DeleteUser: reject when user is in a group (AWS accuracy)
// ----------------------------------------

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
