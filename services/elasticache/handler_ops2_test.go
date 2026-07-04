package elasticache_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------
// DeleteUser
// ----------------------------------------

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

// ----------------------------------------
// DeleteGlobalReplicationGroup
// ----------------------------------------

func TestDeleteGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		groupID string
		wantErr bool
	}{
		{
			name:    "success",
			groupID: "ldgnf-mygrg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-grg"),
					ReplicationGroupDescription: aws.String("test rg"),
				})
				require.NoError(t, err)
				_, err = client.CreateGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.CreateGlobalReplicationGroupInput{
						GlobalReplicationGroupIdSuffix: aws.String("mygrg"),
						PrimaryReplicationGroupId:      aws.String("rg-grg"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			groupID: "ldgnf-nope",
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

			out, err := client.DeleteGlobalReplicationGroup(
				t.Context(),
				&elasticachesdk.DeleteGlobalReplicationGroupInput{
					GlobalReplicationGroupId:      aws.String(tt.groupID),
					RetainPrimaryReplicationGroup: aws.Bool(false),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupID, aws.ToString(out.GlobalReplicationGroup.GlobalReplicationGroupId))
		})
	}
}

// ----------------------------------------
// DescribeGlobalReplicationGroups
// ----------------------------------------

func TestDescribeGlobalReplicationGroups(t *testing.T) {
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
			wantCount: 1,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-dgrg"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
				_, err = client.CreateGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.CreateGlobalReplicationGroupInput{
						GlobalReplicationGroupIdSuffix: aws.String("dgrg"),
						PrimaryReplicationGroupId:      aws.String("rg-dgrg"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			groupID: "ldgnf-nope",
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

			input := &elasticachesdk.DescribeGlobalReplicationGroupsInput{}
			if tt.groupID != "" {
				input.GlobalReplicationGroupId = aws.String(tt.groupID)
			}

			out, err := client.DescribeGlobalReplicationGroups(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			if tt.groupID != "" || tt.wantCount == 0 {
				assert.Len(t, out.GlobalReplicationGroups, tt.wantCount)
			} else {
				assert.GreaterOrEqual(t, len(out.GlobalReplicationGroups), tt.wantCount)
			}
		})
	}
}

// ----------------------------------------
// DisassociateGlobalReplicationGroup
// ----------------------------------------

func TestDisassociateGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		groupID string
		wantErr bool
	}{
		{
			name:    "success",
			groupID: "ldgnf-disgrg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-dis"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
				_, err = client.CreateGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.CreateGlobalReplicationGroupInput{
						GlobalReplicationGroupIdSuffix: aws.String("disgrg"),
						PrimaryReplicationGroupId:      aws.String("rg-dis"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			groupID: "ldgnf-nope",
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

			out, err := client.DisassociateGlobalReplicationGroup(
				t.Context(),
				&elasticachesdk.DisassociateGlobalReplicationGroupInput{
					GlobalReplicationGroupId: aws.String(tt.groupID),
					ReplicationGroupId:       aws.String("rg-dis"),
					ReplicationGroupRegion:   aws.String("us-east-1"),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupID, aws.ToString(out.GlobalReplicationGroup.GlobalReplicationGroupId))
		})
	}
}

// ----------------------------------------
// FailoverGlobalReplicationGroup
// ----------------------------------------

func TestFailoverGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		groupID string
		wantErr bool
	}{
		{
			name:    "success",
			groupID: "ldgnf-fogrg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-fo"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
				_, err = client.CreateGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.CreateGlobalReplicationGroupInput{
						GlobalReplicationGroupIdSuffix: aws.String("fogrg"),
						PrimaryReplicationGroupId:      aws.String("rg-fo"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			groupID: "ldgnf-nope",
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

			out, err := client.FailoverGlobalReplicationGroup(
				t.Context(),
				&elasticachesdk.FailoverGlobalReplicationGroupInput{
					GlobalReplicationGroupId:  aws.String(tt.groupID),
					PrimaryRegion:             aws.String("us-west-2"),
					PrimaryReplicationGroupId: aws.String("rg-secondary"),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupID, aws.ToString(out.GlobalReplicationGroup.GlobalReplicationGroupId))
		})
	}
}

// ----------------------------------------
// IncreaseNodeGroupsInGlobalReplicationGroup
// ----------------------------------------

func TestIncreaseNodeGroupsInGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		groupID string
		wantErr bool
	}{
		{
			name:    "success",
			groupID: "ldgnf-incgrg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-inc"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
				_, err = client.CreateGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.CreateGlobalReplicationGroupInput{
						GlobalReplicationGroupIdSuffix: aws.String("incgrg"),
						PrimaryReplicationGroupId:      aws.String("rg-inc"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			groupID: "ldgnf-nope",
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

			out, err := client.IncreaseNodeGroupsInGlobalReplicationGroup(
				t.Context(),
				&elasticachesdk.IncreaseNodeGroupsInGlobalReplicationGroupInput{
					GlobalReplicationGroupId: aws.String(tt.groupID),
					NodeGroupCount:           aws.Int32(3),
					ApplyImmediately:         aws.Bool(true),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupID, aws.ToString(out.GlobalReplicationGroup.GlobalReplicationGroupId))
		})
	}
}

// ----------------------------------------
// DecreaseNodeGroupsInGlobalReplicationGroup
// ----------------------------------------

func TestDecreaseNodeGroupsInGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		groupID string
		wantErr bool
	}{
		{
			name:    "success",
			groupID: "ldgnf-decgrg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-dec"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
				_, err = client.CreateGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.CreateGlobalReplicationGroupInput{
						GlobalReplicationGroupIdSuffix: aws.String("decgrg"),
						PrimaryReplicationGroupId:      aws.String("rg-dec"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			groupID: "ldgnf-nope",
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

			out, err := client.DecreaseNodeGroupsInGlobalReplicationGroup(
				t.Context(),
				&elasticachesdk.DecreaseNodeGroupsInGlobalReplicationGroupInput{
					GlobalReplicationGroupId: aws.String(tt.groupID),
					NodeGroupCount:           aws.Int32(1),
					ApplyImmediately:         aws.Bool(true),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupID, aws.ToString(out.GlobalReplicationGroup.GlobalReplicationGroupId))
		})
	}
}

// ----------------------------------------
// ModifyGlobalReplicationGroup
// ----------------------------------------

func TestModifyGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		groupID string
		wantErr bool
	}{
		{
			name:    "success",
			groupID: "ldgnf-modgrg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-mod"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
				_, err = client.CreateGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.CreateGlobalReplicationGroupInput{
						GlobalReplicationGroupIdSuffix: aws.String("modgrg"),
						PrimaryReplicationGroupId:      aws.String("rg-mod"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			groupID: "ldgnf-nope",
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

			out, err := client.ModifyGlobalReplicationGroup(
				t.Context(),
				&elasticachesdk.ModifyGlobalReplicationGroupInput{
					GlobalReplicationGroupId:          aws.String(tt.groupID),
					ApplyImmediately:                  aws.Bool(true),
					GlobalReplicationGroupDescription: aws.String("updated"),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupID, aws.ToString(out.GlobalReplicationGroup.GlobalReplicationGroupId))
		})
	}
}

// ----------------------------------------
// RebalanceSlotsInGlobalReplicationGroup
// ----------------------------------------

func TestRebalanceSlotsInGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		groupID string
		wantErr bool
	}{
		{
			name:    "success",
			groupID: "ldgnf-rbgrg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-rb"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
				_, err = client.CreateGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.CreateGlobalReplicationGroupInput{
						GlobalReplicationGroupIdSuffix: aws.String("rbgrg"),
						PrimaryReplicationGroupId:      aws.String("rg-rb"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			groupID: "ldgnf-nope",
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

			out, err := client.RebalanceSlotsInGlobalReplicationGroup(
				t.Context(),
				&elasticachesdk.RebalanceSlotsInGlobalReplicationGroupInput{
					GlobalReplicationGroupId: aws.String(tt.groupID),
					ApplyImmediately:         aws.Bool(true),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.groupID, aws.ToString(out.GlobalReplicationGroup.GlobalReplicationGroupId))
		})
	}
}

// ----------------------------------------
// DescribeReservedCacheNodes
// ----------------------------------------

func TestDescribeReservedCacheNodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		nodeID    string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "empty",
			wantCount: 0,
		},
		{
			name:      "after_purchase",
			wantCount: 1,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.PurchaseReservedCacheNodesOffering(
					t.Context(),
					&elasticachesdk.PurchaseReservedCacheNodesOfferingInput{
						ReservedCacheNodesOfferingId: aws.String("31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa"),
						ReservedCacheNodeId:          aws.String("my-rcn"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			nodeID:  "no-such-rcn",
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

			input := &elasticachesdk.DescribeReservedCacheNodesInput{}
			if tt.nodeID != "" {
				input.ReservedCacheNodeId = aws.String(tt.nodeID)
			}

			out, err := client.DescribeReservedCacheNodes(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.ReservedCacheNodes, tt.wantCount)
		})
	}
}

// ----------------------------------------
// DescribeReservedCacheNodesOfferings
// ----------------------------------------

func TestDescribeReservedCacheNodesOfferings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		offeringID string
		wantCount  int
		wantErr    bool
	}{
		{
			name:      "all_offerings",
			wantCount: 3,
		},
		{
			name:       "specific_offering",
			offeringID: "31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa",
			wantCount:  1,
		},
		{
			name:       "not_found",
			offeringID: "no-such-offering",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			input := &elasticachesdk.DescribeReservedCacheNodesOfferingsInput{}
			if tt.offeringID != "" {
				input.ReservedCacheNodesOfferingId = aws.String(tt.offeringID)
			}

			out, err := client.DescribeReservedCacheNodesOfferings(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.ReservedCacheNodesOfferings, tt.wantCount)
		})
	}
}

// ----------------------------------------
// PurchaseReservedCacheNodesOffering
// ----------------------------------------

func TestPurchaseReservedCacheNodesOffering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		offeringID string
		nodeID     string
		wantErr    bool
	}{
		{
			name:       "success",
			offeringID: "31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa",
			nodeID:     "my-purchase",
		},
		{
			name:       "not_found_offering",
			offeringID: "no-such-offering",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			out, err := client.PurchaseReservedCacheNodesOffering(
				t.Context(),
				&elasticachesdk.PurchaseReservedCacheNodesOfferingInput{
					ReservedCacheNodesOfferingId: aws.String(tt.offeringID),
					ReservedCacheNodeId:          aws.String(tt.nodeID),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.nodeID, aws.ToString(out.ReservedCacheNode.ReservedCacheNodeId))
		})
	}
}

// ----------------------------------------
// DeleteServerlessCache
// ----------------------------------------

func TestDeleteServerlessCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		scName  string
		wantErr bool
	}{
		{
			name:   "success",
			scName: "sc-del-1",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sc-del-1"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			scName:  "no-such-sc",
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

			out, err := client.DeleteServerlessCache(t.Context(), &elasticachesdk.DeleteServerlessCacheInput{
				ServerlessCacheName: aws.String(tt.scName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.scName, aws.ToString(out.ServerlessCache.ServerlessCacheName))
		})
	}
}

// ----------------------------------------
// DeleteServerlessCacheSnapshot
// ----------------------------------------

func TestDeleteServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, client *elasticachesdk.Client)
		name     string
		snapName string
		wantErr  bool
	}{
		{
			name:     "success",
			snapName: "snap-del-1",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sc-snap-del"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("snap-del-1"),
						ServerlessCacheName:         aws.String("sc-snap-del"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:     "not_found",
			snapName: "no-such-snap",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.DeleteServerlessCacheSnapshot(
				t.Context(),
				&elasticachesdk.DeleteServerlessCacheSnapshotInput{
					ServerlessCacheSnapshotName: aws.String(tt.snapName),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.snapName, aws.ToString(out.ServerlessCacheSnapshot.ServerlessCacheSnapshotName))
		})
	}
}

// ----------------------------------------
// DescribeServerlessCaches
// ----------------------------------------

func TestDescribeServerlessCaches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		scName    string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "all_caches",
			wantCount: 2,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				for _, n := range []string{"sc1", "sc2"} {
					_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
						ServerlessCacheName: aws.String(n),
						Engine:              aws.String("redis"),
					})
					require.NoError(t, err)
				}
			},
		},
		{
			name:      "filter_by_name",
			scName:    "sc3",
			wantCount: 1,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sc3"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			scName:  "no-such-sc",
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

			input := &elasticachesdk.DescribeServerlessCachesInput{}
			if tt.scName != "" {
				input.ServerlessCacheName = aws.String(tt.scName)
			}

			out, err := client.DescribeServerlessCaches(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.ServerlessCaches, tt.wantCount)
		})
	}
}

// ----------------------------------------
// DescribeServerlessCacheSnapshots
// ----------------------------------------

func TestDescribeServerlessCacheSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		snapName  string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "all_snapshots",
			wantCount: 1,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sc-dss"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("snap-dss"),
						ServerlessCacheName:         aws.String("sc-dss"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:     "not_found",
			snapName: "no-such-snap",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			input := &elasticachesdk.DescribeServerlessCacheSnapshotsInput{}
			if tt.snapName != "" {
				input.ServerlessCacheSnapshotName = aws.String(tt.snapName)
			}

			out, err := client.DescribeServerlessCacheSnapshots(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.ServerlessCacheSnapshots, tt.wantCount)
		})
	}
}

// ----------------------------------------
// ExportServerlessCacheSnapshot
// ----------------------------------------

func TestExportServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, client *elasticachesdk.Client)
		name     string
		snapName string
		wantErr  bool
	}{
		{
			name:     "success",
			snapName: "snap-exp-1",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sc-exp"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
				_, err = client.CreateServerlessCacheSnapshot(
					t.Context(),
					&elasticachesdk.CreateServerlessCacheSnapshotInput{
						ServerlessCacheSnapshotName: aws.String("snap-exp-1"),
						ServerlessCacheName:         aws.String("sc-exp"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:     "not_found",
			snapName: "no-such-snap",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.ExportServerlessCacheSnapshot(
				t.Context(),
				&elasticachesdk.ExportServerlessCacheSnapshotInput{
					ServerlessCacheSnapshotName: aws.String(tt.snapName),
					S3BucketName:                aws.String("my-bucket"),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.snapName, aws.ToString(out.ServerlessCacheSnapshot.ServerlessCacheSnapshotName))
		})
	}
}

// ----------------------------------------
// ModifyServerlessCache
// ----------------------------------------

func TestModifyServerlessCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		scName  string
		wantErr bool
	}{
		{
			name:   "success",
			scName: "sc-mod-1",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateServerlessCache(t.Context(), &elasticachesdk.CreateServerlessCacheInput{
					ServerlessCacheName: aws.String("sc-mod-1"),
					Engine:              aws.String("redis"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			scName:  "no-such-sc",
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

			out, err := client.ModifyServerlessCache(t.Context(), &elasticachesdk.ModifyServerlessCacheInput{
				ServerlessCacheName: aws.String(tt.scName),
				Description:         aws.String("updated description"),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.scName, aws.ToString(out.ServerlessCache.ServerlessCacheName))
		})
	}
}

// ----------------------------------------
// StartMigration
// ----------------------------------------

func TestStartMigration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		rgID    string
		wantErr bool
	}{
		{
			name: "success",
			rgID: "rg-start-mig",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-start-mig"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			rgID:    "no-such-rg",
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

			out, err := client.StartMigration(t.Context(), &elasticachesdk.StartMigrationInput{
				ReplicationGroupId: aws.String(tt.rgID),
				CustomerNodeEndpointList: []elasticachetypes.CustomerNodeEndpoint{
					{Address: aws.String("1.2.3.4"), Port: aws.Int32(6379)},
				},
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.rgID, aws.ToString(out.ReplicationGroup.ReplicationGroupId))
		})
	}
}

// ----------------------------------------
// TestMigration
// ----------------------------------------

func TestTestMigration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		rgID    string
		wantErr bool
	}{
		{
			name: "success",
			rgID: "rg-test-mig",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-test-mig"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			rgID:    "no-such-rg",
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

			out, err := client.TestMigration(t.Context(), &elasticachesdk.TestMigrationInput{
				ReplicationGroupId: aws.String(tt.rgID),
				CustomerNodeEndpointList: []elasticachetypes.CustomerNodeEndpoint{
					{Address: aws.String("1.2.3.4"), Port: aws.Int32(6379)},
				},
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.rgID, aws.ToString(out.ReplicationGroup.ReplicationGroupId))
		})
	}
}

// ----------------------------------------
// IncreaseReplicaCount
// ----------------------------------------

func TestIncreaseReplicaCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		rgID    string
		wantErr bool
	}{
		{
			name: "success",
			rgID: "rg-inc-rep",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-inc-rep"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			rgID:    "no-such-rg",
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

			out, err := client.IncreaseReplicaCount(t.Context(), &elasticachesdk.IncreaseReplicaCountInput{
				ReplicationGroupId: aws.String(tt.rgID),
				ApplyImmediately:   aws.Bool(true),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.rgID, aws.ToString(out.ReplicationGroup.ReplicationGroupId))
		})
	}
}

// ----------------------------------------
// DecreaseReplicaCount
// ----------------------------------------

func TestDecreaseReplicaCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		rgID    string
		wantErr bool
	}{
		{
			name: "success",
			rgID: "rg-dec-rep",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-dec-rep"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			rgID:    "no-such-rg",
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

			out, err := client.DecreaseReplicaCount(t.Context(), &elasticachesdk.DecreaseReplicaCountInput{
				ReplicationGroupId: aws.String(tt.rgID),
				ApplyImmediately:   aws.Bool(true),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.rgID, aws.ToString(out.ReplicationGroup.ReplicationGroupId))
		})
	}
}

// ----------------------------------------
// ModifyReplicationGroupShardConfiguration
// ----------------------------------------

func TestModifyReplicationGroupShardConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		rgID    string
		wantErr bool
	}{
		{
			name: "success",
			rgID: "rg-shard",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("rg-shard"),
					ReplicationGroupDescription: aws.String("test"),
					ClusterMode:                 elasticachetypes.ClusterModeEnabled,
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			rgID:    "no-such-rg",
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

			out, err := client.ModifyReplicationGroupShardConfiguration(
				t.Context(),
				&elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
					ReplicationGroupId: aws.String(tt.rgID),
					NodeGroupCount:     aws.Int32(2),
					ApplyImmediately:   aws.Bool(true),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.rgID, aws.ToString(out.ReplicationGroup.ReplicationGroupId))
		})
	}
}

// ----------------------------------------
// DescribeCacheEngineVersions
// ----------------------------------------

func TestDescribeCacheEngineVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		engine       string
		wantMinCount int
	}{
		{
			name:         "all_versions",
			wantMinCount: 4,
		},
		{
			name:         "redis_only",
			engine:       "redis",
			wantMinCount: 4,
		},
		{
			name:         "memcached_only",
			engine:       "memcached",
			wantMinCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			input := &elasticachesdk.DescribeCacheEngineVersionsInput{}
			if tt.engine != "" {
				input.Engine = aws.String(tt.engine)
			}

			out, err := client.DescribeCacheEngineVersions(t.Context(), input)

			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(out.CacheEngineVersions), tt.wantMinCount)
		})
	}
}

// ----------------------------------------
// RebootCacheCluster
// ----------------------------------------

func TestRebootCacheCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		clusterID string
		wantErr   bool
	}{
		{
			name:      "success",
			clusterID: "cluster-reboot",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheCluster(t.Context(), &elasticachesdk.CreateCacheClusterInput{
					CacheClusterId: aws.String("cluster-reboot"),
					Engine:         aws.String("redis"),
					CacheNodeType:  aws.String("cache.t3.micro"),
					NumCacheNodes:  aws.Int32(1),
				})
				require.NoError(t, err)
			},
		},
		{
			name:      "not_found",
			clusterID: "no-such-cluster",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.RebootCacheCluster(t.Context(), &elasticachesdk.RebootCacheClusterInput{
				CacheClusterId:       aws.String(tt.clusterID),
				CacheNodeIdsToReboot: []string{"0001"},
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, aws.ToString(out.CacheCluster.CacheClusterId))
		})
	}
}

// ----------------------------------------
// DeleteCacheSecurityGroup
// ----------------------------------------

func TestDeleteCacheSecurityGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		sgName  string
		wantErr bool
	}{
		{
			name:   "success",
			sgName: "sg-del-1",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
					CacheSecurityGroupName: aws.String("sg-del-1"),
					Description:            aws.String("test"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			sgName:  "no-such-sg",
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

			_, err := client.DeleteCacheSecurityGroup(t.Context(), &elasticachesdk.DeleteCacheSecurityGroupInput{
				CacheSecurityGroupName: aws.String(tt.sgName),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// ----------------------------------------
// DescribeCacheSecurityGroups
// ----------------------------------------

func TestDescribeCacheSecurityGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, client *elasticachesdk.Client)
		name      string
		sgName    string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "all_groups",
			wantCount: 2,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				for _, n := range []string{"dsg1", "dsg2"} {
					_, err := client.CreateCacheSecurityGroup(
						t.Context(),
						&elasticachesdk.CreateCacheSecurityGroupInput{
							CacheSecurityGroupName: aws.String(n),
							Description:            aws.String("test"),
						},
					)
					require.NoError(t, err)
				}
			},
		},
		{
			name:      "filter_by_name",
			sgName:    "dsg3",
			wantCount: 1,
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
					CacheSecurityGroupName: aws.String("dsg3"),
					Description:            aws.String("test"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			sgName:  "no-such-sg",
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

			input := &elasticachesdk.DescribeCacheSecurityGroupsInput{}
			if tt.sgName != "" {
				input.CacheSecurityGroupName = aws.String(tt.sgName)
			}

			out, err := client.DescribeCacheSecurityGroups(t.Context(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.CacheSecurityGroups, tt.wantCount)
		})
	}
}

// ----------------------------------------
// RevokeCacheSecurityGroupIngress
// ----------------------------------------

func TestRevokeCacheSecurityGroupIngress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		sgName  string
		wantErr bool
	}{
		{
			name:   "success",
			sgName: "sg-revoke",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateCacheSecurityGroup(t.Context(), &elasticachesdk.CreateCacheSecurityGroupInput{
					CacheSecurityGroupName: aws.String("sg-revoke"),
					Description:            aws.String("test"),
				})
				require.NoError(t, err)
				_, err = client.AuthorizeCacheSecurityGroupIngress(
					t.Context(),
					&elasticachesdk.AuthorizeCacheSecurityGroupIngressInput{
						CacheSecurityGroupName:  aws.String("sg-revoke"),
						EC2SecurityGroupName:    aws.String("my-ec2-sg"),
						EC2SecurityGroupOwnerId: aws.String("123456789012"),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			sgName:  "no-such-sg",
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

			out, err := client.RevokeCacheSecurityGroupIngress(
				t.Context(),
				&elasticachesdk.RevokeCacheSecurityGroupIngressInput{
					CacheSecurityGroupName:  aws.String(tt.sgName),
					EC2SecurityGroupName:    aws.String("my-ec2-sg"),
					EC2SecurityGroupOwnerId: aws.String("123456789012"),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.sgName, aws.ToString(out.CacheSecurityGroup.CacheSecurityGroupName))
		})
	}
}

// ----------------------------------------
// DescribeEngineDefaultParameters
// ----------------------------------------

func TestDescribeEngineDefaultParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		family string
	}{
		{
			name:   "redis7_family",
			family: "redis7",
		},
		{
			name:   "memcached_family",
			family: "memcached1.6",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			out, err := client.DescribeEngineDefaultParameters(
				t.Context(),
				&elasticachesdk.DescribeEngineDefaultParametersInput{
					CacheParameterGroupFamily: aws.String(tt.family),
				},
			)

			require.NoError(t, err)
			assert.NotNil(t, out.EngineDefaults)
		})
	}
}

// ----------------------------------------
// DescribeServiceUpdates
// ----------------------------------------

func TestDescribeServiceUpdates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "empty_list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			out, err := client.DescribeServiceUpdates(t.Context(), &elasticachesdk.DescribeServiceUpdatesInput{})

			require.NoError(t, err)
			assert.NotNil(t, out.ServiceUpdates)
		})
	}
}

// ----------------------------------------
// DescribeUpdateActions
// ----------------------------------------

func TestDescribeUpdateActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{
			name: "empty_list",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			out, err := client.DescribeUpdateActions(t.Context(), &elasticachesdk.DescribeUpdateActionsInput{})

			require.NoError(t, err)
			assert.NotNil(t, out.UpdateActions)
		})
	}
}

// ----------------------------------------
// ListAllowedNodeTypeModifications
// ----------------------------------------

func TestListAllowedNodeTypeModifications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rgID string
	}{
		{
			name: "by_rg_id",
			rgID: "rg-mods",
		},
		{
			name: "by_cluster_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			input := &elasticachesdk.ListAllowedNodeTypeModificationsInput{}
			if tt.rgID != "" {
				input.ReplicationGroupId = aws.String(tt.rgID)
			}

			out, err := client.ListAllowedNodeTypeModifications(t.Context(), input)

			require.NoError(t, err)
			assert.NotEmpty(t, out.ScaleUpModifications)
		})
	}
}
