package elasticache_test

import (
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateGlobalReplicationGroup_NodeGroupCountTracked(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateGlobalReplicationGroup(t.Context(), &elasticachesdk.CreateGlobalReplicationGroupInput{
		GlobalReplicationGroupIdSuffix: aws.String("nodecount-grg"),
		PrimaryReplicationGroupId:      aws.String(""),
	})
	require.NoError(t, err)

	out, err := client.DescribeGlobalReplicationGroups(
		t.Context(),
		&elasticachesdk.DescribeGlobalReplicationGroupsInput{
			GlobalReplicationGroupId: aws.String("ldgnf-nodecount-grg"),
		},
	)
	require.NoError(t, err)
	require.Len(t, out.GlobalReplicationGroups, 1)
	grg := out.GlobalReplicationGroups[0]
	assert.NotNil(t, grg.GlobalReplicationGroupId)
}

func TestHandler_IncreaseNodeGroupsInGRG_UpdatesCount(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateGlobalReplicationGroup(t.Context(), &elasticachesdk.CreateGlobalReplicationGroupInput{
		GlobalReplicationGroupIdSuffix: aws.String("inc-ng"),
		PrimaryReplicationGroupId:      aws.String(""),
	})
	require.NoError(t, err)

	out, err := client.IncreaseNodeGroupsInGlobalReplicationGroup(
		t.Context(),
		&elasticachesdk.IncreaseNodeGroupsInGlobalReplicationGroupInput{
			GlobalReplicationGroupId: aws.String("ldgnf-inc-ng"),
			ApplyImmediately:         aws.Bool(true),
			NodeGroupCount:           aws.Int32(4),
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.GlobalReplicationGroup)
}

// ----------------------------------------
// DescribeServiceUpdates — real data
// ----------------------------------------

func TestHandler_CreateGlobalReplicationGroup_RegionTracking(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("grg-region-primary"),
		ReplicationGroupDescription: aws.String("primary for region test"),
	})
	require.NoError(t, err)

	grgOut, err := client.CreateGlobalReplicationGroup(t.Context(), &elasticachesdk.CreateGlobalReplicationGroupInput{
		GlobalReplicationGroupIdSuffix:    aws.String("region-grg"),
		GlobalReplicationGroupDescription: aws.String("Region tracking GRG"),
		PrimaryReplicationGroupId:         aws.String("grg-region-primary"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(grgOut.GlobalReplicationGroup.GlobalReplicationGroupId))

	// Describe to verify.
	desc, err := client.DescribeGlobalReplicationGroups(
		t.Context(),
		&elasticachesdk.DescribeGlobalReplicationGroupsInput{
			GlobalReplicationGroupId: grgOut.GlobalReplicationGroup.GlobalReplicationGroupId,
		},
	)
	require.NoError(t, err)
	require.Len(t, desc.GlobalReplicationGroups, 1)
}

// ----------------------------------------
// Tags on CacheSubnetGroup
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

func TestCreateGlobalReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(t *testing.T, client *elasticachesdk.Client)
		name        string
		suffix      string
		description string
		primaryRGID string
		wantErr     bool
	}{
		{
			name:        "success",
			suffix:      "my-group",
			description: "Global RG",
			primaryRGID: "primary-rg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("primary-rg"),
					ReplicationGroupDescription: aws.String("Primary"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:        "success_without_primary",
			suffix:      "standalone-group",
			description: "Standalone global RG",
			primaryRGID: "nonexistent-primary",
		},
		{
			name:        "duplicate",
			suffix:      "dup-group",
			description: "First",
			primaryRGID: "",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.CreateGlobalReplicationGroupInput{
						GlobalReplicationGroupIdSuffix:    aws.String("dup-group"),
						GlobalReplicationGroupDescription: aws.String("first"),
						PrimaryReplicationGroupId:         aws.String("primary-rg"),
					},
				)
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

			out, err := client.CreateGlobalReplicationGroup(
				t.Context(),
				&elasticachesdk.CreateGlobalReplicationGroupInput{
					GlobalReplicationGroupIdSuffix:    aws.String(tt.suffix),
					GlobalReplicationGroupDescription: aws.String(tt.description),
					PrimaryReplicationGroupId:         aws.String(tt.primaryRGID),
				},
			)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.GlobalReplicationGroup)
			assert.Contains(t, aws.ToString(out.GlobalReplicationGroup.GlobalReplicationGroupId), tt.suffix)
			assert.Equal(t, tt.description, aws.ToString(out.GlobalReplicationGroup.GlobalReplicationGroupDescription))
		})
	}
}
