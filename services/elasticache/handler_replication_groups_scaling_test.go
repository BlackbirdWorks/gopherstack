package elasticache_test

import (
	"context"
	"net/http"
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------
// ReplicationGroup — replica count, shard configuration, migration, and failover
// ----------------------------------------

func TestHandler_IncreaseDecreaseReplicaCount(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("replica-count-http-rg"),
		ReplicationGroupDescription: aws.String("replica count via HTTP"),
	})
	require.NoError(t, err)

	// Increase to 2.
	increaseOut, err := client.IncreaseReplicaCount(t.Context(), &elasticachesdk.IncreaseReplicaCountInput{
		ReplicationGroupId: aws.String("replica-count-http-rg"),
		NewReplicaCount:    aws.Int32(2),
		ApplyImmediately:   aws.Bool(true),
	})
	require.NoError(t, err)
	assert.NotNil(t, increaseOut.ReplicationGroup)

	// Decrease to 1.
	decreaseOut, err := client.DecreaseReplicaCount(t.Context(), &elasticachesdk.DecreaseReplicaCountInput{
		ReplicationGroupId: aws.String("replica-count-http-rg"),
		NewReplicaCount:    aws.Int32(1),
		ApplyImmediately:   aws.Bool(true),
	})
	require.NoError(t, err)
	assert.NotNil(t, decreaseOut.ReplicationGroup)
}

// ----------------------------------------
// ModifyReplicationGroupShardConfiguration via HTTP (issue #2)
// ----------------------------------------

func TestHandler_ModifyReplicationGroupShardConfiguration_UpdatesNodeGroups(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("shard-config-rg"),
		ReplicationGroupDescription: aws.String("shard config via HTTP"),
		ClusterMode:                 elasticachetypes.ClusterModeEnabled,
		NumNodeGroups:               aws.Int32(2),
	})
	require.NoError(t, err)

	out, err := client.ModifyReplicationGroupShardConfiguration(
		t.Context(),
		&elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
			ReplicationGroupId: aws.String("shard-config-rg"),
			NodeGroupCount:     aws.Int32(4),
			ApplyImmediately:   aws.Bool(true),
		},
	)
	require.NoError(t, err)
	assert.NotNil(t, out.ReplicationGroup)

	// Verify node groups in describe.
	desc, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
		ReplicationGroupId: aws.String("shard-config-rg"),
	})
	require.NoError(t, err)
	rg := desc.ReplicationGroups[0]
	assert.Len(t, rg.NodeGroups, 4)
}

// ----------------------------------------
// Events — source type filtering
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

func TestHandler_TestFailoverReplicationGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, client *elasticachesdk.Client)
		name    string
		rgID    string
		wantErr bool
	}{
		{
			name: "success",
			rgID: "failover-rg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("failover-rg"),
					ReplicationGroupDescription: aws.String("Failover RG"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:    "not_found",
			rgID:    "nonexistent-rg",
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

			out, err := client.TestFailover(t.Context(), &elasticachesdk.TestFailoverInput{
				ReplicationGroupId: aws.String(tt.rgID),
				NodeGroupId:        aws.String("0001"),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ReplicationGroup)
		})
	}
}

// Test_ModifyReplicationGroup_TransitEncryptionRequiresAuthToken covers the
// gap where TransitEncryptionMode="required" could be set via
// ModifyReplicationGroup with no auth token ever enabled -- the backend
// declared ErrTransitEncryptionModeInvalid for exactly this case but no code
// path ever returned it (a disguised stub: the sentinel existed, the guard
// didn't). Fixed in backend_audit1.go's validateTransitEncryptionModify.
func Test_ModifyReplicationGroup_TransitEncryptionRequiresAuthToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input   *elasticachesdk.ModifyReplicationGroupInput
		name    string
		wantErr bool
	}{
		{
			name: "required_mode_without_auth_token_rejected",
			input: &elasticachesdk.ModifyReplicationGroupInput{
				TransitEncryptionMode: elasticachetypes.TransitEncryptionModeRequired,
			},
			wantErr: true,
		},
		{
			name: "required_mode_with_new_auth_token_allowed",
			input: &elasticachesdk.ModifyReplicationGroupInput{
				TransitEncryptionMode:   elasticachetypes.TransitEncryptionModeRequired,
				AuthToken:               aws.String("s3cr3t-token-01234567890123456789"),
				AuthTokenUpdateStrategy: elasticachetypes.AuthTokenUpdateStrategyTypeSet,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			ctx := t.Context()
			_, err := client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
				ReplicationGroupId:          aws.String("te-rg"),
				ReplicationGroupDescription: aws.String("d"),
			})
			require.NoError(t, err)

			tt.input.ReplicationGroupId = aws.String("te-rg")

			_, err = client.ModifyReplicationGroup(ctx, tt.input)

			if tt.wantErr {
				require.Error(t, err)
				requireFault[elasticachetypes.InvalidParameterCombinationException](t, err)
				requireHTTPStatus(t, err, http.StatusBadRequest)

				return
			}

			require.NoError(t, err)
		})
	}
}

// Test_ModifyReplicationGroupShardConfiguration_RequiresClusterMode is the
// wire-level counterpart of TestBackend_ModifyReplicationGroupShardConfiguration_RequiresClusterMode
// in handler_audit1_test.go, which only checked the backend Go error. This
// confirms the HTTP handler actually maps ErrClusterModeRequired to a client
// (400 InvalidParameterCombination) response instead of falling through to
// the generic InternalFailure 500 default case.
func Test_ModifyReplicationGroupShardConfiguration_RequiresClusterMode(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := context.Background()

	_, err := client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("no-cluster-mode-rg"),
		ReplicationGroupDescription: aws.String("d"),
	})
	require.NoError(t, err)

	_, err = client.ModifyReplicationGroupShardConfiguration(
		ctx,
		&elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
			ReplicationGroupId: aws.String("no-cluster-mode-rg"),
			NodeGroupCount:     aws.Int32(2),
			ApplyImmediately:   aws.Bool(true),
		},
	)
	require.Error(t, err)

	target := requireFault[elasticachetypes.InvalidParameterCombinationException](t, err)
	requireHTTPStatus(t, err, http.StatusBadRequest)
	assert.Contains(t, target.ErrorMessage(), "cluster mode")
}

func TestCompleteMigration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup              func(t *testing.T, client *elasticachesdk.Client)
		name               string
		replicationGroupID string
		wantStatus         string
		wantErr            bool
	}{
		{
			name:               "success",
			replicationGroupID: "migration-rg",
			wantStatus:         "available",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()
				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("migration-rg"),
					ReplicationGroupDescription: aws.String("RG for migration"),
				})
				require.NoError(t, err)
			},
		},
		{
			name:               "not_found",
			replicationGroupID: "nonexistent-rg",
			wantErr:            true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			if tt.setup != nil {
				tt.setup(t, client)
			}

			out, err := client.CompleteMigration(t.Context(), &elasticachesdk.CompleteMigrationInput{
				ReplicationGroupId: aws.String(tt.replicationGroupID),
				Force:              aws.Bool(false),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, out.ReplicationGroup)
			assert.Equal(t, tt.wantStatus, aws.ToString(out.ReplicationGroup.Status))
		})
	}
}
