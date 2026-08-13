package elasticache_test

import (
	"net/http"
	"testing"

	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestApplyImmediatelyFalseRejected proves ApplyImmediately actually reaches
// the backend (gopherstack-9kw0) for the five ops where AWS documents
// ApplyImmediately=false as unsupported: IncreaseReplicaCount/
// DecreaseReplicaCount ("ApplyImmediately=False is not currently
// supported"), ModifyReplicationGroupShardConfiguration ("the only permitted
// value for this parameter is true"), and IncreaseNodeGroupsInGlobalReplicationGroup/
// DecreaseNodeGroupsInGlobalReplicationGroup (same "only permitted value ...
// is true" wording). Previously the field was parsed off the request and
// then discarded -- these ops would have silently "succeeded" on
// ApplyImmediately=false exactly as if it had been true, which is
// indistinguishable from the field never being read at all. Now it is
// enforced and false is rejected with InvalidParameterValue.
func TestApplyImmediatelyFalseRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, client *elasticachesdk.Client)
		call  func(t *testing.T, client *elasticachesdk.Client) error
		name  string
	}{
		{
			name: "increase replica count",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()

				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("apply-imm-inc-rc"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
			},
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()

				_, err := client.IncreaseReplicaCount(t.Context(), &elasticachesdk.IncreaseReplicaCountInput{
					ReplicationGroupId: aws.String("apply-imm-inc-rc"),
					NewReplicaCount:    aws.Int32(2),
					ApplyImmediately:   aws.Bool(false),
				})

				return err
			},
		},
		{
			name: "decrease replica count",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()

				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("apply-imm-dec-rc"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)
			},
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()

				_, err := client.DecreaseReplicaCount(t.Context(), &elasticachesdk.DecreaseReplicaCountInput{
					ReplicationGroupId: aws.String("apply-imm-dec-rc"),
					NewReplicaCount:    aws.Int32(0),
					ApplyImmediately:   aws.Bool(false),
				})

				return err
			},
		},
		{
			name: "modify shard configuration",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()

				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("apply-imm-shard"),
					ReplicationGroupDescription: aws.String("test"),
					ClusterMode:                 elasticachetypes.ClusterModeEnabled,
					NumNodeGroups:               aws.Int32(2),
				})
				require.NoError(t, err)
			},
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()

				_, err := client.ModifyReplicationGroupShardConfiguration(
					t.Context(),
					&elasticachesdk.ModifyReplicationGroupShardConfigurationInput{
						ReplicationGroupId: aws.String("apply-imm-shard"),
						NodeGroupCount:     aws.Int32(4),
						ApplyImmediately:   aws.Bool(false),
					},
				)

				return err
			},
		},
		{
			name: "increase node groups in grg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()

				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("apply-imm-inc-grg-rg"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)

				_, err = client.CreateGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.CreateGlobalReplicationGroupInput{
						GlobalReplicationGroupIdSuffix: aws.String("apply-imm-inc-grg"),
						PrimaryReplicationGroupId:      aws.String("apply-imm-inc-grg-rg"),
					},
				)
				require.NoError(t, err)
			},
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()

				_, err := client.IncreaseNodeGroupsInGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.IncreaseNodeGroupsInGlobalReplicationGroupInput{
						GlobalReplicationGroupId: aws.String("ldgnf-apply-imm-inc-grg"),
						NodeGroupCount:           aws.Int32(3),
						ApplyImmediately:         aws.Bool(false),
					},
				)

				return err
			},
		},
		{
			name: "decrease node groups in grg",
			setup: func(t *testing.T, client *elasticachesdk.Client) {
				t.Helper()

				_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
					ReplicationGroupId:          aws.String("apply-imm-dec-grg-rg"),
					ReplicationGroupDescription: aws.String("test"),
				})
				require.NoError(t, err)

				_, err = client.CreateGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.CreateGlobalReplicationGroupInput{
						GlobalReplicationGroupIdSuffix: aws.String("apply-imm-dec-grg"),
						PrimaryReplicationGroupId:      aws.String("apply-imm-dec-grg-rg"),
					},
				)
				require.NoError(t, err)
			},
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()

				_, err := client.DecreaseNodeGroupsInGlobalReplicationGroup(
					t.Context(),
					&elasticachesdk.DecreaseNodeGroupsInGlobalReplicationGroupInput{
						GlobalReplicationGroupId: aws.String("ldgnf-apply-imm-dec-grg"),
						NodeGroupCount:           aws.Int32(1),
						ApplyImmediately:         aws.Bool(false),
					},
				)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			tt.setup(t, client)

			err := tt.call(t, client)
			require.Error(t, err)

			target := requireFault[elasticachetypes.InvalidParameterValueException](t, err)
			requireHTTPStatus(t, err, http.StatusBadRequest)
			assert.Contains(t, target.ErrorMessage(), "ApplyImmediately")
		})
	}
}

// TestApplyImmediatelyTrueStillSucceeds is the control for
// TestApplyImmediatelyFalseRejected: the same op with ApplyImmediately=true
// (the only value AWS documents as supported) must still succeed and apply
// the change, proving the new validation only rejects false rather than
// rejecting the operation outright.
func TestApplyImmediatelyTrueStillSucceeds(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)

	_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("apply-imm-ok-rg"),
		ReplicationGroupDescription: aws.String("test"),
	})
	require.NoError(t, err)

	out, err := client.IncreaseReplicaCount(t.Context(), &elasticachesdk.IncreaseReplicaCountInput{
		ReplicationGroupId: aws.String("apply-imm-ok-rg"),
		NewReplicaCount:    aws.Int32(2),
		ApplyImmediately:   aws.Bool(true),
	})
	require.NoError(t, err)
	require.NotNil(t, out.ReplicationGroup)
	assert.Equal(t, "apply-imm-ok-rg", aws.ToString(out.ReplicationGroup.ReplicationGroupId))
}

// TestMigrationRequiresCustomerNodeEndpoints proves CustomerNodeEndpointList
// (gopherstack-9kw0) reaches the backend for StartMigration/TestMigration: a
// request that omits the required list -- previously silently accepted,
// since the field was never read at all -- is now rejected with
// InvalidParameterValue instead of succeeding as if the endpoints had been
// supplied. The "with endpoints" success case in TestStartMigration/
// TestTestMigration (handler_replication_groups_scaling_test.go) is the
// positive half of this proof: it sends CustomerNodeEndpointList.member.1.*
// through the real SDK-generated query serializer, and now only succeeds
// because parseCustomerNodeEndpoints reads that exact "member"+1-based-index
// wire scheme correctly.
func TestMigrationRequiresCustomerNodeEndpoints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T, client *elasticachesdk.Client, rgID string) error
		name string
		rgID string
	}{
		{
			name: "start migration",
			rgID: "no-endpoints-start-rg",
			call: func(t *testing.T, client *elasticachesdk.Client, rgID string) error {
				t.Helper()

				// A non-nil-but-empty slice passes the SDK's client-side
				// "required field" check (which only rejects nil) so the
				// request reaches the server, exercising this backend's own
				// enforcement of the same required-member contract.
				_, err := client.StartMigration(t.Context(), &elasticachesdk.StartMigrationInput{
					ReplicationGroupId:       aws.String(rgID),
					CustomerNodeEndpointList: []elasticachetypes.CustomerNodeEndpoint{},
				})

				return err
			},
		},
		{
			name: "test migration",
			rgID: "no-endpoints-test-rg",
			call: func(t *testing.T, client *elasticachesdk.Client, rgID string) error {
				t.Helper()

				_, err := client.TestMigration(t.Context(), &elasticachesdk.TestMigrationInput{
					ReplicationGroupId:       aws.String(rgID),
					CustomerNodeEndpointList: []elasticachetypes.CustomerNodeEndpoint{},
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			_, err := client.CreateReplicationGroup(t.Context(), &elasticachesdk.CreateReplicationGroupInput{
				ReplicationGroupId:          aws.String(tt.rgID),
				ReplicationGroupDescription: aws.String("test"),
			})
			require.NoError(t, err)

			err = tt.call(t, client, tt.rgID)
			require.Error(t, err)

			target := requireFault[elasticachetypes.InvalidParameterValueException](t, err)
			requireHTTPStatus(t, err, http.StatusBadRequest)
			assert.Contains(t, target.ErrorMessage(), "CustomerNodeEndpointList")
		})
	}
}
