package elasticache_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elasticachesdk "github.com/aws/aws-sdk-go-v2/service/elasticache"
	elasticachetypes "github.com/aws/aws-sdk-go-v2/service/elasticache/types"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// errorFault constrains PT to be a pointer to T that implements error, so
// [requireFault] can hand back a concretely-typed *T for the caller to
// inspect further while still being driven off a single type argument.
type errorFault[T any] interface {
	*T
	error
}

// requireFault asserts err unwraps to the exact SDK-modeled fault type T via
// [errors.As] and returns it. If the emulator's wire <Code> (or HTTP status,
// checked separately) doesn't match what aws-sdk-go-v2's query-protocol
// deserializer expects for that fault, the SDK falls back to a generic
// smithy error and this assertion fails — this is what makes the check a
// real wire-shape proof rather than a bare "err != nil".
func requireFault[T any, PT errorFault[T]](t *testing.T, err error) PT {
	t.Helper()

	var target PT

	require.ErrorAsf(t, err, &target, "expected error to unwrap to %T, got %v", target, err)

	return target
}

// requireHTTPStatus asserts the HTTP status code the SDK observed on the
// response, independent of which fault type it deserialized to.
func requireHTTPStatus(t *testing.T, err error, want int) {
	t.Helper()

	var respErr *smithyhttp.ResponseError

	require.ErrorAsf(t, err, &respErr, "expected a smithy http.ResponseError in the chain, got %v", err)
	assert.Equal(t, want, respErr.HTTPStatusCode())
}

// Test_ErrorWireShapesMatchAWS is a regression test for the parity-sweep-3
// error-code/HTTP-status audit: many NotFound/AlreadyExists faults were
// wired with the wrong <Code> string (e.g. "ReplicationGroupNotFound"
// instead of the wire-correct "ReplicationGroupNotFoundFault") and/or the
// wrong HTTP status (AWS's query-protocol model marks most *NotFoundFault
// shapes 404, not 400, per api-2.json's per-shape httpStatusCode -- see
// PARITY.md Notes). Each case below drives the real aws-sdk-go-v2 client
// against the emulator and confirms both the typed fault and the status
// code the SDK observed.
func Test_ErrorWireShapesMatchAWS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call       func(t *testing.T, client *elasticachesdk.Client) error
		checkFault func(t *testing.T, err error)
		name       string
		wantStatus int
	}{
		{
			name: "CacheClusterNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeCacheClusters(t.Context(), &elasticachesdk.DescribeCacheClustersInput{
					CacheClusterId: aws.String("no-such-cluster"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.CacheClusterNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "ReplicationGroupNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeReplicationGroups(t.Context(), &elasticachesdk.DescribeReplicationGroupsInput{
					ReplicationGroupId: aws.String("no-such-rg"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.ReplicationGroupNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "CacheParameterGroupNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeCacheParameterGroups(
					t.Context(),
					&elasticachesdk.DescribeCacheParameterGroupsInput{
						CacheParameterGroupName: aws.String("no-such-pg"),
					},
				)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.CacheParameterGroupNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			// AWS's own model marks this one 400, not 404 -- an exception to the
			// otherwise-consistent "NotFoundFault => 404" rule for this API.
			name: "CacheSubnetGroupNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeCacheSubnetGroups(
					t.Context(),
					&elasticachesdk.DescribeCacheSubnetGroupsInput{CacheSubnetGroupName: aws.String("no-such-sng")},
				)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.CacheSubnetGroupNotFoundFault](t, err)
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "CacheSecurityGroupNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeCacheSecurityGroups(
					t.Context(),
					&elasticachesdk.DescribeCacheSecurityGroupsInput{CacheSecurityGroupName: aws.String("no-such-csg")},
				)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.CacheSecurityGroupNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "UserNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeUsers(t.Context(), &elasticachesdk.DescribeUsersInput{
					UserId: aws.String("no-such-user"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.UserNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "UserGroupNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeUserGroups(t.Context(), &elasticachesdk.DescribeUserGroupsInput{
					UserGroupId: aws.String("no-such-ug"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.UserGroupNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "ServerlessCacheNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeServerlessCaches(t.Context(), &elasticachesdk.DescribeServerlessCachesInput{
					ServerlessCacheName: aws.String("no-such-sc"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.ServerlessCacheNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "GlobalReplicationGroupNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeGlobalReplicationGroups(
					t.Context(),
					&elasticachesdk.DescribeGlobalReplicationGroupsInput{
						GlobalReplicationGroupId: aws.String("no-such-grg"),
					},
				)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.GlobalReplicationGroupNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "SnapshotNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DeleteSnapshot(t.Context(), &elasticachesdk.DeleteSnapshotInput{
					SnapshotName: aws.String("no-such-snap"),
				})

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.SnapshotNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "ReservedCacheNodeNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeReservedCacheNodes(
					t.Context(),
					&elasticachesdk.DescribeReservedCacheNodesInput{ReservedCacheNodeId: aws.String("no-such-rcn")},
				)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.ReservedCacheNodeNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "ReservedCacheNodesOfferingNotFound",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				_, err := client.DescribeReservedCacheNodesOfferings(
					t.Context(),
					&elasticachesdk.DescribeReservedCacheNodesOfferingsInput{
						ReservedCacheNodesOfferingId: aws.String("no-such-offering"),
					},
				)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.ReservedCacheNodesOfferingNotFoundFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "UserGroupAlreadyExists",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				input := &elasticachesdk.CreateUserGroupInput{
					UserGroupId: aws.String("dup-ug"),
					Engine:      aws.String("redis"),
				}
				_, err := client.CreateUserGroup(t.Context(), input)
				require.NoError(t, err, "first CreateUserGroup must succeed")

				_, err = client.CreateUserGroup(t.Context(), input)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.UserGroupAlreadyExistsFault](t, err)
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			// AWS models this AlreadyExists fault as 404, not the 409/400 one
			// might expect -- see api-2.json ReservedCacheNodeAlreadyExistsFault.
			name: "ReservedCacheNodeAlreadyExists",
			call: func(t *testing.T, client *elasticachesdk.Client) error {
				t.Helper()
				input := &elasticachesdk.PurchaseReservedCacheNodesOfferingInput{
					ReservedCacheNodesOfferingId: aws.String("31153cd5-4ce6-45a9-b6ce-7f0b6789b8fa"),
					ReservedCacheNodeId:          aws.String("dup-rcn"),
				}
				_, err := client.PurchaseReservedCacheNodesOffering(t.Context(), input)
				require.NoError(t, err, "first purchase must succeed")

				_, err = client.PurchaseReservedCacheNodesOffering(t.Context(), input)

				return err
			},
			checkFault: func(t *testing.T, err error) {
				t.Helper()
				requireFault[elasticachetypes.ReservedCacheNodeAlreadyExistsFault](t, err)
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)

			err := tt.call(t, client)
			require.Error(t, err)

			tt.checkFault(t, err)
			requireHTTPStatus(t, err, tt.wantStatus)
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

// Test_CreateCacheCluster_RestoreFromSnapshot covers the previously-unhandled
// SnapshotName parameter on CreateCacheCluster: AWS restores a new cluster
// from an existing snapshot, and the emulator's handler never even read the
// form field. It's wired now to validate the snapshot exists and inherit the
// snapshot's engine/node type when the caller doesn't override them. A
// missing snapshot surfaces as InvalidParameterValueException (400), not
// SnapshotNotFoundFault -- CreateCacheCluster's modeled error list in
// api-2.json doesn't include SnapshotNotFoundFault at all, so aws-sdk-go-v2
// has no deserializer case for it on this operation.
func Test_CreateCacheCluster_RestoreFromSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		snapshotName string
		wantErr      bool
	}{
		{name: "restores_engine_and_node_type_from_snapshot", snapshotName: "src-snap"},
		{name: "missing_snapshot_is_not_found", snapshotName: "no-such-snap", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestStack(t)
			ctx := t.Context()

			_, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
				CacheClusterId: aws.String("src-cluster"),
				Engine:         aws.String("memcached"),
				CacheNodeType:  aws.String("cache.m5.large"),
				NumCacheNodes:  aws.Int32(1),
			})
			require.NoError(t, err)

			_, err = client.CreateSnapshot(ctx, &elasticachesdk.CreateSnapshotInput{
				SnapshotName:   aws.String("src-snap"),
				CacheClusterId: aws.String("src-cluster"),
			})
			require.NoError(t, err)

			out, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
				CacheClusterId: aws.String("restored-cluster"),
				SnapshotName:   aws.String(tt.snapshotName),
			})

			if tt.wantErr {
				require.Error(t, err)
				requireFault[elasticachetypes.InvalidParameterValueException](t, err)
				requireHTTPStatus(t, err, http.StatusBadRequest)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "memcached", aws.ToString(out.CacheCluster.Engine))
			assert.Equal(t, "cache.m5.large", aws.ToString(out.CacheCluster.CacheNodeType))
		})
	}
}

// Test_CreateReplicationGroup_RestoreFromSnapshot is the replication-group
// counterpart: same previously-dropped SnapshotName parameter, now wired
// through the additive ReplicationGroupCreateOpts.SnapshotName field. As
// with CreateCacheCluster, CreateReplicationGroup's modeled error list also
// omits SnapshotNotFoundFault, so a missing snapshot surfaces as
// InvalidParameterValueException (400) instead.
func Test_CreateReplicationGroup_RestoreFromSnapshot(t *testing.T) {
	t.Parallel()

	client := newTestStack(t)
	ctx := context.Background()

	_, err := client.CreateCacheCluster(ctx, &elasticachesdk.CreateCacheClusterInput{
		CacheClusterId: aws.String("src-cluster-rg"),
		Engine:         aws.String("redis"),
		EngineVersion:  aws.String("7.1.0"),
		CacheNodeType:  aws.String("cache.r6g.large"),
		NumCacheNodes:  aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.CreateSnapshot(ctx, &elasticachesdk.CreateSnapshotInput{
		SnapshotName:   aws.String("src-snap-rg"),
		CacheClusterId: aws.String("src-cluster-rg"),
	})
	require.NoError(t, err)

	out, err := client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("restored-rg"),
		ReplicationGroupDescription: aws.String("restored from snapshot"),
		SnapshotName:                aws.String("src-snap-rg"),
	})
	require.NoError(t, err)
	assert.Equal(t, "cache.r6g.large", aws.ToString(out.ReplicationGroup.CacheNodeType))

	_, err = client.CreateReplicationGroup(ctx, &elasticachesdk.CreateReplicationGroupInput{
		ReplicationGroupId:          aws.String("restored-rg-missing"),
		ReplicationGroupDescription: aws.String("missing snapshot"),
		SnapshotName:                aws.String("no-such-snap-rg"),
	})
	require.Error(t, err)
	requireFault[elasticachetypes.InvalidParameterValueException](t, err)
	requireHTTPStatus(t, err, http.StatusBadRequest)
}
