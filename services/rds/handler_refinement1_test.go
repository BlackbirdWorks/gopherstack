package rds_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestRefinement1_AccountIDAndRegion verifies AccountID() and Region() on the backend.
func TestRefinement1_AccountIDAndRegion(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", "eu-west-1")

	assert.Equal(t, "123456789012", b.AccountID())
	assert.Equal(t, "eu-west-1", b.Region())
}

// TestRefinement1_Reset verifies that Backend.Reset() clears all state.
func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBInstance("inst-1", "mysql", "", "", "", "", 0, rds.DBInstanceOptions{})
	require.NoError(t, err)

	b.AddClusterInternal("cluster-1", "aurora-mysql")

	require.Equal(t, 1, rds.InstanceCount(b))
	require.Equal(t, 1, rds.ClusterCount(b))

	b.Reset()

	assert.Equal(t, 0, rds.InstanceCount(b))
	assert.Equal(t, 0, rds.ClusterCount(b))
}

// TestRefinement1_HandlerReset verifies that Handler.Reset() clears all backend state.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	h := rds.NewHandler(b)
	_, err := b.CreateDBInstance("inst-1", "mysql", "", "", "", "", 0, rds.DBInstanceOptions{})
	require.NoError(t, err)

	require.Equal(t, 1, rds.InstanceCount(b))

	h.Reset()

	assert.Equal(t, 0, rds.InstanceCount(b))
}

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations has the expected count.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	h := rds.NewHandler(b)

	assert.Equal(t, 165, rds.HandlerOpsLen(h))
}

// TestRefinement1_GetSupportedOperationsSorted verifies that GetSupportedOperations is alphabetically sorted.
func TestRefinement1_GetSupportedOperationsSorted(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	h := rds.NewHandler(b)
	ops := h.GetSupportedOperations()

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"GetSupportedOperations not sorted: %s > %s", ops[i-1], ops[i])
	}
}

// TestRefinement1_DeleteDBClusterCascadeClusterRoles verifies that deleting a cluster
// also removes its IAM role associations.
func TestRefinement1_DeleteDBClusterCascadeClusterRoles(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("my-cluster", "aurora-mysql")

	err := b.AddRoleToDBCluster("my-cluster", "arn:aws:iam::000:role/R1")
	require.NoError(t, err)
	require.Equal(t, 1, rds.ClusterRoleCount(b, "my-cluster"))

	_, err = b.DeleteDBCluster("my-cluster")
	require.NoError(t, err)

	assert.Equal(t, 0, rds.ClusterRoleCount(b, "my-cluster"))
}

// TestRefinement1_DeleteDBInstanceCascadeInstanceRoles verifies that deleting an instance
// also removes its IAM role associations.
func TestRefinement1_DeleteDBInstanceCascadeInstanceRoles(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddInstanceInternal("my-inst", "mysql")

	err := b.AddRoleToDBInstance("my-inst", "arn:aws:iam::000:role/R1")
	require.NoError(t, err)
	require.Equal(t, 1, rds.InstanceRoleCount(b, "my-inst"))

	_, err = b.DeleteDBInstance("my-inst")
	require.NoError(t, err)

	assert.Equal(t, 0, rds.InstanceRoleCount(b, "my-inst"))
}

// TestRefinement1_RemoveRoleFromDBCluster exercises RemoveRoleFromDBCluster.
func TestRefinement1_RemoveRoleFromDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		setup     func(b *rds.InMemoryBackend)
		name      string
		clusterID string
		roleARN   string
		wantErr   bool
	}{
		{
			name: "success_removes_role",
			setup: func(b *rds.InMemoryBackend) {
				b.AddClusterInternal("c1", "aurora")
				_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R1")
			},
			clusterID: "c1",
			roleARN:   "arn:aws:iam::000:role/R1",
		},
		{
			name: "noop_when_role_not_associated",
			setup: func(b *rds.InMemoryBackend) {
				b.AddClusterInternal("c2", "aurora")
			},
			clusterID: "c2",
			roleARN:   "arn:aws:iam::000:role/NotAttached",
		},
		{
			name:      "cluster_not_found",
			setup:     func(_ *rds.InMemoryBackend) {},
			clusterID: "no-such-cluster",
			roleARN:   "arn:aws:iam::000:role/R1",
			wantErr:   true,
			wantErrIs: rds.ErrClusterNotFound,
		},
		{
			name:      "empty_cluster_id",
			setup:     func(_ *rds.InMemoryBackend) {},
			clusterID: "",
			roleARN:   "arn:aws:iam::000:role/R1",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "empty_role_arn",
			setup:     func(_ *rds.InMemoryBackend) {},
			clusterID: "c3",
			roleARN:   "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			err := b.RemoveRoleFromDBCluster(tt.clusterID, tt.roleARN)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement1_RemoveRoleFromDBCluster_RoleActuallyRemoved verifies the role is gone after removal.
func TestRefinement1_RemoveRoleFromDBCluster_RoleActuallyRemoved(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("c1", "aurora")
	_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R1")
	_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R2")
	require.Equal(t, 2, rds.ClusterRoleCount(b, "c1"))

	err := b.RemoveRoleFromDBCluster("c1", "arn:aws:iam::000:role/R1")
	require.NoError(t, err)

	assert.Equal(t, 1, rds.ClusterRoleCount(b, "c1"))
}

// TestRefinement1_RemoveRoleFromDBInstance exercises RemoveRoleFromDBInstance.
func TestRefinement1_RemoveRoleFromDBInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs  error
		setup      func(b *rds.InMemoryBackend)
		name       string
		instanceID string
		roleARN    string
		wantErr    bool
	}{
		{
			name: "success_removes_role",
			setup: func(b *rds.InMemoryBackend) {
				b.AddInstanceInternal("i1", "mysql")
				_ = b.AddRoleToDBInstance("i1", "arn:aws:iam::000:role/R1")
			},
			instanceID: "i1",
			roleARN:    "arn:aws:iam::000:role/R1",
		},
		{
			name: "noop_when_role_not_associated",
			setup: func(b *rds.InMemoryBackend) {
				b.AddInstanceInternal("i2", "mysql")
			},
			instanceID: "i2",
			roleARN:    "arn:aws:iam::000:role/NotAttached",
		},
		{
			name:       "instance_not_found",
			setup:      func(_ *rds.InMemoryBackend) {},
			instanceID: "no-such-instance",
			roleARN:    "arn:aws:iam::000:role/R1",
			wantErr:    true,
			wantErrIs:  rds.ErrInstanceNotFound,
		},
		{
			name:       "empty_instance_id",
			setup:      func(_ *rds.InMemoryBackend) {},
			instanceID: "",
			roleARN:    "arn:aws:iam::000:role/R1",
			wantErr:    true,
			wantErrIs:  rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			err := b.RemoveRoleFromDBInstance(tt.instanceID, tt.roleARN)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestRefinement1_RemoveSourceIdentifierFromSubscription exercises the new operation.
func TestRefinement1_RemoveSourceIdentifierFromSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs        error
		setup            func(b *rds.InMemoryBackend)
		name             string
		subscriptionName string
		sourceIdentifier string
		wantIDCount      int
		wantErr          bool
	}{
		{
			name: "success_removes_identifier",
			setup: func(b *rds.InMemoryBackend) {
				b.AddEventSubscriptionInternal("sub1", "arn:aws:sns:us-east-1:000:my-topic")
				_, _ = b.AddSourceIdentifierToSubscription("sub1", "db-id-1")
				_, _ = b.AddSourceIdentifierToSubscription("sub1", "db-id-2")
			},
			subscriptionName: "sub1",
			sourceIdentifier: "db-id-1",
			wantIDCount:      1,
		},
		{
			name: "noop_when_not_present",
			setup: func(b *rds.InMemoryBackend) {
				b.AddEventSubscriptionInternal("sub2", "arn:aws:sns:us-east-1:000:my-topic")
				_, _ = b.AddSourceIdentifierToSubscription("sub2", "db-id-1")
			},
			subscriptionName: "sub2",
			sourceIdentifier: "not-there",
			wantIDCount:      1,
		},
		{
			name:             "subscription_not_found",
			setup:            func(_ *rds.InMemoryBackend) {},
			subscriptionName: "no-such-sub",
			sourceIdentifier: "db-id-1",
			wantErr:          true,
			wantErrIs:        rds.ErrEventSubscriptionNotFound,
		},
		{
			name:             "empty_subscription_name",
			setup:            func(_ *rds.InMemoryBackend) {},
			subscriptionName: "",
			sourceIdentifier: "db-id-1",
			wantErr:          true,
			wantErrIs:        rds.ErrInvalidParameter,
		},
		{
			name:             "empty_source_identifier",
			setup:            func(_ *rds.InMemoryBackend) {},
			subscriptionName: "sub3",
			sourceIdentifier: "",
			wantErr:          true,
			wantErrIs:        rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			sub, err := b.RemoveSourceIdentifierFromSubscription(tt.subscriptionName, tt.sourceIdentifier)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, sub)
			assert.Len(t, sub.SourceIDs, tt.wantIDCount)
		})
	}
}

// TestRefinement1_BacktrackDBCluster_UniqueIDs verifies each call returns a different BacktrackIdentifier.
func TestRefinement1_BacktrackDBCluster_UniqueIDs(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("aurora-cluster", "aurora-mysql")

	bt1, err := b.BacktrackDBCluster("aurora-cluster", "2024-01-01T00:00:00Z")
	require.NoError(t, err)

	bt2, err := b.BacktrackDBCluster("aurora-cluster", "2024-01-02T00:00:00Z")
	require.NoError(t, err)

	assert.NotEqual(t, bt1.BacktrackIdentifier, bt2.BacktrackIdentifier,
		"each BacktrackDBCluster call should return a unique BacktrackIdentifier")
	assert.True(t, strings.HasPrefix(bt1.BacktrackIdentifier, "backtrack-"))
}

// TestRefinement1_BacktrackDBCluster_EmptyBacktrackTo validates that an empty BacktrackTo returns error.
func TestRefinement1_BacktrackDBCluster_EmptyBacktrackTo(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("aurora-cluster", "aurora-mysql")

	_, err := b.BacktrackDBCluster("aurora-cluster", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrInvalidParameter)
}

// TestRefinement1_AddSourceIdentifierToSubscription_DeduplicatesCode verifies idempotent add.
func TestRefinement1_AddSourceIdentifierToSubscription_DeduplicatesCode(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddEventSubscriptionInternal("sub1", "arn:aws:sns:us-east-1:000:my-topic")

	sub1, err := b.AddSourceIdentifierToSubscription("sub1", "db-id-1")
	require.NoError(t, err)
	assert.Len(t, sub1.SourceIDs, 1)

	// Adding same ID again should be idempotent.
	sub2, err := b.AddSourceIdentifierToSubscription("sub1", "db-id-1")
	require.NoError(t, err)
	assert.Len(t, sub2.SourceIDs, 1, "duplicate source identifier should not be added twice")
}

// TestRefinement1_ExportCountHelpers verifies the count helpers work correctly.
func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")

	assert.Equal(t, 0, rds.InstanceCount(b))
	assert.Equal(t, 0, rds.ClusterCount(b))
	assert.Equal(t, 0, rds.SnapshotCount(b))
	assert.Equal(t, 0, rds.EventSubscriptionCount(b))
	assert.Equal(t, 0, rds.SecurityGroupCount(b))
	assert.Equal(t, 0, rds.BlueGreenDeploymentCount(b))

	b.AddInstanceInternal("i1", "mysql")
	b.AddClusterInternal("c1", "aurora")
	b.AddEventSubscriptionInternal("s1", "arn")
	b.AddBlueGreenDeploymentInternal("bgd-1", "arn:aws:rds:us-east-1:000:db:my-db")
	b.AddSecurityGroupInternal("sg1", "test sg")

	assert.Equal(t, 1, rds.InstanceCount(b))
	assert.Equal(t, 1, rds.ClusterCount(b))
	assert.Equal(t, 1, rds.EventSubscriptionCount(b))
	assert.Equal(t, 1, rds.BlueGreenDeploymentCount(b))
	assert.Equal(t, 1, rds.SecurityGroupCount(b))
}

// TestRefinement1_HTTP_RemoveRoleFromDBCluster tests the HTTP handler for RemoveRoleFromDBCluster.
func TestRefinement1_HTTP_RemoveRoleFromDBCluster(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("my-cluster", "aurora-mysql")
	_ = b.AddRoleToDBCluster("my-cluster", "arn:aws:iam::000:role/R1")
	h := rds.NewHandler(b)

	tests := []struct {
		name           string
		body           string
		wantCode       string
		wantStatusCode int
	}{
		{
			name: "success",
			body: "Action=RemoveRoleFromDBCluster&Version=2014-10-31" +
				"&DBClusterIdentifier=my-cluster&RoleArn=arn%3Aaws%3Aiam%3A%3A000%3Arole%2FR1",
			wantStatusCode: http.StatusOK,
		},
		{
			name: "cluster_not_found",
			body: "Action=RemoveRoleFromDBCluster&Version=2014-10-31" +
				"&DBClusterIdentifier=no-such&RoleArn=arn%3Aaws%3Aiam%3A%3A000%3Arole%2FR1",
			wantStatusCode: http.StatusBadRequest,
			wantCode:       "DBClusterNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postRDSForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			if tt.wantCode != "" {
				assert.Contains(t, rec.Body.String(), tt.wantCode)
			}
		})
	}
}

// TestRefinement1_HTTP_RemoveRoleFromDBInstance tests the HTTP handler for RemoveRoleFromDBInstance.
func TestRefinement1_HTTP_RemoveRoleFromDBInstance(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddInstanceInternal("my-inst", "mysql")
	_ = b.AddRoleToDBInstance("my-inst", "arn:aws:iam::000:role/R1")
	h := rds.NewHandler(b)

	tests := []struct {
		name           string
		body           string
		wantCode       string
		wantStatusCode int
	}{
		{
			name: "success",
			body: "Action=RemoveRoleFromDBInstance&Version=2014-10-31" +
				"&DBInstanceIdentifier=my-inst&RoleArn=arn%3Aaws%3Aiam%3A%3A000%3Arole%2FR1",
			wantStatusCode: http.StatusOK,
		},
		{
			name: "instance_not_found",
			body: "Action=RemoveRoleFromDBInstance&Version=2014-10-31" +
				"&DBInstanceIdentifier=no-such&RoleArn=arn%3Aaws%3Aiam%3A%3A000%3Arole%2FR1",
			wantStatusCode: http.StatusBadRequest,
			wantCode:       "DBInstanceNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postRDSForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			if tt.wantCode != "" {
				assert.Contains(t, rec.Body.String(), tt.wantCode)
			}
		})
	}
}

// TestRefinement1_HTTP_RemoveSourceIdentifierFromSubscription tests the HTTP handler.
func TestRefinement1_HTTP_RemoveSourceIdentifierFromSubscription(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddEventSubscriptionInternal("my-sub", "arn:aws:sns:us-east-1:000:my-topic")
	_, _ = b.AddSourceIdentifierToSubscription("my-sub", "db-id-1")
	h := rds.NewHandler(b)

	tests := []struct {
		name           string
		body           string
		wantCode       string
		wantContains   string
		wantStatusCode int
	}{
		{
			name: "success",
			body: "Action=RemoveSourceIdentifierFromSubscription" +
				"&Version=2014-10-31&SubscriptionName=my-sub&SourceIdentifier=db-id-1",
			wantStatusCode: http.StatusOK,
			wantContains:   "RemoveSourceIdentifierFromSubscriptionResponse",
		},
		{
			name: "subscription_not_found",
			body: "Action=RemoveSourceIdentifierFromSubscription" +
				"&Version=2014-10-31&SubscriptionName=no-such&SourceIdentifier=db-id-1",
			wantStatusCode: http.StatusBadRequest,
			wantCode:       "SubscriptionNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postRDSForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			if tt.wantCode != "" {
				assert.Contains(t, rec.Body.String(), tt.wantCode)
			}

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}

// TestRefinement1_HTTP_BacktrackDBCluster_EmptyBacktrackTo validates HTTP error for missing BacktrackTo.
func TestRefinement1_HTTP_BacktrackDBCluster_EmptyBacktrackTo(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("aurora-cluster", "aurora-mysql")
	h := rds.NewHandler(b)

	rec := postRDSForm(t, h, "Action=BacktrackDBCluster&Version=2014-10-31&DBClusterIdentifier=aurora-cluster")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
}

// TestRefinement1_SeedHelpers verifies all seed helpers work correctly.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")

	cluster := b.AddClusterInternal("c1", "aurora-mysql")
	require.NotNil(t, cluster)
	assert.Equal(t, "c1", cluster.DBClusterIdentifier)

	inst := b.AddInstanceInternal("i1", "postgres")
	require.NotNil(t, inst)
	assert.Equal(t, "i1", inst.DBInstanceIdentifier)

	sub := b.AddEventSubscriptionInternal("s1", "arn:aws:sns:us-east-1:000:my-topic")
	require.NotNil(t, sub)
	assert.Equal(t, "s1", sub.SubscriptionName)

	bgd := b.AddBlueGreenDeploymentInternal("myname", "arn:aws:rds:us-east-1:000:db:my-db")
	require.NotNil(t, bgd)
	assert.Equal(t, "myname", bgd.BlueGreenDeploymentName)

	sg := b.AddSecurityGroupInternal("sg1", "my sg")
	require.NotNil(t, sg)
	assert.Equal(t, "sg1", sg.DBSecurityGroupName)
}

// TestRefinement1_StorageBackendInterface verifies InMemoryBackend implements StorageBackend.
func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ rds.StorageBackend = (*rds.InMemoryBackend)(nil)
}

// TestRefinement1_ProviderNilAppContext verifies Init returns error for nil AppContext.
func TestRefinement1_ProviderNilAppContext(t *testing.T) {
	t.Parallel()

	p := &rds.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrNilAppContext)
}

// TestRefinement1_ClusterRoleCountAndInstanceRoleCount verifies the count helpers.
func TestRefinement1_ClusterRoleCountAndInstanceRoleCount(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("c1", "aurora")
	b.AddInstanceInternal("i1", "postgres")

	assert.Equal(t, 0, rds.ClusterRoleCount(b, "c1"))
	assert.Equal(t, 0, rds.InstanceRoleCount(b, "i1"))

	_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R1")
	_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R2")
	_ = b.AddRoleToDBInstance("i1", "arn:aws:iam::000:role/R3")

	assert.Equal(t, 2, rds.ClusterRoleCount(b, "c1"))
	assert.Equal(t, 1, rds.InstanceRoleCount(b, "i1"))
}

// TestRefinement1_HTTP_RemoveRoleFromDBCluster_RoleActuallyRemoved verifies end-to-end via HTTP.
func TestRefinement1_HTTP_RemoveRoleFromDBCluster_RoleActuallyRemoved(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("c1", "aurora")
	_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R1")
	_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R2")
	h := rds.NewHandler(b)

	require.Equal(t, 2, rds.ClusterRoleCount(b, "c1"))

	rec := postRDSForm(
		t,
		h,
		"Action=RemoveRoleFromDBCluster&Version=2014-10-31"+
			"&DBClusterIdentifier=c1&RoleArn=arn%3Aaws%3Aiam%3A%3A000%3Arole%2FR1",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, rds.ClusterRoleCount(b, "c1"))
}

// TestRefinement1_SnapshotCount verifies the SnapshotCount export helper.
func TestRefinement1_SnapshotCount(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, rds.SnapshotCount(b))

	_, err := b.CreateDBInstance("i1", "mysql", "", "", "", "", 0, rds.DBInstanceOptions{})
	require.NoError(t, err)
	_, err = b.CreateDBSnapshot("snap1", "i1")
	require.NoError(t, err)

	assert.Equal(t, 1, rds.SnapshotCount(b))
}
