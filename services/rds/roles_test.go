package rds_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRemoveRoleFromDBCluster exercises RemoveRoleFromDBCluster.
func TestRemoveRoleFromDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		setup       func(b *rds.InMemoryBackend)
		name        string
		clusterID   string
		roleARN     string
		featureName string
		wantErr     bool
	}{
		{
			name: "success_removes_role",
			setup: func(b *rds.InMemoryBackend) {
				b.AddClusterInternal("c1", "aurora")
				_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R1", "")
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
			name: "noop_when_feature_name_does_not_match",
			setup: func(b *rds.InMemoryBackend) {
				b.AddClusterInternal("c4", "aurora")
				_ = b.AddRoleToDBCluster("c4", "arn:aws:iam::000:role/R1", "S3_INTEGRATION")
			},
			clusterID:   "c4",
			roleARN:     "arn:aws:iam::000:role/R1",
			featureName: "SQLSERVER_AUDIT",
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

			err := b.RemoveRoleFromDBCluster(tt.clusterID, tt.roleARN, tt.featureName)

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

// TestRemoveRoleFromDBCluster_RoleActuallyRemoved verifies the role is gone after removal.
func TestRemoveRoleFromDBCluster_RoleActuallyRemoved(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("c1", "aurora")
	_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R1", "")
	_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R2", "")
	require.Equal(t, 2, rds.ClusterRoleCount(b, "c1"))

	err := b.RemoveRoleFromDBCluster("c1", "arn:aws:iam::000:role/R1", "")
	require.NoError(t, err)

	assert.Equal(t, 1, rds.ClusterRoleCount(b, "c1"))
}

// TestRemoveRoleFromDBInstance exercises RemoveRoleFromDBInstance.
func TestRemoveRoleFromDBInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		setup       func(b *rds.InMemoryBackend)
		name        string
		instanceID  string
		roleARN     string
		featureName string
		wantErr     bool
	}{
		{
			name: "success_removes_role",
			setup: func(b *rds.InMemoryBackend) {
				b.AddInstanceInternal("i1", "mysql")
				_ = b.AddRoleToDBInstance("i1", "arn:aws:iam::000:role/R1", "S3_INTEGRATION")
			},
			instanceID:  "i1",
			roleARN:     "arn:aws:iam::000:role/R1",
			featureName: "S3_INTEGRATION",
		},
		{
			name: "noop_when_role_not_associated",
			setup: func(b *rds.InMemoryBackend) {
				b.AddInstanceInternal("i2", "mysql")
			},
			instanceID:  "i2",
			roleARN:     "arn:aws:iam::000:role/NotAttached",
			featureName: "S3_INTEGRATION",
		},
		{
			name: "noop_when_feature_name_does_not_match",
			setup: func(b *rds.InMemoryBackend) {
				b.AddInstanceInternal("i4", "mysql")
				_ = b.AddRoleToDBInstance("i4", "arn:aws:iam::000:role/R1", "S3_INTEGRATION")
			},
			instanceID:  "i4",
			roleARN:     "arn:aws:iam::000:role/R1",
			featureName: "SQLSERVER_AUDIT",
		},
		{
			name:        "instance_not_found",
			setup:       func(_ *rds.InMemoryBackend) {},
			instanceID:  "no-such-instance",
			roleARN:     "arn:aws:iam::000:role/R1",
			featureName: "S3_INTEGRATION",
			wantErr:     true,
			wantErrIs:   rds.ErrInstanceNotFound,
		},
		{
			name:        "empty_instance_id",
			setup:       func(_ *rds.InMemoryBackend) {},
			instanceID:  "",
			roleARN:     "arn:aws:iam::000:role/R1",
			featureName: "S3_INTEGRATION",
			wantErr:     true,
			wantErrIs:   rds.ErrInvalidParameter,
		},
		{
			name: "empty_feature_name",
			setup: func(b *rds.InMemoryBackend) {
				b.AddInstanceInternal("i3", "mysql")
			},
			instanceID: "i3",
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

			err := b.RemoveRoleFromDBInstance(tt.instanceID, tt.roleARN, tt.featureName)

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

// TestHTTP_RemoveRoleFromDBCluster tests the HTTP handler for RemoveRoleFromDBCluster.
func TestHTTP_RemoveRoleFromDBCluster(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("my-cluster", "aurora-mysql")
	_ = b.AddRoleToDBCluster("my-cluster", "arn:aws:iam::000:role/R1", "")
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

// TestHTTP_RemoveRoleFromDBInstance tests the HTTP handler for RemoveRoleFromDBInstance.
func TestHTTP_RemoveRoleFromDBInstance(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddInstanceInternal("my-inst", "mysql")
	_ = b.AddRoleToDBInstance("my-inst", "arn:aws:iam::000:role/R1", "S3_INTEGRATION")
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
				"&DBInstanceIdentifier=my-inst&RoleArn=arn%3Aaws%3Aiam%3A%3A000%3Arole%2FR1&FeatureName=S3_INTEGRATION",
			wantStatusCode: http.StatusOK,
		},
		{
			name: "instance_not_found",
			body: "Action=RemoveRoleFromDBInstance&Version=2014-10-31" +
				"&DBInstanceIdentifier=no-such&RoleArn=arn%3Aaws%3Aiam%3A%3A000%3Arole%2FR1&FeatureName=S3_INTEGRATION",
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

// TestClusterRoleCountAndInstanceRoleCount verifies the count helpers.
func TestClusterRoleCountAndInstanceRoleCount(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("c1", "aurora")
	b.AddInstanceInternal("i1", "postgres")

	assert.Equal(t, 0, rds.ClusterRoleCount(b, "c1"))
	assert.Equal(t, 0, rds.InstanceRoleCount(b, "i1"))

	_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R1", "")
	_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R2", "")
	_ = b.AddRoleToDBInstance("i1", "arn:aws:iam::000:role/R3", "S3_INTEGRATION")

	assert.Equal(t, 2, rds.ClusterRoleCount(b, "c1"))
	assert.Equal(t, 1, rds.InstanceRoleCount(b, "i1"))
}

// TestHTTP_RemoveRoleFromDBCluster_RoleActuallyRemoved verifies end-to-end via HTTP.
func TestHTTP_RemoveRoleFromDBCluster_RoleActuallyRemoved(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddClusterInternal("c1", "aurora")
	_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R1", "")
	_ = b.AddRoleToDBCluster("c1", "arn:aws:iam::000:role/R2", "")
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

// TestRDSBackend_AddRoleToDBCluster tests AddRoleToDBCluster backend method.
func TestRDSBackend_AddRoleToDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		setup       func(b *rds.InMemoryBackend)
		name        string
		clusterID   string
		roleARN     string
		featureName string
		wantErr     bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBCluster(
					"my-cluster",
					"aurora-postgresql",
					"admin",
					"mydb",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
			},
			clusterID: "my-cluster",
			roleARN:   "arn:aws:iam::000000000000:role/MyRole",
		},
		{
			name:      "cluster_not_found",
			setup:     func(_ *rds.InMemoryBackend) {},
			clusterID: "no-such-cluster",
			roleARN:   "arn:aws:iam::000000000000:role/MyRole",
			wantErr:   true,
			wantErrIs: rds.ErrClusterNotFound,
		},
		{
			name:      "empty_cluster_id",
			setup:     func(_ *rds.InMemoryBackend) {},
			clusterID: "",
			roleARN:   "arn:aws:iam::000000000000:role/MyRole",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name: "empty_role_arn",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBCluster("my-cluster", "aurora-postgresql", "", "", "", 0, nil, rds.DBClusterOptions{})
			},
			clusterID: "my-cluster",
			roleARN:   "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name: "idempotent_duplicate",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBCluster("my-cluster", "aurora-postgresql", "", "", "", 0, nil, rds.DBClusterOptions{})
				_ = b.AddRoleToDBCluster("my-cluster", "arn:aws:iam::000000000000:role/MyRole", "")
			},
			clusterID: "my-cluster",
			roleARN:   "arn:aws:iam::000000000000:role/MyRole",
		},
		{
			name: "empty_feature_name_ok_since_optional",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBCluster("my-cluster", "aurora-postgresql", "", "", "", 0, nil, rds.DBClusterOptions{})
			},
			clusterID: "my-cluster",
			roleARN:   "arn:aws:iam::000000000000:role/MyRole",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			err := b.AddRoleToDBCluster(tt.clusterID, tt.roleARN, tt.featureName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestRDSBackend_AddRoleToDBInstance tests AddRoleToDBInstance backend method.
func TestRDSBackend_AddRoleToDBInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs   error
		setup       func(b *rds.InMemoryBackend)
		name        string
		instanceID  string
		roleARN     string
		featureName string
		wantErr     bool
	}{
		{
			name: "success",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance("my-db", "postgres", "", "", "", "", 20, rds.DBInstanceOptions{})
			},
			instanceID:  "my-db",
			roleARN:     "arn:aws:iam::000000000000:role/MyRole",
			featureName: "S3_INTEGRATION",
		},
		{
			name:        "instance_not_found",
			setup:       func(_ *rds.InMemoryBackend) {},
			instanceID:  "no-such-db",
			roleARN:     "arn:aws:iam::000000000000:role/MyRole",
			featureName: "S3_INTEGRATION",
			wantErr:     true,
			wantErrIs:   rds.ErrInstanceNotFound,
		},
		{
			name:        "empty_instance_id",
			setup:       func(_ *rds.InMemoryBackend) {},
			instanceID:  "",
			roleARN:     "arn:aws:iam::000000000000:role/MyRole",
			featureName: "S3_INTEGRATION",
			wantErr:     true,
			wantErrIs:   rds.ErrInvalidParameter,
		},
		{
			name: "empty_role_arn",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance("my-db", "postgres", "", "", "", "", 20, rds.DBInstanceOptions{})
			},
			instanceID:  "my-db",
			roleARN:     "",
			featureName: "S3_INTEGRATION",
			wantErr:     true,
			wantErrIs:   rds.ErrInvalidParameter,
		},
		{
			name: "empty_feature_name",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateDBInstance("my-db", "postgres", "", "", "", "", 20, rds.DBInstanceOptions{})
			},
			instanceID: "my-db",
			roleARN:    "arn:aws:iam::000000000000:role/MyRole",
			wantErr:    true,
			wantErrIs:  rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			err := b.AddRoleToDBInstance(tt.instanceID, tt.roleARN, tt.featureName)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestAddRoleToDBInstance_FeatureNameKeepsRolesSeparate verifies the fix for the bug where
// AddRoleToDBInstance/RemoveRoleFromDBInstance dropped FeatureName: two roles for different
// features must not collapse into one, and RemoveRoleFromDBInstance must only remove the
// (feature, role) pair that actually matches. Goes through the real HTTP form handler so the
// FeatureName form field is exercised exactly as a real client sends it, not just the backend
// method directly.
func TestAddRoleToDBInstance_FeatureNameKeepsRolesSeparate(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddInstanceInternal("multi-feature-inst", "mysql")
	h := rds.NewHandler(b)

	addS3 := postRDSForm(t, h, "Action=AddRoleToDBInstance&Version=2014-10-31"+
		"&DBInstanceIdentifier=multi-feature-inst"+
		"&RoleArn=arn%3Aaws%3Aiam%3A%3A000%3Arole%2FS3Role&FeatureName=S3_INTEGRATION")
	require.Equal(t, http.StatusOK, addS3.Code)

	addAudit := postRDSForm(t, h, "Action=AddRoleToDBInstance&Version=2014-10-31"+
		"&DBInstanceIdentifier=multi-feature-inst"+
		"&RoleArn=arn%3Aaws%3Aiam%3A%3A000%3Arole%2FAuditRole&FeatureName=SQLSERVER_AUDIT")
	require.Equal(t, http.StatusOK, addAudit.Code)

	require.Equal(t, 2, rds.InstanceRoleCount(b, "multi-feature-inst"),
		"roles for two different features must not collapse into one association")

	// Removing with a FeatureName that doesn't match the stored association is a no-op.
	removeWrongFeature := postRDSForm(t, h, "Action=RemoveRoleFromDBInstance&Version=2014-10-31"+
		"&DBInstanceIdentifier=multi-feature-inst"+
		"&RoleArn=arn%3Aaws%3Aiam%3A%3A000%3Arole%2FS3Role&FeatureName=SQLSERVER_AUDIT")
	require.Equal(t, http.StatusOK, removeWrongFeature.Code)
	assert.Equal(t, 2, rds.InstanceRoleCount(b, "multi-feature-inst"),
		"remove must not touch a role under the wrong feature")

	removeS3 := postRDSForm(t, h, "Action=RemoveRoleFromDBInstance&Version=2014-10-31"+
		"&DBInstanceIdentifier=multi-feature-inst"+
		"&RoleArn=arn%3Aaws%3Aiam%3A%3A000%3Arole%2FS3Role&FeatureName=S3_INTEGRATION")
	require.Equal(t, http.StatusOK, removeS3.Code)
	assert.Equal(t, 1, rds.InstanceRoleCount(b, "multi-feature-inst"),
		"only the matching (feature, role) association should be removed")
}
