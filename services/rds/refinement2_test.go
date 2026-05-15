package rds_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestCreateDBSecurityGroupR2(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs   error
		name        string
		groupName   string
		description string
		wantErr     bool
	}{
		{
			name:        "success",
			groupName:   "my-sg",
			description: "test group",
		},
		{
			name:      "empty name",
			groupName: "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "already exists",
			groupName: "dup-sg",
			wantErr:   true,
			wantErrIs: rds.ErrDBSecurityGroupAlreadyExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "already exists" {
				_, err := b.CreateDBSecurityGroup(tt.groupName, tt.description)
				require.NoError(t, err)
			}
			got, err := b.CreateDBSecurityGroup(tt.groupName, tt.description)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.groupName, got.DBSecurityGroupName)
			assert.Equal(t, tt.description, got.DBSecurityGroupDescription)
		})
	}
}

func TestRemoveFromGlobalCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs       error
		name            string
		globalClusterID string
		dbClusterARN    string
		wantErr         bool
	}{
		{
			name:            "success",
			globalClusterID: "my-global",
			dbClusterARN:    "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
		},
		{
			name:            "not found",
			globalClusterID: "missing",
			dbClusterARN:    "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
			wantErr:         true,
			wantErrIs:       rds.ErrGlobalClusterNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateGlobalCluster(tt.globalClusterID, "aurora-mysql", "", false, false)
				require.NoError(t, err)
			}
			got, err := b.RemoveFromGlobalCluster(tt.globalClusterID, tt.dbClusterARN)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.globalClusterID, got.GlobalClusterIdentifier)
		})
	}
}

func TestFailoverGlobalCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs       error
		name            string
		globalClusterID string
		target          string
		wantErr         bool
	}{
		{
			name:            "success",
			globalClusterID: "my-global",
			target:          "arn:aws:rds:us-east-1:123456789012:cluster:target",
		},
		{
			name:            "not found",
			globalClusterID: "missing",
			target:          "arn:aws:rds:us-east-1:123456789012:cluster:target",
			wantErr:         true,
			wantErrIs:       rds.ErrGlobalClusterNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateGlobalCluster(tt.globalClusterID, "aurora-mysql", "", false, false)
				require.NoError(t, err)
			}
			got, err := b.FailoverGlobalCluster(tt.globalClusterID, tt.target)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.globalClusterID, got.GlobalClusterIdentifier)
			assert.Equal(t, "available", got.Status)
		})
	}
}

func TestSwitchoverGlobalCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs       error
		name            string
		globalClusterID string
		target          string
		wantErr         bool
	}{
		{
			name:            "success",
			globalClusterID: "my-global",
			target:          "arn:aws:rds:us-east-1:123456789012:cluster:target",
		},
		{
			name:            "not found",
			globalClusterID: "missing",
			target:          "arn:aws:rds:us-east-1:123456789012:cluster:target",
			wantErr:         true,
			wantErrIs:       rds.ErrGlobalClusterNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateGlobalCluster(tt.globalClusterID, "aurora-mysql", "", false, false)
				require.NoError(t, err)
			}
			got, err := b.SwitchoverGlobalCluster(tt.globalClusterID, tt.target)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.globalClusterID, got.GlobalClusterIdentifier)
			assert.Equal(t, "available", got.Status)
		})
	}
}

func TestSwitchoverReadReplica(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs  error
		name       string
		instanceID string
		wantErr    bool
	}{
		{
			name:       "success",
			instanceID: "my-instance",
		},
		{
			name:       "not found",
			instanceID: "missing",
			wantErr:    true,
			wantErrIs:  rds.ErrInstanceNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBInstance(
					tt.instanceID,
					"mysql",
					"db.t3.micro",
					"",
					"admin",
					"",
					20,
					rds.DBInstanceOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.SwitchoverReadReplica(tt.instanceID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.instanceID, got.DBInstanceIdentifier)
			assert.Empty(t, got.ReplicaSourceDBInstanceIdentifier)
		})
	}
}

func TestPromoteReadReplicaDBCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		clusterID string
		wantErr   bool
	}{
		{
			name:      "success",
			clusterID: "my-cluster",
		},
		{
			name:      "not found",
			clusterID: "missing",
			wantErr:   true,
			wantErrIs: rds.ErrClusterNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.PromoteReadReplicaDBCluster(tt.clusterID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, got.DBClusterIdentifier)
			assert.Equal(t, "available", got.Status)
		})
	}
}

func TestDescribeAccountAttributes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		wantCount int
	}{
		{name: "returns attributes", wantCount: 8},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeAccountAttributes()
			assert.Len(t, got, tt.wantCount)
			assert.Equal(t, "DBInstances", got[1].AttributeName)
		})
	}
}

func TestDescribeCertificates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		certID    string
		wantCount int
		wantErr   bool
	}{
		{name: "all certs", certID: "", wantCount: 4},
		{name: "specific cert", certID: "rds-ca-2019", wantCount: 1},
		{name: "not found", certID: "missing-cert", wantErr: true, wantErrIs: rds.ErrInvalidParameter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got, err := b.DescribeCertificates(tt.certID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestModifyCertificates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		certID    string
		wantErr   bool
	}{
		{name: "success", certID: "rds-ca-rsa2048-g1"},
		{name: "not found", certID: "missing", wantErr: true, wantErrIs: rds.ErrInvalidParameter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got, err := b.ModifyCertificates(tt.certID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.certID, got.CertificateIdentifier)
		})
	}
}

func TestDescribePendingMaintenanceActions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		resourceARN string
		wantCount   int
	}{
		{name: "all empty", resourceARN: "", wantCount: 0},
		{name: "specific resource empty", resourceARN: "arn:aws:rds:us-east-1:123456789012:db:my-db", wantCount: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribePendingMaintenanceActions(tt.resourceARN)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestDescribeSourceRegions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		regionName string
		wantCount  int
	}{
		{name: "all regions", regionName: "", wantCount: 12},
		{name: "specific region", regionName: "us-east-1", wantCount: 1},
		{name: "no match", regionName: "ap-south-2", wantCount: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeSourceRegions(tt.regionName)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestDescribeDBMajorEngineVersions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		engine    string
		wantEmpty bool
	}{
		{name: "all engines", engine: ""},
		{name: "mysql only", engine: "mysql"},
		{name: "postgres only", engine: "postgres"},
		{name: "no match", engine: "nonexistent", wantEmpty: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeDBMajorEngineVersions(tt.engine)
			if tt.wantEmpty {
				assert.Empty(t, got)

				return
			}
			assert.NotEmpty(t, got)
			if tt.engine != "" {
				for _, v := range got {
					assert.Equal(t, tt.engine, v.Engine)
				}
			}
		})
	}
}

func TestDescribeEngineDefaultParameters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		family string
	}{
		{name: "mysql family", family: "mysql8.0"},
		{name: "empty family", family: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeEngineDefaultParameters(tt.family)
			assert.NotNil(t, got)
		})
	}
}

func TestDescribeEngineDefaultClusterParameters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		family string
	}{
		{name: "aurora-mysql family", family: "aurora-mysql8.0"},
		{name: "empty family", family: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeEngineDefaultClusterParameters(tt.family)
			assert.NotNil(t, got)
		})
	}
}

func TestDescribeDBSnapshotAttributes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs  error
		name       string
		snapshotID string
		wantErr    bool
	}{
		{name: "success returns empty attrs", snapshotID: "snap-1"},
		{name: "not found", snapshotID: "missing", wantErr: true, wantErrIs: rds.ErrSnapshotNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBInstance(
					"db-1",
					"mysql",
					"db.t3.micro",
					"",
					"admin",
					"",
					20,
					rds.DBInstanceOptions{},
				)
				require.NoError(t, err)
				_, err = b.CreateDBSnapshot(tt.snapshotID, "db-1")
				require.NoError(t, err)
			}
			got, err := b.DescribeDBSnapshotAttributes(tt.snapshotID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.snapshotID, got.DBSnapshotIdentifier)
		})
	}
}

func TestModifyDBSnapshot(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs       error
		name            string
		snapshotID      string
		optionGroupName string
		engineVersion   string
		wantErr         bool
	}{
		{
			name:            "success update option group",
			snapshotID:      "snap-1",
			optionGroupName: "my-og",
			engineVersion:   "8.0.28",
		},
		{
			name:       "not found",
			snapshotID: "missing",
			wantErr:    true,
			wantErrIs:  rds.ErrSnapshotNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBInstance(
					"db-1",
					"mysql",
					"db.t3.micro",
					"",
					"admin",
					"",
					20,
					rds.DBInstanceOptions{},
				)
				require.NoError(t, err)
				_, err = b.CreateDBSnapshot(tt.snapshotID, "db-1")
				require.NoError(t, err)
			}
			got, err := b.ModifyDBSnapshot(tt.snapshotID, tt.optionGroupName, tt.engineVersion)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.snapshotID, got.DBSnapshotIdentifier)
			if tt.optionGroupName != "" {
				assert.Equal(t, tt.optionGroupName, got.OptionGroupName)
			}
			if tt.engineVersion != "" {
				assert.Equal(t, tt.engineVersion, got.EngineVersion)
			}
		})
	}
}

func TestModifyDBSnapshotAttribute(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs      error
		name           string
		snapshotID     string
		attributeName  string
		valuesToAdd    []string
		valuesToRemove []string
		wantCount      int
		wantErr        bool
	}{
		{
			name:          "add values",
			snapshotID:    "snap-1",
			attributeName: "restore",
			valuesToAdd:   []string{"123456789012", "all"},
			wantCount:     2,
		},
		{
			name:           "remove values",
			snapshotID:     "snap-1",
			attributeName:  "restore",
			valuesToAdd:    []string{"123456789012"},
			valuesToRemove: []string{"123456789012"},
			wantCount:      0,
		},
		{
			name:          "not found",
			snapshotID:    "missing",
			attributeName: "restore",
			wantErr:       true,
			wantErrIs:     rds.ErrSnapshotNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBInstance(
					"db-1",
					"mysql",
					"db.t3.micro",
					"",
					"admin",
					"",
					20,
					rds.DBInstanceOptions{},
				)
				require.NoError(t, err)
				_, err = b.CreateDBSnapshot(tt.snapshotID, "db-1")
				require.NoError(t, err)
			}
			got, err := b.ModifyDBSnapshotAttribute(tt.snapshotID, tt.attributeName, tt.valuesToAdd, tt.valuesToRemove)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			require.Len(t, got.DBSnapshotAttributes, 1)
			assert.Equal(t, tt.attributeName, got.DBSnapshotAttributes[0].AttributeName)
			assert.Len(t, got.DBSnapshotAttributes[0].AttributeValues, tt.wantCount)
		})
	}
}

func TestDescribeDBClusterSnapshotAttributes(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs  error
		name       string
		snapshotID string
		wantErr    bool
	}{
		{name: "success returns empty attrs", snapshotID: "cluster-snap-1"},
		{name: "not found", snapshotID: "missing", wantErr: true, wantErrIs: rds.ErrSnapshotNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBCluster(
					"my-cluster",
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
				_, err = b.CreateDBClusterSnapshot(tt.snapshotID, "my-cluster")
				require.NoError(t, err)
			}
			got, err := b.DescribeDBClusterSnapshotAttributes(tt.snapshotID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.snapshotID, got.DBClusterSnapshotIdentifier)
		})
	}
}

func TestModifyDBClusterSnapshotAttribute(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs      error
		name           string
		snapshotID     string
		attributeName  string
		valuesToAdd    []string
		valuesToRemove []string
		wantCount      int
		wantErr        bool
	}{
		{
			name:          "add values",
			snapshotID:    "cluster-snap-1",
			attributeName: "restore",
			valuesToAdd:   []string{"123456789012"},
			wantCount:     1,
		},
		{
			name:          "not found",
			snapshotID:    "missing",
			attributeName: "restore",
			wantErr:       true,
			wantErrIs:     rds.ErrSnapshotNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBCluster(
					"my-cluster",
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
				_, err = b.CreateDBClusterSnapshot(tt.snapshotID, "my-cluster")
				require.NoError(t, err)
			}
			got, err := b.ModifyDBClusterSnapshotAttribute(
				tt.snapshotID,
				tt.attributeName,
				tt.valuesToAdd,
				tt.valuesToRemove,
			)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			require.Len(t, got.DBClusterSnapshotAttributes, 1)
			assert.Equal(t, tt.attributeName, got.DBClusterSnapshotAttributes[0].AttributeName)
			assert.Len(t, got.DBClusterSnapshotAttributes[0].AttributeValues, tt.wantCount)
		})
	}
}

func TestDescribeDBClusterBacktracks(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		clusterID string
		wantErr   bool
	}{
		{name: "success returns empty", clusterID: "my-cluster"},
		{name: "not found", clusterID: "missing", wantErr: true, wantErrIs: rds.ErrClusterNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.DescribeDBClusterBacktracks(tt.clusterID)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Empty(t, got)
		})
	}
}

func TestEnableDisableHttpEndpoint(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs   error
		name        string
		clusterID   string
		resourceARN string
		enable      bool
		wantErr     bool
	}{
		{
			name:        "enable success",
			clusterID:   "my-cluster",
			resourceARN: "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
			enable:      true,
		},
		{
			name:        "disable success",
			clusterID:   "my-cluster",
			resourceARN: "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
			enable:      false,
		},
		{
			name:        "not found enable",
			resourceARN: "arn:aws:rds:us-east-1:123456789012:cluster:missing",
			enable:      true,
			wantErr:     true,
			wantErrIs:   rds.ErrClusterNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.clusterID != "" {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
			}
			var err error
			if tt.enable {
				err = b.EnableHTTPEndpoint(tt.resourceARN)
			} else {
				err = b.DisableHTTPEndpoint(tt.resourceARN)
			}
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestModifyCurrentDBClusterCapacity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		clusterID string
		capacity  int
		wantErr   bool
	}{
		{name: "success", clusterID: "my-cluster", capacity: 8},
		{name: "not found", clusterID: "missing", capacity: 8, wantErr: true, wantErrIs: rds.ErrClusterNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					"aurora-mysql",
					"admin",
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.ModifyCurrentDBClusterCapacity(tt.clusterID, tt.capacity)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.capacity, got.ServerlessCapacity)
		})
	}
}

func TestRestoreDBInstanceFromS3(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs       error
		name            string
		instanceID      string
		engine          string
		dbInstanceClass string
		s3Bucket        string
		wantErr         bool
	}{
		{
			name:            "success",
			instanceID:      "restored-db",
			engine:          "mysql",
			dbInstanceClass: "db.t3.micro",
			s3Bucket:        "my-backup-bucket",
		},
		{
			name:       "empty bucket",
			instanceID: "restored-db",
			engine:     "mysql",
			s3Bucket:   "",
			wantErr:    true,
			wantErrIs:  rds.ErrInvalidParameter,
		},
		{
			name:       "empty id",
			instanceID: "",
			engine:     "mysql",
			s3Bucket:   "my-bucket",
			wantErr:    true,
			wantErrIs:  rds.ErrInvalidParameter,
		},
		{
			name:       "already exists",
			instanceID: "existing-db",
			engine:     "mysql",
			s3Bucket:   "my-bucket",
			wantErr:    true,
			wantErrIs:  rds.ErrInstanceAlreadyExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "already exists" {
				_, err := b.CreateDBInstance(
					tt.instanceID,
					tt.engine,
					tt.dbInstanceClass,
					"",
					"admin",
					"",
					20,
					rds.DBInstanceOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.RestoreDBInstanceFromS3(tt.instanceID, tt.engine, tt.dbInstanceClass, tt.s3Bucket)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.instanceID, got.DBInstanceIdentifier)
			assert.Equal(t, tt.engine, got.Engine)
		})
	}
}

func TestRestoreDBClusterFromS3(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs      error
		name           string
		clusterID      string
		engine         string
		masterUsername string
		s3Bucket       string
		wantErr        bool
	}{
		{
			name:           "success",
			clusterID:      "restored-cluster",
			engine:         "aurora-mysql",
			masterUsername: "admin",
			s3Bucket:       "my-backup-bucket",
		},
		{
			name:      "empty bucket",
			clusterID: "restored-cluster",
			engine:    "aurora-mysql",
			s3Bucket:  "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "empty id",
			clusterID: "",
			engine:    "aurora-mysql",
			s3Bucket:  "my-bucket",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "already exists",
			clusterID: "existing-cluster",
			engine:    "aurora-mysql",
			s3Bucket:  "my-bucket",
			wantErr:   true,
			wantErrIs: rds.ErrClusterAlreadyExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "already exists" {
				_, err := b.CreateDBCluster(
					tt.clusterID,
					tt.engine,
					tt.masterUsername,
					"",
					"",
					0,
					nil,
					rds.DBClusterOptions{},
				)
				require.NoError(t, err)
			}
			got, err := b.RestoreDBClusterFromS3(tt.clusterID, tt.engine, tt.masterUsername, tt.s3Bucket)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, got.DBClusterIdentifier)
			assert.Equal(t, tt.engine, got.Engine)
		})
	}
}

func TestModifyDBRecommendation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		recID     string
		status    string
		wantErr   bool
	}{
		{
			name:      "not found",
			recID:     "missing-rec",
			status:    "accepted",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got, err := b.ModifyDBRecommendation(tt.recID, tt.status)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.status, got.Status)
		})
	}
}

func TestDescribeDBRecommendations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		recID     string
		status    string
		wantCount int
	}{
		{name: "all empty", recID: "", status: "", wantCount: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeDBRecommendations(tt.recID, tt.status)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestPurchaseReservedDBInstancesOffering(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs       error
		name            string
		offeringID      string
		reservedID      string
		dbInstanceCount int
		wantErr         bool
	}{
		{
			name:            "success with known offering",
			offeringID:      "01f5e8a3-2f47-4f47-8a7f-1234567890ab",
			reservedID:      "my-reserved-1",
			dbInstanceCount: 1,
		},
		{
			name:            "success with unknown offering",
			offeringID:      "unknown-offering-id",
			reservedID:      "my-reserved-2",
			dbInstanceCount: 2,
		},
		{
			name:            "zero count",
			offeringID:      "some-offering",
			reservedID:      "my-reserved-3",
			dbInstanceCount: 0,
			wantErr:         true,
			wantErrIs:       rds.ErrInvalidParameter,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got, err := b.PurchaseReservedDBInstancesOffering(tt.offeringID, tt.reservedID, tt.dbInstanceCount)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.offeringID, got.ReservedDBInstancesOfferingID)
			assert.Equal(t, tt.dbInstanceCount, got.DBInstanceCount)
			assert.Equal(t, "active", got.State)
		})
	}
}

func TestDescribeReservedDBInstances(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		filterID    string
		filterClass string
		wantCount   int
	}{
		{name: "all empty", filterID: "", filterClass: "", wantCount: 1},
		{name: "match by ID", filterID: "my-ri", filterClass: "", wantCount: 1},
		{name: "no match by ID", filterID: "missing", filterClass: "", wantCount: 0},
		{name: "match by class", filterID: "", filterClass: "db.t3.micro", wantCount: 1},
		{name: "no match by class", filterID: "", filterClass: "db.r5.large", wantCount: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			_, err := b.PurchaseReservedDBInstancesOffering("01f5e8a3-2f47-4f47-8a7f-1234567890ab", "my-ri", 1)
			require.NoError(t, err)
			got := b.DescribeReservedDBInstances(tt.filterID, tt.filterClass)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestDescribeReservedDBInstancesOfferings(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		offeringID  string
		classFilter string
		wantMin     int
	}{
		{name: "all offerings", offeringID: "", classFilter: "", wantMin: 5},
		{name: "filter by ID", offeringID: "01f5e8a3-2f47-4f47-8a7f-1234567890ab", classFilter: "", wantMin: 1},
		{name: "filter by class", offeringID: "", classFilter: "db.m5.large", wantMin: 2},
		{name: "no match", offeringID: "nonexistent", classFilter: "", wantMin: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeReservedDBInstancesOfferings(tt.offeringID, tt.classFilter)
			assert.GreaterOrEqual(t, len(got), tt.wantMin)
		})
	}
}
