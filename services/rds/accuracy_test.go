package rds_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

func newAccuracyRDSHandler() *rds.Handler {
	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return rds.NewHandler(b)
}

func doAccuracyRDS(t *testing.T, h *rds.Handler, vals url.Values) *httptest.ResponseRecorder {
	t.Helper()
	body := vals.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func mustCreateAccuracyRDSInstance(t *testing.T, h *rds.Handler, id string) {
	t.Helper()
	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {id},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
}

// TestIopsAndStorageThroughputPersisted verifies Iops and StorageThroughput are stored and returned.
func TestIopsAndStorageThroughputPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"iops-test"},
		"DBInstanceClass":      {"db.r6g.large"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"100"},
		"StorageType":          {"io1"},
		"Iops":                 {"3000"},
		"StorageThroughput":    {"125"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				Iops              int `xml:"Iops"`
				StorageThroughput int `xml:"StorageThroughput"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 3000, resp.Result.DBInstance.Iops)
	assert.Equal(t, 125, resp.Result.DBInstance.StorageThroughput)
}

// TestVpcSecurityGroupsPersisted verifies VpcSecurityGroups are stored and returned.
func TestVpcSecurityGroupsPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"sg-test"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
		"VpcSecurityGroupIds.VpcSecurityGroupID.1": {"sg-11111111"},
		"VpcSecurityGroupIds.VpcSecurityGroupID.2": {"sg-22222222"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				VpcSecurityGroups struct {
					Members []struct {
						VpcSecurityGroupID string `xml:"VpcSecurityGroupId"`
						Status             string `xml:"Status"`
					} `xml:"VpcSecurityGroupMembership"`
				} `xml:"VpcSecurityGroups"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.DBInstance.VpcSecurityGroups.Members, 2)
	assert.Equal(t, "sg-11111111", resp.Result.DBInstance.VpcSecurityGroups.Members[0].VpcSecurityGroupID)
	assert.Equal(t, "active", resp.Result.DBInstance.VpcSecurityGroups.Members[0].Status)
}

// TestLicenseModelPersisted verifies LicenseModel is stored and returned.
func TestLicenseModelPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"license-test"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"oracle-ee"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"100"},
		"LicenseModel":         {"bring-your-own-license"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				LicenseModel string `xml:"LicenseModel"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "bring-your-own-license", resp.Result.DBInstance.LicenseModel)
}

// TestMonitoringFieldsPersisted verifies monitoring fields are stored and returned.
func TestMonitoringFieldsPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"monitoring-test"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
		"MonitoringInterval":   {"60"},
		"MonitoringRoleArn":    {"arn:aws:iam::123456789012:role/rds-monitoring"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				MonitoringRoleArn  string `xml:"MonitoringRoleArn"`
				MonitoringInterval int    `xml:"MonitoringInterval"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 60, resp.Result.DBInstance.MonitoringInterval)
	assert.Equal(t, "arn:aws:iam::123456789012:role/rds-monitoring", resp.Result.DBInstance.MonitoringRoleArn)
}

// TestPreferredWindowsPersisted verifies preferred windows are stored and returned.
func TestPreferredWindowsPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CreateDBInstance"},
		"Version":                    {"2014-10-31"},
		"DBInstanceIdentifier":       {"windows-test"},
		"DBInstanceClass":            {"db.t3.micro"},
		"Engine":                     {"postgres"},
		"MasterUsername":             {"admin"},
		"AllocatedStorage":           {"20"},
		"PreferredMaintenanceWindow": {"mon:03:00-mon:04:00"},
		"PreferredBackupWindow":      {"02:00-03:00"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				PreferredMaintenanceWindow string `xml:"PreferredMaintenanceWindow"`
				PreferredBackupWindow      string `xml:"PreferredBackupWindow"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "mon:03:00-mon:04:00", resp.Result.DBInstance.PreferredMaintenanceWindow)
	assert.Equal(t, "02:00-03:00", resp.Result.DBInstance.PreferredBackupWindow)
}

// TestCopyTagsToSnapshotPersisted verifies CopyTagsToSnapshot is stored.
func TestCopyTagsToSnapshotPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"copytags-test"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
		"CopyTagsToSnapshot":   {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				CopyTagsToSnapshot bool `xml:"CopyTagsToSnapshot"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result.DBInstance.CopyTagsToSnapshot)
}

// TestModifyDBInstanceNewFields verifies that ApplyImmediately, AllowMajorVersionUpgrade,
// VpcSecurityGroupIds, and MonitoringInterval can be modified.
func TestModifyDBInstanceNewFields(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "modify-new-fields")

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                   {"ModifyDBInstance"},
		"Version":                  {"2014-10-31"},
		"DBInstanceIdentifier":     {"modify-new-fields"},
		"ApplyImmediately":         {"true"},
		"AllowMajorVersionUpgrade": {"true"},
		"MonitoringInterval":       {"30"},
		"MonitoringRoleArn":        {"arn:aws:iam::123456789012:role/rds-mon"},
		"VpcSecurityGroupIds.VpcSecurityGroupID.1": {"sg-aaaaaaaa"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				VpcSecurityGroups struct {
					Members []struct {
						VpcSecurityGroupID string `xml:"VpcSecurityGroupId"`
					} `xml:"VpcSecurityGroupMembership"`
				} `xml:"VpcSecurityGroups"`
				MonitoringInterval int `xml:"MonitoringInterval"`
			} `xml:"DBInstance"`
		} `xml:"ModifyDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 30, resp.Result.DBInstance.MonitoringInterval)
	require.Len(t, resp.Result.DBInstance.VpcSecurityGroups.Members, 1)
	assert.Equal(t, "sg-aaaaaaaa", resp.Result.DBInstance.VpcSecurityGroups.Members[0].VpcSecurityGroupID)
}

// TestCopyDBSnapshotWithKmsKeyId verifies KmsKeyID and SourceRegion are persisted on copy.
func TestCopyDBSnapshotWithKmsKeyId(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateDBInstance("snap-src-inst", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{
		StorageEncrypted: true,
		KmsKeyID:         "arn:aws:kms:us-east-1:123:key/original",
	})
	require.NoError(t, err)

	snap, err := b.CreateDBSnapshot("src-snap", "snap-src-inst")
	require.NoError(t, err)
	require.Equal(t, "src-snap", snap.DBSnapshotIdentifier)

	// Copy with a new KMS key.
	copied, err := b.CopyDBSnapshot("src-snap", "dst-snap", rds.CopyDBSnapshotOptions{
		KmsKeyID:     "arn:aws:kms:us-east-1:123:key/new",
		SourceRegion: "us-west-2",
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/new", copied.KmsKeyID)
	assert.Equal(t, "us-west-2", copied.SourceRegion)
}

// TestReadReplicaIdentifiersTrackedOnSource verifies source instance tracks replicas.
func TestReadReplicaIdentifiersTrackedOnSource(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "replica-src")

	// Create two read replicas.
	for _, replicaID := range []string{"replica-a", "replica-b"} {
		rec := doAccuracyRDS(t, h, url.Values{
			"Action":                     {"CreateDBInstanceReadReplica"},
			"Version":                    {"2014-10-31"},
			"DBInstanceIdentifier":       {replicaID},
			"SourceDBInstanceIdentifier": {"replica-src"},
		})
		require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())
	}

	// DescribeDBInstances should show the source has ReadReplicaIdentifiers.
	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)
	_, err := b.CreateDBInstance(
		"replica-src-b",
		"postgres",
		"db.t3.micro",
		"",
		"admin",
		"",
		20,
		rds.DBInstanceOptions{},
	)
	require.NoError(t, err)
	_, err = b.CreateDBInstanceReadReplica("rep-x", "replica-src-b", "")
	require.NoError(t, err)
	_, err = b.CreateDBInstanceReadReplica("rep-y", "replica-src-b", "")
	require.NoError(t, err)

	instances, err := b.DescribeDBInstances("replica-src-b")
	require.NoError(t, err)
	require.Len(t, instances, 1)
	assert.Contains(t, instances[0].ReadReplicaIdentifiers, "rep-x")
	assert.Contains(t, instances[0].ReadReplicaIdentifiers, "rep-y")
}

// TestCreateCustomDBEngineVersionCRUD verifies full CRUD for custom engine versions.
func TestCreateCustomDBEngineVersionCRUD(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	// Create.
	rec := doAccuracyRDS(t, h, url.Values{
		"Action":        {"CreateCustomDBEngineVersion"},
		"Version":       {"2014-10-31"},
		"Engine":        {"custom-oracle-ee"},
		"EngineVersion": {"19.0.0.0.ru-2023-01.rur-2023-01.r1"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var createResp struct {
		Result struct {
			CustomDBEngineVersion struct {
				Engine        string `xml:"Engine"`
				EngineVersion string `xml:"EngineVersion"`
				Status        string `xml:"Status"`
			} `xml:"CustomDBEngineVersion"`
		} `xml:"CreateCustomDBEngineVersionResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Equal(t, "custom-oracle-ee", createResp.Result.CustomDBEngineVersion.Engine)
	assert.Equal(t, "available", createResp.Result.CustomDBEngineVersion.Status)

	// Modify.
	modRec := doAccuracyRDS(t, h, url.Values{
		"Action":        {"ModifyCustomDBEngineVersion"},
		"Version":       {"2014-10-31"},
		"Engine":        {"custom-oracle-ee"},
		"EngineVersion": {"19.0.0.0.ru-2023-01.rur-2023-01.r1"},
		"Status":        {"inactive"},
	})
	require.Equal(t, http.StatusOK, modRec.Code)

	var modResp struct {
		Result struct {
			CustomDBEngineVersion struct {
				Status string `xml:"Status"`
			} `xml:"CustomDBEngineVersion"`
		} `xml:"ModifyCustomDBEngineVersionResult"`
	}
	require.NoError(t, xml.Unmarshal(modRec.Body.Bytes(), &modResp))
	assert.Equal(t, "inactive", modResp.Result.CustomDBEngineVersion.Status)

	// Delete.
	delRec := doAccuracyRDS(t, h, url.Values{
		"Action":        {"DeleteCustomDBEngineVersion"},
		"Version":       {"2014-10-31"},
		"Engine":        {"custom-oracle-ee"},
		"EngineVersion": {"19.0.0.0.ru-2023-01.rur-2023-01.r1"},
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	var delResp struct {
		Result struct {
			CustomDBEngineVersion struct {
				Status string `xml:"Status"`
			} `xml:"CustomDBEngineVersion"`
		} `xml:"DeleteCustomDBEngineVersionResult"`
	}
	require.NoError(t, xml.Unmarshal(delRec.Body.Bytes(), &delResp))
	assert.Equal(t, "deleting", delResp.Result.CustomDBEngineVersion.Status)

	// Modify after delete should fail.
	modRec2 := doAccuracyRDS(t, h, url.Values{
		"Action":        {"ModifyCustomDBEngineVersion"},
		"Version":       {"2014-10-31"},
		"Engine":        {"custom-oracle-ee"},
		"EngineVersion": {"19.0.0.0.ru-2023-01.rur-2023-01.r1"},
		"Status":        {"available"},
	})
	assert.Equal(t, http.StatusBadRequest, modRec2.Code)
}

// TestDBClusterMembersEmitted verifies DBClusterMembers are populated and returned.
func TestDBClusterMembersEmitted(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateDBCluster("test-cluster", "aurora-postgresql", "admin", "mydb", "", 0, nil, rds.DBClusterOptions{
		EngineVersion:              "15.4",
		BacktrackWindow:            86400,
		PreferredBackupWindow:      "02:00-03:00",
		PreferredMaintenanceWindow: "sun:04:00-sun:05:00",
		MultiAZ:                    true,
		CopyTagsToSnapshot:         true,
	})
	require.NoError(t, err)

	clusters, err := b.DescribeDBClusters("test-cluster")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	c := clusters[0]
	assert.Equal(t, "15.4", c.EngineVersion)
	assert.Equal(t, int64(86400), c.BacktrackWindow)
	assert.Equal(t, "02:00-03:00", c.PreferredBackupWindow)
	assert.Equal(t, "sun:04:00-sun:05:00", c.PreferredMaintenanceWindow)
	assert.True(t, c.MultiAZ)
	assert.True(t, c.CopyTagsToSnapshot)
}

// TestModifyDBClusterPersistsNewFields verifies ModifyDBCluster persists the new fields.
func TestModifyDBClusterPersistsNewFields(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateDBCluster("mod-cluster", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	_, err = b.ModifyDBCluster("mod-cluster", "", rds.DBClusterOptions{
		EngineVersion:              "15.5",
		BacktrackWindow:            3600,
		PreferredBackupWindow:      "03:00-04:00",
		PreferredMaintenanceWindow: "wed:05:00-wed:06:00",
		MultiAZ:                    true,
	})
	require.NoError(t, err)

	clusters, err := b.DescribeDBClusters("mod-cluster")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	c := clusters[0]
	assert.Equal(t, "15.5", c.EngineVersion)
	assert.Equal(t, int64(3600), c.BacktrackWindow)
	assert.Equal(t, "03:00-04:00", c.PreferredBackupWindow)
	assert.True(t, c.MultiAZ)
}

// TestCreateDBClusterViaHandler verifies the handler passes new cluster opts.
func TestCreateDBClusterViaHandler(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CreateDBCluster"},
		"Version":                    {"2014-10-31"},
		"DBClusterIdentifier":        {"handler-cluster"},
		"Engine":                     {"aurora-postgresql"},
		"MasterUsername":             {"admin"},
		"EngineVersion":              {"15.4"},
		"BacktrackWindow":            {"86400"},
		"PreferredBackupWindow":      {"01:00-02:00"},
		"PreferredMaintenanceWindow": {"sat:03:00-sat:04:00"},
		"MultiAZ":                    {"true"},
		"StorageEncrypted":           {"true"},
		"CopyTagsToSnapshot":         {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp struct {
		Result struct {
			DBCluster struct {
				EngineVersion              string `xml:"EngineVersion"`
				PreferredBackupWindow      string `xml:"PreferredBackupWindow"`
				PreferredMaintenanceWindow string `xml:"PreferredMaintenanceWindow"`
				BacktrackWindow            int64  `xml:"BacktrackWindow"`
				MultiAZ                    bool   `xml:"MultiAZ"`
				StorageEncrypted           bool   `xml:"StorageEncrypted"`
				CopyTagsToSnapshot         bool   `xml:"CopyTagsToSnapshot"`
			} `xml:"DBCluster"`
		} `xml:"CreateDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	c := resp.Result.DBCluster
	assert.Equal(t, "15.4", c.EngineVersion)
	assert.Equal(t, int64(86400), c.BacktrackWindow)
	assert.Equal(t, "01:00-02:00", c.PreferredBackupWindow)
	assert.True(t, c.MultiAZ)
	assert.True(t, c.StorageEncrypted)
	assert.True(t, c.CopyTagsToSnapshot)
}

// TestGlobalClusterPrimaryRegionAndMembers verifies GlobalCluster has PrimaryRegion and Members.
func TestGlobalClusterPrimaryRegionAndMembers(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateGlobalCluster("test-global", "aurora-postgresql", "15.4", false, false)
	require.NoError(t, err)

	clusters, err := b.DescribeGlobalClusters("")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	// GlobalCluster struct created - verify it exists
	assert.Equal(t, "test-global", clusters[0].GlobalClusterIdentifier)
}

// TestSnapshotKmsKeyIdViaHandler verifies KmsKeyID is returned in snapshot response.
func TestSnapshotKmsKeyIdViaHandler(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateDBInstance("kms-inst", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{
		StorageEncrypted: true,
		KmsKeyID:         "arn:aws:kms:us-east-1:123:key/orig",
	})
	require.NoError(t, err)

	_, err = b.CreateDBSnapshot("kms-snap", "kms-inst")
	require.NoError(t, err)

	copied, err := b.CopyDBSnapshot("kms-snap", "kms-snap-copy", rds.CopyDBSnapshotOptions{
		KmsKeyID:     "arn:aws:kms:us-east-1:123:key/new-key",
		SourceRegion: "us-west-2",
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/new-key", copied.KmsKeyID)
	assert.Equal(t, "us-west-2", copied.SourceRegion)
}

// TestModifyDBClusterViaHandler verifies ModifyDBCluster handler passes new fields.
func TestModifyDBClusterViaHandler(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	// Create cluster first.
	createRec := doAccuracyRDS(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"modify-handler-cluster"},
		"Engine":              {"aurora-postgresql"},
		"MasterUsername":      {"admin"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Modify it.
	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                     {"ModifyDBCluster"},
		"Version":                    {"2014-10-31"},
		"DBClusterIdentifier":        {"modify-handler-cluster"},
		"EngineVersion":              {"15.5"},
		"PreferredBackupWindow":      {"04:00-05:00"},
		"PreferredMaintenanceWindow": {"fri:06:00-fri:07:00"},
		"BacktrackWindow":            {"7200"},
		"CopyTagsToSnapshot":         {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	var resp struct {
		Result struct {
			DBCluster struct {
				EngineVersion              string `xml:"EngineVersion"`
				PreferredBackupWindow      string `xml:"PreferredBackupWindow"`
				PreferredMaintenanceWindow string `xml:"PreferredMaintenanceWindow"`
				BacktrackWindow            int64  `xml:"BacktrackWindow"`
				CopyTagsToSnapshot         bool   `xml:"CopyTagsToSnapshot"`
			} `xml:"DBCluster"`
		} `xml:"ModifyDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	c := resp.Result.DBCluster
	assert.Equal(t, "15.5", c.EngineVersion)
	assert.Equal(t, int64(7200), c.BacktrackWindow)
	assert.Equal(t, "04:00-05:00", c.PreferredBackupWindow)
	assert.True(t, c.CopyTagsToSnapshot)
}

// TestVpcSecurityGroupsViaModify verifies VpcSecurityGroups update via ModifyDBInstance.
func TestVpcSecurityGroupsViaModify(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "sg-modify-inst")

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"ModifyDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"sg-modify-inst"},
		"VpcSecurityGroupIds.VpcSecurityGroupID.1": {"sg-ffffffff"},
		"VpcSecurityGroupIds.VpcSecurityGroupID.2": {"sg-eeeeeeee"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				VpcSecurityGroups struct {
					Members []struct {
						VpcSecurityGroupID string `xml:"VpcSecurityGroupId"`
					} `xml:"VpcSecurityGroupMembership"`
				} `xml:"VpcSecurityGroups"`
			} `xml:"DBInstance"`
		} `xml:"ModifyDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.DBInstance.VpcSecurityGroups.Members, 2)
}

// TestCopyDBSnapshotViaHandler verifies KmsKeyID in CopyDBSnapshot response.
func TestCopyDBSnapshotViaHandler(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	// Create instance + snapshot.
	mustCreateAccuracyRDSInstance(t, h, "cpy-snap-inst")
	snapRec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBSnapshot"},
		"Version":              {"2014-10-31"},
		"DBSnapshotIdentifier": {"src-snap-2"},
		"DBInstanceIdentifier": {"cpy-snap-inst"},
	})
	require.Equal(t, http.StatusOK, snapRec.Code)

	// Copy with KmsKeyId.
	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CopyDBSnapshot"},
		"Version":                    {"2014-10-31"},
		"SourceDBSnapshotIdentifier": {"src-snap-2"},
		"TargetDBSnapshotIdentifier": {"dst-snap-2"},
		"KmsKeyId":                   {"arn:aws:kms:us-east-1:123:key/copy-key"},
		"SourceRegion":               {"us-west-2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBSnapshot struct {
				KmsKeyID     string `xml:"KmsKeyId"`
				SourceRegion string `xml:"SourceRegion"`
			} `xml:"DBSnapshot"`
		} `xml:"CopyDBSnapshotResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/copy-key", resp.Result.DBSnapshot.KmsKeyID)
	assert.Equal(t, "us-west-2", resp.Result.DBSnapshot.SourceRegion)
}

// TestCreateCustomDBEngineVersionDuplicateRejected verifies duplicates are rejected.
func TestCreateCustomDBEngineVersionDuplicateRejected(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	doAccuracyRDS(t, h, url.Values{
		"Action":        {"CreateCustomDBEngineVersion"},
		"Version":       {"2014-10-31"},
		"Engine":        {"custom-oracle-ee"},
		"EngineVersion": {"19.0.0"},
	})

	// Second create should fail.
	rec := doAccuracyRDS(t, h, url.Values{
		"Action":        {"CreateCustomDBEngineVersion"},
		"Version":       {"2014-10-31"},
		"Engine":        {"custom-oracle-ee"},
		"EngineVersion": {"19.0.0"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestReadReplicaIdentifiersInXML verifies ReadReplicaDBInstanceIdentifiers is emitted in XML.
func TestReadReplicaIdentifiersInXML(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "source-inst")

	// Create a read replica.
	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CreateDBInstanceReadReplica"},
		"Version":                    {"2014-10-31"},
		"DBInstanceIdentifier":       {"replica-inst"},
		"SourceDBInstanceIdentifier": {"source-inst"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe source - should show replica ID in ReadReplicaDBInstanceIdentifiers.
	rec = doAccuracyRDS(t, h, url.Values{
		"Action":               {"DescribeDBInstances"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"source-inst"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstances struct {
				Members []struct {
					ReadReplicaDBInstanceIdentifiers struct {
						Members []string `xml:"ReadReplicaDBInstanceIdentifier"`
					} `xml:"ReadReplicaDBInstanceIdentifiers"`
				} `xml:"DBInstance"`
			} `xml:"DBInstances"`
		} `xml:"DescribeDBInstancesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	members := resp.Result.DBInstances.Members
	require.Len(t, members, 1)
	assert.Equal(t, []string{"replica-inst"}, members[0].ReadReplicaDBInstanceIdentifiers.Members)
}

// TestDeleteReplicaClearsSourceReadReplicaIdentifiers verifies deleting a replica removes it from the source.
func TestDeleteReplicaClearsSourceReadReplicaIdentifiers(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "src-del")

	doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CreateDBInstanceReadReplica"},
		"Version":                    {"2014-10-31"},
		"DBInstanceIdentifier":       {"rep-del"},
		"SourceDBInstanceIdentifier": {"src-del"},
	})

	// Delete the replica.
	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"DeleteDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"rep-del"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Source should no longer list rep-del in ReadReplicaDBInstanceIdentifiers.
	rec = doAccuracyRDS(t, h, url.Values{
		"Action":               {"DescribeDBInstances"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"src-del"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstances struct {
				Members []struct {
					ReadReplicaDBInstanceIdentifiers struct {
						Members []string `xml:"ReadReplicaDBInstanceIdentifier"`
					} `xml:"ReadReplicaDBInstanceIdentifiers"`
				} `xml:"DBInstance"`
			} `xml:"DBInstances"`
		} `xml:"DescribeDBInstancesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	members := resp.Result.DBInstances.Members
	require.Len(t, members, 1)
	assert.Empty(t, members[0].ReadReplicaDBInstanceIdentifiers.Members)
}

// TestPromoteReadReplicaClearsSourceList verifies PromoteReadReplica removes from source's list.
func TestPromoteReadReplicaClearsSourceList(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "src-promote")

	doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CreateDBInstanceReadReplica"},
		"Version":                    {"2014-10-31"},
		"DBInstanceIdentifier":       {"rep-promote"},
		"SourceDBInstanceIdentifier": {"src-promote"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"PromoteReadReplica"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"rep-promote"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doAccuracyRDS(t, h, url.Values{
		"Action":               {"DescribeDBInstances"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"src-promote"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstances struct {
				Members []struct {
					ReadReplicaDBInstanceIdentifiers struct {
						Members []string `xml:"ReadReplicaDBInstanceIdentifier"`
					} `xml:"ReadReplicaDBInstanceIdentifiers"`
				} `xml:"DBInstance"`
			} `xml:"DBInstances"`
		} `xml:"DescribeDBInstancesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	members := resp.Result.DBInstances.Members
	require.Len(t, members, 1)
	assert.Empty(t, members[0].ReadReplicaDBInstanceIdentifiers.Members)
}

// TestEnabledCloudwatchLogsExportsInInstanceXML verifies EnabledCloudwatchLogsExports is emitted for instances.
func TestEnabledCloudwatchLogsExportsInInstanceXML(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                               {"CreateDBInstance"},
		"Version":                              {"2014-10-31"},
		"DBInstanceIdentifier":                 {"logs-inst"},
		"DBInstanceClass":                      {"db.t3.micro"},
		"Engine":                               {"mysql"},
		"MasterUsername":                       {"admin"},
		"AllocatedStorage":                     {"20"},
		"EnableCloudwatchLogsExports.member.1": {"error"},
		"EnableCloudwatchLogsExports.member.2": {"slowquery"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				EnabledCloudwatchLogsExports struct {
					Members []string `xml:"member"`
				} `xml:"EnabledCloudwatchLogsExports"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"error", "slowquery"}, resp.Result.DBInstance.EnabledCloudwatchLogsExports.Members)
}

// TestEnabledCloudwatchLogsExportsInClusterXML verifies EnabledCloudwatchLogsExports is emitted for clusters.
func TestEnabledCloudwatchLogsExportsInClusterXML(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                               {"CreateDBCluster"},
		"Version":                              {"2014-10-31"},
		"DBClusterIdentifier":                  {"logs-cluster"},
		"Engine":                               {"aurora-mysql"},
		"MasterUsername":                       {"admin"},
		"EnableCloudwatchLogsExports.member.1": {"audit"},
		"EnableCloudwatchLogsExports.member.2": {"error"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBCluster struct {
				EnabledCloudwatchLogsExports struct {
					Members []string `xml:"member"`
				} `xml:"EnabledCloudwatchLogsExports"`
			} `xml:"DBCluster"`
		} `xml:"CreateDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"audit", "error"}, resp.Result.DBCluster.EnabledCloudwatchLogsExports.Members)
}

// TestMonitoringIntervalValidation verifies invalid MonitoringInterval values are rejected.
func TestMonitoringIntervalValidation(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	// Valid values should succeed.
	for _, v := range []string{"0", "1", "5", "10", "15", "30", "60"} {
		rec := doAccuracyRDS(t, h, url.Values{
			"Action":               {"CreateDBInstance"},
			"Version":              {"2014-10-31"},
			"DBInstanceIdentifier": {"mi-valid-" + v},
			"DBInstanceClass":      {"db.t3.micro"},
			"Engine":               {"postgres"},
			"MasterUsername":       {"admin"},
			"AllocatedStorage":     {"20"},
			"MonitoringInterval":   {v},
		})
		assert.Equal(t, http.StatusOK, rec.Code, "expected OK for MonitoringInterval=%s", v)
	}

	// Invalid value should fail.
	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"mi-invalid"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
		"MonitoringInterval":   {"7"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestMonitoringIntervalValidationModify verifies MonitoringInterval validation in ModifyDBInstance.
func TestMonitoringIntervalValidationModify(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "mi-modify")

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"ModifyDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"mi-modify"},
		"MonitoringInterval":   {"3"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestBackupRetentionPeriodValidation verifies out-of-range BackupRetentionPeriod is rejected.
func TestBackupRetentionPeriodValidation(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                {"CreateDBInstance"},
		"Version":               {"2014-10-31"},
		"DBInstanceIdentifier":  {"brp-invalid"},
		"DBInstanceClass":       {"db.t3.micro"},
		"Engine":                {"postgres"},
		"MasterUsername":        {"admin"},
		"AllocatedStorage":      {"20"},
		"BackupRetentionPeriod": {"36"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestBackupRetentionPeriodValidationModify verifies BackupRetentionPeriod > 35 rejected in modify.
func TestBackupRetentionPeriodValidationModify(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "brp-modify")

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                {"ModifyDBInstance"},
		"Version":               {"2014-10-31"},
		"DBInstanceIdentifier":  {"brp-modify"},
		"BackupRetentionPeriod": {"40"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestEngineVersionPersisted verifies EngineVersion is stored and returned.
func TestEngineVersionPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"ev-inst"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"EngineVersion":        {"14.5"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				EngineVersion string `xml:"EngineVersion"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "14.5", resp.Result.DBInstance.EngineVersion)
}

// TestModifyDBInstanceUpdatesEngineVersion verifies EngineVersion is updated in ModifyDBInstance.
func TestModifyDBInstanceUpdatesEngineVersion(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"ev-modify"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"EngineVersion":        {"14.5"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"ModifyDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"ev-modify"},
		"EngineVersion":        {"15.0"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				EngineVersion string `xml:"EngineVersion"`
			} `xml:"DBInstance"`
		} `xml:"ModifyDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "15.0", resp.Result.DBInstance.EngineVersion)
}

// TestReadReplicaInheritsEngineVersion verifies a read replica inherits the source's EngineVersion.
func TestReadReplicaInheritsEngineVersion(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"ev-source"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"EngineVersion":        {"15.1"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                     {"CreateDBInstanceReadReplica"},
		"Version":                    {"2014-10-31"},
		"DBInstanceIdentifier":       {"ev-replica"},
		"SourceDBInstanceIdentifier": {"ev-source"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				EngineVersion string `xml:"EngineVersion"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceReadReplicaResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "15.1", resp.Result.DBInstance.EngineVersion)
}

// TestPubliclyAccessiblePersisted verifies PubliclyAccessible is stored and returned.
func TestPubliclyAccessiblePersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"pa-inst"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
		"PubliclyAccessible":   {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				PubliclyAccessible bool `xml:"PubliclyAccessible"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result.DBInstance.PubliclyAccessible)
}

// TestPerformanceInsightsEnabledPersisted verifies PerformanceInsightsEnabled is stored and returned.
func TestPerformanceInsightsEnabledPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                    {"CreateDBInstance"},
		"Version":                   {"2014-10-31"},
		"DBInstanceIdentifier":      {"pi-inst"},
		"DBInstanceClass":           {"db.t3.micro"},
		"Engine":                    {"postgres"},
		"MasterUsername":            {"admin"},
		"AllocatedStorage":          {"20"},
		"EnablePerformanceInsights": {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				PerformanceInsightsEnabled bool `xml:"PerformanceInsightsEnabled"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result.DBInstance.PerformanceInsightsEnabled)
}

// TestDBClusterIdentifierPersisted verifies DBClusterIdentifier is stored and returned on instance.
func TestDBClusterIdentifierPersisted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	// Create the cluster first.
	doAccuracyRDS(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"my-aurora"},
		"Engine":              {"aurora-postgresql"},
		"MasterUsername":      {"admin"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"cluster-member"},
		"DBInstanceClass":      {"db.r6g.large"},
		"Engine":               {"aurora-postgresql"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"0"},
		"DBClusterIdentifier":  {"my-aurora"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				DBClusterIdentifier string `xml:"DBClusterIdentifier"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-aurora", resp.Result.DBInstance.DBClusterIdentifier)
}

// TestCreateDBSnapshotCopiesKmsKeyId verifies KmsKeyID is copied from encrypted instances.
func TestCreateDBSnapshotCopiesKmsKeyId(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"enc-inst"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
		"StorageEncrypted":     {"true"},
		"KmsKeyId":             {"arn:aws:kms:us-east-1:123:key/mykey"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBSnapshot"},
		"Version":              {"2014-10-31"},
		"DBSnapshotIdentifier": {"enc-snap"},
		"DBInstanceIdentifier": {"enc-inst"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBSnapshot struct {
				KmsKeyID     string `xml:"KmsKeyId"`
				SnapshotType string `xml:"SnapshotType"`
			} `xml:"DBSnapshot"`
		} `xml:"CreateDBSnapshotResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/mykey", resp.Result.DBSnapshot.KmsKeyID)
	assert.Equal(t, "manual", resp.Result.DBSnapshot.SnapshotType)
}

// TestCreateDBSnapshotNoKmsKeyIdForUnencrypted verifies KmsKeyID is not copied for unencrypted instances.
func TestCreateDBSnapshotNoKmsKeyIdForUnencrypted(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "unenc-inst")

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBSnapshot"},
		"Version":              {"2014-10-31"},
		"DBSnapshotIdentifier": {"unenc-snap"},
		"DBInstanceIdentifier": {"unenc-inst"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBSnapshot struct {
				KmsKeyID     string `xml:"KmsKeyId"`
				SnapshotType string `xml:"SnapshotType"`
			} `xml:"DBSnapshot"`
		} `xml:"CreateDBSnapshotResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.Result.DBSnapshot.KmsKeyID)
	assert.Equal(t, "manual", resp.Result.DBSnapshot.SnapshotType)
}

// TestDBClusterDeletionProtection verifies DeletionProtection is stored for clusters.
func TestDBClusterDeletionProtection(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"dp-cluster"},
		"Engine":              {"aurora-mysql"},
		"MasterUsername":      {"admin"},
		"DeletionProtection":  {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBCluster struct {
				DeletionProtection bool `xml:"DeletionProtection"`
			} `xml:"DBCluster"`
		} `xml:"CreateDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result.DBCluster.DeletionProtection)
}

// TestDBClusterAvailabilityZones verifies AvailabilityZones are stored and returned for clusters.
func TestDBClusterAvailabilityZones(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":                               {"CreateDBCluster"},
		"Version":                              {"2014-10-31"},
		"DBClusterIdentifier":                  {"az-cluster"},
		"Engine":                               {"aurora-postgresql"},
		"MasterUsername":                       {"admin"},
		"AvailabilityZones.AvailabilityZone.1": {"us-east-1a"},
		"AvailabilityZones.AvailabilityZone.2": {"us-east-1b"},
		"AvailabilityZones.AvailabilityZone.3": {"us-east-1c"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBCluster struct {
				AvailabilityZones struct {
					Members []string `xml:"AvailabilityZone"`
				} `xml:"AvailabilityZones"`
			} `xml:"DBCluster"`
		} `xml:"CreateDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(
		t,
		[]string{"us-east-1a", "us-east-1b", "us-east-1c"},
		resp.Result.DBCluster.AvailabilityZones.Members,
	)
}

// TestModifyDBClusterStorageEncryptedChanged verifies StorageEncrypted can be set via ModifyDBCluster.
func TestModifyDBClusterStorageEncryptedChanged(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	doAccuracyRDS(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"enc-cluster"},
		"Engine":              {"aurora-mysql"},
		"MasterUsername":      {"admin"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":              {"ModifyDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"enc-cluster"},
		"StorageEncrypted":    {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBCluster struct {
				StorageEncrypted bool `xml:"StorageEncrypted"`
			} `xml:"DBCluster"`
		} `xml:"ModifyDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result.DBCluster.StorageEncrypted)
}

// TestPendingModifiedValuesEmittedWhenModifying verifies PendingModifiedValues is emitted on modifying state.
func TestPendingModifiedValuesEmittedWhenModifying(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "pmv-inst")

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"ModifyDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"pmv-inst"},
		"DBInstanceClass":      {"db.r6g.large"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// The response should have a PendingModifiedValues element since instance is now modifying.
	body := rec.Body.String()
	assert.Contains(t, body, "PendingModifiedValues")
}

// TestDBInstanceJoinsClusterMemberList verifies creating an instance with DBClusterIdentifier
// adds it to cluster members.
func TestDBInstanceJoinsClusterMemberList(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	doAccuracyRDS(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"member-cluster"},
		"Engine":              {"aurora-postgresql"},
		"MasterUsername":      {"admin"},
	})

	doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"member-inst"},
		"DBInstanceClass":      {"db.r6g.large"},
		"Engine":               {"aurora-postgresql"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"0"},
		"DBClusterIdentifier":  {"member-cluster"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"member-cluster"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBClusters struct {
				Members []struct {
					DBClusterMembers struct {
						Members []struct {
							DBInstanceIdentifier string `xml:"DBInstanceIdentifier"`
						} `xml:"DBClusterMember"`
					} `xml:"DBClusterMembers"`
				} `xml:"DBCluster"`
			} `xml:"DBClusters"`
		} `xml:"DescribeDBClustersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	clusters := resp.Result.DBClusters.Members
	require.Len(t, clusters, 1)
	members := clusters[0].DBClusterMembers.Members
	require.Len(t, members, 1)
	assert.Equal(t, "member-inst", members[0].DBInstanceIdentifier)
}

// TestCreateDBSnapshotOptionGroupName verifies OptionGroupName is copied to snapshot.
func TestCreateDBSnapshotOptionGroupName(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	// Create option group.
	doAccuracyRDS(t, h, url.Values{
		"Action":                 {"CreateOptionGroup"},
		"Version":                {"2014-10-31"},
		"OptionGroupName":        {"myog"},
		"EngineName":             {"postgres"},
		"MajorEngineVersion":     {"14"},
		"OptionGroupDescription": {"test"},
	})

	doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"og-inst"},
		"DBInstanceClass":      {"db.t3.micro"},
		"Engine":               {"postgres"},
		"MasterUsername":       {"admin"},
		"AllocatedStorage":     {"20"},
		"OptionGroupName":      {"myog"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"CreateDBSnapshot"},
		"Version":              {"2014-10-31"},
		"DBSnapshotIdentifier": {"og-snap"},
		"DBInstanceIdentifier": {"og-inst"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBSnapshot struct {
				OptionGroupName string `xml:"OptionGroupName"`
			} `xml:"DBSnapshot"`
		} `xml:"CreateDBSnapshotResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "myog", resp.Result.DBSnapshot.OptionGroupName)
}

// TestModifyDBClusterDeletionProtection verifies DeletionProtection can be set via ModifyDBCluster.
func TestModifyDBClusterDeletionProtection(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()

	doAccuracyRDS(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"modifydp-cluster"},
		"Engine":              {"aurora-mysql"},
		"MasterUsername":      {"admin"},
	})

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":              {"ModifyDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"modifydp-cluster"},
		"DeletionProtection":  {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBCluster struct {
				DeletionProtection bool `xml:"DeletionProtection"`
			} `xml:"DBCluster"`
		} `xml:"ModifyDBClusterResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result.DBCluster.DeletionProtection)
}

// TestCloudwatchLogsExportsModifyInstance verifies CloudWatch log exports can be updated on an instance.
func TestCloudwatchLogsExportsModifyInstance(t *testing.T) {
	t.Parallel()

	h := newAccuracyRDSHandler()
	mustCreateAccuracyRDSInstance(t, h, "cwl-modify")

	rec := doAccuracyRDS(t, h, url.Values{
		"Action":               {"ModifyDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"cwl-modify"},
		"CloudwatchLogsExportConfiguration.EnableLogTypes.member.1": {"postgresql"},
		"CloudwatchLogsExportConfiguration.EnableLogTypes.member.2": {"upgrade"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				EnabledCloudwatchLogsExports struct {
					Members []string `xml:"member"`
				} `xml:"EnabledCloudwatchLogsExports"`
			} `xml:"DBInstance"`
		} `xml:"ModifyDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, []string{"postgresql", "upgrade"}, resp.Result.DBInstance.EnabledCloudwatchLogsExports.Members)
}
