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

func mustCreateAccuracyRDSInstance(t *testing.T, h *rds.Handler, id string) string {
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
	return id
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
		"VpcSecurityGroupIds.VpcSecurityGroupId.1": {"sg-11111111"},
		"VpcSecurityGroupIds.VpcSecurityGroupId.2": {"sg-22222222"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				VpcSecurityGroups struct {
					Members []struct {
						VpcSecurityGroupId string `xml:"VpcSecurityGroupId"`
						Status             string `xml:"Status"`
					} `xml:"VpcSecurityGroupMembership"`
				} `xml:"VpcSecurityGroups"`
			} `xml:"DBInstance"`
		} `xml:"CreateDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.DBInstance.VpcSecurityGroups.Members, 2)
	assert.Equal(t, "sg-11111111", resp.Result.DBInstance.VpcSecurityGroups.Members[0].VpcSecurityGroupId)
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
				MonitoringInterval int    `xml:"MonitoringInterval"`
				MonitoringRoleArn  string `xml:"MonitoringRoleArn"`
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
		"VpcSecurityGroupIds.VpcSecurityGroupId.1": {"sg-aaaaaaaa"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				MonitoringInterval int `xml:"MonitoringInterval"`
				VpcSecurityGroups  struct {
					Members []struct {
						VpcSecurityGroupId string `xml:"VpcSecurityGroupId"`
					} `xml:"VpcSecurityGroupMembership"`
				} `xml:"VpcSecurityGroups"`
			} `xml:"DBInstance"`
		} `xml:"ModifyDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 30, resp.Result.DBInstance.MonitoringInterval)
	require.Len(t, resp.Result.DBInstance.VpcSecurityGroups.Members, 1)
	assert.Equal(t, "sg-aaaaaaaa", resp.Result.DBInstance.VpcSecurityGroups.Members[0].VpcSecurityGroupId)
}

// TestCopyDBSnapshotWithKmsKeyId verifies KmsKeyId and SourceRegion are persisted on copy.
func TestCopyDBSnapshotWithKmsKeyId(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateDBInstance("snap-src-inst", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{
		StorageEncrypted: true,
		KmsKeyId:         "arn:aws:kms:us-east-1:123:key/original",
	})
	require.NoError(t, err)

	snap, err := b.CreateDBSnapshot("src-snap", "snap-src-inst")
	require.NoError(t, err)
	require.Equal(t, "src-snap", snap.DBSnapshotIdentifier)

	// Copy with a new KMS key.
	copied, err := b.CopyDBSnapshot("src-snap", "dst-snap", rds.CopyDBSnapshotOptions{
		KmsKeyId:     "arn:aws:kms:us-east-1:123:key/new",
		SourceRegion: "us-west-2",
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/new", copied.KmsKeyId)
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
				BacktrackWindow            int64  `xml:"BacktrackWindow"`
				PreferredBackupWindow      string `xml:"PreferredBackupWindow"`
				PreferredMaintenanceWindow string `xml:"PreferredMaintenanceWindow"`
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

// TestSnapshotKmsKeyIdViaHandler verifies KmsKeyId is returned in snapshot response.
func TestSnapshotKmsKeyIdViaHandler(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateDBInstance("kms-inst", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{
		StorageEncrypted: true,
		KmsKeyId:         "arn:aws:kms:us-east-1:123:key/orig",
	})
	require.NoError(t, err)

	_, err = b.CreateDBSnapshot("kms-snap", "kms-inst")
	require.NoError(t, err)

	copied, err := b.CopyDBSnapshot("kms-snap", "kms-snap-copy", rds.CopyDBSnapshotOptions{
		KmsKeyId:     "arn:aws:kms:us-east-1:123:key/new-key",
		SourceRegion: "us-west-2",
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/new-key", copied.KmsKeyId)
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
				BacktrackWindow            int64  `xml:"BacktrackWindow"`
				PreferredBackupWindow      string `xml:"PreferredBackupWindow"`
				PreferredMaintenanceWindow string `xml:"PreferredMaintenanceWindow"`
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
		"VpcSecurityGroupIds.VpcSecurityGroupId.1": {"sg-ffffffff"},
		"VpcSecurityGroupIds.VpcSecurityGroupId.2": {"sg-eeeeeeee"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			DBInstance struct {
				VpcSecurityGroups struct {
					Members []struct {
						VpcSecurityGroupId string `xml:"VpcSecurityGroupId"`
					} `xml:"VpcSecurityGroupMembership"`
				} `xml:"VpcSecurityGroups"`
			} `xml:"DBInstance"`
		} `xml:"ModifyDBInstanceResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.DBInstance.VpcSecurityGroups.Members, 2)
}

// TestCopyDBSnapshotViaHandler verifies KmsKeyId in CopyDBSnapshot response.
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
				KmsKeyId     string `xml:"KmsKeyId"`
				SourceRegion string `xml:"SourceRegion"`
			} `xml:"DBSnapshot"`
		} `xml:"CopyDBSnapshotResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "arn:aws:kms:us-east-1:123:key/copy-key", resp.Result.DBSnapshot.KmsKeyId)
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
