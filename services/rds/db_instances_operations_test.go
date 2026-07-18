package rds_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRDSBackend_ModifyDBInstance_NewFields tests ModifyDBInstance with new fields.
func TestRDSBackend_ModifyDBInstance_NewFields(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBInstance("mod-db", "postgres", "db.t3.micro", "", "", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)

	opts := rds.DBInstanceOptions{
		StorageType:           "io1",
		BackupRetentionPeriod: 14,
		MultiAZ:               true,
		ApplyImmediately:      true,
	}

	inst, err := b.ModifyDBInstance("mod-db", "db.r5.large", 100, opts)
	require.NoError(t, err)
	assert.Equal(t, "db.r5.large", inst.DBInstanceClass)
	assert.Equal(t, 100, inst.AllocatedStorage)
	assert.Equal(t, "io1", inst.StorageType)
	assert.Equal(t, 14, inst.BackupRetentionPeriod)
	assert.True(t, inst.MultiAZ)
}

func TestRDSBackend_InstanceModifyTransitionAndDeletePublishesEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create modify delete transitions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			const instanceID = "transition-db"

			created, err := b.CreateDBInstance(instanceID, "postgres", "", "", "", "", 20, rds.DBInstanceOptions{})
			require.NoError(t, err)
			assert.Equal(t, "creating", created.DBInstanceStatus)

			modified, err := b.ModifyDBInstance(instanceID, "db.r5.large", 100, rds.DBInstanceOptions{})
			require.NoError(t, err)
			assert.Equal(t, "modifying", modified.DBInstanceStatus)

			require.Eventually(t, func() bool {
				instances, describeErr := b.DescribeDBInstances(instanceID)
				if describeErr != nil || len(instances) != 1 {
					return false
				}

				return instances[0].DBInstanceStatus == "available" && instances[0].DBInstanceClass == "db.r5.large"
			}, 3*time.Second, 20*time.Millisecond)

			deleted, err := b.DeleteDBInstance(instanceID)
			require.NoError(t, err)
			assert.Equal(t, "deleting", deleted.DBInstanceStatus)
			_, err = b.DescribeDBInstances(instanceID)
			require.ErrorIs(t, err, rds.ErrInstanceNotFound)

			messages := rds.EventMessagesForSource(b, instanceID)
			assert.Contains(t, messages, "DB instance created")
			assert.Contains(t, messages, "DB instance is now available")
			assert.Contains(t, messages, "DB instance modification started")
			assert.Contains(t, messages, "DB instance deletion started")
			assert.Contains(t, messages, "DB instance deleted")
		})
	}
}

// TestCreateDBInstance_IdentifierValidation verifies that
// CreateDBInstance enforces AWS identifier constraints:
// must start with a letter, contain only alphanumeric/hyphens, 1–63 chars.
func TestCreateDBInstance_IdentifierValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		id         string
		wantStatus int
	}{
		{
			name:       "valid_simple",
			id:         "my-db-instance",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_single_letter",
			id:         "a",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid_63_chars",
			id:         "a123456789012345678901234567890123456789012345678901234567890ab",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_starts_with_digit",
			id:         "1mydb",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_starts_with_hyphen",
			id:         "-mydb",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_underscore",
			id:         "my_db",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_space",
			id:         "my db",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_64_chars",
			id:         "a1234567890123456789012345678901234567890123456789012345678901234",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyRDSHandler()
			rec := doAccuracyRDS(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {tt.id},
				"Engine":               {"postgres"},
				"MasterUsername":       {"admin"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code, "id=%q", tt.id)
		})
	}
}

// TestCreateDBInstance_AllocatedStorageBound verifies that CreateDBInstance
// rejects an out-of-range AllocatedStorage (AWS bound: 20–65536 GiB) and
// accepts in-range values.
func TestCreateDBInstance_AllocatedStorageBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		id         string
		storage    string
		wantStatus int
	}{
		{name: "below min", id: "as-below-min", storage: "10", wantStatus: http.StatusBadRequest},
		{name: "at min", id: "as-at-min", storage: "20", wantStatus: http.StatusOK},
		{name: "mid range", id: "as-mid-range", storage: "100", wantStatus: http.StatusOK},
		{name: "at max", id: "as-at-max", storage: "65536", wantStatus: http.StatusOK},
		{name: "above max", id: "as-above-max", storage: "65537", wantStatus: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyRDSHandler()
			rec := doAccuracyRDS(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {tc.id},
				"DBInstanceClass":      {"db.t3.micro"},
				"Engine":               {"postgres"},
				"MasterUsername":       {"admin"},
				"AllocatedStorage":     {tc.storage},
			})
			assert.Equal(t, tc.wantStatus, rec.Code, "AllocatedStorage=%s", tc.storage)
		})
	}
}

// TestRDS_CreateDBInstance_IdentifierValidation asserts that invalid identifiers are rejected.
func TestRDS_CreateDBInstance_IdentifierValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		dbInstanceID     string
		dbInstanceClass  string
		engine           string
		wantErrorContain string
		wantCode         int
	}{
		{
			name:            "valid_identifier",
			dbInstanceID:    "my-db-instance",
			dbInstanceClass: "db.t3.micro",
			engine:          "mysql",
			wantCode:        http.StatusOK,
		},
		{
			name:             "empty_identifier",
			dbInstanceID:     "",
			dbInstanceClass:  "db.t3.micro",
			engine:           "mysql",
			wantCode:         http.StatusBadRequest,
			wantErrorContain: "Error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			body := "Action=CreateDBInstance" +
				"&DBInstanceIdentifier=" + tt.dbInstanceID +
				"&DBInstanceClass=" + tt.dbInstanceClass +
				"&Engine=" + tt.engine

			rec := postRDSForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantErrorContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrorContain)
			}
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

// TestInstanceCreateTime verifies that InstanceCreateTime is set on CreateDBInstance.
func TestInstanceCreateTime(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	before := time.Now().UTC()
	inst, err := b.CreateDBInstance("db-ts", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)
	after := time.Now().UTC()
	assert.False(t, inst.InstanceCreateTime.IsZero(), "InstanceCreateTime should be set")
	assert.False(t, inst.InstanceCreateTime.Before(before), "InstanceCreateTime should be after test start")
	assert.False(t, inst.InstanceCreateTime.After(after), "InstanceCreateTime should be before test end")
}

// TestDescribeDBInstancesSorted verifies deterministic sort order.
func TestDescribeDBInstancesSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	for _, id := range []string{"db-z", "db-a", "db-m"} {
		_, err := b.CreateDBInstance(id, "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
		require.NoError(t, err)
	}
	got, err := b.DescribeDBInstances("")
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "db-a", got[0].DBInstanceIdentifier)
	assert.Equal(t, "db-m", got[1].DBInstanceIdentifier)
	assert.Equal(t, "db-z", got[2].DBInstanceIdentifier)
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
