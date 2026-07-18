package rds_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		"SkipFinalSnapshot":    {"true"},
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

// TestInstanceFieldRangeValidation verifies MonitoringInterval and BackupRetentionPeriod
// range validation on both CreateDBInstance and ModifyDBInstance. It merges what were
// previously four separate but structurally identical tests (TestMonitoringIntervalValidation,
// TestMonitoringIntervalValidationModify, TestBackupRetentionPeriodValidation,
// TestBackupRetentionPeriodValidationModify) into one table, preserving every case.
func TestInstanceFieldRangeValidation(t *testing.T) {
	t.Parallel()

	type fieldRangeCase struct {
		name        string
		id          string
		action      string
		modifyField string
		modifyValue string
		wantCode    int
	}

	tests := make([]fieldRangeCase, 0, 11)
	tests = append(tests,
		fieldRangeCase{
			name: "monitoring interval 7 invalid on create", id: "mi-invalid", action: "CreateDBInstance",
			modifyField: "MonitoringInterval", modifyValue: "7", wantCode: http.StatusBadRequest,
		},
		fieldRangeCase{
			name: "monitoring interval 3 invalid on modify", id: "mi-modify", action: "ModifyDBInstance",
			modifyField: "MonitoringInterval", modifyValue: "3", wantCode: http.StatusBadRequest,
		},
		fieldRangeCase{
			name: "backup retention period 36 invalid on create", id: "brp-invalid", action: "CreateDBInstance",
			modifyField: "BackupRetentionPeriod", modifyValue: "36", wantCode: http.StatusBadRequest,
		},
		fieldRangeCase{
			name: "backup retention period 40 invalid on modify", id: "brp-modify", action: "ModifyDBInstance",
			modifyField: "BackupRetentionPeriod", modifyValue: "40", wantCode: http.StatusBadRequest,
		},
	)
	for _, v := range []string{"0", "1", "5", "10", "15", "30", "60"} {
		tests = append(tests, fieldRangeCase{
			name: "monitoring interval " + v + " valid on create", id: "mi-valid-" + v, action: "CreateDBInstance",
			modifyField: "MonitoringInterval", modifyValue: v, wantCode: http.StatusOK,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyRDSHandler()
			id := tt.id

			if tt.action == "ModifyDBInstance" {
				mustCreateAccuracyRDSInstance(t, h, id)
				rec := doAccuracyRDS(t, h, url.Values{
					"Action":               {"ModifyDBInstance"},
					"Version":              {"2014-10-31"},
					"DBInstanceIdentifier": {id},
					tt.modifyField:         {tt.modifyValue},
				})
				assert.Equal(t, tt.wantCode, rec.Code)

				return
			}

			rec := doAccuracyRDS(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {id},
				"DBInstanceClass":      {"db.t3.micro"},
				"Engine":               {"postgres"},
				"MasterUsername":       {"admin"},
				"AllocatedStorage":     {"20"},
				tt.modifyField:         {tt.modifyValue},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
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
		"ApplyImmediately":     {"true"},
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
