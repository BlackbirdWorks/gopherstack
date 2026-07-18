package rds_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestDBCluster_ReaderEndpointGenerated(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("my-cluster", "aurora-mysql", "admin", "mydb", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	assert.NotEmpty(t, c.ReaderEndpoint, "ReaderEndpoint should be set on cluster creation")
	assert.Contains(t, c.ReaderEndpoint, "my-cluster")
	assert.Contains(t, c.ReaderEndpoint, "-ro")
}

func TestDBCluster_NetworkTypeIPV4Default(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("net-cluster", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	assert.Equal(t, "IPV4", c.NetworkType)
}

func TestDBCluster_NetworkTypeDual(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("dual-cluster", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{NetworkType: "DUAL"})
	require.NoError(t, err)

	assert.Equal(t, "DUAL", c.NetworkType)
}

func TestDBCluster_StorageTypeAuroraIOOptimized(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("iopt-cluster", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{StorageType: "aurora-iopt1"})
	require.NoError(t, err)

	assert.Equal(t, "aurora-iopt1", c.StorageType)
}

func TestDBCluster_EngineLifecycleSupport(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("els-cluster", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{
			EngineLifecycleSupport: "open-source-rds-extended-support",
		})
	require.NoError(t, err)

	assert.Equal(t, "open-source-rds-extended-support", c.EngineLifecycleSupport)
}

func TestDBCluster_OptimizedWritesEnabled(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("ow-cluster", "aurora-mysql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{OptimizedWrites: true})
	require.NoError(t, err)

	assert.True(t, c.OptimizedWrites)
}

func TestDBCluster_ModifyStorageTypeAndNetworkType(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBCluster("mod-cluster", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	modified, err := b.ModifyDBCluster("mod-cluster", "", rds.DBClusterOptions{
		StorageType: "aurora-iopt1",
		NetworkType: "DUAL",
	})
	require.NoError(t, err)

	assert.Equal(t, "aurora-iopt1", modified.StorageType)
	assert.Equal(t, "DUAL", modified.NetworkType)
}

func TestDBCluster_NewFieldsViaHandler(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler()

	rec := postRDSForm(t, h,
		"Action=CreateDBCluster&Version=2014-10-31"+
			"&DBClusterIdentifier=handler-cluster&Engine=aurora-mysql&MasterUsername=admin"+
			"&StorageType=aurora-iopt1&NetworkType=DUAL"+
			"&EngineLifecycleSupport=open-source-rds-extended-support"+
			"&EnableOptimizedWrites=true")
	require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "aurora-iopt1", "StorageType should be in response")
	assert.Contains(t, respStr, "DUAL", "NetworkType should be in response")
	assert.Contains(t, respStr, "open-source-rds-extended-support")
	assert.Contains(t, respStr, "ReaderEndpoint")
}

func TestValidateStorageTypeForCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		val     string
		wantErr bool
	}{
		{"empty", "", false},
		{"aurora", "aurora", false},
		{"aurora-iopt1", "aurora-iopt1", false},
		{"invalid-io1", "io1", true},
		{"invalid-gp3", "gp3", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := rds.ValidateStorageTypeForCluster(tt.val)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDescribeDBClusters_ReaderEndpointInResponse(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	_, err := b.CreateDBCluster("re-cluster", "aurora-mysql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	rec := postRDSForm(t, h,
		"Action=DescribeDBClusters&Version=2014-10-31&DBClusterIdentifier=re-cluster")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "ReaderEndpoint")
	assert.Contains(t, respStr, "re-cluster")
	assert.Contains(t, respStr, "-ro")
}

func TestDBCluster_ReaderEndpointPersistedAndDescribed(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	created, err := b.CreateDBCluster("persist-re", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, created.ReaderEndpoint)

	clusters, err := b.DescribeDBClusters("persist-re")
	require.NoError(t, err)
	require.Len(t, clusters, 1)

	assert.Equal(t, created.ReaderEndpoint, clusters[0].ReaderEndpoint)
}

func TestPersistence_ClusterNewFields(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBCluster("cls-snap", "aurora-mysql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{
			StorageType:            "aurora-iopt1",
			NetworkType:            "DUAL",
			EngineLifecycleSupport: "open-source-rds-extended-support",
			OptimizedWrites:        true,
		})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	clusters, err := b2.DescribeDBClusters("cls-snap")
	require.NoError(t, err)
	require.Len(t, clusters, 1)

	c := clusters[0]
	assert.Equal(t, "aurora-iopt1", c.StorageType, "StorageType should survive round-trip")
	assert.Equal(t, "DUAL", c.NetworkType, "NetworkType should survive round-trip")
	assert.Equal(t, "open-source-rds-extended-support", c.EngineLifecycleSupport)
	assert.True(t, c.OptimizedWrites)
	assert.NotEmpty(t, c.ReaderEndpoint, "ReaderEndpoint should survive round-trip")
}

func TestDBCluster_MultiAZEndpoints(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	c, err := b.CreateDBCluster("rwg-cluster", "aurora-postgresql", "admin", "prod", "", 0, nil,
		rds.DBClusterOptions{MultiAZ: true})
	require.NoError(t, err)

	assert.NotEmpty(t, c.Endpoint, "writer endpoint should be set")
	assert.NotEmpty(t, c.ReaderEndpoint, "reader endpoint should be set for Multi-AZ")
	assert.True(t, c.MultiAZ)

	assert.NotEqual(t, c.Endpoint, c.ReaderEndpoint,
		"writer and reader endpoints should be different")
}

func TestDBCluster_ServerlessV2WithIOOptimized(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	serverlessCfg := &rds.ServerlessV2ScalingConfiguration{
		MinCapacity: 0.5,
		MaxCapacity: 128.0,
	}

	c, err := b.CreateDBCluster("sl2-iopt", "aurora-postgresql", "admin", "", "", 0, serverlessCfg,
		rds.DBClusterOptions{
			StorageType:            "aurora-iopt1",
			EngineLifecycleSupport: "open-source-rds-extended-support-disabled",
		})
	require.NoError(t, err)

	assert.NotNil(t, c.ServerlessV2ScalingConfig)
	assert.Equal(t, "aurora-iopt1", c.StorageType)
	assert.Equal(t, "open-source-rds-extended-support-disabled", c.EngineLifecycleSupport)
}

func TestDBCluster_ModifyEngineLifecycleSupportViaHandler(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	_, err := b.CreateDBCluster("els-mod", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	rec := postRDSForm(t, h,
		"Action=ModifyDBCluster&Version=2014-10-31&DBClusterIdentifier=els-mod"+
			"&EngineLifecycleSupport=open-source-rds-extended-support")
	require.Equal(t, http.StatusOK, rec.Code)

	clusters, err := b.DescribeDBClusters("els-mod")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	assert.Equal(t, "open-source-rds-extended-support", clusters[0].EngineLifecycleSupport)
}

func TestDBCluster_ModifyOptimizedWritesViaBackend(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.CreateDBCluster("ow-mod", "aurora-mysql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)

	clusters, err := b.DescribeDBClusters("ow-mod")
	require.NoError(t, err)
	assert.False(t, clusters[0].OptimizedWrites, "OptimizedWrites should be false by default")

	_, err = b.ModifyDBCluster("ow-mod", "", rds.DBClusterOptions{OptimizedWrites: true})
	require.NoError(t, err)

	clusters, err = b.DescribeDBClusters("ow-mod")
	require.NoError(t, err)
	assert.True(t, clusters[0].OptimizedWrites, "OptimizedWrites should be set after modify")
}
