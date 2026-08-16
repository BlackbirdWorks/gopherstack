package neptune_test

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func TestHandler_CreateDescribeDeleteDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		setup        func(*neptune.Handler)
		name         string
		action       string
		wantContains string
		wantStatus   int
	}{
		{
			name:   "create_cluster",
			action: "CreateDBCluster",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "test-cluster",
		},
		{
			name: "describe_clusters",
			setup: func(h *neptune.Handler) {
				createCluster(t, h, "test-cluster")
			},
			vals: url.Values{
				"Action":  {"DescribeDBClusters"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBClustersResponse",
		},
		{
			name: "delete_cluster",
			setup: func(h *neptune.Handler) {
				createCluster(t, h, "test-cluster")
			},
			vals: url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
				"SkipFinalSnapshot":   {"true"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterResponse",
		},
		{
			name: "modify_cluster_not_found",
			vals: url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"mod-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_StopStartFailoverDBCluster(t *testing.T) {
	t.Parallel()

	t.Run("stop_cluster", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createCluster(t, h, "stop-cluster")
		rr := doRequest(t, h, url.Values{
			"Action":              {"StopDBCluster"},
			"Version":             {"2014-10-31"},
			"DBClusterIdentifier": {"stop-cluster"},
		})
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("start_cluster", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createCluster(t, h, "stop-cluster")
		// Must stop before starting.
		doRequest(t, h, url.Values{
			"Action":              {"StopDBCluster"},
			"Version":             {"2014-10-31"},
			"DBClusterIdentifier": {"stop-cluster"},
		})
		rr := doRequest(t, h, url.Values{
			"Action":              {"StartDBCluster"},
			"Version":             {"2014-10-31"},
			"DBClusterIdentifier": {"stop-cluster"},
		})
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("failover_cluster", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createCluster(t, h, "failover-stop-cluster")
		createInstance(t, h, "failover-stop-writer", "failover-stop-cluster")
		createInstance(t, h, "failover-stop-reader", "failover-stop-cluster")
		rr := doRequest(t, h, url.Values{
			"Action":              {"FailoverDBCluster"},
			"Version":             {"2014-10-31"},
			"DBClusterIdentifier": {"failover-stop-cluster"},
		})
		assert.Equal(t, http.StatusOK, rr.Code)
	})
}

func TestHandler_DescribeDBClusters_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, id := range []string{"cluster-1", "cluster-2", "cluster-3"} {
		doRequest(t, h, url.Values{
			"Action":              {"CreateDBCluster"},
			"Version":             {"2014-10-31"},
			"DBClusterIdentifier": {id},
		})
	}

	tests := []struct {
		vals       url.Values
		name       string
		wantCode   int
		wantMarker bool
	}{
		{
			name: "all clusters",
			vals: url.Values{
				"Action":  {"DescribeDBClusters"},
				"Version": {"2014-10-31"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "paginated with MaxRecords=1",
			vals: url.Values{
				"Action":     {"DescribeDBClusters"},
				"Version":    {"2014-10-31"},
				"MaxRecords": {"1"},
			},
			wantCode:   http.StatusOK,
			wantMarker: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantCode, rr.Code)
			assert.Contains(t, rr.Body.String(), "DescribeDBClustersResponse")

			if tt.wantMarker {
				assert.Contains(t, rr.Body.String(), "<Marker>")
			}
		})
	}
}

func TestHandler_AddRoleToDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*neptune.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "add_role_success",
			setup: func(h *neptune.Handler) {
				createCluster(t, h, "role-cluster")
			},
			vals: url.Values{
				"Action":              {"AddRoleToDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"role-cluster"},
				"RoleArn":             {"arn:aws:iam::000000000000:role/neptune-role"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "AddRoleToDBClusterResponse",
		},
		{
			name: "add_role_cluster_not_found",
			vals: url.Values{
				"Action":              {"AddRoleToDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"no-such-cluster"},
				"RoleArn":             {"arn:aws:iam::000000000000:role/neptune-role"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterNotFoundFault",
		},
		{
			name: "add_role_missing_role_arn",
			setup: func(h *neptune.Handler) {
				createCluster(t, h, "role-cluster2")
			},
			vals: url.Values{
				"Action":              {"AddRoleToDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"role-cluster2"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// --- ServerlessV2ScalingConfiguration ---

func TestCreateDBCluster_ServerlessV2ScalingConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"sv2-cluster"},
		"ServerlessV2ScalingConfiguration.MinCapacity": {"1.0"},
		"ServerlessV2ScalingConfiguration.MaxCapacity": {"128.0"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "sv2-cluster")
	assert.Contains(t, body, "ServerlessV2ScalingConfiguration")
	assert.Contains(t, body, "1")
	assert.Contains(t, body, "128")
}

func TestModifyDBCluster_ServerlessV2ScalingConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "sv2-mod")

	rr := doRequest(t, h, url.Values{
		"Action":              {"ModifyDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"sv2-mod"},
		"ServerlessV2ScalingConfiguration.MinCapacity": {"2.5"},
		"ServerlessV2ScalingConfiguration.MaxCapacity": {"64.0"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "ServerlessV2ScalingConfiguration")
	assert.Contains(t, body, "2.5")
	assert.Contains(t, body, "64")
}

func TestDescribeDBClusters_ServerlessV2ScalingConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"sv2-desc"},
		"ServerlessV2ScalingConfiguration.MinCapacity": {"0.5"},
		"ServerlessV2ScalingConfiguration.MaxCapacity": {"16.0"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"sv2-desc"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "ServerlessV2ScalingConfiguration")
	assert.Contains(t, body, "0.5")
	assert.Contains(t, body, "16")
}

func TestServerlessV2ScalingConfiguration_InvalidMin(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"sv2-bad"},
		"ServerlessV2ScalingConfiguration.MinCapacity": {"not-a-number"},
		"ServerlessV2ScalingConfiguration.MaxCapacity": {"64.0"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

func TestServerlessV2ScalingConfiguration_InvalidMax(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"sv2-bad2"},
		"ServerlessV2ScalingConfiguration.MinCapacity": {"1.0"},
		"ServerlessV2ScalingConfiguration.MaxCapacity": {"not-a-number"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

// --- EngineMode (Neptune-Serverless) ---

func TestCreateDBCluster_EngineMode_Provisioned(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"prov-cluster"},
		"EngineMode":          {"provisioned"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "prov-cluster")
	assert.Contains(t, body, "provisioned")
}

func TestCreateDBCluster_EngineMode_Serverless(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"serverless-cluster"},
		"EngineMode":          {"serverless"},
		"ServerlessV2ScalingConfiguration.MinCapacity": {"1.0"},
		"ServerlessV2ScalingConfiguration.MaxCapacity": {"128.0"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "serverless-cluster")
	assert.Contains(t, body, "serverless")
	assert.Contains(t, body, "ServerlessV2ScalingConfiguration")
}

func TestCreateDBCluster_EngineMode_DefaultIsProvisioned(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"default-mode-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "provisioned")
}

// --- IAM Authentication ---

func TestCreateDBCluster_EnableIAMDatabaseAuthentication(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                          {"CreateDBCluster"},
		"Version":                         {"2014-10-31"},
		"DBClusterIdentifier":             {"iam-cluster"},
		"EnableIAMDatabaseAuthentication": {"true"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "iam-cluster")
	assert.Contains(t, body, "IAMDatabaseAuthenticationEnabled")
	assert.Contains(t, body, "true")
}

func TestModifyDBCluster_EnableIAMDatabaseAuthentication(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "iam-mod")

	// Enable IAM auth
	rr := doRequest(t, h, url.Values{
		"Action":                          {"ModifyDBCluster"},
		"Version":                         {"2014-10-31"},
		"DBClusterIdentifier":             {"iam-mod"},
		"EnableIAMDatabaseAuthentication": {"true"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "true")

	// Verify via describe
	rr = doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"iam-mod"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "true")
}

func TestModifyDBCluster_DisableIAMDatabaseAuthentication(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create with IAM auth enabled
	doRequest(t, h, url.Values{
		"Action":                          {"CreateDBCluster"},
		"Version":                         {"2014-10-31"},
		"DBClusterIdentifier":             {"iam-disable"},
		"EnableIAMDatabaseAuthentication": {"true"},
	})

	// Disable
	rr := doRequest(t, h, url.Values{
		"Action":                          {"ModifyDBCluster"},
		"Version":                         {"2014-10-31"},
		"DBClusterIdentifier":             {"iam-disable"},
		"EnableIAMDatabaseAuthentication": {"false"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// --- ManageMasterUserPassword ---

func TestCreateDBCluster_ManageMasterUserPassword(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                   {"CreateDBCluster"},
		"Version":                  {"2014-10-31"},
		"DBClusterIdentifier":      {"mup-cluster"},
		"ManageMasterUserPassword": {"true"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "mup-cluster")
	assert.Contains(t, body, "MasterUserManagedSecret")
	assert.Contains(t, body, "secretsmanager")
	assert.Contains(t, body, "active")
}

func TestModifyDBCluster_ManageMasterUserPassword(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "mup-mod")

	rr := doRequest(t, h, url.Values{
		"Action":                   {"ModifyDBCluster"},
		"Version":                  {"2014-10-31"},
		"DBClusterIdentifier":      {"mup-mod"},
		"ManageMasterUserPassword": {"true"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "MasterUserManagedSecret")
}

func TestDescribeDBClusters_ManageMasterUserPassword(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                   {"CreateDBCluster"},
		"Version":                  {"2014-10-31"},
		"DBClusterIdentifier":      {"mup-desc"},
		"ManageMasterUserPassword": {"true"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"mup-desc"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "MasterUserManagedSecret")
	assert.Contains(t, body, "secretsmanager")
}

// --- DeletionProtection ---

func TestCreateDBCluster_DeletionProtection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"dp-cluster"},
		"DeletionProtection":  {"true"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "dp-cluster")
	assert.Contains(t, body, "DeletionProtection")
	assert.Contains(t, body, "true")
}

func TestModifyDBCluster_DeletionProtection_Enable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "dp-mod")

	rr := doRequest(t, h, url.Values{
		"Action":              {"ModifyDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"dp-mod"},
		"DeletionProtection":  {"true"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "true")
}

func TestModifyDBCluster_DeletionProtection_Disable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"dp-disable"},
		"DeletionProtection":  {"true"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":              {"ModifyDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"dp-disable"},
		"DeletionProtection":  {"false"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// --- StorageEncrypted + KmsKeyId ---

func TestCreateDBCluster_StorageEncrypted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"enc-cluster"},
		"StorageEncrypted":    {"true"},
		"KmsKeyId":            {"arn:aws:kms:us-east-1:000000000000:key/test-key-id"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "StorageEncrypted")
	assert.Contains(t, body, "KmsKeyId")
	assert.Contains(t, body, "test-key-id")
}

// --- PreferredBackupWindow + PreferredMaintenanceWindow ---

func TestCreateDBCluster_PreferredWindows(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                     {"CreateDBCluster"},
		"Version":                    {"2014-10-31"},
		"DBClusterIdentifier":        {"win-cluster"},
		"PreferredBackupWindow":      {"02:00-03:00"},
		"PreferredMaintenanceWindow": {"sun:05:00-sun:06:00"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "02:00-03:00")
	assert.Contains(t, body, "sun:05:00-sun:06:00")
}

func TestModifyDBCluster_PreferredWindows(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "win-mod")

	rr := doRequest(t, h, url.Values{
		"Action":                     {"ModifyDBCluster"},
		"Version":                    {"2014-10-31"},
		"DBClusterIdentifier":        {"win-mod"},
		"PreferredBackupWindow":      {"03:00-04:00"},
		"PreferredMaintenanceWindow": {"mon:06:00-mon:07:00"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "03:00-04:00")
	assert.Contains(t, body, "mon:06:00-mon:07:00")
}

// --- EngineVersion ---

func TestCreateDBCluster_EngineVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"ev-cluster"},
		"EngineVersion":       {"1.2.0.0"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "1.2.0.0")
}

func TestModifyDBCluster_EngineVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ev-mod")

	rr := doRequest(t, h, url.Values{
		"Action":              {"ModifyDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"ev-mod"},
		"EngineVersion":       {"1.4.0.0"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "1.4.0.0")
}

// --- Combined: Neptune-Serverless full config ---

func TestNeptuneServerless_FullConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"nsl-full"},
		"EngineMode":          {"serverless"},
		"ServerlessV2ScalingConfiguration.MinCapacity": {"2.0"},
		"ServerlessV2ScalingConfiguration.MaxCapacity": {"128.0"},
		"EnableIAMDatabaseAuthentication":              {"true"},
		"ManageMasterUserPassword":                     {"true"},
		"StorageEncrypted":                             {"true"},
		"DeletionProtection":                           {"true"},
		"PreferredBackupWindow":                        {"01:00-02:00"},
		"PreferredMaintenanceWindow":                   {"sat:05:00-sat:06:00"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "nsl-full")
	assert.Contains(t, body, "serverless")
	assert.Contains(t, body, "ServerlessV2ScalingConfiguration")
	assert.Contains(t, body, "IAMDatabaseAuthenticationEnabled")
	assert.Contains(t, body, "MasterUserManagedSecret")
	assert.Contains(t, body, "secretsmanager")
	assert.Contains(t, body, "DeletionProtection")
	assert.Contains(t, body, "01:00-02:00")

	// Describe to verify persistence
	rr = doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"nsl-full"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body = rr.Body.String()
	assert.Contains(t, body, "serverless")
	assert.Contains(t, body, "2")
	assert.Contains(t, body, "128")
}

// --- Backend unit tests ---

func TestBackend_CreateDBCluster_ServerlessV2(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	sv2 := &neptune.ServerlessV2ScalingConfiguration{MinCapacity: 1.0, MaxCapacity: 64.0}
	cluster, err := b.CreateDBCluster(context.Background(), "sv2-unit", "", 0, neptune.DBClusterCreateOptions{
		ServerlessV2ScalingConfig: sv2,
		EngineMode:                "serverless",
	})
	require.NoError(t, err)
	require.NotNil(t, cluster.ServerlessV2ScalingConfig)
	assert.InEpsilon(t, 1.0, cluster.ServerlessV2ScalingConfig.MinCapacity, 0.001)
	assert.InEpsilon(t, 64.0, cluster.ServerlessV2ScalingConfig.MaxCapacity, 0.001)
	assert.Equal(t, "serverless", cluster.EngineMode)
}

func TestBackend_CreateDBCluster_IAMAuth(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	cluster, err := b.CreateDBCluster(context.Background(), "iam-unit", "", 0, neptune.DBClusterCreateOptions{
		EnableIAMDatabaseAuthentication: true,
	})
	require.NoError(t, err)
	assert.True(t, cluster.EnableIAMDatabaseAuthentication)
}

func TestBackend_CreateDBCluster_ManageMasterUserPassword(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	cluster, err := b.CreateDBCluster(context.Background(), "mup-unit", "", 0, neptune.DBClusterCreateOptions{
		ManageMasterUserPassword: true,
	})
	require.NoError(t, err)
	require.NotNil(t, cluster.MasterUserManagedSecret)
	assert.NotEmpty(t, cluster.MasterUserManagedSecret.SecretARN)
	assert.Equal(t, "active", cluster.MasterUserManagedSecret.SecretStatus)
}

func TestBackend_ModifyDBCluster_IamAuth_SetAndUnset(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(context.Background(), "iam-mod-unit", "", 0, neptune.DBClusterCreateOptions{
		EnableIAMDatabaseAuthentication: true,
	})
	require.NoError(t, err)

	// Verify enabled
	clusters, err := b.DescribeDBClusters(context.Background(), "iam-mod-unit", neptune.DBClusterFilters{})
	require.NoError(t, err)
	assert.True(t, clusters[0].EnableIAMDatabaseAuthentication)

	// Disable via modify
	_, err = b.ModifyDBCluster(context.Background(), "iam-mod-unit", "", neptune.DBClusterModifyOptions{
		EnableIAMDatabaseAuthentication: false,
		IamAuthSet:                      true,
	})
	require.NoError(t, err)
	clusters, err = b.DescribeDBClusters(context.Background(), "iam-mod-unit", neptune.DBClusterFilters{})
	require.NoError(t, err)
	assert.False(t, clusters[0].EnableIAMDatabaseAuthentication)
}

func TestBackend_ModifyDBCluster_IamAuth_NotSet_NoChange(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(context.Background(), "iam-nochange", "", 0, neptune.DBClusterCreateOptions{
		EnableIAMDatabaseAuthentication: true,
	})
	require.NoError(t, err)

	// Modify without IamAuthSet - should not change IAM auth
	_, err = b.ModifyDBCluster(context.Background(), "iam-nochange", "", neptune.DBClusterModifyOptions{
		EnableIAMDatabaseAuthentication: false,
		IamAuthSet:                      false,
	})
	require.NoError(t, err)
	clusters, err := b.DescribeDBClusters(context.Background(), "iam-nochange", neptune.DBClusterFilters{})
	require.NoError(t, err)
	assert.True(t, clusters[0].EnableIAMDatabaseAuthentication)
}

func TestBackend_ModifyDBCluster_ServerlessV2(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(context.Background(), "sv2-modify-unit", "", 0, neptune.DBClusterCreateOptions{})
	require.NoError(t, err)

	sv2 := &neptune.ServerlessV2ScalingConfiguration{MinCapacity: 4.0, MaxCapacity: 32.0}
	cluster, err := b.ModifyDBCluster(context.Background(), "sv2-modify-unit", "", neptune.DBClusterModifyOptions{
		ServerlessV2ScalingConfig: sv2,
	})
	require.NoError(t, err)
	require.NotNil(t, cluster.ServerlessV2ScalingConfig)
	assert.InEpsilon(t, 4.0, cluster.ServerlessV2ScalingConfig.MinCapacity, 0.001)
	assert.InEpsilon(t, 32.0, cluster.ServerlessV2ScalingConfig.MaxCapacity, 0.001)
}

func TestBackend_ModifyDBCluster_DeletionProtection(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(context.Background(), "dp-unit", "", 0, neptune.DBClusterCreateOptions{})
	require.NoError(t, err)

	cluster, err := b.ModifyDBCluster(context.Background(), "dp-unit", "", neptune.DBClusterModifyOptions{
		DeletionProtection:    true,
		DeletionProtectionSet: true,
	})
	require.NoError(t, err)
	assert.True(t, cluster.DeletionProtection)

	cluster, err = b.ModifyDBCluster(context.Background(), "dp-unit", "", neptune.DBClusterModifyOptions{
		DeletionProtection:    false,
		DeletionProtectionSet: true,
	})
	require.NoError(t, err)
	assert.False(t, cluster.DeletionProtection)
}

func TestBackend_ModifyDBCluster_DeletionProtection_NotSet_NoChange(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(context.Background(), "dp-nochange", "", 0, neptune.DBClusterCreateOptions{
		DeletionProtection: true,
	})
	require.NoError(t, err)

	// No DeletionProtectionSet - should not change
	cluster, err := b.ModifyDBCluster(context.Background(), "dp-nochange", "", neptune.DBClusterModifyOptions{
		DeletionProtection:    false,
		DeletionProtectionSet: false,
	})
	require.NoError(t, err)
	assert.True(t, cluster.DeletionProtection)
}

func TestBackend_CreateDBCluster_DefaultEngineMode(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	cluster, err := b.CreateDBCluster(context.Background(), "default-mode", "", 0, neptune.DBClusterCreateOptions{})
	require.NoError(t, err)
	assert.Equal(t, "provisioned", cluster.EngineMode)
}

func TestBackend_CloneCluster_ServerlessV2_NilSafe(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	cluster, err := b.CreateDBCluster(context.Background(), "no-sv2", "", 0, neptune.DBClusterCreateOptions{})
	require.NoError(t, err)
	assert.Nil(t, cluster.ServerlessV2ScalingConfig)
	assert.Nil(t, cluster.MasterUserManagedSecret)
}

func TestBackend_ModifyDBCluster_ManageMasterUserPassword_Idempotent(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(context.Background(), "mup-idem", "", 0, neptune.DBClusterCreateOptions{
		ManageMasterUserPassword: true,
	})
	require.NoError(t, err)

	// Enable again - should not create a second secret
	cluster, err := b.ModifyDBCluster(context.Background(), "mup-idem", "", neptune.DBClusterModifyOptions{
		ManageMasterUserPassword: true,
	})
	require.NoError(t, err)
	require.NotNil(t, cluster.MasterUserManagedSecret)
}

// --- Persistence roundtrip with new fields ---

func TestPersistence_ServerlessV2(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	sv2 := &neptune.ServerlessV2ScalingConfiguration{MinCapacity: 2.0, MaxCapacity: 16.0}
	_, err := b.CreateDBCluster(context.Background(), "sv2-persist", "", 0, neptune.DBClusterCreateOptions{
		ServerlessV2ScalingConfig:       sv2,
		EngineMode:                      "serverless",
		EnableIAMDatabaseAuthentication: true,
		ManageMasterUserPassword:        true,
		DeletionProtection:              true,
	})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	err = b2.Restore(t.Context(), snap)
	require.NoError(t, err)

	clusters, err := b2.DescribeDBClusters(context.Background(), "sv2-persist", neptune.DBClusterFilters{})
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	c := clusters[0]
	require.NotNil(t, c.ServerlessV2ScalingConfig)
	assert.InEpsilon(t, 2.0, c.ServerlessV2ScalingConfig.MinCapacity, 0.001)
	assert.InEpsilon(t, 16.0, c.ServerlessV2ScalingConfig.MaxCapacity, 0.001)
	assert.Equal(t, "serverless", c.EngineMode)
	assert.True(t, c.EnableIAMDatabaseAuthentication)
	assert.True(t, c.DeletionProtection)
	require.NotNil(t, c.MasterUserManagedSecret)
	assert.Equal(t, "active", c.MasterUserManagedSecret.SecretStatus)
}

// --- DeleteDBCluster: DeletionProtection ---
