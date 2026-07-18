package docdb_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateClusterNewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_cluster_with_deletion_protection",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"protected-cluster"},
				"DeletionProtection":  {"true"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "protected-cluster",
		},
		{
			name: "create_cluster_with_backup_retention",
			vals: url.Values{
				"Action":                {"CreateDBCluster"},
				"Version":               {"2014-10-31"},
				"DBClusterIdentifier":   {"backup-cluster"},
				"BackupRetentionPeriod": {"7"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "backup-cluster",
		},
		{
			name: "create_cluster_with_storage_encrypted",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"encrypted-cluster"},
				"StorageEncrypted":    {"true"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "encrypted-cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestCreateCluster_VpcSecurityGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		vpcSGIDs     []string
		wantStatus   int
	}{
		{
			name:         "single_vpc_sg",
			vpcSGIDs:     []string{"sg-aaaa1111"},
			wantContains: "sg-aaaa1111",
			wantStatus:   200,
		},
		{
			name:         "multiple_vpc_sgs",
			vpcSGIDs:     []string{"sg-aaaa1111", "sg-bbbb2222"},
			wantContains: "sg-aaaa1111",
			wantStatus:   200,
		},
		{
			name:         "no_vpc_sgs",
			vpcSGIDs:     nil,
			wantContains: "CreateDBClusterResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"vpc-test-cluster"},
			}
			for i, sgID := range tt.vpcSGIDs {
				vals.Set(fmt.Sprintf("VpcSecurityGroupIds.VpcSecurityGroupId.%d", i+1), sgID)
			}

			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestCreateCluster_IAMDatabaseAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		paramVal     string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "iam_auth_enabled",
			paramVal:     "true",
			wantContains: "IAMDatabaseAuthenticationEnabled",
			wantStatus:   200,
		},
		{
			name:         "iam_auth_disabled",
			paramVal:     "false",
			wantContains: "CreateDBClusterResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":                          {"CreateDBCluster"},
				"Version":                         {"2014-10-31"},
				"DBClusterIdentifier":             {"iam-auth-cluster"},
				"EnableIAMDatabaseAuthentication": {tt.paramVal},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestCreateCluster_KmsKeyId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		kmsKeyID     string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "kms_key_set",
			kmsKeyID:     "arn:aws:kms:us-east-1:000000000000:key/test-key-id",
			wantContains: "test-key-id",
			wantStatus:   200,
		},
		{
			name:         "no_kms_key",
			kmsKeyID:     "",
			wantContains: "CreateDBClusterResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"kms-cluster"},
			}
			if tt.kmsKeyID != "" {
				vals.Set("KmsKeyId", tt.kmsKeyID)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestCreateCluster_CloudwatchLogsExports(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		logTypes     []string
		wantStatus   int
	}{
		{
			name:         "profiler_log_enabled",
			logTypes:     []string{"profiler"},
			wantContains: "profiler",
			wantStatus:   200,
		},
		{
			name:         "multiple_logs",
			logTypes:     []string{"profiler", "audit"},
			wantContains: "profiler",
			wantStatus:   200,
		},
		{
			name:         "no_logs",
			logTypes:     nil,
			wantContains: "CreateDBClusterResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"logs-cluster"},
			}
			for i, lt := range tt.logTypes {
				vals.Set(fmt.Sprintf("EnableCloudwatchLogsExports.member.%d", i+1), lt)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestModifyCluster_EngineVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engineVersion string
		wantContains  string
		wantStatus    int
	}{
		{
			name:          "upgrade_to_5_0",
			engineVersion: "5.0.0",
			wantContains:  "5.0.0",
			wantStatus:    200,
		},
		{
			name:          "no_version_change",
			engineVersion: "",
			wantContains:  "ModifyDBClusterResponse",
			wantStatus:    200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-ev-cluster"},
			})
			vals := url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-ev-cluster"},
			}
			if tt.engineVersion != "" {
				vals.Set("EngineVersion", tt.engineVersion)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestModifyCluster_CloudwatchLogs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		enableLogs   []string
		disableLogs  []string
		wantStatus   int
	}{
		{
			name:         "enable_profiler_log",
			enableLogs:   []string{"profiler"},
			wantContains: "profiler",
			wantStatus:   200,
		},
		{
			name:         "enable_then_disable",
			enableLogs:   []string{"profiler"},
			disableLogs:  []string{"profiler"},
			wantContains: "ModifyDBClusterResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-logs-cluster"},
			})
			vals := url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-logs-cluster"},
			}
			for i, lt := range tt.enableLogs {
				vals.Set(fmt.Sprintf("CloudwatchLogsExportConfiguration.EnableLogTypes.member.%d", i+1), lt)
			}
			for i, lt := range tt.disableLogs {
				vals.Set(fmt.Sprintf("CloudwatchLogsExportConfiguration.DisableLogTypes.member.%d", i+1), lt)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestModifyCluster_Port(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		port         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "change_port",
			port:         "27018",
			wantContains: "ModifyDBClusterResponse",
			wantStatus:   200,
		},
		{
			name:         "no_port_change",
			port:         "",
			wantContains: "ModifyDBClusterResponse",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-port-cluster"},
			})
			vals := url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-port-cluster"},
			}
			if tt.port != "" {
				vals.Set("Port", tt.port)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestClusterVpcSGPersistedToBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantFirst string
		sgIDs     []string
		wantLen   int
	}{
		{
			name:      "two_sgs_stored",
			sgIDs:     []string{"sg-aaa", "sg-bbb"},
			wantLen:   2,
			wantFirst: "sg-aaa",
		},
		{
			name:    "no_sgs",
			sgIDs:   nil,
			wantLen: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"sg-test-cluster"},
			}
			for i, sgID := range tt.sgIDs {
				vals.Set(fmt.Sprintf("VpcSecurityGroupIds.VpcSecurityGroupId.%d", i+1), sgID)
			}
			doRequest(t, h, vals)

			clusters, err := h.Backend.DescribeDBClusters(context.Background(), "sg-test-cluster")
			require.NoError(t, err)
			require.Len(t, clusters, 1)
			assert.Len(t, clusters[0].VpcSecurityGroupIDs, tt.wantLen)
			if tt.wantFirst != "" {
				assert.Equal(t, tt.wantFirst, clusters[0].VpcSecurityGroupIDs[0])
			}
		})
	}
}

func TestModifyCluster_VpcSecurityGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newSGIDs []string
		wantLen  int
	}{
		{
			name:     "replace_with_two_sgs",
			newSGIDs: []string{"sg-new1", "sg-new2"},
			wantLen:  2,
		},
		{
			name:     "no_sg_update",
			newSGIDs: nil,
			wantLen:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-sg-cluster"},
			})
			vals := url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"modify-sg-cluster"},
			}
			for i, sgID := range tt.newSGIDs {
				vals.Set(fmt.Sprintf("VpcSecurityGroupIds.VpcSecurityGroupId.%d", i+1), sgID)
			}
			doRequest(t, h, vals)

			clusters, err := h.Backend.DescribeDBClusters(context.Background(), "modify-sg-cluster")
			require.NoError(t, err)
			require.Len(t, clusters, 1)
			assert.Len(t, clusters[0].VpcSecurityGroupIDs, tt.wantLen)
		})
	}
}

func TestModifyCluster_CloudwatchEnableDisable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		enableLogs   []string
		disableLogs  []string
		wantLogCount int
	}{
		{
			name:         "enable_two_log_types",
			enableLogs:   []string{"profiler", "audit"},
			wantLogCount: 2,
		},
		{
			name:         "enable_then_disable_one",
			enableLogs:   []string{"profiler", "audit"},
			disableLogs:  []string{"profiler"},
			wantLogCount: 1,
		},
		{
			name:         "disable_non_enabled_is_noop",
			disableLogs:  []string{"profiler"},
			wantLogCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"cw-cluster"},
			})
			vals := url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"cw-cluster"},
			}
			for i, lt := range tt.enableLogs {
				vals.Set(fmt.Sprintf("CloudwatchLogsExportConfiguration.EnableLogTypes.member.%d", i+1), lt)
			}
			for i, lt := range tt.disableLogs {
				vals.Set(fmt.Sprintf("CloudwatchLogsExportConfiguration.DisableLogTypes.member.%d", i+1), lt)
			}
			doRequest(t, h, vals)

			clusters, err := h.Backend.DescribeDBClusters(context.Background(), "cw-cluster")
			require.NoError(t, err)
			require.Len(t, clusters, 1)
			assert.Len(t, clusters[0].EnabledCloudwatchLogsExports, tt.wantLogCount)
		})
	}
}

func TestMasterUserPassword_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		password     string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "valid_password",
			password:     "ValidPass1",
			wantContains: "CreateDBClusterResponse",
			wantStatus:   http.StatusOK,
		},
		{
			name:         "too_short",
			password:     "short",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "too_long",
			password:     strings.Repeat("a", 101),
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "contains_slash",
			password:     "password/here",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "contains_at_sign",
			password:     "password@here",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "no_password_ok",
			password:     "",
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBClusterResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"pw-cluster"},
				"Engine":              {"docdb"},
			}
			if tt.password != "" {
				vals.Set("MasterUserPassword", tt.password)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestEngineVersion_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engineVersion string
		wantContains  string
		wantStatus    int
	}{
		{
			name:          "valid_3_6",
			engineVersion: "3.6.0",
			wantStatus:    http.StatusOK,
			wantContains:  "3.6.0",
		},
		{
			name:          "valid_4_0",
			engineVersion: "4.0.0",
			wantStatus:    http.StatusOK,
			wantContains:  "4.0.0",
		},
		{
			name:          "valid_5_0",
			engineVersion: "5.0.0",
			wantStatus:    http.StatusOK,
			wantContains:  "5.0.0",
		},
		{
			name:          "invalid_version",
			engineVersion: "6.0.0",
			wantStatus:    http.StatusBadRequest,
			wantContains:  "InvalidParameterValue",
		},
		{
			name:          "empty_defaults_to_4_0",
			engineVersion: "",
			wantStatus:    http.StatusOK,
			wantContains:  "4.0.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"ev-cluster"},
				"Engine":              {"docdb"},
			}
			if tt.engineVersion != "" {
				vals.Set("EngineVersion", tt.engineVersion)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestBackupRetentionPeriod_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		retention    string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "valid_7",
			retention:    "7",
			wantStatus:   http.StatusOK,
			wantContains: "<BackupRetentionPeriod>7</BackupRetentionPeriod>",
		},
		{
			name:         "valid_35",
			retention:    "35",
			wantStatus:   http.StatusOK,
			wantContains: "<BackupRetentionPeriod>35</BackupRetentionPeriod>",
		},
		{
			name:         "too_large_36",
			retention:    "36",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "default_1",
			retention:    "",
			wantStatus:   http.StatusOK,
			wantContains: "<BackupRetentionPeriod>1</BackupRetentionPeriod>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"brp-cluster"},
				"Engine":              {"docdb"},
			}
			if tt.retention != "" {
				vals.Set("BackupRetentionPeriod", tt.retention)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}
