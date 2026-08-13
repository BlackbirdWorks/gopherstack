package docdb_test

import (
	"context"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func TestHandler_CreateDescribeDeleteDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
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
				"Engine":              {"docdb"},
				"MasterUsername":      {"admin"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "test-cluster",
		},
		{
			name:   "describe_clusters_all",
			action: "DescribeDBClusters",
			vals: url.Values{
				"Action":  {"DescribeDBClusters"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBClustersResponse",
		},
		{
			name:   "describe_cluster_by_id",
			action: "DescribeDBClusters",
			vals: url.Values{
				"Action":              {"DescribeDBClusters"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "test-cluster",
		},
		{
			name:   "delete_cluster",
			action: "DeleteDBCluster",
			vals: url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
				"SkipFinalSnapshot":   {"true"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			localH := newTestHandler(t)
			if tt.action != "CreateDBCluster" {
				createVals := url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"test-cluster"},
					"Engine":              {"docdb"},
				}
				doRequest(t, localH, createVals)
			}
			rr := doRequest(t, localH, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_ClusterOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "modify_cluster",
			vals: url.Values{
				"Action":                      {"ModifyDBCluster"},
				"Version":                     {"2014-10-31"},
				"DBClusterIdentifier":         {"my-cluster"},
				"DBClusterParameterGroupName": {"new-param-group"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "ModifyDBClusterResponse",
		},
		{
			name: "stop_cluster",
			vals: url.Values{
				"Action":              {"StopDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "stopped",
		},
		{
			name: "start_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"StopDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":              {"StartDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "available",
		},
		{
			name: "failover_cluster",
			vals: url.Values{
				"Action":              {"FailoverDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "FailoverDBClusterResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			})

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_XMLResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"xml-test"},
		"Engine":              {"docdb"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	body, err := io.ReadAll(rr.Body)
	require.NoError(t, err)

	var resp struct {
		XMLName xml.Name `xml:"CreateDBClusterResponse"`
	}
	err = xml.Unmarshal(body[len("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"):], &resp)
	require.NoError(t, err)
	assert.Equal(t, "CreateDBClusterResponse", resp.XMLName.Local)
}

func TestSortedDescribeClusters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ids  []string
		want []string
	}{
		{
			name: "sorted_order",
			ids:  []string{"c-beta", "c-alpha", "c-gamma"},
			want: []string{"c-alpha", "c-beta", "c-gamma"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			for _, id := range tt.ids {
				b.AddDBClusterInternal(&docdb.DBCluster{DBClusterIdentifier: id})
			}

			got, err := b.DescribeDBClusters(context.Background(), "")
			require.NoError(t, err)

			gotIDs := make([]string, len(got))
			for i, c := range got {
				gotIDs[i] = c.DBClusterIdentifier
			}

			assert.Equal(t, tt.want, gotIDs)
		})
	}
}

func TestClusterARNInResponse(t *testing.T) {
	t.Parallel()

	type wantFields struct {
		arnPrefix string
		engine    string
	}

	tests := []struct {
		name  string
		id    string
		wantF wantFields
	}{
		{
			name:  "arn_present",
			id:    "my-cluster",
			wantF: wantFields{arnPrefix: "arn:aws:rds:", engine: "docdb"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              []string{"CreateDBCluster"},
				"Version":             []string{"2014-10-31"},
				"DBClusterIdentifier": []string{tt.id},
			}
			resp := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, resp.Code)

			body := resp.Body.String()
			assert.Contains(t, body, "DBClusterArn")
			assert.Contains(t, body, tt.wantF.arnPrefix)
		})
	}
}

func TestTagsOnCreate_Cluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      string
		tagKey  string
		tagVal  string
		wantLen int
	}{
		{
			name:    "tags_stored",
			id:      "tagged-cluster",
			tagKey:  "env",
			tagVal:  "test",
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              []string{"CreateDBCluster"},
				"Version":             []string{"2014-10-31"},
				"DBClusterIdentifier": []string{tt.id},
				"Tags.Tag.1.Key":      []string{tt.tagKey},
				"Tags.Tag.1.Value":    []string{tt.tagVal},
			}
			resp := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, resp.Code)

			clusters, err := h.Backend.DescribeDBClusters(context.Background(), tt.id)
			require.NoError(t, err)
			require.Len(t, clusters, 1)

			assert.Len(t, clusters[0].Tags, tt.wantLen)
			assert.Equal(t, tt.tagVal, clusters[0].Tags[tt.tagKey])
		})
	}
}

func TestDeleteCluster_RequiresId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		id      string
		wantErr bool
	}{
		{name: "empty_id_returns_error", id: "", wantErr: true},
		{name: "missing_cluster_returns_error", id: "nonexistent", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.DeleteDBCluster(context.Background(), tt.id, nil)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestEngineVersionInResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		id            string
		wantEngineVer string
	}{
		{
			name:          "engine_version_present",
			id:            "engine-ver-cluster",
			wantEngineVer: "4.0.0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":              []string{"CreateDBCluster"},
				"Version":             []string{"2014-10-31"},
				"DBClusterIdentifier": []string{tt.id},
			}
			resp := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, resp.Code)

			body := resp.Body.String()
			assert.Contains(t, body, "EngineVersion")
			assert.Contains(t, body, tt.wantEngineVer)
		})
	}
}

func TestClusterResponseFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
	}{
		{
			name: "all_expected_fields_present",
			wantContains: []string{
				"DBClusterArn",
				"DBClusterIdentifier",
				"Engine",
				"Status",
				"Endpoint",
				"ReaderEndpoint",
				"Port",
				"EngineVersion",
				"VpcSecurityGroups",
				"DBClusterMembers",
				"EnabledCloudwatchLogsExports",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"field-check-cluster"},
				"Engine":              {"docdb"},
				"MasterUsername":      {"admin"},
			})
			require.Equal(t, 200, rr.Code)
			body := rr.Body.String()
			for _, field := range tt.wantContains {
				assert.Contains(t, body, field, "field %q missing from response", field)
			}
		})
	}
}

func TestClusterInheritedDefaults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		wantEngine          string
		wantEngineVersion   string
		wantPort            int
		wantBackupRetention int
	}{
		{
			name:                "defaults_applied",
			wantEngine:          "docdb",
			wantEngineVersion:   "4.0.0",
			wantPort:            27017,
			wantBackupRetention: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			cluster, err := b.CreateDBCluster(
				context.Background(),
				"defaults-cluster", "", "", "admin", "", "", "", "",
				0, false, false, 0, "", "", nil, nil, nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantEngine, cluster.Engine)
			assert.Equal(t, tt.wantEngineVersion, cluster.EngineVersion)
			assert.Equal(t, tt.wantPort, cluster.Port)
			assert.Equal(t, tt.wantBackupRetention, cluster.BackupRetentionPeriod)
		})
	}
}

// TestParity_DefaultParamGroupName verifies engine-version-specific default param group names.
func TestDefaultParamGroupName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engineVersion string
		wantPGName    string
	}{
		{"v3.6", "3.6.0", "default.docdb3.6"},
		{"v4.0", "4.0.0", "default.docdb4.0"},
		{"v5.0", "5.0.0", "default.docdb5.0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"pg-test-" + tc.name},
				"Engine":              {"docdb"},
				"EngineVersion":       {tc.engineVersion},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			assert.Contains(t, body, "<DBClusterParameterGroup>"+tc.wantPGName+"</DBClusterParameterGroup>",
				"engine version %s should use param group %s", tc.engineVersion, tc.wantPGName)
		})
	}
}

// TestParity_ModifyDBCluster_NewIdentifier verifies cluster rename via NewDBClusterIdentifier.
func TestModifyDBCluster_NewIdentifier(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	pbCreateCluster(t, h, "rename-src")

	rr := doRequest(t, h, url.Values{
		"Action":                 {"ModifyDBCluster"},
		"Version":                {"2014-10-31"},
		"DBClusterIdentifier":    {"rename-src"},
		"NewDBClusterIdentifier": {"rename-dst"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Old ID should now 404.
	rr2 := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"rename-src"},
	})
	assert.Equal(t, http.StatusBadRequest, rr2.Code)

	// New ID should exist.
	rr3 := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"rename-dst"},
	})
	assert.Equal(t, http.StatusOK, rr3.Code)
	assert.Contains(t, rr3.Body.String(), "rename-dst")
}

// TestParity_ClusterAvailabilityZones verifies AZs appear in CreateDBCluster response.
func TestClusterAvailabilityZones(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	rr := doRequest(t, h, url.Values{
		"Action":                               {"CreateDBCluster"},
		"Version":                              {"2014-10-31"},
		"DBClusterIdentifier":                  {"az-cluster"},
		"Engine":                               {"docdb"},
		"AvailabilityZones.AvailabilityZone.1": {"us-east-1a"},
		"AvailabilityZones.AvailabilityZone.2": {"us-east-1b"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "us-east-1a")
	assert.Contains(t, body, "us-east-1b")
}
