package docdb_test

import (
	"encoding/xml"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func newParityBHandler(t *testing.T) *docdb.Handler {
	t.Helper()
	backend := docdb.NewInMemoryBackend("000000000000", "us-east-1")

	return docdb.NewHandler(backend)
}

func pbCreateCluster(t *testing.T, h *docdb.Handler, clusterID string, extraVals url.Values) {
	t.Helper()
	vals := url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {clusterID},
		"Engine":              {"docdb"},
	}
	maps.Copy(vals, extraVals)
	rr := doRequest(t, h, vals)
	require.Equal(t, http.StatusOK, rr.Code, "create cluster %s: %s", clusterID, rr.Body.String())
}

func pbCreateInstance(t *testing.T, h *docdb.Handler, instanceID, clusterID string) {
	t.Helper()
	rr := doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {instanceID},
		"DBClusterIdentifier":  {clusterID},
		"DBInstanceClass":      {"db.t3.medium"},
		"Engine":               {"docdb"},
	})
	require.Equal(t, http.StatusOK, rr.Code, "create instance %s: %s", instanceID, rr.Body.String())
}

func pbExtractErrorCode(t *testing.T, body string) string {
	t.Helper()
	var errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body), &errResp))

	return errResp.Error.Code
}

// TestParity_InstanceErrorCodes verifies that instance not-found and already-exists
// errors carry the AWS-accurate "Fault" suffix.
func TestParity_InstanceErrorCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		vals     url.Values
		wantCode string
	}{
		{
			name: "instance_not_found",
			vals: url.Values{
				"Action":               {"DeleteDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"nonexistent-inst"},
			},
			wantCode: "DBInstanceNotFound",
		},
		{
			name: "instance_already_exists",
			vals: url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"dup-inst"},
				"DBClusterIdentifier":  {"some-cluster"},
				"DBInstanceClass":      {"db.t3.medium"},
				"Engine":               {"docdb"},
			},
			wantCode: "DBClusterNotFoundFault",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newParityBHandler(t)
			rr := doRequest(t, h, tc.vals)
			assert.NotEqual(t, http.StatusOK, rr.Code)
			code := pbExtractErrorCode(t, rr.Body.String())
			assert.Equal(t, tc.wantCode, code)
		})
	}
}

// TestParity_InstanceAlreadyExists verifies DBInstanceAlreadyExists.
func TestParity_InstanceAlreadyExists(t *testing.T) {
	t.Parallel()
	h := newParityBHandler(t)
	pbCreateCluster(t, h, "cluster-for-dup", nil)
	pbCreateInstance(t, h, "dup-inst", "cluster-for-dup")
	// Second create same ID should return AlreadyExists with Fault suffix.
	rr := doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"dup-inst"},
		"DBClusterIdentifier":  {"cluster-for-dup"},
		"DBInstanceClass":      {"db.t3.medium"},
		"Engine":               {"docdb"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Equal(t, "DBInstanceAlreadyExists", pbExtractErrorCode(t, rr.Body.String()))
}

// TestParity_SubnetGroupStatus verifies subnet group status is lowercase "complete".
func TestParity_SubnetGroupStatus(t *testing.T) {
	t.Parallel()
	h := newParityBHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                       {"CreateDBSubnetGroup"},
		"Version":                      {"2014-10-31"},
		"DBSubnetGroupName":            {"parity-sg"},
		"DBSubnetGroupDescription":     {"parity test"},
		"SubnetIds.SubnetIdentifier.1": {"subnet-aabbccdd"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "<SubnetGroupStatus>complete</SubnetGroupStatus>",
		"status must be lowercase 'complete', not 'Complete'")
}

// TestParity_DefaultParamGroupName verifies engine-version-specific default param group names.
func TestParity_DefaultParamGroupName(t *testing.T) {
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
			h := newParityBHandler(t)
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

// TestParity_DeleteDBCluster_SkipFinalSnapshot verifies SkipFinalSnapshot validation.
func TestParity_DeleteDBCluster_SkipFinalSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extraVals  url.Values
		name       string
		wantCode   string
		wantStatus int
	}{
		{
			name:       "missing_skip_and_identifier_rejected",
			extraVals:  url.Values{},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameterValue",
		},
		{
			name:       "skip_final_snapshot_true_ok",
			extraVals:  url.Values{"SkipFinalSnapshot": {"true"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "final_snapshot_identifier_ok",
			extraVals: url.Values{
				"FinalDBClusterSnapshotIdentifier": {"my-final-snap"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newParityBHandler(t)
			pbCreateCluster(t, h, "del-cluster", nil)
			vals := url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-cluster"},
			}
			maps.Copy(vals, tc.extraVals)
			rr := doRequest(t, h, vals)
			assert.Equal(t, tc.wantStatus, rr.Code)
			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, pbExtractErrorCode(t, rr.Body.String()))
			}
		})
	}
}

// TestParity_ModifyDBCluster_NewIdentifier verifies cluster rename via NewDBClusterIdentifier.
func TestParity_ModifyDBCluster_NewIdentifier(t *testing.T) {
	t.Parallel()
	h := newParityBHandler(t)
	pbCreateCluster(t, h, "rename-src", nil)

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

// TestParity_ParameterGroupStorage verifies parameters are stored and returned.
func TestParity_ParameterGroupStorage(t *testing.T) {
	t.Parallel()
	h := newParityBHandler(t)

	// Create param group.
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"my-pg"},
		"DBParameterGroupFamily":      {"docdb4.0"},
		"Description":                 {"test pg"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Modify: set a parameter.
	rr2 := doRequest(t, h, url.Values{
		"Action":                                {"ModifyDBClusterParameterGroup"},
		"Version":                               {"2014-10-31"},
		"DBClusterParameterGroupName":           {"my-pg"},
		"Parameters.Parameter.1.ParameterName":  {"tls"},
		"Parameters.Parameter.1.ParameterValue": {"disabled"},
		"Parameters.Parameter.1.ApplyMethod":    {"immediate"},
	})
	require.Equal(t, http.StatusOK, rr2.Code)

	// Describe params: should reflect user value.
	rr3 := doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameters"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"my-pg"},
	})
	require.Equal(t, http.StatusOK, rr3.Code)
	body := rr3.Body.String()
	assert.Contains(t, body, "<ParameterName>tls</ParameterName>")
	assert.Contains(t, body, "<ParameterValue>disabled</ParameterValue>")
	assert.Contains(t, body, "<Source>user</Source>")
}

// TestParity_ClusterAvailabilityZones verifies AZs appear in CreateDBCluster response.
func TestParity_ClusterAvailabilityZones(t *testing.T) {
	t.Parallel()
	h := newParityBHandler(t)

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

// TestParity_SubnetAvailabilityZone verifies SubnetStatus and SubnetAvailabilityZone in response.
func TestParity_SubnetAvailabilityZone(t *testing.T) {
	t.Parallel()
	h := newParityBHandler(t)

	rr := doRequest(t, h, url.Values{
		"Action":                       {"CreateDBSubnetGroup"},
		"Version":                      {"2014-10-31"},
		"DBSubnetGroupName":            {"az-sg"},
		"DBSubnetGroupDescription":     {"parity test"},
		"SubnetIds.SubnetIdentifier.1": {"subnet-aabb1122"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "<SubnetStatus>Active</SubnetStatus>")
}

// TestParity_EngineVersions_36 verifies 3.6.0 appears in DescribeDBEngineVersions.
func TestParity_EngineVersions_36(t *testing.T) {
	t.Parallel()
	h := newParityBHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeDBEngineVersions"},
		"Version": {"2014-10-31"},
		"Engine":  {"docdb"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "3.6.0", "DescribeDBEngineVersions must include version 3.6.0")
}

// TestParity_ParameterGroupPagination verifies Marker pagination on DescribeDBClusterParameterGroups.
func TestParity_ParameterGroupPagination(t *testing.T) {
	t.Parallel()
	h := newParityBHandler(t)

	// Create 3 param groups.
	for i := range 3 {
		rr := doRequest(t, h, url.Values{
			"Action":                      {"CreateDBClusterParameterGroup"},
			"Version":                     {"2014-10-31"},
			"DBClusterParameterGroupName": {strings.NewReplacer("{{i}}", string(rune('a'+i))).Replace("pg-{{i}}")},
			"DBParameterGroupFamily":      {"docdb4.0"},
			"Description":                 {"pg test"},
		})
		require.Equal(t, http.StatusOK, rr.Code)
	}

	// Fetch first page of 2.
	rr1 := doRequest(t, h, url.Values{
		"Action":     {"DescribeDBClusterParameterGroups"},
		"Version":    {"2014-10-31"},
		"MaxRecords": {"2"},
	})
	require.Equal(t, http.StatusOK, rr1.Code)
	body1 := rr1.Body.String()

	var page1 struct {
		XMLName xml.Name `xml:"DescribeDBClusterParameterGroupsResponse"`
		Result  struct {
			Marker string `xml:"Marker"`
		} `xml:"DescribeDBClusterParameterGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body1), &page1))
	assert.NotEmpty(t, page1.Result.Marker, "Marker must be set when more pages exist")

	// Fetch next page with marker.
	rr2 := doRequest(t, h, url.Values{
		"Action":     {"DescribeDBClusterParameterGroups"},
		"Version":    {"2014-10-31"},
		"MaxRecords": {"2"},
		"Marker":     {page1.Result.Marker},
	})
	require.Equal(t, http.StatusOK, rr2.Code)
	body2 := rr2.Body.String()
	assert.Contains(t, body2, "<DBClusterParameterGroupName>")

	var page2 struct {
		XMLName xml.Name `xml:"DescribeDBClusterParameterGroupsResponse"`
		Result  struct {
			Marker string `xml:"Marker"`
		} `xml:"DescribeDBClusterParameterGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal([]byte(body2), &page2))
	assert.Empty(t, page2.Result.Marker, "Marker must be empty on last page")
}
