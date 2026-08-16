package neptune_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func TestHandler_CreateDBClusterEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*neptune.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_endpoint_success",
			setup: func(h *neptune.Handler) {
				createCluster(t, h, "ep-cluster")
			},
			vals: url.Values{
				"Action":                      {"CreateDBClusterEndpoint"},
				"Version":                     {"2014-10-31"},
				"DBClusterEndpointIdentifier": {"my-endpoint"},
				"DBClusterIdentifier":         {"ep-cluster"},
				"EndpointType":                {"READER"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-endpoint",
		},
		{
			name: "create_endpoint_cluster_not_found",
			vals: url.Values{
				"Action":                      {"CreateDBClusterEndpoint"},
				"Version":                     {"2014-10-31"},
				"DBClusterEndpointIdentifier": {"ep2"},
				"DBClusterIdentifier":         {"no-such-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterNotFoundFault",
		},
		{
			name: "create_endpoint_missing_id",
			setup: func(h *neptune.Handler) {
				createCluster(t, h, "ep-cluster3")
			},
			vals: url.Values{
				"Action":              {"CreateDBClusterEndpoint"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"ep-cluster3"},
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

// ---- ModifyDBClusterEndpoint comprehensive coverage ----

func TestModifyDBClusterEndpoint_AllTypes(t *testing.T) {
	t.Parallel()

	for _, epType := range []string{"READER", "ANY", "CUSTOM"} {
		t.Run(epType, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createCluster(t, h, "ep-type-cluster-"+epType)

			doRequest(t, h, url.Values{
				"Action":                      {"CreateDBClusterEndpoint"},
				"Version":                     {"2014-10-31"},
				"DBClusterEndpointIdentifier": {"ep-" + epType},
				"DBClusterIdentifier":         {"ep-type-cluster-" + epType},
				"EndpointType":                {"READER"},
			})

			rr := doRequest(t, h, url.Values{
				"Action":                      {"ModifyDBClusterEndpoint"},
				"Version":                     {"2014-10-31"},
				"DBClusterEndpointIdentifier": {"ep-" + epType},
				"EndpointType":                {epType},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			assert.Contains(t, rr.Body.String(), epType)
		})
	}
}

func TestModifyDBClusterEndpoint_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                      {"ModifyDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"nonexistent-ep"},
		"EndpointType":                {"READER"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterEndpointNotFoundFault")
}

func TestDBClusterEndpoint_FilterByCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-filter-a")
	createCluster(t, h, "ep-filter-b")

	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-a-1"},
		"DBClusterIdentifier":         {"ep-filter-a"},
		"EndpointType":                {"READER"},
	})
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-b-1"},
		"DBClusterIdentifier":         {"ep-filter-b"},
		"EndpointType":                {"READER"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusterEndpoints"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"ep-filter-a"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "ep-a-1")
	assert.NotContains(t, body, "ep-b-1")
}

func TestDBClusterEndpoint_InvalidType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-invalid-type-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-bad"},
		"DBClusterIdentifier":         {"ep-invalid-type-cluster"},
		"EndpointType":                {"INVALID"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

func TestDBClusterEndpoint_EndpointURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-url-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-url-test"},
		"DBClusterIdentifier":         {"ep-url-cluster"},
		"EndpointType":                {"READER"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "ep-url-test")
	assert.Contains(t, body, "cluster-custom.neptune")
}

// TestDeleteDBCluster_CascadesClearEndpoints verifies endpoints are removed when cluster is deleted.
func TestDeleteDBCluster_CascadesClearEndpoints(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	hb := neptune.NewHandler(backend)

	backend.AddClusterInternal("ep-cluster")

	doRequest(t, hb, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-1"},
		"DBClusterIdentifier":         {"ep-cluster"},
		"EndpointType":                {"READER"},
	})
	require.Equal(t, 1, neptune.ClusterEndpointCount(backend))

	doRequest(t, hb, url.Values{
		"Action":              {"DeleteDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"ep-cluster"},
		"SkipFinalSnapshot":   {"true"},
	})

	assert.Equal(t, 0, neptune.ClusterEndpointCount(backend))
}

// TestEndpointType_Validation verifies valid/invalid EndpointType.
func TestEndpointType_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		endpointType string
		wantStatus   int
	}{
		{name: "reader_valid", endpointType: "READER", wantStatus: http.StatusOK},
		{name: "writer_valid", endpointType: "WRITER", wantStatus: http.StatusOK},
		{name: "custom_valid", endpointType: "CUSTOM", wantStatus: http.StatusOK},
		{name: "any_valid", endpointType: "ANY", wantStatus: http.StatusOK},
		{name: "empty_defaults_reader", endpointType: "", wantStatus: http.StatusOK},
		{name: "invalid_type", endpointType: "INVALID", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createCluster(t, h, "ep-type-cluster")

			epID := tt.name + "-ep"
			vals := url.Values{
				"Action":                      {"CreateDBClusterEndpoint"},
				"Version":                     {"2014-10-31"},
				"DBClusterEndpointIdentifier": {epID},
				"DBClusterIdentifier":         {"ep-type-cluster"},
			}
			if tt.endpointType != "" {
				vals.Set("EndpointType", tt.endpointType)
			}

			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

// TestCreateDBClusterEndpoint_MissingCluster verifies error on missing cluster.
func TestCreateDBClusterEndpoint_MissingCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-missing"},
		"DBClusterIdentifier":         {"nonexistent"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
}

// TestClusterEndpointAlreadyExists verifies duplicate endpoint error.
func TestClusterEndpointAlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-dup-cluster")

	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"dup-ep"},
		"DBClusterIdentifier":         {"ep-dup-cluster"},
		"EndpointType":                {"READER"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"dup-ep"},
		"DBClusterIdentifier":         {"ep-dup-cluster"},
		"EndpointType":                {"READER"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterEndpointAlreadyExistsFault")
}

// --- Cluster endpoint lifecycle ---

func TestClusterEndpoint_DescribeModifyDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-lifecycle-cluster")

	// create
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-lifecycle"},
		"DBClusterIdentifier":         {"ep-lifecycle-cluster"},
		"EndpointType":                {"READER"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "ep-lifecycle")

	// describe by endpoint ID
	rr = doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterEndpoints"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-lifecycle"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "ep-lifecycle")
	assert.Contains(t, rr.Body.String(), "READER")

	// describe by cluster ID
	rr = doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusterEndpoints"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"ep-lifecycle-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "ep-lifecycle")

	// describe all (no filter)
	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeDBClusterEndpoints"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "ep-lifecycle")

	// modify
	rr = doRequest(t, h, url.Values{
		"Action":                      {"ModifyDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-lifecycle"},
		"EndpointType":                {"WRITER"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "ep-lifecycle")

	// delete
	rr = doRequest(t, h, url.Values{
		"Action":                      {"DeleteDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-lifecycle"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	// delete again must fail
	rr = doRequest(t, h, url.Values{
		"Action":                      {"DeleteDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-lifecycle"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterEndpointNotFoundFault")
}

func TestClusterEndpoint_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vals         url.Values
		wantContains string
	}{
		{
			name: "modify_not_found",
			vals: url.Values{
				"Action":                      {"ModifyDBClusterEndpoint"},
				"Version":                     {"2014-10-31"},
				"DBClusterEndpointIdentifier": {"no-such"},
				"EndpointType":                {"READER"},
			},
			wantContains: "DBClusterEndpointNotFoundFault",
		},
		{
			name: "delete_not_found",
			vals: url.Values{
				"Action":                      {"DeleteDBClusterEndpoint"},
				"Version":                     {"2014-10-31"},
				"DBClusterEndpointIdentifier": {"no-such"},
			},
			wantContains: "DBClusterEndpointNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// --- Cluster Endpoint operations ---

// TestCreateDescribeDeleteDBClusterEndpoint tests CRUD for cluster endpoints.
func TestCreateDescribeDeleteDBClusterEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-cluster")

	// Create endpoint
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-01"},
		"DBClusterIdentifier":         {"ep-cluster"},
		"EndpointType":                {"READER"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "ep-01")

	// Describe
	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeDBClusterEndpoints"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "ep-01")

	// Modify endpoint
	rr = doRequest(t, h, url.Values{
		"Action":                      {"ModifyDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-01"},
		"EndpointType":                {"ANY"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Delete endpoint
	rr = doRequest(t, h, url.Values{
		"Action":                      {"DeleteDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-01"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// TestModifyDBClusterEndpoint_StaticAndExcludedMembersPersist locks the core
// fix: ModifyDBClusterEndpoint used to silently ignore
// StaticMembers.member.N/ExcludedMembers.member.N even though the real API
// accepts and applies them -- only EndpointType was ever mutated.
func TestModifyDBClusterEndpoint_StaticAndExcludedMembersPersist(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-members-cluster")
	createInstance(t, h, "ep-members-inst-1", "ep-members-cluster")
	createInstance(t, h, "ep-members-inst-2", "ep-members-cluster")
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-members"},
		"DBClusterIdentifier":         {"ep-members-cluster"},
		"EndpointType":                {"CUSTOM"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"ModifyDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-members"},
		"StaticMembers.member.1":      {"ep-members-inst-1"},
		"ExcludedMembers.member.1":    {"ep-members-inst-2"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "ep-members-inst-1")
	assert.Contains(t, body, "ep-members-inst-2")

	rr = doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterEndpoints"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-members"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body = rr.Body.String()
	assert.Contains(t, body, "ep-members-inst-1")
	assert.Contains(t, body, "ep-members-inst-2")
}
