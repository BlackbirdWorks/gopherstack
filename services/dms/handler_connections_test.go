package dms_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTestConnection(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// Create a replication instance and endpoint via handler to get ARNs.
	riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "tc-ri",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, riRec.Code)
	riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	epRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "tc-ep",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, epRec.Code)
	epArn := parseJSON(t, epRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	rec := doDMS(t, h, "TestConnection", map[string]any{
		"ReplicationInstanceArn": riArn,
		"EndpointArn":            epArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestDeleteConnection_AfterTestConnection(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "del-conn-ri",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, riRec.Code)
	riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	epRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "del-conn-ep",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, epRec.Code)
	epArn := parseJSON(t, epRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	// TestConnection records the connection.
	testRec := doDMS(t, h, "TestConnection", map[string]any{
		"ReplicationInstanceArn": riArn,
		"EndpointArn":            epArn,
	})
	require.Equal(t, http.StatusOK, testRec.Code)

	// DeleteConnection must succeed (not 404).
	delRec := doDMS(t, h, "DeleteConnection", map[string]any{
		"ReplicationInstanceArn": riArn,
		"EndpointArn":            epArn,
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	conn := parseJSON(t, delRec)["Connection"].(map[string]any)
	assert.Equal(t, riArn, conn["ReplicationInstanceArn"])
	assert.Equal(t, epArn, conn["EndpointArn"])
	assert.Equal(t, "successful", conn["Status"])

	// A second delete must return 404.
	del2Rec := doDMS(t, h, "DeleteConnection", map[string]any{
		"ReplicationInstanceArn": riArn,
		"EndpointArn":            epArn,
	})
	require.Equal(t, http.StatusNotFound, del2Rec.Code)
}

// TestDescribeConnections_Pagination asserts DescribeConnections honors
// MaxRecords/Marker like every other Describe op in this service (real AWS
// DescribeConnectionsResponse carries a Marker field). Previously this
// handler ignored both and always returned every connection in one page.
func TestDescribeConnections_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "page-ri",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, riRec.Code)
	riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	epArns := make([]string, 0, 2)
	for _, id := range []string{"page-ep-1", "page-ep-2"} {
		epRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": id,
			"EndpointType":       "source",
			"EngineName":         "mysql",
		})
		require.Equal(t, http.StatusOK, epRec.Code)
		epArns = append(epArns, parseJSON(t, epRec)["Endpoint"].(map[string]any)["EndpointArn"].(string))
	}

	for _, epArn := range epArns {
		testRec := doDMS(t, h, "TestConnection", map[string]any{
			"ReplicationInstanceArn": riArn,
			"EndpointArn":            epArn,
		})
		require.Equal(t, http.StatusOK, testRec.Code)
	}

	// Page 1: MaxRecords=1 must return exactly one connection and a non-empty Marker.
	page1Rec := doDMS(t, h, "DescribeConnections", map[string]any{"MaxRecords": 1})
	require.Equal(t, http.StatusOK, page1Rec.Code)
	page1 := parseJSON(t, page1Rec)
	conns1, ok := page1["Connections"].([]any)
	require.True(t, ok)
	require.Len(t, conns1, 1)
	marker, ok := page1["Marker"].(string)
	require.True(t, ok, "Marker must be set when more records remain")
	assert.NotEmpty(t, marker)

	// Page 2: following Marker must return the remaining connection.
	page2Rec := doDMS(t, h, "DescribeConnections", map[string]any{"MaxRecords": 1, "Marker": marker})
	require.Equal(t, http.StatusOK, page2Rec.Code)
	page2 := parseJSON(t, page2Rec)
	conns2, ok := page2["Connections"].([]any)
	require.True(t, ok)
	require.Len(t, conns2, 1)
}

func TestHandler_TestConnectionAndDescribeConnections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "test_connection_records_result",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "conn-ri",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, riRec.Code)
				riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

				epRec := doDMS(t, h, "CreateEndpoint", map[string]any{
					"EndpointIdentifier": "conn-ep",
					"EndpointType":       "source",
					"EngineName":         "mysql",
				})
				require.Equal(t, http.StatusOK, epRec.Code)
				epArn := parseJSON(t, epRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

				testRec := doDMS(t, h, "TestConnection", map[string]any{
					"ReplicationInstanceArn": riArn,
					"EndpointArn":            epArn,
				})
				assert.Equal(t, http.StatusOK, testRec.Code)
				testResp := parseJSON(t, testRec)
				conn, ok := testResp["Connection"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "successful", conn["Status"])
				assert.Equal(t, "conn-ri", conn["ReplicationInstanceIdentifier"])
				assert.Equal(t, "conn-ep", conn["EndpointIdentifier"])

				descRec := doDMS(t, h, "DescribeConnections", map[string]any{})
				assert.Equal(t, http.StatusOK, descRec.Code)
				descResp := parseJSON(t, descRec)
				conns, ok := descResp["Connections"].([]any)
				require.True(t, ok)
				assert.Len(t, conns, 1)
				assert.Equal(t, "successful", conns[0].(map[string]any)["Status"])
			},
		},
		{
			name: "test_connection_missing_instance",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "TestConnection", map[string]any{
					"ReplicationInstanceArn": "arn:nonexistent",
					"EndpointArn":            "arn:ep",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "test_connection_missing_endpoint",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
					"ReplicationInstanceIdentifier": "conn2-ri",
					"ReplicationInstanceClass":      "dms.t3.medium",
				})
				require.Equal(t, http.StatusOK, riRec.Code)
				riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

				rec := doDMS(t, h, "TestConnection", map[string]any{
					"ReplicationInstanceArn": riArn,
					"EndpointArn":            "arn:nonexistent-ep",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "describe_connections_empty_initially",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DescribeConnections", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)
				conns := parseJSON(t, rec)["Connections"].([]any)
				assert.Empty(t, conns)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}
