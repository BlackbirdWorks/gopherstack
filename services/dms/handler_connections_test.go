package dms_test

import (
	"net/http"
	"testing"

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
