package memorydb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ExportSnapshot verifies ExportSnapshot returns the named snapshot.
func TestParity_ExportSnapshot(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "export-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})
	require.Equal(t, http.StatusOK, createRec.Code, "create cluster: %s", createRec.Body)

	snapRec := doRequest(t, h, "CreateSnapshot", map[string]any{
		"ClusterName":  "export-cluster",
		"SnapshotName": "export-snap",
	})
	require.Equal(t, http.StatusOK, snapRec.Code, "create snapshot: %s", snapRec.Body)

	rec := doRequest(t, h, "ExportSnapshot", map[string]any{
		"SnapshotName": "export-snap",
		"S3BucketName": "my-bucket",
	})
	require.Equal(t, http.StatusOK, rec.Code, "export snapshot: %s", rec.Body)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	snap, _ := resp["Snapshot"].(map[string]any)
	assert.Equal(t, "export-snap", snap["Name"], "Snapshot.Name must match")
	assert.NotEmpty(t, snap["ARN"], "Snapshot.ARN must be present")
}

// TestParity_ExportSnapshot_NotFound verifies ExportSnapshot returns an error for missing snapshot.
func TestParity_ExportSnapshot_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ExportSnapshot", map[string]any{
		"SnapshotName": "no-such-snap",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestParity_ClusterResponse_ParameterGroupStatus verifies ParameterGroupStatus is present.
func TestParity_ClusterResponse_ParameterGroupStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "pgstatus-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})
	require.Equal(t, http.StatusOK, createRec.Code, "create cluster: %s", createRec.Body)

	descRec := doRequest(t, h, "DescribeClusters", map[string]any{
		"ClusterName": "pgstatus-cluster",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	clusters, _ := resp["Clusters"].([]any)
	require.Len(t, clusters, 1)
	cl, _ := clusters[0].(map[string]any)
	assert.NotEmpty(t, cl["ParameterGroupStatus"], "ParameterGroupStatus must be present")
}

// TestParity_ClusterResponse_MultiRegionClusterName verifies MultiRegionClusterName is in response.
func TestParity_ClusterResponse_MultiRegionClusterName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "mrc-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	cl, _ := createResp["Cluster"].(map[string]any)

	// Field must be present (empty string is fine for a cluster not in multi-region).
	_, hasField := cl["MultiRegionClusterName"]
	assert.True(t, hasField, "MultiRegionClusterName field must be present in cluster response")
}

// TestParity_UserResponse_ACLNames verifies ACLNames is returned instead of UserGroupCount.
func TestParity_UserResponse_ACLNames(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateUser", map[string]any{
		"UserName":           "acl-user",
		"AccessString":       "on ~* &* +@all",
		"AuthenticationMode": map[string]any{"Type": "no-password-required"},
	})

	doRequest(t, h, "CreateACL", map[string]any{
		"ACLName":   "acl-alpha",
		"UserNames": []string{"acl-user"},
	})
	doRequest(t, h, "CreateACL", map[string]any{
		"ACLName":   "acl-beta",
		"UserNames": []string{"acl-user"},
	})

	users := doDescribeUsers(t, h, "acl-user")
	require.Len(t, users, 1)
	user, _ := users[0].(map[string]any)

	aclNames, _ := user["ACLNames"].([]any)
	assert.Len(t, aclNames, 2, "ACLNames must contain both ACLs")

	names := make([]string, 0, len(aclNames))
	for _, n := range aclNames {
		if s, ok := n.(string); ok {
			names = append(names, s)
		}
	}

	assert.Contains(t, names, "acl-alpha")
	assert.Contains(t, names, "acl-beta")

	_, hasOld := user["UserGroupCount"]
	assert.False(t, hasOld, "UserGroupCount must not appear in response")
}

// TestParity_UserResponse_Engine verifies Engine field is present in user response.
func TestParity_UserResponse_Engine(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateUser", map[string]any{
		"UserName":           "engine-user",
		"AccessString":       "on ~* &* +@all",
		"AuthenticationMode": map[string]any{"Type": "no-password-required"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	u, _ := resp["User"].(map[string]any)
	assert.NotEmpty(t, u["Engine"], "Engine field must be present in user response")
}

// TestParity_UserResponse_ACLNames_CreateReturnsEmpty verifies newly created user has empty ACLNames.
func TestParity_UserResponse_ACLNames_CreateReturnsEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateUser", map[string]any{
		"UserName":           "fresh-user",
		"AccessString":       "on ~* &* +@all",
		"AuthenticationMode": map[string]any{"Type": "no-password-required"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	u, _ := resp["User"].(map[string]any)

	aclNames, hasField := u["ACLNames"]
	assert.True(t, hasField, "ACLNames must be present in CreateUser response")
	aclList, _ := aclNames.([]any)
	assert.Empty(t, aclList, "newly created user must have empty ACLNames")
}

// TestParity_ExportSnapshot_MissingSnapshotName verifies validation.
func TestParity_ExportSnapshot_MissingSnapshotName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ExportSnapshot", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestParity_ClusterParameterGroupStatus_InSync verifies default value is "in-sync".
func TestParity_ClusterParameterGroupStatus_InSync(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "insync-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &resp))
	cl, _ := resp["Cluster"].(map[string]any)
	assert.Equal(t, "in-sync", cl["ParameterGroupStatus"],
		"default ParameterGroupStatus must be in-sync")
}
