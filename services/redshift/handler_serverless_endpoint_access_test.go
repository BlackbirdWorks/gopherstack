package redshift_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestServerless_EndpointAccess_CRUD(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()
	rec := doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "ea-ns"})
	require.Equal(t, http.StatusOK, rec.Code)
	rec = doServerlessOp(t, h, "CreateWorkgroup", map[string]any{"workgroupName": "ea-wg", "namespaceName": "ea-ns"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "CreateEndpointAccess", map[string]any{
		"endpointName":        "ea-endpoint",
		"workgroupName":       "ea-wg",
		"subnetIds":           []string{"subnet-1", "subnet-2"},
		"vpcSecurityGroupIds": []string{"sg-1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	ep, _ := createResp["endpoint"].(map[string]any)
	require.NotNil(t, ep)
	assert.Equal(t, "ea-endpoint", ep["endpointName"])
	assert.Equal(t, "ea-wg", ep["workgroupName"])
	assert.NotEmpty(t, ep["endpointArn"])
	assert.NotEmpty(t, ep["endpointCreateTime"])
	assert.NotEmpty(t, ep["address"])
	groups, _ := ep["vpcSecurityGroups"].([]any)
	require.Len(t, groups, 1)
	group, _ := groups[0].(map[string]any)
	assert.Equal(t, "sg-1", group["vpcSecurityGroupId"])
	assert.NotEmpty(t, group["status"])
	assert.Nil(t, ep["vpcEndpoint"], "VpcEndpoint is deliberately unmodeled -- no per-ENI AZ/IP data exists")

	rec = doServerlessOp(t, h, "GetEndpointAccess", map[string]any{"endpointName": "ea-endpoint"})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&getResp))
	got, _ := getResp["endpoint"].(map[string]any)
	require.NotNil(t, got)
	assert.Equal(t, "ea-endpoint", got["endpointName"])

	rec = doServerlessOp(t, h, "ListEndpointAccess", map[string]any{"workgroupName": "ea-wg"})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&listResp))
	list, _ := listResp["endpoints"].([]any)
	require.Len(t, list, 1)

	rec = doServerlessOp(t, h, "UpdateEndpointAccess", map[string]any{
		"endpointName":        "ea-endpoint",
		"vpcSecurityGroupIds": []string{"sg-2", "sg-3"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&updateResp))
	updated, _ := updateResp["endpoint"].(map[string]any)
	require.NotNil(t, updated)
	groups, _ = updated["vpcSecurityGroups"].([]any)
	require.Len(t, groups, 2)

	rec = doServerlessOp(t, h, "DeleteEndpointAccess", map[string]any{"endpointName": "ea-endpoint"})
	require.Equal(t, http.StatusOK, rec.Code)

	var deleteResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&deleteResp))
	deleted, _ := deleteResp["endpoint"].(map[string]any)
	require.NotNil(t, deleted, "DeleteEndpointAccessResponse echoes the deleted object on the real wire")
	assert.Equal(t, "ea-endpoint", deleted["endpointName"])

	rec = doServerlessOp(t, h, "GetEndpointAccess", map[string]any{"endpointName": "ea-endpoint"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestServerless_EndpointAccess_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantType   string
		wantStatus int
	}{
		{
			name:       "create missing endpoint name",
			op:         "CreateEndpointAccess",
			body:       map[string]any{"workgroupName": "no-such-wg"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name:       "create unknown workgroup",
			op:         "CreateEndpointAccess",
			body:       map[string]any{"endpointName": "e1", "workgroupName": "no-such-wg"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
		{
			name:       "get missing name",
			op:         "GetEndpointAccess",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
		{
			name:       "get unknown name",
			op:         "GetEndpointAccess",
			body:       map[string]any{"endpointName": "nope"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
		{
			name:       "update unknown name",
			op:         "UpdateEndpointAccess",
			body:       map[string]any{"endpointName": "nope"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
		{
			name:       "delete unknown name",
			op:         "DeleteEndpointAccess",
			body:       map[string]any{"endpointName": "nope"},
			wantStatus: http.StatusBadRequest,
			wantType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newServerlessHandler()

			rec := doServerlessOp(t, h, tt.op, tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			var errResp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
			assert.Equal(t, tt.wantType, errResp["__type"])
		})
	}
}

func TestServerless_EndpointAccess_DuplicateName(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()
	doServerlessOp(t, h, "CreateNamespace", map[string]any{"namespaceName": "dup-ns"})
	doServerlessOp(t, h, "CreateWorkgroup", map[string]any{"workgroupName": "dup-wg", "namespaceName": "dup-ns"})

	rec := doServerlessOp(t, h, "CreateEndpointAccess", map[string]any{
		"endpointName": "dup-endpoint", "workgroupName": "dup-wg",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doServerlessOp(t, h, "CreateEndpointAccess", map[string]any{
		"endpointName": "dup-endpoint", "workgroupName": "dup-wg",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
	assert.Equal(t, "ConflictException", errResp["__type"])
}

func TestServerless_ListManagedWorkgroups_AlwaysEmpty(t *testing.T) {
	t.Parallel()

	h := newServerlessHandler()

	rec := doServerlessOp(t, h, "ListManagedWorkgroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
	list, ok := resp["managedWorkgroups"].([]any)
	require.True(t, ok, "managedWorkgroups must be a correctly-shaped (empty) array, not absent")
	assert.Empty(t, list)
	assert.NotContains(t, resp, "nextToken")
}
