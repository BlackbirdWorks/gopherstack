package memorydb_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_ACL_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		aclName string
		wantErr bool
	}{
		{
			name:    "create_and_describe",
			aclName: "test-acl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			req := &memorydb.ExportedCreateACLRequest{
				ACLName: tt.aclName,
			}

			a, err := b.CreateACL(context.Background(), req)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.aclName, a.Name)
			assert.NotEmpty(t, a.ARN)

			acls, err := b.DescribeACLs(context.Background(), tt.aclName)
			require.NoError(t, err)
			require.Len(t, acls, 1)

			_, err = b.DeleteACL(context.Background(), tt.aclName)
			require.NoError(t, err)

			_, err = b.DescribeACLs(context.Background(), tt.aclName)
			require.Error(t, err)
		})
	}
}

func TestBackend_OpenAccessACL_Preseeded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		aclName string
	}{
		{
			name:    "open_access_exists",
			aclName: "open-access",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			acls, err := b.DescribeACLs(context.Background(), tt.aclName)
			require.NoError(t, err)
			require.Len(t, acls, 1)
			assert.Equal(t, tt.aclName, acls[0].Name)
		})
	}
}

// TestRefinement1_UpdateACLSliceNoAlias verifies UpdateACL remove doesn't alias old slice.
func TestUpdateACLSliceNoAlias(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create ACL with users
	doRequest(t, h, "CreateACL", map[string]any{
		"ACLName":   "test-acl",
		"UserNames": []string{"u1", "u2", "u3"},
	})

	// Remove u2
	rec := doRequest(t, h, "UpdateACL", map[string]any{
		"ACLName":           "test-acl",
		"UserNamesToRemove": []string{"u2"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify the ACL now has only u1 and u3
	descRec := doRequest(t, h, "DescribeACLs", map[string]any{"ACLName": "test-acl"})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &resp))
	acls := resp["ACLs"].([]any)
	acl := acls[0].(map[string]any)
	users := acl["UserNames"].([]any)

	assert.Len(t, users, 2)
	assert.NotContains(t, users, "u2")
}
