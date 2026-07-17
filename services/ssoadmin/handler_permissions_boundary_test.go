package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPermissionsBoundaryUnionType verifies put/get boundary with both ManagedPolicy and CMPR.
func TestPermissionsBoundaryUnionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		managedPolicyArn string
		cmprName         string
		cmprPath         string
		wantErrPut       bool
	}{
		{
			name:             "managed policy boundary accepted",
			managedPolicyArn: "arn:aws:iam::aws:policy/ReadOnlyAccess",
			wantErrPut:       false,
		},
		{
			name:       "customer managed policy reference accepted",
			cmprName:   "my-policy",
			cmprPath:   "/",
			wantErrPut: false,
		},
		{
			name:             "both set rejected",
			managedPolicyArn: "arn:aws:iam::aws:policy/ReadOnlyAccess",
			cmprName:         "my-policy",
			wantErrPut:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "boundary-inst")
			psArn := createPermissionSet(t, h, instanceArn, "BoundaryPS")

			boundary := map[string]any{}
			if tt.managedPolicyArn != "" {
				boundary["ManagedPolicyArn"] = tt.managedPolicyArn
			}
			if tt.cmprName != "" {
				cmpr := map[string]any{"Name": tt.cmprName}
				if tt.cmprPath != "" {
					cmpr["Path"] = tt.cmprPath
				}
				boundary["CustomerManagedPolicyReference"] = cmpr
			}

			rec := doRequest(t, h, "PutPermissionsBoundaryToPermissionSet", map[string]any{
				"InstanceArn":         instanceArn,
				"PermissionSetArn":    psArn,
				"PermissionsBoundary": boundary,
			})
			if tt.wantErrPut {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doRequest(t, h, "GetPermissionsBoundaryForPermissionSet", map[string]any{
					"InstanceArn":      instanceArn,
					"PermissionSetArn": psArn,
				})
				require.Equal(t, http.StatusOK, rec2.Code)
				resp2 := parseResponse(t, rec2)
				pb := resp2["PermissionsBoundary"].(map[string]any)
				if tt.managedPolicyArn != "" {
					assert.Equal(t, tt.managedPolicyArn, pb["ManagedPolicyArn"])
				} else {
					cmpr := pb["CustomerManagedPolicyReference"].(map[string]any)
					assert.Equal(t, tt.cmprName, cmpr["Name"])
				}
			}
		})
	}
}
