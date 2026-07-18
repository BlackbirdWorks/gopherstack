package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCustomerManagedPolicyReferenceValidation verifies CMPR name/path validation.
func TestCustomerManagedPolicyReferenceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cmprName   string
		cmprPath   string
		wantStatus int
	}{
		{
			name:       "valid name and path accepted",
			cmprName:   "my-policy",
			cmprPath:   "/",
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty name rejected",
			cmprName:   "",
			cmprPath:   "/",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid path rejected",
			cmprName:   "my-policy",
			cmprPath:   "no-leading-slash",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "cmpr-val-inst")
			psArn := createPermissionSet(t, h, instanceArn, "CMPRPS")

			rec := doRequest(t, h, "AttachCustomerManagedPolicyReferenceToPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"CustomerManagedPolicyReference": map[string]any{
					"Name": tt.cmprName,
					"Path": tt.cmprPath,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAttachCustomerManagedPolicyReferenceToPermissionSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		policyName      string
		policyPath      string
		wantStatus      int
		useInvalidPSArn bool
	}{
		{
			name:       "attach policy to permission set",
			policyName: "MyPolicy",
			policyPath: "/",
			wantStatus: http.StatusOK,
		},
		{
			name:       "attach policy with empty path",
			policyName: "AnotherPolicy",
			policyPath: "",
			wantStatus: http.StatusOK,
		},
		{
			name:            "attach to nonexistent permission set",
			policyName:      "MyPolicy",
			policyPath:      "/",
			wantStatus:      http.StatusNotFound,
			useInvalidPSArn: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "cmp-test-instance")
			psArn := createPermissionSet(t, h, instanceArn, "cmp-ps")
			if tt.useInvalidPSArn {
				psArn = "arn:aws:sso:::permissionSet/ssoins-bad/bad"
			}
			rec := doRequest(t, h, "AttachCustomerManagedPolicyReferenceToPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"CustomerManagedPolicyReference": map[string]any{
					"Name": tt.policyName,
					"Path": tt.policyPath,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAttachCustomerManagedPolicyIdempotent verifies attaching the same policy twice is idempotent.
func TestAttachCustomerManagedPolicyIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "cmp-idempotent-inst")
	psArn := createPermissionSet(t, h, instanceArn, "cmp-idempotent-ps")

	req := map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"CustomerManagedPolicyReference": map[string]any{
			"Name": "MyPolicy",
			"Path": "/",
		},
	}

	rec1 := doRequest(t, h, "AttachCustomerManagedPolicyReferenceToPermissionSet", req)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRequest(t, h, "AttachCustomerManagedPolicyReferenceToPermissionSet", req)
	assert.Equal(t, http.StatusOK, rec2.Code)
}

// TestDetachManagedPolicyNotFound verifies that DetachManagedPolicyFromPermissionSet
// returns ResourceNotFoundException when the policy is not attached.
func TestDetachManagedPolicyNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policyArn  string
		wantStatus int
	}{
		{
			name:       "detach_policy_not_attached",
			policyArn:  "arn:aws:iam::aws:policy/NotAttachedPolicy",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-detach-inst")
			psArn := createPermissionSet(t, h, instanceArn, "r3-detach-ps")

			rec := doRequest(t, h, "DetachManagedPolicyFromPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"ManagedPolicyArn": tt.policyArn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAttachManagedPolicyRequiresFields verifies that
// AttachManagedPolicyToPermissionSet rejects requests with missing required fields.
// Real AWS returns ValidationException for empty InstanceArn, PermissionSetArn,
// or ManagedPolicyArn; the emulator previously forwarded them to the backend and
// returned a 404 instead.
func TestAttachManagedPolicyRequiresFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name: "absent_instance_arn_rejected",
			body: map[string]any{
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-1/ps-1",
				"ManagedPolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty_instance_arn_rejected",
			body: map[string]any{
				"InstanceArn":      "",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-1/ps-1",
				"ManagedPolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "absent_permission_set_arn_rejected",
			body: map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-1",
				"ManagedPolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "absent_managed_policy_arn_rejected",
			body: map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-1",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-1/ps-1",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "AttachManagedPolicyToPermissionSet", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"AttachManagedPolicyToPermissionSet status for case %q", tt.name)
		})
	}
}

func TestManagedPolicyCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		managedPolicyArn string
		wantPolicyName   string
	}{
		{
			name:             "attach, list, and detach managed policy",
			managedPolicyArn: "arn:aws:iam::aws:policy/ReadOnlyAccess",
			wantPolicyName:   "ReadOnlyAccess",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			psArn := createPermissionSet(t, h, instanceArn, "PS")

			attachRec := doRequest(t, h, "AttachManagedPolicyToPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"ManagedPolicyArn": tt.managedPolicyArn,
			})
			require.Equal(t, http.StatusOK, attachRec.Code)

			listRec := doRequest(t, h, "ListManagedPoliciesInPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, listRec.Code)
			listResp := parseResponse(t, listRec)
			policies, ok := listResp["AttachedManagedPolicies"].([]any)
			require.True(t, ok)
			require.Len(t, policies, 1)
			policy := policies[0].(map[string]any)
			assert.Equal(t, tt.wantPolicyName, policy["Name"])
			assert.Equal(t, tt.managedPolicyArn, policy["Arn"])

			detachRec := doRequest(t, h, "DetachManagedPolicyFromPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"ManagedPolicyArn": tt.managedPolicyArn,
			})
			require.Equal(t, http.StatusOK, detachRec.Code)

			listRec2 := doRequest(t, h, "ListManagedPoliciesInPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, listRec2.Code)
			listResp2 := parseResponse(t, listRec2)
			policies2, ok := listResp2["AttachedManagedPolicies"].([]any)
			require.True(t, ok)
			assert.Empty(t, policies2)
		})
	}
}
