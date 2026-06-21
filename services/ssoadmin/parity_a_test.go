package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestParity_AttachManagedPolicyRequiresFields verifies that
// AttachManagedPolicyToPermissionSet rejects requests with missing required fields.
// Real AWS returns ValidationException for empty InstanceArn, PermissionSetArn,
// or ManagedPolicyArn; the emulator previously forwarded them to the backend and
// returned a 404 instead.
func TestParity_AttachManagedPolicyRequiresFields(t *testing.T) {
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
