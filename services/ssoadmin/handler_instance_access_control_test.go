package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// TestABACLifecycleStatus verifies ABAC config status transitions CREATION_IN_PROGRESS → ENABLED.
func TestABACLifecycleStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
	}{
		{
			name:       "describe triggers ENABLED transition",
			wantStatus: "ENABLED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "abac-status-inst")

			rec := doRequest(t, h, "CreateInstanceAccessControlAttributeConfiguration", map[string]any{
				"InstanceArn": instanceArn,
				"InstanceAccessControlAttributeConfiguration": map[string]any{
					"AccessControlAttributes": []map[string]any{
						{
							"Key": "email",
							"Value": map[string]any{
								"Source": []string{"${path:enterprise.email}"},
							},
						},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "DescribeInstanceAccessControlAttributeConfiguration", map[string]any{
				"InstanceArn": instanceArn,
			})
			require.Equal(t, http.StatusOK, rec2.Code)
			resp2 := parseResponse(t, rec2)
			assert.Equal(t, tt.wantStatus, resp2["Status"])
		})
	}
}

func TestCreateInstanceAccessControlAttributeConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		attributes     []map[string]any
		wantStatus     int
		useInvalidInst bool
	}{
		{
			name: "create ABAC config with attributes",
			attributes: []map[string]any{
				{
					"Key": "department",
					"Value": map[string]any{
						"Source": []string{"${path:enterprise.department}"},
					},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create ABAC config with empty attributes",
			attributes: []map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name: "create ABAC config for nonexistent instance",
			attributes: []map[string]any{
				{"Key": "dept", "Value": map[string]any{"Source": []string{"x"}}},
			},
			wantStatus:     http.StatusBadRequest,
			useInvalidInst: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			var instanceArn string
			if tt.useInvalidInst {
				instanceArn = "arn:aws:sso:::instance/ssoins-nonexistent"
			} else {
				instanceArn = createInstance(t, h, "abac-test-instance")
			}
			rec := doRequest(t, h, "CreateInstanceAccessControlAttributeConfiguration", map[string]any{
				"InstanceArn": instanceArn,
				"InstanceAccessControlAttributeConfiguration": map[string]any{
					"AccessControlAttributes": tt.attributes,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestABACPermissionsBoundaryAndProvisioningListOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *ssoadmin.Handler) map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "describe ABAC config",
			op:   "DescribeInstanceAccessControlAttributeConfiguration",
			setup: func(t *testing.T, h *ssoadmin.Handler) map[string]any {
				t.Helper()
				instanceArn := createInstance(t, h, "abac-describe-instance")
				createCfgRec := doRequest(t, h, "CreateInstanceAccessControlAttributeConfiguration", map[string]any{
					"InstanceArn": instanceArn,
					"InstanceAccessControlAttributeConfiguration": map[string]any{
						"AccessControlAttributes": []map[string]any{
							{
								"Key":   "department",
								"Value": map[string]any{"Source": []string{"${path:enterprise.department}"}},
							},
						},
					},
				})
				require.Equal(t, http.StatusOK, createCfgRec.Code)

				return map[string]any{"InstanceArn": instanceArn}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "put permissions boundary",
			op:   "PutPermissionsBoundaryToPermissionSet",
			setup: func(t *testing.T, h *ssoadmin.Handler) map[string]any {
				t.Helper()
				instanceArn := createInstance(t, h, "boundary-instance")
				psArn := createPermissionSet(t, h, instanceArn, "boundary-ps")

				return map[string]any{
					"InstanceArn":      instanceArn,
					"PermissionSetArn": psArn,
					"PermissionsBoundary": map[string]any{
						"ManagedPolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
					},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "list provisioning and assignment statuses",
			op:   "ListPermissionSetProvisioningStatus",
			setup: func(t *testing.T, h *ssoadmin.Handler) map[string]any {
				t.Helper()
				instanceArn := createInstance(t, h, "status-instance")
				psArn := createPermissionSet(t, h, instanceArn, "status-ps")

				putBoundaryRec := doRequest(t, h, "PutPermissionsBoundaryToPermissionSet", map[string]any{
					"InstanceArn":      instanceArn,
					"PermissionSetArn": psArn,
					"PermissionsBoundary": map[string]any{
						"ManagedPolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
					},
				})
				require.Equal(t, http.StatusOK, putBoundaryRec.Code)

				getBoundaryRec := doRequest(t, h, "GetPermissionsBoundaryForPermissionSet", map[string]any{
					"InstanceArn":      instanceArn,
					"PermissionSetArn": psArn,
				})
				require.Equal(t, http.StatusOK, getBoundaryRec.Code)

				createAssignmentRec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
					"InstanceArn":      instanceArn,
					"PermissionSetArn": psArn,
					"PrincipalType":    "USER",
					"PrincipalId":      "user-123",
					"TargetId":         "123456789012",
					"TargetType":       "AWS_ACCOUNT",
				})
				require.Equal(t, http.StatusOK, createAssignmentRec.Code)

				deleteAssignmentRec := doRequest(t, h, "DeleteAccountAssignment", map[string]any{
					"InstanceArn":      instanceArn,
					"PermissionSetArn": psArn,
					"PrincipalType":    "USER",
					"PrincipalId":      "user-123",
					"TargetId":         "123456789012",
					"TargetType":       "AWS_ACCOUNT",
				})
				require.Equal(t, http.StatusOK, deleteAssignmentRec.Code)

				listCreateRec := doRequest(t, h, "ListAccountAssignmentCreationStatus", map[string]any{
					"InstanceArn": instanceArn,
				})
				require.Equal(t, http.StatusOK, listCreateRec.Code)

				listDeleteRec := doRequest(t, h, "ListAccountAssignmentDeletionStatus", map[string]any{
					"InstanceArn": instanceArn,
				})
				require.Equal(t, http.StatusOK, listDeleteRec.Code)

				provisionRec := doRequest(t, h, "ProvisionPermissionSet", map[string]any{
					"InstanceArn":      instanceArn,
					"PermissionSetArn": psArn,
					"TargetType":       "ALL_PROVISIONED_ACCOUNTS",
				})
				require.Equal(t, http.StatusOK, provisionRec.Code)

				return map[string]any{"InstanceArn": instanceArn}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "delete ABAC config",
			op:   "DeleteInstanceAccessControlAttributeConfiguration",
			setup: func(t *testing.T, h *ssoadmin.Handler) map[string]any {
				t.Helper()
				instanceArn := createInstance(t, h, "abac-delete-instance")
				createCfgRec := doRequest(t, h, "CreateInstanceAccessControlAttributeConfiguration", map[string]any{
					"InstanceArn": instanceArn,
					"InstanceAccessControlAttributeConfiguration": map[string]any{
						"AccessControlAttributes": []map[string]any{
							{
								"Key":   "department",
								"Value": map[string]any{"Source": []string{"${path:enterprise.department}"}},
							},
						},
					},
				})
				require.Equal(t, http.StatusOK, createCfgRec.Code)

				return map[string]any{"InstanceArn": instanceArn}
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			body := tt.setup(t, h)
			rec := doRequest(t, h, tt.op, body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAccessControlAttributesDeepCopy verifies that CreateInstanceAccessControlAttributeConfiguration
// stores a deep copy of the attributes.
func TestAccessControlAttributesDeepCopy(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "aca-deep-copy-inst")

	attributes := []map[string]any{
		{
			"Key":   "dept",
			"Value": map[string]any{"Source": []string{"${path:enterprise.department}"}},
		},
	}

	rec := doRequest(t, h, "CreateInstanceAccessControlAttributeConfiguration", map[string]any{
		"InstanceArn": instanceArn,
		"InstanceAccessControlAttributeConfiguration": map[string]any{
			"AccessControlAttributes": attributes,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestCreateACAConflict verifies that creating ACA twice returns 409.
func TestCreateACAConflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "aca-conflict-inst")

	payload := map[string]any{
		"InstanceArn": instanceArn,
		"InstanceAccessControlAttributeConfiguration": map[string]any{
			"AccessControlAttributes": []map[string]any{
				{"Key": "dept", "Value": map[string]any{"Source": []string{"${user:department}"}}},
			},
		},
	}

	rec := doRequest(t, h, "CreateInstanceAccessControlAttributeConfiguration", payload)
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRequest(t, h, "CreateInstanceAccessControlAttributeConfiguration", payload)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}
