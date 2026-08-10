package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestUserProfiles verifies user profile CRUD.
func TestUserProfiles(t *testing.T) {
	t.Parallel()

	const testArn = "arn:aws:iam::000000000000:user/test-user"

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "CreateUserProfile returns IAM ARN",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "CreateUserProfile", map[string]any{
					"IamUserArn":  testArn,
					"SshUsername": "testuser",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.Equal(t, testArn, resp["IamUserArn"])
			},
		},
		{
			name: "DescribeUserProfiles returns created profile",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				doTarget(t, h, "CreateUserProfile", map[string]any{
					"IamUserArn":  testArn,
					"SshUsername": "testuser",
				})
				rec := doTarget(t, h, "DescribeUserProfiles", map[string]any{})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				profiles := resp["UserProfiles"].([]any)
				require.Len(t, profiles, 1)
				p := profiles[0].(map[string]any)
				assert.Equal(t, testArn, p["IamUserArn"])
				assert.Equal(t, "testuser", p["SshUsername"])
			},
		},
		{
			name: "UpdateUserProfile changes SSH username",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				doTarget(t, h, "CreateUserProfile", map[string]any{
					"IamUserArn":  testArn,
					"SshUsername": "oldname",
				})
				rec := doTarget(t, h, "UpdateUserProfile", map[string]any{
					"IamUserArn":  testArn,
					"SshUsername": "newname",
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeUserProfiles", map[string]any{
					"IamUserArns": []string{testArn},
				})
				profiles := parseJSON(t, rec.Body.Bytes())["UserProfiles"].([]any)
				assert.Equal(t, "newname", profiles[0].(map[string]any)["SshUsername"])
			},
		},
		{
			name: "DeleteUserProfile removes profile",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				doTarget(t, h, "CreateUserProfile", map[string]any{
					"IamUserArn":  testArn,
					"SshUsername": "user",
				})
				rec := doTarget(t, h, "DeleteUserProfile", map[string]any{
					"IamUserArn": testArn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeUserProfiles", map[string]any{
					"IamUserArns": []string{testArn},
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "DescribeMyUserProfile returns profile",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "DescribeMyUserProfile", nil)
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				profile := resp["UserProfile"].(map[string]any)
				assert.NotEmpty(t, profile["IamUserArn"])
			},
		},
		{
			name: "UpdateMyUserProfile returns OK",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "UpdateMyUserProfile", map[string]any{
					"SshPublicKey": "ssh-rsa AAAA test",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}

// TestPermissions verifies SetPermission and DescribePermissions.
func TestPermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "SetPermission and DescribePermissions round-trip",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				const arn = "arn:aws:iam::000000000000:user/dev"
				rec := doTarget(t, h, "SetPermission", map[string]any{
					"StackId":    stackID,
					"IamUserArn": arn,
					"Level":      "deploy",
					"AllowSsh":   true,
					"AllowSudo":  false,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribePermissions", map[string]any{
					"StackId":    stackID,
					"IamUserArn": arn,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				perms := parseJSON(t, rec.Body.Bytes())["Permissions"].([]any)
				require.Len(t, perms, 1)
				p := perms[0].(map[string]any)
				assert.Equal(t, "deploy", p["Level"])
				assert.Equal(t, true, p["AllowSsh"])
				assert.Equal(t, false, p["AllowSudo"])
			},
		},
		{
			name: "SetPermission on nonexistent stack returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "SetPermission", map[string]any{
					"StackId":    "none",
					"IamUserArn": "arn:aws:iam::000000000000:user/x",
					"Level":      "manage",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}

// TestSetPermissionValidation verifies SetPermission rejects a missing
// IamUserArn or StackId with ValidationException rather than either
// silently accepting an empty IamUserArn or falling through to the
// stack-lookup's ResourceNotFoundException. IamUserArn and StackId are
// both "This member is required" on the real SetPermissionInput (confirmed
// against aws-sdk-go-v2/service/opsworks@v1.31.0's api_op_SetPermission.go).
func TestSetPermissionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing IamUserArn", body: map[string]any{"StackId": "some-stack"}},
		{name: "missing StackId", body: map[string]any{"IamUserArn": "arn:aws:iam::000000000000:user/x"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doTarget(t, h, "SetPermission", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "ValidationException")
		})
	}
}

// TestSetPermissionLevelValidation verifies SetPermission rejects a Level
// outside the real API's documented closed set (deny/show/deploy/manage/
// iam_only -- confirmed against aws-sdk-go-v2/service/opsworks@v1.31.0's
// api_op_SetPermission.go doc comment). A previous pass accepted any string.
func TestSetPermissionLevelValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	stackID := createTestStack(t, h)
	rec := doTarget(t, h, "SetPermission", map[string]any{
		"StackId":    stackID,
		"IamUserArn": "arn:aws:iam::000000000000:user/x",
		"Level":      "superadmin",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ValidationException")
}

// TestGrantAccess verifies GrantAccess returns temporary credentials.
func TestGrantAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "GrantAccess returns temporary credentials",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)

				rec := doTarget(t, h, "GrantAccess", map[string]any{
					"InstanceId":        instanceID,
					"ValidForInMinutes": 60,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				creds := resp["TemporaryCredential"].(map[string]any)
				assert.NotEmpty(t, creds["Username"])
				assert.NotEmpty(t, creds["Password"])
				assert.InEpsilon(t, float64(60), creds["ValidForInMinutes"], 0.001)
			},
		},
		{
			name: "GrantAccess on nonexistent instance returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "GrantAccess", map[string]any{
					"InstanceId": "nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}
