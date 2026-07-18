package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPasswordPolicy_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*iam.InMemoryBackend)
		params      map[string]string
		name        string
		action      string
		wantContain string
		wantCode    int
	}{
		{
			name:        "GetAccountPasswordPolicy",
			action:      "GetAccountPasswordPolicy",
			setup:       func(_ *iam.InMemoryBackend) {},
			wantCode:    http.StatusOK,
			wantContain: "GetAccountPasswordPolicyResponse",
		},
		{
			name:   "UpdateAccountPasswordPolicy",
			action: "UpdateAccountPasswordPolicy",
			setup:  func(_ *iam.InMemoryBackend) {},
			params: map[string]string{
				"MinimumPasswordLength": "12",
				"RequireNumbers":        "true",
			},
			wantCode:    http.StatusOK,
			wantContain: "UpdateAccountPasswordPolicyResponse",
		},
		{
			name:   "DeleteAccountPasswordPolicy",
			action: "DeleteAccountPasswordPolicy",
			setup: func(b *iam.InMemoryBackend) {
				_ = b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{MinimumPasswordLength: 10})
			},
			wantCode:    http.StatusOK,
			wantContain: "DeleteAccountPasswordPolicyResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)
			h := iam.NewHandler(b)

			e := echo.New()
			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContain)
		})
	}
}

// TestListAccountAliases_Handler tests the account alias handlers.

func TestListAccountAliases_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*iam.InMemoryBackend)
		params      map[string]string
		name        string
		action      string
		wantContain string
		wantCode    int
	}{
		{
			name:        "ListAccountAliases_empty",
			action:      "ListAccountAliases",
			setup:       func(_ *iam.InMemoryBackend) {},
			wantCode:    http.StatusOK,
			wantContain: "ListAccountAliasesResponse",
		},
		{
			name:   "ListAccountAliases_with_alias",
			action: "ListAccountAliases",
			setup: func(b *iam.InMemoryBackend) {
				_ = b.CreateAccountAlias("mycompany")
			},
			wantCode:    http.StatusOK,
			wantContain: "mycompany",
		},
		{
			name:   "DeleteAccountAlias",
			action: "DeleteAccountAlias",
			setup: func(b *iam.InMemoryBackend) {
				_ = b.CreateAccountAlias("mycompany")
			},
			params:      map[string]string{"AccountAlias": "mycompany"},
			wantCode:    http.StatusOK,
			wantContain: "DeleteAccountAliasResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)
			h := iam.NewHandler(b)

			e := echo.New()
			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContain)
		})
	}
}

func TestHandler_SnapshotRestore_Delegation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(b *iam.InMemoryBackend)
		name  string
	}{
		{
			name: "snapshot_and_restore_via_handler",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("snap-user", "/", "")
			},
		},
		{
			name:  "empty_backend_snapshot_and_restore",
			setup: func(_ *iam.InMemoryBackend) {},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			h := iam.NewHandler(b)
			tt.setup(b)

			snap := h.Snapshot(t.Context())
			require.NotNil(t, snap)

			freshB := iam.NewInMemoryBackend()
			freshH := iam.NewHandler(freshB)
			require.NoError(t, freshH.Restore(t.Context(), snap))

			if tt.name == "snapshot_and_restore_via_handler" {
				u, err := freshB.GetUser("snap-user")
				require.NoError(t, err)
				assert.Equal(t, "snap-user", u.UserName)
			}
		})
	}
}

// TestCredentialReport_Columns verifies that GetCredentialReport returns
// a base64-encoded CSV containing all required AWS credential report columns.

func TestHandler_AccountPasswordPolicy_GetAfterUpdate(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// Update.
	req := iamRequest("UpdateAccountPasswordPolicy", map[string]string{
		"MinimumPasswordLength":      "14",
		"RequireSymbols":             "true",
		"RequireNumbers":             "true",
		"RequireUppercaseCharacters": "true",
		"RequireLowercaseCharacters": "true",
		"MaxPasswordAge":             "60",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// Get.
	req2 := iamRequest("GetAccountPasswordPolicy", nil)
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)

	body := rec2.Body.String()
	assert.Contains(t, body, "14", "minimum length must be reflected")
	assert.Contains(t, body, "60", "MaxPasswordAge must be reflected")
}

func TestHandler_AccountPasswordPolicy_Delete(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// First set a policy.
	req := iamRequest("UpdateAccountPasswordPolicy", map[string]string{
		"MinimumPasswordLength": "10",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete.
	req2 := iamRequest("DeleteAccountPasswordPolicy", nil)
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestCreateAccountAlias_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		alias   string
		wantErr bool
	}{
		{
			name:  "create_alias_success",
			alias: "my-test-alias",
		},
		{
			name:    "empty_alias_returns_error",
			alias:   "",
			wantErr: true,
		},
		{
			name:  "replace_existing_alias",
			alias: "second-alias",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			// seed with a prior alias to test replace behaviour
			_ = b.CreateAccountAlias("old-alias")

			err := b.CreateAccountAlias(tt.alias)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDelegationRequest_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		target  string
		wantErr bool
	}{
		{
			name:   "create_and_accept",
			target: "111122223333",
		},
		{
			name:   "create_empty_target",
			target: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()

			req, err := b.CreateDelegationRequest(tt.target)
			require.NoError(t, err)
			require.NotNil(t, req)
			assert.Equal(t, "PENDING", req.Status)
			assert.NotEmpty(t, req.DelegationID)

			// Accept
			err = b.AcceptDelegationRequest(req.DelegationID)
			require.NoError(t, err)

			// Associate
			err = b.AssociateDelegationRequest(req.DelegationID, "arn:aws:iam::123456789012:policy/ReadOnly")
			require.NoError(t, err)
		})
	}
}

func TestAcceptDelegationRequest_NotFound(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	err := b.AcceptDelegationRequest("non-existent-id")
	require.Error(t, err)
}

func TestAssociateDelegationRequest_NotFound(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()
	err := b.AssociateDelegationRequest("non-existent-id", "arn:aws:iam::123:policy/Test")
	require.Error(t, err)
}

func TestChangePassword_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		newPassword string
		wantErr     bool
	}{
		{
			name:        "valid_password_succeeds",
			newPassword: "NewSecureP@ss1!",
		},
		{
			name:        "empty_password_returns_error",
			newPassword: "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			err := b.ChangePassword(tt.newPassword)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHandler_AccountPasswordPolicy_CRUD(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	// UpdateAccountPasswordPolicy.
	req := iamRequest("UpdateAccountPasswordPolicy", map[string]string{
		"MinimumPasswordLength":      "12",
		"RequireUppercaseCharacters": "true",
		"RequireNumbers":             "true",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetAccountPasswordPolicy.
	req2 := iamRequest("GetAccountPasswordPolicy", map[string]string{})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "12")

	// DeleteAccountPasswordPolicy.
	req3 := iamRequest("DeleteAccountPasswordPolicy", map[string]string{})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}

func TestHandler_ChangePassword_PolicyEnforced(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, b := newTestHandler(t)
	require.NoError(t, b.UpdateAccountPasswordPolicy(iam.PasswordPolicy{
		MinimumPasswordLength: 12,
	}))

	// Short password must fail.
	req := iamRequest("ChangePassword", map[string]string{
		"OldPassword": "OldPassword1!",
		"NewPassword": "short",
	})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp iam.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidInput", errResp.Error.Code)

	// Valid password must succeed.
	req2 := iamRequest("ChangePassword", map[string]string{
		"OldPassword": "OldPassword1!",
		"NewPassword": "ValidPassword12!",
	})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
}

func TestHandler_AccountAlias_RoundTrip(t *testing.T) {
	t.Parallel()

	e := echo.New()
	h, _ := newTestHandler(t)

	req := iamRequest("CreateAccountAlias", map[string]string{"AccountAlias": "myalias"})
	rec := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req, rec)))
	assert.Equal(t, http.StatusOK, rec.Code)

	req2 := iamRequest("ListAccountAliases", map[string]string{})
	rec2 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req2, rec2)))
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "myalias")

	req3 := iamRequest("DeleteAccountAlias", map[string]string{"AccountAlias": "myalias"})
	rec3 := httptest.NewRecorder()
	require.NoError(t, h.Handler()(e.NewContext(req3, rec3)))
	assert.Equal(t, http.StatusOK, rec3.Code)
}
