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
		oldPassword string
		newPassword string
		wantErr     bool
	}{
		{
			name:        "valid password succeeds",
			oldPassword: "OldSecureP@ss1!",
			newPassword: "NewSecureP@ss1!",
		},
		{
			name:        "empty new password returns error",
			oldPassword: "OldSecureP@ss1!",
			newPassword: "",
			wantErr:     true,
		},
		{
			name:        "empty old password returns error",
			oldPassword: "",
			newPassword: "NewSecureP@ss1!",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			err := b.ChangePassword(tt.oldPassword, tt.newPassword)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestChangePassword_WrongOldPasswordRejected proves that once an account has a
// current password on file, a ChangePassword call presenting a non-matching
// OldPassword is rejected instead of silently succeeding. Real IAM requires
// OldPassword (iam@v1.58.1 api_op_ChangePassword.go:57-59) to prove the caller
// knows the current password before setting a new one.
func TestChangePassword_WrongOldPasswordRejected(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()

	require.NoError(t, b.ChangePassword("FirstP@ssw0rd!", "SecondP@ssw0rd!"),
		"first ChangePassword call establishes the current password")

	err := b.ChangePassword("WrongP@ssw0rd!", "ThirdP@ssw0rd!")
	require.Error(t, err)
	require.ErrorIs(t, err, iam.ErrOldPasswordIncorrect,
		"a non-matching OldPassword must be rejected, not silently accepted")

	require.NoError(t, b.ChangePassword("SecondP@ssw0rd!", "ThirdP@ssw0rd!"),
		"the real current password must still be accepted")
}

// TestHandler_ChangePassword_OldPasswordOnWire proves the handler reads
// OldPassword off the raw form request rather than dropping it: the real
// current password must be accepted (200) and a wrong one rejected as
// PasswordPolicyViolation (iam@v1.58.1 deserializers.go:766-816 lists it as
// one of ChangePassword's real exception codes) — not a silent 200 OK
// regardless of what OldPassword the caller sends. If the handler stops
// reading "OldPassword" from vals (e.g. reverts to always passing ""), the
// "correct old password" case starts failing because the backend would see
// an empty OldPassword instead of the real one.
func TestHandler_ChangePassword_OldPasswordOnWire(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		oldPassword string
		wantCode2   string
		wantCode    int
	}{
		{
			name:        "correct old password accepted",
			oldPassword: "FirstP@ssw0rd!",
			wantCode:    http.StatusOK,
		},
		{
			name:        "wrong old password rejected",
			oldPassword: "WrongP@ssw0rd!",
			wantCode:    http.StatusBadRequest,
			wantCode2:   "PasswordPolicyViolation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			require.NoError(t, b.ChangePassword("FirstP@ssw0rd!", "FirstP@ssw0rd!"))

			req := iamRequest("ChangePassword", map[string]string{
				"OldPassword": tt.oldPassword,
				"NewPassword": "ThirdP@ssw0rd!",
			})
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			require.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())

			if tt.wantCode2 == "" {
				return
			}

			var errResp iam.ErrorResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantCode2, errResp.Error.Code)
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

// TestOutboundWebIdentityFederation_Handler exercises Enable/Disable
// OutboundWebIdentityFederation and GetOutboundWebIdentityFederationInfo over
// HTTP, verifying the real SDK's wire field names (IssuerIdentifier,
// JwtVendingEnabled -- confirmed against aws-sdk-go-v2/service/iam@v1.55.0's
// api_op_{Enable,Disable,GetOutboundWebIdentityFederationInfo}.go) appear on
// the wire instead of the previous no-op stub's generic empty body.
func TestOutboundWebIdentityFederation_Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*iam.InMemoryBackend)
		name        string
		action      string
		wantContain []string
		wantCode    int
	}{
		{
			name:   "GetOutboundWebIdentityFederationInfo_default_enabled",
			action: "GetOutboundWebIdentityFederationInfo",
			setup:  func(_ *iam.InMemoryBackend) {},
			wantContain: []string{
				"GetOutboundWebIdentityFederationInfoResponse",
				"<JwtVendingEnabled>true</JwtVendingEnabled>",
				"<IssuerIdentifier>https://",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "DisableOutboundWebIdentityFederation",
			action: "DisableOutboundWebIdentityFederation",
			setup:  func(_ *iam.InMemoryBackend) {},
			wantContain: []string{
				"DisableOutboundWebIdentityFederationResponse",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "GetOutboundWebIdentityFederationInfo_after_disable",
			action: "GetOutboundWebIdentityFederationInfo",
			setup: func(b *iam.InMemoryBackend) {
				b.DisableOutboundWebIdentityFederation()
			},
			wantContain: []string{
				"<JwtVendingEnabled>false</JwtVendingEnabled>",
			},
			wantCode: http.StatusOK,
		},
		{
			name:   "EnableOutboundWebIdentityFederation",
			action: "EnableOutboundWebIdentityFederation",
			setup: func(b *iam.InMemoryBackend) {
				b.DisableOutboundWebIdentityFederation()
			},
			wantContain: []string{
				"EnableOutboundWebIdentityFederationResponse",
				"<IssuerIdentifier>https://",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)
			h := iam.NewHandler(b)

			e := echo.New()
			req := iamRequest(tt.action, map[string]string{})
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, want := range tt.wantContain {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}
