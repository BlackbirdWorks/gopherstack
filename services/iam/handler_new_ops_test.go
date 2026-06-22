package iam_test

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// ---- CreateAccountAlias ----

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

// ---- CreatePolicyVersion ----

func TestCreatePolicyVersion_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(b *iam.InMemoryBackend) string
		name          string
		policyDoc     string
		wantVersionID string
		setAsDefault  bool
		wantErr       bool
	}{
		{
			name: "create_version_success",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreatePolicy(
					"ReadOnly",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)

				return p.Arn
			},
			policyDoc:     `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			wantVersionID: "v2",
		},
		{
			name: "create_version_set_as_default",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreatePolicy(
					"WritePolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)

				return p.Arn
			},
			policyDoc:     `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			setAsDefault:  true,
			wantVersionID: "v2",
		},
		{
			name: "policy_not_found_returns_error",
			setup: func(_ *iam.InMemoryBackend) string {
				return "arn:aws:iam::000000000000:policy/NonExistent"
			},
			policyDoc: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			wantErr:   true,
		},
		{
			name: "empty_document_returns_error",
			setup: func(_ *iam.InMemoryBackend) string {
				return "arn:aws:iam::000000000000:policy/SomePolicy"
			},
			policyDoc: "",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			policyArn := tt.setup(b)

			pv, err := b.CreatePolicyVersion(policyArn, tt.policyDoc, tt.setAsDefault)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, pv)
			assert.Equal(t, tt.wantVersionID, pv.VersionID)
			assert.Equal(t, tt.setAsDefault, pv.IsDefaultVersion)
		})
	}
}

// ---- CreateServiceLinkedRole ----

func TestCreateServiceLinkedRole_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup          func(b *iam.InMemoryBackend)
		name           string
		serviceName    string
		description    string
		customSuffix   string
		wantRolePrefix string
		wantErr        bool
	}{
		{
			name:           "create_role_success",
			setup:          func(_ *iam.InMemoryBackend) {},
			serviceName:    "elasticloadbalancing.amazonaws.com",
			wantRolePrefix: "AWSServiceRoleFor",
		},
		{
			name:           "create_role_with_suffix",
			setup:          func(_ *iam.InMemoryBackend) {},
			serviceName:    "ec2.amazonaws.com",
			customSuffix:   "MyApp",
			wantRolePrefix: "AWSServiceRoleFor",
		},
		{
			name:        "empty_service_name_returns_error",
			setup:       func(_ *iam.InMemoryBackend) {},
			serviceName: "",
			wantErr:     true,
		},
		{
			name: "duplicate_role_returns_error",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateServiceLinkedRole("elasticloadbalancing.amazonaws.com", "", "")
			},
			serviceName: "elasticloadbalancing.amazonaws.com",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			role, err := b.CreateServiceLinkedRole(tt.serviceName, tt.description, tt.customSuffix)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, role)
			assert.Contains(t, role.RoleName, tt.wantRolePrefix)
			assert.NotEmpty(t, role.Arn)
		})
	}
}

// ---- CreateServiceSpecificCredential ----

func TestCreateServiceSpecificCredential_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *iam.InMemoryBackend)
		name        string
		userName    string
		serviceName string
		wantErr     bool
	}{
		{
			name: "create_credential_success",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("alice", "/", "")
			},
			userName:    "alice",
			serviceName: "codecommit.amazonaws.com",
		},
		{
			name:        "user_not_found_returns_error",
			setup:       func(_ *iam.InMemoryBackend) {},
			userName:    "ghost",
			serviceName: "codecommit.amazonaws.com",
			wantErr:     true,
		},
		{
			name: "empty_service_name_returns_error",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("bob", "/", "")
			},
			userName:    "bob",
			serviceName: "",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			cred, err := b.CreateServiceSpecificCredential(tt.userName, tt.serviceName)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, cred)
			assert.Equal(t, tt.userName, cred.UserName)
			assert.Equal(t, tt.serviceName, cred.ServiceName)
			assert.NotEmpty(t, cred.ServiceSpecificCredentialID)
			assert.NotEmpty(t, cred.ServicePassword)
			assert.Equal(t, "Active", cred.Status)
		})
	}
}

// ---- CreateVirtualMFADevice ----

func TestCreateVirtualMFADevice_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *iam.InMemoryBackend)
		name       string
		deviceName string
		path       string
		wantErr    bool
	}{
		{
			name:       "create_device_success",
			setup:      func(_ *iam.InMemoryBackend) {},
			deviceName: "MyMFADevice",
			path:       "/",
		},
		{
			name:       "device_with_path",
			setup:      func(_ *iam.InMemoryBackend) {},
			deviceName: "TeamMFA",
			path:       "/team/",
		},
		{
			name:       "empty_device_name_returns_error",
			setup:      func(_ *iam.InMemoryBackend) {},
			deviceName: "",
			wantErr:    true,
		},
		{
			name: "duplicate_device_returns_error",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateVirtualMFADevice("MyMFADevice", "/")
			},
			deviceName: "MyMFADevice",
			path:       "/",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			tt.setup(b)

			device, err := b.CreateVirtualMFADevice(tt.deviceName, tt.path)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, device)
			assert.NotEmpty(t, device.SerialNumber)
			assert.Contains(t, device.SerialNumber, tt.deviceName)
		})
	}
}

// ---- Delegation Requests ----

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

// ---- ChangePassword ----

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

// ---- AddClientIDToOpenIDConnectProvider ----

func TestAddClientIDToOpenIDConnectProvider_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrType error
		setup       func(b *iam.InMemoryBackend) string
		name        string
		clientID    string
		wantErr     bool
	}{
		{
			name: "add_client_id_success",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateOpenIDConnectProvider("https://token.actions.githubusercontent.com", nil, nil)

				return p.Arn
			},
			clientID: "sts.amazonaws.com",
		},
		{
			name: "add_idempotent_client_id",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateOpenIDConnectProvider(
					"https://token.actions.githubusercontent.com",
					[]string{"existing-id"},
					nil,
				)

				return p.Arn
			},
			clientID: "existing-id",
		},
		{
			name: "provider_not_found",
			setup: func(_ *iam.InMemoryBackend) string {
				return "arn:aws:iam::000000000000:oidc-provider/nonexistent"
			},
			clientID:    "sts.amazonaws.com",
			wantErr:     true,
			wantErrType: iam.ErrOIDCProviderNotFound,
		},
		{
			name: "empty_client_id_returns_error",
			setup: func(b *iam.InMemoryBackend) string {
				p, _ := b.CreateOpenIDConnectProvider("https://token.actions.githubusercontent.com", nil, nil)

				return p.Arn
			},
			clientID: "",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iam.NewInMemoryBackend()
			providerArn := tt.setup(b)

			err := b.AddClientIDToOpenIDConnectProvider(providerArn, tt.clientID)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrType != nil {
					require.ErrorIs(t, err, tt.wantErrType)
				}

				return
			}

			require.NoError(t, err)

			// Verify the client ID was added (and is idempotent).
			p, getErr := b.GetOpenIDConnectProvider(providerArn)
			require.NoError(t, getErr)

			found := slices.Contains(p.ClientIDList, tt.clientID)
			assert.True(t, found, "clientID %q should be in ClientIDList", tt.clientID)

			// For idempotent case, ensure no duplicates.
			count := 0
			for _, id := range p.ClientIDList {
				if id == tt.clientID {
					count++
				}
			}
			assert.Equal(t, 1, count, "clientID should appear exactly once")
		})
	}
}

// ---- Handler HTTP integration tests ----

func TestIAMHandler_NewOpsDispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *iam.InMemoryBackend)
		params      map[string]string
		name        string
		action      string
		wantContain string
		wantCode    int
	}{
		// CreateAccountAlias
		{
			name:        "CreateAccountAlias_success",
			action:      "CreateAccountAlias",
			params:      map[string]string{"AccountAlias": "my-alias"},
			wantCode:    http.StatusOK,
			wantContain: "CreateAccountAliasResponse",
		},
		{
			name:     "CreateAccountAlias_empty_returns_400",
			action:   "CreateAccountAlias",
			params:   map[string]string{"AccountAlias": ""},
			wantCode: http.StatusBadRequest,
		},
		// CreatePolicyVersion
		{
			name:   "CreatePolicyVersion_success",
			action: "CreatePolicyVersion",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreatePolicy(
					"ReadOnly",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
			},
			params: map[string]string{
				"PolicyArn":      "arn:aws:iam::000000000000:policy/ReadOnly",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				"SetAsDefault":   "false",
			},
			wantCode:    http.StatusOK,
			wantContain: "CreatePolicyVersionResponse",
		},
		{
			name:   "CreatePolicyVersion_not_found_returns_400",
			action: "CreatePolicyVersion",
			params: map[string]string{
				"PolicyArn":      "arn:aws:iam::000000000000:policy/Ghost",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			},
			wantCode: http.StatusBadRequest,
		},
		// CreateServiceLinkedRole
		{
			name:   "CreateServiceLinkedRole_success",
			action: "CreateServiceLinkedRole",
			params: map[string]string{
				"AWSServiceName": "elasticloadbalancing.amazonaws.com",
			},
			wantCode:    http.StatusOK,
			wantContain: "CreateServiceLinkedRoleResponse",
		},
		{
			name:   "CreateServiceLinkedRole_empty_service_returns_400",
			action: "CreateServiceLinkedRole",
			params: map[string]string{
				"AWSServiceName": "",
			},
			wantCode: http.StatusBadRequest,
		},
		// CreateServiceSpecificCredential
		{
			name:   "CreateServiceSpecificCredential_success",
			action: "CreateServiceSpecificCredential",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("dev-user", "/", "")
			},
			params: map[string]string{
				"UserName":    "dev-user",
				"ServiceName": "codecommit.amazonaws.com",
			},
			wantCode:    http.StatusOK,
			wantContain: "CreateServiceSpecificCredentialResponse",
		},
		{
			name:   "CreateServiceSpecificCredential_user_not_found",
			action: "CreateServiceSpecificCredential",
			params: map[string]string{
				"UserName":    "ghost",
				"ServiceName": "codecommit.amazonaws.com",
			},
			wantCode: http.StatusBadRequest,
		},
		// CreateVirtualMFADevice
		{
			name:   "CreateVirtualMFADevice_success",
			action: "CreateVirtualMFADevice",
			params: map[string]string{
				"VirtualMFADeviceName": "MyDevice",
				"Path":                 "/",
			},
			wantCode:    http.StatusOK,
			wantContain: "CreateVirtualMFADeviceResponse",
		},
		{
			name:   "CreateVirtualMFADevice_empty_name_returns_400",
			action: "CreateVirtualMFADevice",
			params: map[string]string{
				"VirtualMFADeviceName": "",
			},
			wantCode: http.StatusBadRequest,
		},
		// CreateDelegationRequest
		{
			name:   "CreateDelegationRequest_success",
			action: "CreateDelegationRequest",
			params: map[string]string{
				"TargetAccountId": "111122223333",
			},
			wantCode:    http.StatusOK,
			wantContain: "CreateDelegationRequestResponse",
		},
		// AcceptDelegationRequest
		{
			name:   "AcceptDelegationRequest_not_found",
			action: "AcceptDelegationRequest",
			params: map[string]string{
				"DelegationId": "nonexistent-id",
			},
			wantCode: http.StatusBadRequest,
		},
		// AssociateDelegationRequest
		{
			name:   "AssociateDelegationRequest_not_found",
			action: "AssociateDelegationRequest",
			params: map[string]string{
				"DelegationId": "nonexistent-id",
				"PolicyArn":    "arn:aws:iam::123:policy/ReadOnly",
			},
			wantCode: http.StatusBadRequest,
		},
		// ChangePassword
		{
			name:        "ChangePassword_success",
			action:      "ChangePassword",
			params:      map[string]string{"NewPassword": "SecureP@ss1!", "OldPassword": "OldP@ss1!"},
			wantCode:    http.StatusOK,
			wantContain: "ChangePasswordResponse",
		},
		{
			name:     "ChangePassword_empty_new_password_returns_400",
			action:   "ChangePassword",
			params:   map[string]string{"NewPassword": "", "OldPassword": "OldP@ss1!"},
			wantCode: http.StatusBadRequest,
		},
		// AddClientIDToOpenIDConnectProvider
		{
			name:   "AddClientIDToOpenIDConnectProvider_success",
			action: "AddClientIDToOpenIDConnectProvider",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateOpenIDConnectProvider("https://token.actions.githubusercontent.com", nil, nil)
			},
			params: map[string]string{
				"OpenIDConnectProviderArn": "arn:aws:iam::000000000000:oidc-provider/token.actions.githubusercontent.com",
				"ClientID":                 "sts.amazonaws.com",
			},
			wantCode:    http.StatusOK,
			wantContain: "AddClientIDToOpenIDConnectProviderResponse",
		},
		{
			name:   "AddClientIDToOpenIDConnectProvider_not_found",
			action: "AddClientIDToOpenIDConnectProvider",
			params: map[string]string{
				"OpenIDConnectProviderArn": "arn:aws:iam::000000000000:oidc-provider/nonexistent",
				"ClientID":                 "sts.amazonaws.com",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContain)
			}
		})
	}
}

// ---- Persistence round-trip for new operations ----

func TestIAMHandler_ListPolicyVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params           map[string]string
		name             string
		wantBodyContains []string
		wantCode         int
		setupData        bool
	}{
		{
			name:      "returns_multiple_versions",
			setupData: true,
			params: map[string]string{
				"PolicyArn": "arn:aws:iam::000000000000:policy/VersionListPolicy",
			},
			wantCode: http.StatusOK,
			wantBodyContains: []string{
				"<VersionId>v1</VersionId>",
				"<VersionId>v2</VersionId>",
				"<IsDefaultVersion>false</IsDefaultVersion>",
				"<IsDefaultVersion>true</IsDefaultVersion>",
			},
		},
		{
			name: "policy_not_found_returns_bad_request",
			params: map[string]string{
				"PolicyArn": "arn:aws:iam::000000000000:policy/VersionListPolicy",
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			if tt.setupData {
				pol, setupErr := b.CreatePolicy(
					"VersionListPolicy",
					"/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
				require.NoError(t, setupErr)
				_, setupErr = b.CreatePolicyVersion(
					pol.Arn,
					`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`,
					true,
				)
				require.NoError(t, setupErr)
			}

			req := iamRequest("ListPolicyVersions", tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, expected := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), expected)
			}
		})
	}
}

func TestNewOps_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := iam.NewInMemoryBackend()

	// Seed data for all new ops.
	require.NoError(t, b.CreateAccountAlias("test-alias"))

	pol, err := b.CreatePolicy(
		"VersionedPolicy",
		"/",
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
	)
	require.NoError(t, err)
	policyArn := pol.Arn

	_, err = b.CreatePolicyVersion(
		policyArn,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
		true,
	)
	require.NoError(t, err)

	_, err = b.CreateUser("svc-user", "/", "")
	require.NoError(t, err)
	_, err = b.CreateServiceSpecificCredential("svc-user", "codecommit.amazonaws.com")
	require.NoError(t, err)

	_, err = b.CreateVirtualMFADevice("MyMFA", "/")
	require.NoError(t, err)

	req, err := b.CreateDelegationRequest("111122223333")
	require.NoError(t, err)
	require.NoError(t, b.AcceptDelegationRequest(req.DelegationID))

	// Snapshot and restore.
	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := iam.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Creating another version should work as there's already 1 extra version.
	pv, err := b2.CreatePolicyVersion(
		policyArn,
		`{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}`,
		false,
	)
	require.NoError(t, err)
	assert.Equal(t, "v3", pv.VersionID)
}
