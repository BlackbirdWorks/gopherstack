package iam_test

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

func TestIAMHandler_GroupProfileAccountDispatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(b *iam.InMemoryBackend)
		params      map[string]string
		name        string
		action      string
		wantContain string
		wantCode    int
	}{
		{
			name:   "GetGroup_success",
			action: "GetGroup",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("my-group", "/")
			},
			params:      map[string]string{"GroupName": "my-group"},
			wantCode:    http.StatusOK,
			wantContain: "GetGroupResponse",
		},
		{
			name:   "GetGroup_not_found",
			action: "GetGroup",
			params: map[string]string{"GroupName": "ghost"},
			// NoSuchEntity is 404 on real AWS IAM.
			wantCode: http.StatusNotFound,
		},
		{
			name:   "RemoveUserFromGroup_success",
			action: "RemoveUserFromGroup",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateGroup("grp", "/")
				_, _ = b.CreateUser("usr", "/", "")
				_ = b.AddUserToGroup("grp", "usr")
			},
			params: map[string]string{
				"GroupName": "grp",
				"UserName":  "usr",
			},
			wantCode:    http.StatusOK,
			wantContain: "RemoveUserFromGroupResponse",
		},
		{
			name:   "AddRoleToInstanceProfile_success",
			action: "AddRoleToInstanceProfile",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateInstanceProfile("my-profile", "/")
				_, _ = b.CreateRole("my-role", "/", "{}", "")
			},
			params: map[string]string{
				"InstanceProfileName": "my-profile",
				"RoleName":            "my-role",
			},
			wantCode:    http.StatusOK,
			wantContain: "AddRoleToInstanceProfileResponse",
		},
		{
			name:   "AddRoleToInstanceProfile_profile_not_found",
			action: "AddRoleToInstanceProfile",
			params: map[string]string{
				"InstanceProfileName": "ghost-profile",
				"RoleName":            "some-role",
			},
			// NoSuchEntity is 404 on real AWS IAM.
			wantCode: http.StatusNotFound,
		},
		{
			name:   "RemoveRoleFromInstanceProfile_success",
			action: "RemoveRoleFromInstanceProfile",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateInstanceProfile("my-profile", "/")
				_, _ = b.CreateRole("my-role", "/", "{}", "")
				_ = b.AddRoleToInstanceProfile("my-profile", "my-role")
			},
			params: map[string]string{
				"InstanceProfileName": "my-profile",
				"RoleName":            "my-role",
			},
			wantCode:    http.StatusOK,
			wantContain: "RemoveRoleFromInstanceProfileResponse",
		},
		{
			name:        "GetAccountSummary_success",
			action:      "GetAccountSummary",
			wantCode:    http.StatusOK,
			wantContain: "GetAccountSummaryResponse",
		},
		{
			name:   "CreateLoginProfile_empty_password_returns_400",
			action: "CreateLoginProfile",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("pass-user", "/", "")
			},
			params: map[string]string{
				"UserName": "pass-user",
				"Password": "",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "CreateLoginProfile_with_password_success",
			action: "CreateLoginProfile",
			setup: func(b *iam.InMemoryBackend) {
				_, _ = b.CreateUser("pass-user2", "/", "")
			},
			params: map[string]string{
				"UserName": "pass-user2",
				"Password": "SecureP@ss1!",
			},
			wantCode:    http.StatusOK,
			wantContain: "CreateLoginProfileResponse",
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

// iamCall wraps the IAM test pattern: create context, run handler, return recorder.

func Test_ErrorHTTPStatusCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, b *iam.InMemoryBackend)
		params     map[string]string
		name       string
		action     string
		wantCode   string
		wantStatus int
	}{
		{
			name:       "NoSuchEntity_is_404",
			action:     "GetUser",
			params:     map[string]string{"UserName": "ghost"},
			setup:      func(_ *testing.T, _ *iam.InMemoryBackend) {},
			wantCode:   "NoSuchEntity",
			wantStatus: http.StatusNotFound,
		},
		{
			name:   "EntityAlreadyExists_is_409",
			action: "CreateUser",
			params: map[string]string{"UserName": "dup"},
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateUser("dup", "/", "")
				require.NoError(t, err)
			},
			wantCode:   "EntityAlreadyExists",
			wantStatus: http.StatusConflict,
		},
		{
			name:   "DeleteConflict_is_409",
			action: "DeleteUser",
			params: map[string]string{"UserName": "attached"},
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateUser("attached", "/", "")
				require.NoError(t, err)
				pol, err := b.CreatePolicy(
					"StuckPolicy", "/",
					`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
				)
				require.NoError(t, err)
				require.NoError(t, b.AttachUserPolicy("attached", pol.Arn))
			},
			wantCode:   "DeleteConflict",
			wantStatus: http.StatusConflict,
		},
		{
			name:   "LimitExceeded_is_409",
			action: "CreateAccessKey",
			params: map[string]string{"UserName": "keyed"},
			setup: func(t *testing.T, b *iam.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateUser("keyed", "/", "")
				require.NoError(t, err)
				_, err = b.CreateAccessKey("keyed")
				require.NoError(t, err)
				_, err = b.CreateAccessKey("keyed")
				require.NoError(t, err)
			},
			wantCode:   "LimitExceeded",
			wantStatus: http.StatusConflict,
		},
		{
			name:       "MalformedPolicyDocument_is_400",
			action:     "CreatePolicy",
			params:     map[string]string{"PolicyName": "Bad", "PolicyDocument": "not-json"},
			setup:      func(_ *testing.T, _ *iam.InMemoryBackend) {},
			wantCode:   "MalformedPolicyDocument",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h, b := newTestHandler(t)
			tt.setup(t, b)

			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.wantStatus, rec.Code)

			var errResp iam.ErrorResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantCode, errResp.Error.Code)
		})
	}
}

// mergeParams returns a new map containing all entries of base and extra.

func TestIAMHandler_AdditionalActionsDispatch(t *testing.T) {
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
			name:   "CreatePolicyVersion_not_found_returns_404",
			action: "CreatePolicyVersion",
			params: map[string]string{
				"PolicyArn":      "arn:aws:iam::000000000000:policy/Ghost",
				"PolicyDocument": `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`,
			},
			// NoSuchEntity is 404 on real AWS IAM.
			wantCode: http.StatusNotFound,
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
			// NoSuchEntity is 404 on real AWS IAM.
			wantCode: http.StatusNotFound,
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
				"OwnerAccountId":                "111122223333",
				"Description":                   "test delegation",
				"NotificationChannel":           "arn:aws:sns:us-east-1:000000000000:topic",
				"RequestorWorkflowId":           "workflow-1",
				"SessionDuration":               "3600",
				"Permissions.PolicyTemplateArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
			},
			wantCode:    http.StatusOK,
			wantContain: "CreateDelegationRequestResponse",
		},
		// AcceptDelegationRequest declares NoSuchEntity (404), not InvalidAction (400),
		// for an unknown DelegationRequestId (api_op_AcceptDelegationRequest.go
		// deserializeOpError switch: ConcurrentModification/NoSuchEntity/ServiceFailure).
		{
			name:   "AcceptDelegationRequest_not_found",
			action: "AcceptDelegationRequest",
			params: map[string]string{
				"DelegationRequestId": "nonexistent-id",
			},
			wantCode:    http.StatusNotFound,
			wantContain: "NoSuchEntity",
		},
		// AssociateDelegationRequest declares NoSuchEntity (404) the same way; the real
		// AssociateDelegationRequestInput has no PolicyArn (api_op_AssociateDelegationRequest.go
		// carries only DelegationRequestId), so none is sent here.
		{
			name:   "AssociateDelegationRequest_not_found",
			action: "AssociateDelegationRequest",
			params: map[string]string{
				"DelegationRequestId": "nonexistent-id",
			},
			wantCode:    http.StatusNotFound,
			wantContain: "NoSuchEntity",
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
			// NoSuchEntity is 404 on real AWS IAM.
			wantCode: http.StatusNotFound,
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
