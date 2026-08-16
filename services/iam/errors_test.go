package iam_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

// TestErrorSentinels_Distinctness verifies that each "not found" sentinel
// has a unique error message, enabling message-based inspection to determine which
// resource type was missing. All sentinels share the "NoSuchEntity" prefix (matching
// the AWS error code) but carry a distinct resource-type suffix.
func TestErrorSentinels_Distinctness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sentinel error
		name     string
		wantMsg  string
	}{
		{
			name:     "user_not_found",
			sentinel: iam.ErrUserNotFound,
			wantMsg:  "NoSuchEntity: user",
		},
		{
			name:     "role_not_found",
			sentinel: iam.ErrRoleNotFound,
			wantMsg:  "NoSuchEntity: role",
		},
		{
			name:     "policy_not_found",
			sentinel: iam.ErrPolicyNotFound,
			wantMsg:  "NoSuchEntity: policy",
		},
		{
			name:     "group_not_found",
			sentinel: iam.ErrGroupNotFound,
			wantMsg:  "NoSuchEntity: group",
		},
		{
			name:     "access_key_not_found",
			sentinel: iam.ErrAccessKeyNotFound,
			wantMsg:  "NoSuchEntity: access key",
		},
		{
			name:     "instance_profile_not_found",
			sentinel: iam.ErrInstanceProfileNotFound,
			wantMsg:  "NoSuchEntity: instance profile",
		},
		{
			name:     "inline_policy_not_found",
			sentinel: iam.ErrInlinePolicyNotFound,
			wantMsg:  "NoSuchEntity: inline policy",
		},
		{
			name:     "saml_provider_not_found",
			sentinel: iam.ErrSAMLProviderNotFound,
			wantMsg:  "NoSuchEntity: SAML provider",
		},
		{
			name:     "oidc_provider_not_found",
			sentinel: iam.ErrOIDCProviderNotFound,
			wantMsg:  "NoSuchEntity: OIDC provider",
		},
		{
			name:     "login_profile_not_found",
			sentinel: iam.ErrLoginProfileNotFound,
			wantMsg:  "NoSuchEntity: login profile",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.wantMsg, tt.sentinel.Error(),
				"sentinel message must be distinct for message-based inspection")
		})
	}
}

// TestErrorSentinels_Uniqueness verifies that each "not found" sentinel
// is a distinct Go error value (pointer inequality), so errors.Is can distinguish them.
func TestErrorSentinels_Uniqueness(t *testing.T) {
	t.Parallel()

	type namedErr struct {
		err  error
		name string
	}

	sentinels := []namedErr{
		{name: "user", err: iam.ErrUserNotFound},
		{name: "role", err: iam.ErrRoleNotFound},
		{name: "policy", err: iam.ErrPolicyNotFound},
		{name: "group", err: iam.ErrGroupNotFound},
		{name: "access_key", err: iam.ErrAccessKeyNotFound},
		{name: "instance_profile", err: iam.ErrInstanceProfileNotFound},
		{name: "inline_policy", err: iam.ErrInlinePolicyNotFound},
		{name: "saml_provider", err: iam.ErrSAMLProviderNotFound},
		{name: "oidc_provider", err: iam.ErrOIDCProviderNotFound},
		{name: "login_profile", err: iam.ErrLoginProfileNotFound},
	}

	for i, a := range sentinels {
		for j, b := range sentinels {
			if i == j {
				continue
			}

			t.Run(fmt.Sprintf("%s_ne_%s", a.name, b.name), func(t *testing.T) {
				t.Parallel()
				assert.NotErrorIs(t, a.err, b.err,
					"sentinels %q and %q must not match via errors.Is", a.name, b.name)
			})
		}
	}
}

// TestErrorSentinels_Wrapping verifies that backend errors wrap the
// correct sentinel so callers can use errors.Is to detect which resource type
// was missing — without relying on message text.
func TestErrorSentinels_Wrapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		triggerErr  func(b *iam.InMemoryBackend) error
		sentinel    error
		notSentinel error
		name        string
	}{
		{
			name:        "get_user_wraps_ErrUserNotFound",
			sentinel:    iam.ErrUserNotFound,
			notSentinel: iam.ErrRoleNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.GetUser("ghost")

				return err
			},
		},
		{
			name:        "get_role_wraps_ErrRoleNotFound",
			sentinel:    iam.ErrRoleNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.GetRole("ghost")

				return err
			},
		},
		{
			name:        "get_policy_wraps_ErrPolicyNotFound",
			sentinel:    iam.ErrPolicyNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.GetPolicy("arn:aws:iam::123456789012:policy/ghost")

				return err
			},
		},
		{
			name:        "get_group_wraps_ErrGroupNotFound",
			sentinel:    iam.ErrGroupNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.GetGroup("ghost")

				return err
			},
		},
		{
			name:        "delete_access_key_wraps_ErrAccessKeyNotFound",
			sentinel:    iam.ErrAccessKeyNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.CreateUser("u1", "/", "")
				require.NoError(t, err)

				return b.DeleteAccessKey("u1", "AKIAXXXXXXXXXXXXXXXX")
			},
		},
		{
			name:        "delete_instance_profile_wraps_ErrInstanceProfileNotFound",
			sentinel:    iam.ErrInstanceProfileNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				return b.DeleteInstanceProfile("ghost")
			},
		},
		{
			name:        "get_user_policy_wraps_ErrInlinePolicyNotFound",
			sentinel:    iam.ErrInlinePolicyNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.CreateUser("u2", "/", "")
				require.NoError(t, err)
				_, err = b.GetUserPolicy("u2", "ghost-policy")

				return err
			},
		},
		{
			name:        "get_login_profile_wraps_ErrLoginProfileNotFound",
			sentinel:    iam.ErrLoginProfileNotFound,
			notSentinel: iam.ErrUserNotFound,
			triggerErr: func(b *iam.InMemoryBackend) error {
				_, err := b.CreateUser("u3", "/", "")
				require.NoError(t, err)
				_, err = b.GetLoginProfile("u3")

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := iam.NewInMemoryBackend()
			err := tt.triggerErr(b)
			require.Error(t, err, "expected an error")
			require.ErrorIs(t, err, tt.sentinel,
				"error %q must wrap sentinel %q", err, tt.sentinel)
			assert.NotErrorIs(t, err, tt.notSentinel,
				"error %q must NOT wrap sentinel %q", err, tt.notSentinel)
		})
	}
}

// TestHandler_NoSuchEntityCode verifies that all "not found" errors
// translate to the "NoSuchEntity" XML error code in HTTP responses, matching AWS.
// Real AWS IAM returns HTTP 404 for NoSuchEntity even though the body is XML
// (query protocol): every per-operation error reference page (e.g. GetRole,
// CreateUser, DeleteUser) documents "NoSuchEntity ... HTTP Status Code: 404".
// See https://docs.aws.amazon.com/IAM/latest/APIReference/API_GetRole.html#API_GetRole_Errors.
func TestHandler_NoSuchEntityCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params map[string]string
		name   string
		action string
	}{
		{
			name:   "get_user_not_found",
			action: "GetUser",
			params: map[string]string{"UserName": "ghost"},
		},
		{
			name:   "get_role_not_found",
			action: "GetRole",
			params: map[string]string{"RoleName": "ghost"},
		},
		{
			name:   "get_policy_not_found",
			action: "GetPolicy",
			params: map[string]string{"PolicyArn": "arn:aws:iam::123456789012:policy/ghost"},
		},
		{
			name:   "get_group_not_found",
			action: "GetGroup",
			params: map[string]string{"GroupName": "ghost"},
		},
		{
			name:   "get_instance_profile_not_found",
			action: "GetInstanceProfile",
			params: map[string]string{"InstanceProfileName": "ghost"},
		},
		{
			name:   "get_login_profile_not_found",
			action: "GetLoginProfile",
			params: map[string]string{"UserName": "ghost"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newTestHandler(t)
			e := echo.New()
			req := iamRequest(tt.action, tt.params)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusNotFound, rec.Code,
				"action %s must return 404 for NoSuchEntity", tt.action)

			var errResp iam.ErrorResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, "NoSuchEntity", errResp.Error.Code,
				"action %s must return NoSuchEntity error code", tt.action)
		})
	}
}
