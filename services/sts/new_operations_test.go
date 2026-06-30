package sts_test

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// makeJWT constructs a minimal unsigned JWT with the given JSON payload (no signature validation needed in mock).
func makeJWT(payload string) string {
	// header: {"alg":"none","typ":"JWT"}
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	encodedPayload := base64.RawURLEncoding.EncodeToString([]byte(payload))

	return header + "." + encodedPayload + ".sig"
}

// ---- GetFederationToken tests ----------------------------------------------

func TestGetFederationToken_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    *sts.GetFederationTokenInput
		wantName string
	}{
		{
			name:     "default_duration",
			input:    &sts.GetFederationTokenInput{Name: "alice"},
			wantName: "alice",
		},
		{
			name:     "custom_duration",
			input:    &sts.GetFederationTokenInput{Name: "bob", DurationSeconds: 3600},
			wantName: "bob",
		},
		{
			name: "max_duration",
			input: &sts.GetFederationTokenInput{
				Name:            "charlie",
				DurationSeconds: sts.MaxFederationTokenDurationSeconds,
			},
			wantName: "charlie",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.GetFederationToken(tt.input)
			require.NoError(t, err)
			require.NotNil(t, resp)

			creds := resp.GetFederationTokenResult.Credentials
			assert.True(t, strings.HasPrefix(creds.AccessKeyID, "ASIA"))
			assert.NotEmpty(t, creds.SecretAccessKey)
			assert.NotEmpty(t, creds.SessionToken)
			assert.NotEmpty(t, creds.Expiration)

			fu := resp.GetFederationTokenResult.FederatedUser
			assert.Contains(t, fu.Arn, "federated-user/"+tt.wantName)
			assert.Contains(t, fu.FederatedUserID, tt.wantName)
			assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
		})
	}
}

func TestGetFederationToken_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		input   *sts.GetFederationTokenInput
		name    string
	}{
		{
			name:    "missing_name",
			input:   &sts.GetFederationTokenInput{},
			wantErr: sts.ErrMissingFederationTokenName,
		},
		{
			name:    "duration_too_short",
			input:   &sts.GetFederationTokenInput{Name: "user", DurationSeconds: 100},
			wantErr: sts.ErrInvalidDuration,
		},
		{
			name: "duration_too_long",
			input: &sts.GetFederationTokenInput{
				Name:            "user",
				DurationSeconds: sts.MaxFederationTokenDurationSeconds + 1,
			},
			wantErr: sts.ErrInvalidDuration,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.GetFederationToken(tt.input)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestGetFederationToken_SessionTrackedForCallerIdentity(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.GetFederationToken(&sts.GetFederationTokenInput{Name: "feduser"})
	require.NoError(t, err)

	accessKeyID := resp.GetFederationTokenResult.Credentials.AccessKeyID

	ciResp, err := b.GetCallerIdentity(accessKeyID, "")
	require.NoError(t, err)
	assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "federated-user/feduser")
	assert.Contains(t, ciResp.GetCallerIdentityResult.UserID, "feduser")
	assert.Equal(t, 1, b.SessionCount())
}

func TestHandler_GetFederationToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		formValues url.Values
		name       string
		wantCode   string
		wantStatus int
	}{
		{
			name: "success",
			formValues: url.Values{
				"Action":  {"GetFederationToken"},
				"Version": {"2011-06-15"},
				"Name":    {"testuser"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_name_returns_400",
			formValues: url.Values{
				"Action":  {"GetFederationToken"},
				"Version": {"2011-06-15"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name: "invalid_duration_returns_400",
			formValues: url.Values{
				"Action":          {"GetFederationToken"},
				"Version":         {"2011-06-15"},
				"Name":            {"testuser"},
				"DurationSeconds": {"100"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "ValidationError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			h := sts.NewHandler(b)
			e := echo.New()

			rec := postForm(t, e, h, tt.formValues)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCode != "" {
				var errResp sts.ErrorResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantCode, errResp.Error.Code)
			} else {
				var resp sts.GetFederationTokenResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.GetFederationTokenResult.Credentials.AccessKeyID)
				assert.Contains(t, resp.GetFederationTokenResult.FederatedUser.Arn, "federated-user/testuser")
			}
		})
	}
}

// ---- AssumeRoleWithWebIdentity tests ----------------------------------------

func TestAssumeRoleWithWebIdentity_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		input       *sts.AssumeRoleWithWebIdentityInput
		wantSubject string
	}{
		{
			name: "opaque_token_uses_placeholder",
			input: &sts.AssumeRoleWithWebIdentityInput{
				RoleArn:          "arn:aws:iam::123456789012:role/WebRole",
				RoleSessionName:  "web-session",
				WebIdentityToken: "some-opaque-token",
			},
			wantSubject: "WebIdentitySubject",
		},
		{
			name: "jwt_token_subject_extracted",
			input: &sts.AssumeRoleWithWebIdentityInput{
				RoleArn:          "arn:aws:iam::123456789012:role/WebRole",
				RoleSessionName:  "jwt-session",
				WebIdentityToken: makeJWT(`{"sub":"user-12345","aud":"my-app"}`),
			},
			wantSubject: "user-12345",
		},
		{
			name: "custom_provider_is_used",
			input: &sts.AssumeRoleWithWebIdentityInput{
				RoleArn:          "arn:aws:iam::123456789012:role/WebRole",
				RoleSessionName:  "provider-session",
				WebIdentityToken: "token",
				ProviderID:       "www.amazon.com",
			},
			wantSubject: "WebIdentitySubject",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.AssumeRoleWithWebIdentity(tt.input)
			require.NoError(t, err)
			require.NotNil(t, resp)

			res := resp.AssumeRoleWithWebIdentityResult
			assert.True(t, strings.HasPrefix(res.Credentials.AccessKeyID, "ASIA"))
			assert.NotEmpty(t, res.Credentials.SecretAccessKey)
			assert.NotEmpty(t, res.Credentials.SessionToken)
			assert.NotEmpty(t, res.Credentials.Expiration)
			assert.Contains(t, res.AssumedRoleUser.Arn, "assumed-role")
			assert.Contains(t, res.AssumedRoleUser.Arn, tt.input.RoleSessionName)
			assert.Equal(t, tt.wantSubject, res.SubjectFromWebIdentityToken)
			assert.NotEmpty(t, res.Provider)
		})
	}
}

func TestAssumeRoleWithWebIdentity_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		input   *sts.AssumeRoleWithWebIdentityInput
		name    string
	}{
		{
			name: "missing_role_arn",
			input: &sts.AssumeRoleWithWebIdentityInput{
				RoleSessionName:  "session",
				WebIdentityToken: "token",
			},
			wantErr: sts.ErrMissingRoleArn,
		},
		{
			name: "missing_session_name",
			input: &sts.AssumeRoleWithWebIdentityInput{
				RoleArn:          "arn:aws:iam::123456789012:role/WebRole",
				WebIdentityToken: "token",
			},
			wantErr: sts.ErrMissingSessionName,
		},
		{
			name: "missing_web_identity_token",
			input: &sts.AssumeRoleWithWebIdentityInput{
				RoleArn:         "arn:aws:iam::123456789012:role/WebRole",
				RoleSessionName: "session",
			},
			wantErr: sts.ErrMissingWebIdentityToken,
		},
		{
			name: "duration_too_short",
			input: &sts.AssumeRoleWithWebIdentityInput{
				RoleArn:          "arn:aws:iam::123456789012:role/WebRole",
				RoleSessionName:  "session",
				WebIdentityToken: "token",
				DurationSeconds:  100,
			},
			wantErr: sts.ErrInvalidDuration,
		},
		{
			name: "duration_too_long",
			input: &sts.AssumeRoleWithWebIdentityInput{
				RoleArn:          "arn:aws:iam::123456789012:role/WebRole",
				RoleSessionName:  "session",
				WebIdentityToken: "token",
				DurationSeconds:  sts.MaxDurationSeconds + 1,
			},
			wantErr: sts.ErrInvalidDuration,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.AssumeRoleWithWebIdentity(tt.input)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestAssumeRoleWithWebIdentity_SessionTrackedForCallerIdentity(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
		RoleArn:          "arn:aws:iam::123456789012:role/WebRole",
		RoleSessionName:  "web-session",
		WebIdentityToken: "some-token",
	})
	require.NoError(t, err)

	accessKeyID := resp.AssumeRoleWithWebIdentityResult.Credentials.AccessKeyID

	ciResp, err := b.GetCallerIdentity(accessKeyID, "")
	require.NoError(t, err)
	assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "assumed-role")
	assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "WebRole")
}

func TestHandler_AssumeRoleWithWebIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		formValues url.Values
		name       string
		wantCode   string
		wantStatus int
	}{
		{
			name: "success",
			formValues: url.Values{
				"Action":           {"AssumeRoleWithWebIdentity"},
				"Version":          {"2011-06-15"},
				"RoleArn":          {"arn:aws:iam::123456789012:role/WebRole"},
				"RoleSessionName":  {"web-session"},
				"WebIdentityToken": {"some-token"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_web_identity_token_returns_400",
			formValues: url.Values{
				"Action":          {"AssumeRoleWithWebIdentity"},
				"Version":         {"2011-06-15"},
				"RoleArn":         {"arn:aws:iam::123456789012:role/WebRole"},
				"RoleSessionName": {"session"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name: "missing_role_arn_returns_400",
			formValues: url.Values{
				"Action":           {"AssumeRoleWithWebIdentity"},
				"Version":          {"2011-06-15"},
				"RoleSessionName":  {"session"},
				"WebIdentityToken": {"token"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			h := sts.NewHandler(b)
			e := echo.New()

			rec := postForm(t, e, h, tt.formValues)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCode != "" {
				var errResp sts.ErrorResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantCode, errResp.Error.Code)
			} else {
				var resp sts.AssumeRoleWithWebIdentityResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.AssumeRoleWithWebIdentityResult.Credentials.AccessKeyID)
				assert.Contains(t, resp.AssumeRoleWithWebIdentityResult.AssumedRoleUser.Arn, "assumed-role")
			}
		})
	}
}
