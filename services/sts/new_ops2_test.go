package sts_test

import (
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

// ---- AssumeRoleWithSAML tests -----------------------------------------------

func TestAssumeRoleWithSAML_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input *sts.AssumeRoleWithSAMLInput
		name  string
	}{
		{
			name: "default_duration",
			input: &sts.AssumeRoleWithSAMLInput{
				RoleArn:       "arn:aws:iam::123456789012:role/SAMLRole",
				PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
				SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4NCjwvc2FtbHA6QXNzZXJ0aW9uPg==",
			},
		},
		{
			name: "custom_duration",
			input: &sts.AssumeRoleWithSAMLInput{
				RoleArn:         "arn:aws:iam::123456789012:role/SAMLRole",
				PrincipalArn:    "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
				SAMLAssertion:   "PHNhbWxwOkFzc2VydGlvbj4NCjwvc2FtbHA6QXNzZXJ0aW9uPg==",
				DurationSeconds: 1800,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.AssumeRoleWithSAML(tt.input)
			require.NoError(t, err)
			require.NotNil(t, resp)

			res := resp.AssumeRoleWithSAMLResult
			assert.True(t, strings.HasPrefix(res.Credentials.AccessKeyID, "ASIA"))
			assert.NotEmpty(t, res.Credentials.SecretAccessKey)
			assert.NotEmpty(t, res.Credentials.SessionToken)
			assert.NotEmpty(t, res.Credentials.Expiration)
			assert.Contains(t, res.AssumedRoleUser.Arn, "assumed-role")
			assert.Contains(t, res.AssumedRoleUser.Arn, "SAMLRole")
			assert.NotEmpty(t, res.Audience)
			assert.NotEmpty(t, res.Issuer)
			assert.NotEmpty(t, res.Subject)
			assert.NotEmpty(t, res.SubjectType)
			assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
			assert.Equal(t, sts.STSNamespace, resp.Xmlns)
		})
	}
}

func TestAssumeRoleWithSAML_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		input   *sts.AssumeRoleWithSAMLInput
		name    string
	}{
		{
			name: "missing_role_arn",
			input: &sts.AssumeRoleWithSAMLInput{
				PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
				SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
			},
			wantErr: sts.ErrMissingRoleArn,
		},
		{
			name: "missing_principal_arn",
			input: &sts.AssumeRoleWithSAMLInput{
				RoleArn:       "arn:aws:iam::123456789012:role/SAMLRole",
				SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
			},
			wantErr: sts.ErrMissingPrincipalArn,
		},
		{
			name: "missing_saml_assertion",
			input: &sts.AssumeRoleWithSAMLInput{
				RoleArn:      "arn:aws:iam::123456789012:role/SAMLRole",
				PrincipalArn: "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
			},
			wantErr: sts.ErrMissingSAMLAssertion,
		},
		{
			name: "duration_too_short",
			input: &sts.AssumeRoleWithSAMLInput{
				RoleArn:         "arn:aws:iam::123456789012:role/SAMLRole",
				PrincipalArn:    "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
				SAMLAssertion:   "PHNhbWxwOkFzc2VydGlvbj4=",
				DurationSeconds: 100,
			},
			wantErr: sts.ErrInvalidDuration,
		},
		{
			name: "duration_too_long",
			input: &sts.AssumeRoleWithSAMLInput{
				RoleArn:         "arn:aws:iam::123456789012:role/SAMLRole",
				PrincipalArn:    "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
				SAMLAssertion:   "PHNhbWxwOkFzc2VydGlvbj4=",
				DurationSeconds: sts.MaxDurationSeconds + 1,
			},
			wantErr: sts.ErrInvalidDuration,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.AssumeRoleWithSAML(tt.input)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestAssumeRoleWithSAML_SessionTrackedForCallerIdentity(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
		RoleArn:       "arn:aws:iam::123456789012:role/SAMLRole",
		PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MySAMLIdP",
		SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
	})
	require.NoError(t, err)

	accessKeyID := resp.AssumeRoleWithSAMLResult.Credentials.AccessKeyID

	ciResp, err := b.GetCallerIdentity(accessKeyID, "")
	require.NoError(t, err)
	assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "assumed-role")
}

func TestHandler_AssumeRoleWithSAML(t *testing.T) {
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
				"Action":        {"AssumeRoleWithSAML"},
				"Version":       {"2011-06-15"},
				"RoleArn":       {"arn:aws:iam::123456789012:role/SAMLRole"},
				"PrincipalArn":  {"arn:aws:iam::123456789012:saml-provider/MySAMLIdP"},
				"SAMLAssertion": {"PHNhbWxwOkFzc2VydGlvbj4="},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_principal_arn_returns_400",
			formValues: url.Values{
				"Action":        {"AssumeRoleWithSAML"},
				"Version":       {"2011-06-15"},
				"RoleArn":       {"arn:aws:iam::123456789012:role/SAMLRole"},
				"SAMLAssertion": {"PHNhbWxwOkFzc2VydGlvbj4="},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name: "missing_saml_assertion_returns_400",
			formValues: url.Values{
				"Action":       {"AssumeRoleWithSAML"},
				"Version":      {"2011-06-15"},
				"RoleArn":      {"arn:aws:iam::123456789012:role/SAMLRole"},
				"PrincipalArn": {"arn:aws:iam::123456789012:saml-provider/MySAMLIdP"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name: "missing_role_arn_returns_400",
			formValues: url.Values{
				"Action":        {"AssumeRoleWithSAML"},
				"Version":       {"2011-06-15"},
				"PrincipalArn":  {"arn:aws:iam::123456789012:saml-provider/MySAMLIdP"},
				"SAMLAssertion": {"PHNhbWxwOkFzc2VydGlvbj4="},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name: "invalid_duration_returns_400",
			formValues: url.Values{
				"Action":          {"AssumeRoleWithSAML"},
				"Version":         {"2011-06-15"},
				"RoleArn":         {"arn:aws:iam::123456789012:role/SAMLRole"},
				"PrincipalArn":    {"arn:aws:iam::123456789012:saml-provider/MySAMLIdP"},
				"SAMLAssertion":   {"PHNhbWxwOkFzc2VydGlvbj4="},
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
				var resp sts.AssumeRoleWithSAMLResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.AssumeRoleWithSAMLResult.Credentials.AccessKeyID)
				assert.Contains(t, resp.AssumeRoleWithSAMLResult.AssumedRoleUser.Arn, "assumed-role")
			}
		})
	}
}

// ---- AssumeRoot tests -------------------------------------------------------

func TestAssumeRoot_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input *sts.AssumeRootInput
		name  string
	}{
		{
			name: "default_duration",
			input: &sts.AssumeRootInput{
				TargetPrincipal: "123456789012",
				TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
			},
		},
		{
			name: "custom_duration",
			input: &sts.AssumeRootInput{
				TargetPrincipal: "123456789012",
				TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
				DurationSeconds: sts.MaxRootDurationSeconds,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.AssumeRoot(tt.input)
			require.NoError(t, err)
			require.NotNil(t, resp)

			creds := resp.AssumeRootResult.Credentials
			assert.True(t, strings.HasPrefix(creds.AccessKeyID, "ASIA"))
			assert.NotEmpty(t, creds.SecretAccessKey)
			assert.NotEmpty(t, creds.SessionToken)
			assert.NotEmpty(t, creds.Expiration)
			assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
			assert.Equal(t, sts.STSNamespace, resp.Xmlns)
		})
	}
}

func TestAssumeRoot_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		input   *sts.AssumeRootInput
		name    string
	}{
		{
			name: "missing_target_principal",
			input: &sts.AssumeRootInput{
				TaskPolicyArn: "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
			},
			wantErr: sts.ErrMissingTargetPrincipal,
		},
		{
			name: "missing_task_policy_arn",
			input: &sts.AssumeRootInput{
				TargetPrincipal: "123456789012",
			},
			wantErr: sts.ErrMissingTaskPolicyArn,
		},
		{
			name: "duration_too_long",
			input: &sts.AssumeRootInput{
				TargetPrincipal: "123456789012",
				TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
				DurationSeconds: sts.MaxRootDurationSeconds + 1,
			},
			wantErr: sts.ErrInvalidDuration,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.AssumeRoot(tt.input)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestAssumeRoot_SessionTrackedForCallerIdentity(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.AssumeRoot(&sts.AssumeRootInput{
		TargetPrincipal: "123456789012",
		TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
	})
	require.NoError(t, err)

	accessKeyID := resp.AssumeRootResult.Credentials.AccessKeyID

	ciResp, err := b.GetCallerIdentity(accessKeyID, "")
	require.NoError(t, err)
	assert.Contains(t, ciResp.GetCallerIdentityResult.Arn, "assumed-root")
	assert.Equal(t, 1, b.SessionCount())
}

func TestHandler_AssumeRoot(t *testing.T) {
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
				"Action":          {"AssumeRoot"},
				"Version":         {"2011-06-15"},
				"TargetPrincipal": {"123456789012"},
				"TaskPolicyArn":   {"arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_target_principal_returns_400",
			formValues: url.Values{
				"Action":        {"AssumeRoot"},
				"Version":       {"2011-06-15"},
				"TaskPolicyArn": {"arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name: "missing_task_policy_arn_returns_400",
			formValues: url.Values{
				"Action":          {"AssumeRoot"},
				"Version":         {"2011-06-15"},
				"TargetPrincipal": {"123456789012"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name: "duration_too_long_returns_400",
			formValues: url.Values{
				"Action":          {"AssumeRoot"},
				"Version":         {"2011-06-15"},
				"TargetPrincipal": {"123456789012"},
				"TaskPolicyArn":   {"arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials"},
				"DurationSeconds": {"901"},
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
				var resp sts.AssumeRootResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.AssumeRootResult.Credentials.AccessKeyID)
				assert.NotEmpty(t, resp.AssumeRootResult.Credentials.Expiration)
			}
		})
	}
}

// ---- GetDelegatedAccessToken tests -----------------------------------------

func TestGetDelegatedAccessToken_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input *sts.GetDelegatedAccessTokenInput
		name  string
	}{
		{
			name:  "valid_trade_in_token",
			input: &sts.GetDelegatedAccessTokenInput{TradeInToken: "some-trade-in-token"},
		},
		{
			name:  "another_valid_token",
			input: &sts.GetDelegatedAccessTokenInput{TradeInToken: "token-abc-123"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.GetDelegatedAccessToken(tt.input)
			require.NoError(t, err)
			require.NotNil(t, resp)

			res := resp.GetDelegatedAccessTokenResult
			assert.True(t, strings.HasPrefix(res.Credentials.AccessKeyID, "ASIA"))
			assert.NotEmpty(t, res.Credentials.SecretAccessKey)
			assert.NotEmpty(t, res.Credentials.SessionToken)
			assert.NotEmpty(t, res.Credentials.Expiration)
			assert.NotEmpty(t, res.AssumedPrincipal)
			assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
			assert.Equal(t, sts.STSNamespace, resp.Xmlns)
		})
	}
}

func TestGetDelegatedAccessToken_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		input   *sts.GetDelegatedAccessTokenInput
		name    string
	}{
		{
			name:    "missing_trade_in_token",
			input:   &sts.GetDelegatedAccessTokenInput{},
			wantErr: sts.ErrMissingTradeInToken,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.GetDelegatedAccessToken(tt.input)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestGetDelegatedAccessToken_SessionTrackedForCallerIdentity(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.GetDelegatedAccessToken(&sts.GetDelegatedAccessTokenInput{TradeInToken: "my-token"})
	require.NoError(t, err)

	accessKeyID := resp.GetDelegatedAccessTokenResult.Credentials.AccessKeyID

	ciResp, err := b.GetCallerIdentity(accessKeyID, "")
	require.NoError(t, err)
	assert.NotEmpty(t, ciResp.GetCallerIdentityResult.Arn)
	assert.Equal(t, 1, b.SessionCount())
}

func TestHandler_GetDelegatedAccessToken(t *testing.T) {
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
				"Action":       {"GetDelegatedAccessToken"},
				"Version":      {"2011-06-15"},
				"TradeInToken": {"some-trade-in-token"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_trade_in_token_returns_400",
			formValues: url.Values{
				"Action":  {"GetDelegatedAccessToken"},
				"Version": {"2011-06-15"},
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
				var resp sts.GetDelegatedAccessTokenResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.GetDelegatedAccessTokenResult.Credentials.AccessKeyID)
				assert.NotEmpty(t, resp.GetDelegatedAccessTokenResult.AssumedPrincipal)
			}
		})
	}
}

// ---- GetWebIdentityToken tests ----------------------------------------------

func TestGetWebIdentityToken_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input *sts.GetWebIdentityTokenInput
		name  string
	}{
		{
			name: "default_duration",
			input: &sts.GetWebIdentityTokenInput{
				Audience:         []string{"https://example.com"},
				SigningAlgorithm: "RS256",
			},
		},
		{
			name: "custom_duration",
			input: &sts.GetWebIdentityTokenInput{
				Audience:         []string{"https://example.com", "https://other.example.com"},
				SigningAlgorithm: "ES384",
				DurationSeconds:  600,
			},
		},
		{
			name: "max_duration",
			input: &sts.GetWebIdentityTokenInput{
				Audience:         []string{"my-app"},
				SigningAlgorithm: "RS256",
				DurationSeconds:  sts.MaxWebIdentityTokenDurationSeconds,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.GetWebIdentityToken(tt.input)
			require.NoError(t, err)
			require.NotNil(t, resp)

			res := resp.GetWebIdentityTokenResult
			assert.NotEmpty(t, res.WebIdentityToken)
			assert.NotEmpty(t, res.Expiration)
			// Token should have 3 JWT parts
			parts := strings.Split(res.WebIdentityToken, ".")
			assert.Len(t, parts, 3)
			assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
			assert.Equal(t, sts.STSNamespace, resp.Xmlns)
		})
	}
}

func TestGetWebIdentityToken_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		input   *sts.GetWebIdentityTokenInput
		name    string
	}{
		{
			name: "missing_audience",
			input: &sts.GetWebIdentityTokenInput{
				SigningAlgorithm: "RS256",
			},
			wantErr: sts.ErrMissingAudience,
		},
		{
			name: "missing_signing_algorithm",
			input: &sts.GetWebIdentityTokenInput{
				Audience: []string{"https://example.com"},
			},
			wantErr: sts.ErrMissingSigningAlgorithm,
		},
		{
			name: "duration_too_short",
			input: &sts.GetWebIdentityTokenInput{
				Audience:         []string{"https://example.com"},
				SigningAlgorithm: "RS256",
				DurationSeconds:  sts.MinWebIdentityTokenDurationSeconds - 1,
			},
			wantErr: sts.ErrInvalidDuration,
		},
		{
			name: "duration_too_long",
			input: &sts.GetWebIdentityTokenInput{
				Audience:         []string{"https://example.com"},
				SigningAlgorithm: "RS256",
				DurationSeconds:  sts.MaxWebIdentityTokenDurationSeconds + 1,
			},
			wantErr: sts.ErrInvalidDuration,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.GetWebIdentityToken(tt.input)
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestHandler_GetWebIdentityToken(t *testing.T) {
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
				"Action":            {"GetWebIdentityToken"},
				"Version":           {"2011-06-15"},
				"Audience.member.1": {"https://example.com"},
				"SigningAlgorithm":  {"RS256"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_audience_returns_400",
			formValues: url.Values{
				"Action":           {"GetWebIdentityToken"},
				"Version":          {"2011-06-15"},
				"SigningAlgorithm": {"RS256"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name: "missing_signing_algorithm_returns_400",
			formValues: url.Values{
				"Action":            {"GetWebIdentityToken"},
				"Version":           {"2011-06-15"},
				"Audience.member.1": {"https://example.com"},
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "MissingParameter",
		},
		{
			name: "invalid_duration_returns_400",
			formValues: url.Values{
				"Action":            {"GetWebIdentityToken"},
				"Version":           {"2011-06-15"},
				"Audience.member.1": {"https://example.com"},
				"SigningAlgorithm":  {"RS256"},
				"DurationSeconds":   {"10"},
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
				var resp sts.GetWebIdentityTokenResponse
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp.GetWebIdentityTokenResult.WebIdentityToken)
				assert.NotEmpty(t, resp.GetWebIdentityTokenResult.Expiration)
			}
		})
	}
}
