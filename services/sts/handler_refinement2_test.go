package sts_test

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// TestRefinement2_AssumeRootWithPolicyDescriptorType verifies TaskPolicyArn.arn form field is parsed.
func TestRefinement2_AssumeRootWithPolicyDescriptorType(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)

	rec := r1PostForm(t, h, url.Values{
		"Action":            {"AssumeRoot"},
		"Version":           {"2011-06-15"},
		"TargetPrincipal":   {"000000000000"},
		"TaskPolicyArn.arn": {"arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRefinement2_AssumeRootMissingTargetPrincipal verifies empty TargetPrincipal gives 400.
func TestRefinement2_AssumeRootMissingTargetPrincipal(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)

	rec := r1PostForm(t, h, url.Values{
		"Action":            {"AssumeRoot"},
		"Version":           {"2011-06-15"},
		"TaskPolicyArn.arn": {"arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "MissingParameter", errResp.Error.Code)
}

// TestRefinement2_AssumeRootMissingTaskPolicyArn verifies missing TaskPolicyArn.arn gives 400.
func TestRefinement2_AssumeRootMissingTaskPolicyArn(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)

	rec := r1PostForm(t, h, url.Values{
		"Action":          {"AssumeRoot"},
		"Version":         {"2011-06-15"},
		"TargetPrincipal": {"000000000000"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "MissingParameter", errResp.Error.Code)
}

// TestRefinement2_RoleSessionNameTooShort verifies a 1-char session name gives ValidationError.
func TestRefinement2_RoleSessionNameTooShort(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	_, err := b.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::000000000000:role/test-role",
		RoleSessionName: "x",
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, sts.ErrInvalidSessionName)
}

// TestRefinement2_RoleSessionNameTooLong verifies a 65-char session name gives ValidationError.
func TestRefinement2_RoleSessionNameTooLong(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	_, err := b.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::000000000000:role/test-role",
		RoleSessionName: strings.Repeat("a", 65),
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, sts.ErrInvalidSessionName)
}

// TestRefinement2_RoleSessionNameValid verifies 2-char and 64-char names succeed.
func TestRefinement2_RoleSessionNameValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sessionName string
	}{
		{name: "min_length", sessionName: "ab"},
		{name: "max_length", sessionName: strings.Repeat("a", 64)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::000000000000:role/test-role",
				RoleSessionName: tt.sessionName,
			})

			require.NoError(t, err)
			assert.NotEmpty(t, resp.AssumeRoleResult.Credentials.AccessKeyID)
		})
	}
}

// TestRefinement2_FederationTokenNameTooShort verifies a 1-char name gives error.
func TestRefinement2_FederationTokenNameTooShort(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	_, err := b.GetFederationToken(&sts.GetFederationTokenInput{
		Name: "x",
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, sts.ErrInvalidFederationName)
}

// TestRefinement2_FederationTokenNameTooLong verifies a 33-char name gives error.
func TestRefinement2_FederationTokenNameTooLong(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	_, err := b.GetFederationToken(&sts.GetFederationTokenInput{
		Name: strings.Repeat("a", 33),
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, sts.ErrInvalidFederationName)
}

// TestRefinement2_FederationTokenNameValid verifies a 2-char name succeeds.
func TestRefinement2_FederationTokenNameValid(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.GetFederationToken(&sts.GetFederationTokenInput{
		Name: "ab",
	})

	require.NoError(t, err)
	assert.NotEmpty(t, resp.GetFederationTokenResult.Credentials.AccessKeyID)
}

// TestRefinement2_GetSessionTokenSessionStored verifies GetSessionToken stores session;
// GetCallerIdentity returns non-empty ARN.
func TestRefinement2_GetSessionTokenSessionStored(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.GetSessionToken(&sts.GetSessionTokenInput{})
	require.NoError(t, err)

	accessKeyID := resp.GetSessionTokenResult.Credentials.AccessKeyID
	ci, err := b.GetCallerIdentity(accessKeyID, "")
	require.NoError(t, err)
	assert.NotEmpty(t, ci.GetCallerIdentityResult.Arn)
}

// TestGetCallerIdentity_SessionTokenMismatch verifies that a mismatched session token
// is rejected as InvalidClientTokenId (HTTP 400 via ErrUnknownAccessKeyID), matching AWS,
// rather than AccessDenied (403).
func TestGetCallerIdentity_SessionTokenMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrKind error
		name        string
		wrongToken  string
		useRealTok  bool
		wantErr     bool
	}{
		{name: "matching_token", useRealTok: true, wantErr: false},
		{name: "mismatched_token", wrongToken: "wrong-token", wantErr: true, wantErrKind: sts.ErrUnknownAccessKeyID},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.GetSessionToken(&sts.GetSessionTokenInput{})
			require.NoError(t, err)

			accessKeyID := resp.GetSessionTokenResult.Credentials.AccessKeyID
			token := tt.wrongToken
			if tt.useRealTok {
				token = resp.GetSessionTokenResult.Credentials.SessionToken
			}

			_, err = b.GetCallerIdentity(accessKeyID, token)
			if tt.wantErr {
				require.ErrorIs(t, err, tt.wantErrKind)

				return
			}
			require.NoError(t, err)
		})
	}
}

// TestRefinement2_AssumeRoleWithPolicyArns verifies PolicyArns are parsed and present in input.
func TestRefinement2_AssumeRoleWithPolicyArns(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)

	rec := r1PostForm(t, h, url.Values{
		"Action":                  {"AssumeRole"},
		"Version":                 {"2011-06-15"},
		"RoleArn":                 {"arn:aws:iam::000000000000:role/test-role"},
		"RoleSessionName":         {"my-session"},
		"PolicyArns.member.1.arn": {"arn:aws:iam::aws:policy/ReadOnlyAccess"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var result sts.AssumeRoleResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.NotEmpty(t, result.AssumeRoleResult.Credentials.AccessKeyID)
}

// TestRefinement2_AssumeRoleWithProvidedContexts verifies ProvidedContexts form fields are parsed.
func TestRefinement2_AssumeRoleWithProvidedContexts(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)

	rec := r1PostForm(t, h, url.Values{
		"Action":                                {"AssumeRole"},
		"Version":                               {"2011-06-15"},
		"RoleArn":                               {"arn:aws:iam::000000000000:role/test-role"},
		"RoleSessionName":                       {"my-session"},
		"ProvidedContexts.member.1.ProviderArn": {"arn:aws:iam::000000000000:oidc-provider/example.com"},
		"ProvidedContexts.member.1.ContextAssertion": {"assertion-value"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var result sts.AssumeRoleResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.NotEmpty(t, result.AssumeRoleResult.Credentials.AccessKeyID)
}

// TestRefinement2_AssumeRoleWithWebIdentityWithPolicyArns verifies PolicyArns parsed in OIDC request.
func TestRefinement2_AssumeRoleWithWebIdentityWithPolicyArns(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)

	rec := r1PostForm(t, h, url.Values{
		"Action":                  {"AssumeRoleWithWebIdentity"},
		"Version":                 {"2011-06-15"},
		"RoleArn":                 {"arn:aws:iam::000000000000:role/test-role"},
		"RoleSessionName":         {"my-session"},
		"WebIdentityToken":        {"eyJhbGciOiJtb2NrIn0.eyJzdWIiOiJzdWIxIn0.sig"},
		"PolicyArns.member.1.arn": {"arn:aws:iam::aws:policy/ReadOnlyAccess"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var result sts.AssumeRoleWithWebIdentityResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.NotEmpty(t, result.AssumeRoleWithWebIdentityResult.Credentials.AccessKeyID)
}

// TestRefinement2_AssumeRoleWithSAMLWithPolicyArns verifies PolicyArns parsed in SAML request.
func TestRefinement2_AssumeRoleWithSAMLWithPolicyArns(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)

	rec := r1PostForm(t, h, url.Values{
		"Action":                  {"AssumeRoleWithSAML"},
		"Version":                 {"2011-06-15"},
		"RoleArn":                 {"arn:aws:iam::000000000000:role/test-role"},
		"PrincipalArn":            {"arn:aws:iam::000000000000:saml-provider/MyIdP"},
		"SAMLAssertion":           {"dGVzdA=="},
		"PolicyArns.member.1.arn": {"arn:aws:iam::aws:policy/ReadOnlyAccess"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var result sts.AssumeRoleWithSAMLResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.NotEmpty(t, result.AssumeRoleWithSAMLResult.Credentials.AccessKeyID)
}

// parseJWTClaims decodes the payload section of a JWT and returns its claims.
func parseJWTClaims(t *testing.T, tokenStr string) map[string]any {
	t.Helper()

	parts := strings.Split(tokenStr, ".")
	require.Len(t, parts, 3, "JWT must have 3 parts")

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	require.NoError(t, err)

	var claims map[string]any
	require.NoError(t, json.Unmarshal(payloadBytes, &claims))

	return claims
}

// TestRefinement2_GetWebIdentityTokenJWTHasNbf verifies returned JWT has nbf claim.
func TestRefinement2_GetWebIdentityTokenJWTHasNbf(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.GetWebIdentityToken(&sts.GetWebIdentityTokenInput{
		Audience:         []string{"https://example.com"},
		SigningAlgorithm: "RS256",
	})
	require.NoError(t, err)

	claims := parseJWTClaims(t, resp.GetWebIdentityTokenResult.WebIdentityToken)
	_, hasNbf := claims["nbf"]
	assert.True(t, hasNbf, "JWT should contain nbf claim")
}

// TestRefinement2_GetWebIdentityTokenJWTHasIss verifies returned JWT has iss claim.
func TestRefinement2_GetWebIdentityTokenJWTHasIss(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.GetWebIdentityToken(&sts.GetWebIdentityTokenInput{
		Audience:         []string{"https://example.com"},
		SigningAlgorithm: "RS256",
	})
	require.NoError(t, err)

	claims := parseJWTClaims(t, resp.GetWebIdentityTokenResult.WebIdentityToken)
	iss, ok := claims["iss"].(string)
	assert.True(t, ok, "JWT should contain iss claim as string")
	assert.NotEmpty(t, iss)
	assert.Contains(t, iss, "sts.mock.aws.com")
}

// TestRefinement2_GetWebIdentityTokenInvalidSigningAlgo verifies unsupported alg gives error.
func TestRefinement2_GetWebIdentityTokenInvalidSigningAlgo(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	_, err := b.GetWebIdentityToken(&sts.GetWebIdentityTokenInput{
		Audience:         []string{"https://example.com"},
		SigningAlgorithm: "HS256",
	})

	require.Error(t, err)
	assert.ErrorIs(t, err, sts.ErrValidation)
}

// TestRefinement2_GetWebIdentityTokenValidSigningAlgorithms verifies RS256/ES256/PS256 all succeed.
func TestRefinement2_GetWebIdentityTokenValidSigningAlgorithms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		alg  string
	}{
		{name: "RS256", alg: "RS256"},
		{name: "ES256", alg: "ES256"},
		{name: "PS256", alg: "PS256"},
		{name: "RS384", alg: "RS384"},
		{name: "RS512", alg: "RS512"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.GetWebIdentityToken(&sts.GetWebIdentityTokenInput{
				Audience:         []string{"https://example.com"},
				SigningAlgorithm: tt.alg,
			})

			require.NoError(t, err)
			assert.NotEmpty(t, resp.GetWebIdentityTokenResult.WebIdentityToken)
		})
	}
}

// TestRefinement2_DecodeAuthorizationMessageEmpty verifies empty EncodedMessage gives 400.
func TestRefinement2_DecodeAuthorizationMessageEmpty(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)

	rec := r1PostForm(t, h, url.Values{
		"Action":  {"DecodeAuthorizationMessage"},
		"Version": {"2011-06-15"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	// AWS returns InvalidParameter (not MissingParameter) for an empty EncodedMessage.
	assert.Equal(t, "InvalidParameter", errResp.Error.Code)
}

// TestRefinement2_PackedPolicySizeNonZero verifies AssumeRole with Policy returns non-zero PackedPolicySize.
func TestRefinement2_PackedPolicySizeNonZero(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`
	resp, err := b.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::000000000000:role/test-role",
		RoleSessionName: "my-session",
		Policy:          policy,
	})

	require.NoError(t, err)
	// PackedPolicySize uses ceiling division: (len*100 + 2047) / 2048, minimum 1.
	expectedSize := max(int32((len(policy)*100+2047)/2048), 1)
	assert.Equal(t, expectedSize, resp.AssumeRoleResult.PackedPolicySize)
	assert.Positive(t, resp.AssumeRoleResult.PackedPolicySize)
}

// TestRefinement2_PackedPolicySizeZero verifies AssumeRole without Policy returns zero PackedPolicySize.
func TestRefinement2_PackedPolicySizeZero(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::000000000000:role/test-role",
		RoleSessionName: "my-session",
	})

	require.NoError(t, err)
	assert.Equal(t, int32(0), resp.AssumeRoleResult.PackedPolicySize)
}

// TestRefinement2_AssumeRoleWithWebIdentityExtractsIssuer verifies Provider comes from JWT iss claim.
func TestRefinement2_AssumeRoleWithWebIdentityExtractsIssuer(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()

	// Generate a token with a known issuer.
	tokenResp, err := b.GetWebIdentityToken(&sts.GetWebIdentityTokenInput{
		Audience:         []string{"https://example.com"},
		SigningAlgorithm: "RS256",
	})
	require.NoError(t, err)

	claims := parseJWTClaims(t, tokenResp.GetWebIdentityTokenResult.WebIdentityToken)
	expectedIssuer, _ := claims["iss"].(string)
	require.NotEmpty(t, expectedIssuer)

	resp, err := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
		RoleArn:          "arn:aws:iam::000000000000:role/test-role",
		RoleSessionName:  "my-session",
		WebIdentityToken: tokenResp.GetWebIdentityTokenResult.WebIdentityToken,
	})
	require.NoError(t, err)
	assert.Equal(t, expectedIssuer, resp.AssumeRoleWithWebIdentityResult.Provider)
}
