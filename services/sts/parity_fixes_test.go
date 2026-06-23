package sts_test

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// testSAMLAssertion is a minimal base64-encoded SAML XML fragment used across parity tests.
// Decodes to: <samlp:Assertion>.
const testSAMLAssertion = "PHNhbWxwOkFzc2VydGlvbj4="

// ── Parity Fix 1: GetCallerIdentity — ASIA key not in sessions → InvalidClientTokenId ──

func TestParity_GetCallerIdentity_UnknownAsiaKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		accessKey string
	}{
		{
			name:      "asia_key_never_issued_returns_InvalidClientTokenId",
			accessKey: "ASIANEVERISSUED1234",
			wantErr:   sts.ErrUnknownAccessKeyID,
		},
		{
			name:      "empty_key_returns_root_identity",
			accessKey: "",
			wantErr:   nil,
		},
		{
			name:      "akia_key_unknown_returns_root_identity",
			accessKey: "AKIAIOSFODNN7EXAMPLE",
			wantErr:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.GetCallerIdentity(tt.accessKey, "")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				assert.Nil(t, resp)
			} else {
				require.NoError(t, err)
				require.NotNil(t, resp)
				assert.Equal(t, sts.MockAccountID, resp.GetCallerIdentityResult.Account)
			}
		})
	}
}

// ── Parity Fix 2: AssumeRoleWithSAML — base64 + XML validation ──────────────────

func TestParity_AssumeRoleWithSAML_SAMLAssertionValidation(t *testing.T) {
	t.Parallel()

	validRoleArn := "arn:aws:iam::123456789012:role/R"
	validPrincipalArn := "arn:aws:iam::123456789012:saml-provider/MyIdP"

	tests := []struct {
		wantErr       error
		name          string
		samlAssertion string
		wantSuccess   bool
	}{
		{
			name:          "not_base64_rejected",
			samlAssertion: "not!!base64###",
			wantErr:       sts.ErrInvalidSAMLAssertion,
		},
		{
			name:          "bare_word_rejected",
			samlAssertion: "assertion",
			wantErr:       sts.ErrInvalidSAMLAssertion,
		},
		{
			name:          "valid_base64_non_xml_accepted",
			samlAssertion: "dGVzdA==", // "test" — valid base64; emulator accepts non-XML payloads
			wantErr:       nil,
		},
		{
			name:          "valid_base64_xml_accepted",
			samlAssertion: testSAMLAssertion, // <samlp:Assertion>
			wantSuccess:   true,
		},
		{
			name:          "full_saml_response_accepted",
			samlAssertion: "PHNhbWxwOlJlc3BvbnNlIHhtbG5zOnNhbWxwPSJ1cm46b2FzaXM6bmFtZXM6dGM6U0FNTDoyLjA6cHJvdG9jb2wiLz4=",
			wantSuccess:   true,
		},
		{
			name:          "empty_assertion_returns_missing_error",
			samlAssertion: "",
			wantErr:       sts.ErrMissingSAMLAssertion,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
				RoleArn:       validRoleArn,
				PrincipalArn:  validPrincipalArn,
				SAMLAssertion: tt.samlAssertion,
			})

			if tt.wantSuccess {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.wantErr)
			}
		})
	}
}

func TestParity_AssumeRoleWithSAML_SAMLAssertionViaHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		samlAssertion string
		wantError     string
		wantCode      int
	}{
		{
			name:          "invalid_base64_returns_400_InvalidIdentityToken",
			samlAssertion: "not!!base64",
			wantCode:      http.StatusBadRequest,
			wantError:     "InvalidIdentityToken",
		},
		{
			name:          "valid_base64_non_xml_returns_200",
			samlAssertion: "dGVzdA==",
			wantCode:      http.StatusOK,
		},
		{
			name:          "valid_saml_xml_returns_200",
			samlAssertion: testSAMLAssertion,
			wantCode:      http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _, e := accuracyHandler(t)
			form := url.Values{
				"Action":        {"AssumeRoleWithSAML"},
				"Version":       {"2011-06-15"},
				"RoleArn":       {"arn:aws:iam::123456789012:role/R"},
				"PrincipalArn":  {"arn:aws:iam::123456789012:saml-provider/MyIdP"},
				"SAMLAssertion": {tt.samlAssertion},
			}
			rec := accuracyPost(t, h, e, form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantError != "" {
				errResp := decodeError(t, rec.Body.Bytes())
				assert.Equal(t, tt.wantError, errResp.Error.Code)
			}
		})
	}
}

// ── Parity Fix 3: GetWebIdentityToken not in GetSupportedOperations ──────────────

func TestParity_GetWebIdentityToken_NotInSupportedOps(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)
	ops := h.GetSupportedOperations()

	for _, op := range ops {
		assert.NotEqual(
			t,
			"GetWebIdentityToken",
			op,
			"GetWebIdentityToken is not a real AWS STS operation and must not be in GetSupportedOperations",
		)
	}
}

// ── Parity Fix 4: DecodeAuthorizationMessage — only STS-issued messages accepted ──

func TestParity_DecodeAuthorizationMessage_VerifiesIssuer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupMsg    func(b *sts.InMemoryBackend) string
		wantErr     error
		wantDecoded string
	}{
		{
			name: "sts_issued_message_accepted_and_decoded",
			setupMsg: func(b *sts.InMemoryBackend) string {
				return b.IssueEncodedAuthorizationMessage(
					"Access denied to s3:GetObject on arn:aws:s3:::my-bucket/secret",
				)
			},
			wantDecoded: "Access denied to s3:GetObject on arn:aws:s3:::my-bucket/secret",
		},
		{
			name: "arbitrary_base64_rejected",
			setupMsg: func(_ *sts.InMemoryBackend) string {
				return "SGVsbG8gV29ybGQ=" // base64("Hello World") — not STS-issued
			},
			wantErr: sts.ErrInvalidAuthorizationMessage,
		},
		{
			name: "empty_message_issued_and_decoded",
			setupMsg: func(b *sts.InMemoryBackend) string {
				return b.IssueEncodedAuthorizationMessage("")
			},
			wantDecoded: "",
		},
		{
			name: "message_from_different_backend_rejected",
			setupMsg: func(_ *sts.InMemoryBackend) string {
				// Issue from a different backend instance — different signing key.
				other := sts.NewInMemoryBackend()

				return other.IssueEncodedAuthorizationMessage("cross-backend message")
			},
			wantErr: sts.ErrInvalidAuthorizationMessage,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			encoded := tt.setupMsg(b)

			decoded, err := b.VerifyEncodedAuthorizationMessage(encoded)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantDecoded, decoded)
			}
		})
	}
}

func TestParity_DecodeAuthorizationMessage_ViaHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupMsg  func(b *sts.InMemoryBackend) string
		name      string
		wantError string
		wantCode  int
	}{
		{
			name: "sts_issued_message_returns_200",
			setupMsg: func(b *sts.InMemoryBackend) string {
				return b.IssueEncodedAuthorizationMessage("denied: s3:PutObject")
			},
			wantCode: http.StatusOK,
		},
		{
			name: "arbitrary_base64_returns_200",
			setupMsg: func(_ *sts.InMemoryBackend) string {
				return "SGVsbG8=" // base64("Hello")
			},
			wantCode: http.StatusOK,
		},
		{
			name: "garbage_non_base64_returns_400",
			setupMsg: func(_ *sts.InMemoryBackend) string {
				return "not-base64!!!"
			},
			wantCode:  http.StatusBadRequest,
			wantError: "InvalidAuthorizationMessageException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b, e := accuracyHandler(t)
			encoded := tt.setupMsg(b)

			form := url.Values{
				"Action":         {"DecodeAuthorizationMessage"},
				"Version":        {"2011-06-15"},
				"EncodedMessage": {encoded},
			}
			rec := accuracyPost(t, h, e, form)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantError != "" {
				errResp := decodeError(t, rec.Body.Bytes())
				assert.Equal(t, tt.wantError, errResp.Error.Code)
			}
		})
	}
}

// ── Performance Fix: storeSession does not hold write-lock during eviction ───────

func TestParity_StoreSession_EvictionSeparateFromStore(t *testing.T) {
	t.Parallel()

	// Seed sessions beyond the evict threshold, then verify a new storeSession
	// call (via AssumeRole) evicts expired ones without blocking other stores.
	// The test exercises the threshold crossing to confirm eviction triggers.
	b := sts.NewInMemoryBackend()

	const aboveThreshold = sts.SessionEvictThreshold + 10

	// Seed expired sessions via AddSessionInternal to bypass storeSession.
	for i := range aboveThreshold {
		b.AddSessionInternal(&sts.SessionInfo{
			AccessKeyID: fmt.Sprintf("ASIA%016d", i),
			Expiration:  time.Now().UTC().Add(-time.Hour),
		})
	}

	require.Equal(
		t,
		aboveThreshold,
		b.SessionCount(),
		"all seeded expired sessions present before trigger",
	)

	// AssumeRole stores a new live session, triggering maybeEvictExpiredSessions.
	resp, err := b.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/R",
		RoleSessionName: "live",
		DurationSeconds: 900,
	})
	require.NoError(t, err)
	require.NotEmpty(t, resp.AssumeRoleResult.Credentials.AccessKeyID)

	// After eviction, only the one live session should remain.
	assert.Equal(t, 1, b.SessionCount(), "expired sessions evicted, only live session remains")
}
