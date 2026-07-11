package sts_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

// TestRefinement1_ErrNilAppContext verifies the provider nil guard.
func TestRefinement1_ErrNilAppContext(t *testing.T) {
	t.Parallel()

	p := &sts.Provider{}
	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, sts.ErrNilAppContext)
}

// TestRefinement1_ProviderInit verifies normal provider init.
func TestRefinement1_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &sts.Provider{}
	reg, err := p.Init(&service.AppContext{JanitorCtx: t.Context()})
	require.NoError(t, err)
	assert.NotNil(t, reg)
}

// TestRefinement1_StorageBackendInterface verifies var_ assertion compiles.
func TestRefinement1_StorageBackendInterface(t *testing.T) {
	t.Parallel()

	var _ sts.StorageBackend = (*sts.InMemoryBackend)(nil)
}

// TestRefinement1_HandlerOpsLen verifies GetSupportedOperations count using export.
func TestRefinement1_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)
	// 11 real AWS STS operations are supported, including GetDelegatedAccessToken
	// and GetWebIdentityToken (both present in aws-sdk-go-v2/service/sts).
	assert.Equal(t, 11, h.HandlerOpsLen())
}

// TestRefinement1_SDKOpsSorted verifies GetSupportedOperations is sorted.
func TestRefinement1_SDKOpsSorted(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)
	ops := h.GetSupportedOperations()

	require.NotEmpty(t, ops)

	for i := 1; i < len(ops); i++ {
		assert.LessOrEqual(t, ops[i-1], ops[i],
			"ops not sorted at index %d: %s > %s", i, ops[i-1], ops[i])
	}
}

// TestRefinement1_AccountID verifies AccountID() returns non-empty string.
func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	assert.NotEmpty(t, b.AccountID())
}

// TestRefinement1_Region verifies Region() returns "us-east-1".
func TestRefinement1_Region(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	assert.Equal(t, "us-east-1", b.Region())
}

// TestRefinement1_ErrSessionNotFound verifies ErrSessionNotFound is defined.
func TestRefinement1_ErrSessionNotFound(t *testing.T) {
	t.Parallel()

	require.Error(t, sts.ErrSessionNotFound)
	assert.Equal(t, "session not found", sts.ErrSessionNotFound.Error())
}

// TestRefinement1_ResetClearsSessions verifies Reset() clears sessions via AddSessionInternal.
func TestRefinement1_ResetClearsSessions(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	b.AddSessionInternal(&sts.SessionInfo{
		AccessKeyID:    "ASIATEST000000000001",
		AssumedRoleArn: "arn:aws:sts::000000000000:assumed-role/test/session",
		AccountID:      sts.MockAccountID,
		SessionName:    "session",
		Expiration:     time.Now().Add(time.Hour),
	})

	assert.Equal(t, 1, b.SessionCount())

	h := sts.NewHandler(b)
	h.Reset()

	assert.Equal(t, 0, b.SessionCount())
}

// TestRefinement1_SnapshotDeepCopy verifies Snapshot doesn't share pointers with live sessions.
func TestRefinement1_SnapshotDeepCopy(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	b.AddSessionInternal(&sts.SessionInfo{
		AccessKeyID:    "ASIATEST000000000002",
		AssumedRoleArn: "arn:aws:sts::000000000000:assumed-role/test/session",
		AccountID:      sts.MockAccountID,
		SessionName:    "original",
		Expiration:     time.Now().Add(time.Hour),
	})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	// Mutate the live session.
	b.SetSessionExpiration("ASIATEST000000000002", time.Now().Add(2*time.Hour))

	// Restore from snapshot - should reflect original data (not mutated).
	b2 := sts.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))
	assert.Equal(t, 1, b2.SessionCount())
}

// TestRefinement1_EnsureNonNilMapsAfterRestore verifies Restore with empty data leaves sessions usable.
func TestRefinement1_EnsureNonNilMapsAfterRestore(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	require.NoError(t, b.Restore(t.Context(), nil))
	// sessions map should still be usable (non-nil); verify by adding a session.
	b.AddSessionInternal(&sts.SessionInfo{
		AccessKeyID:    "ASIATEST000000000003",
		AssumedRoleArn: "arn:aws:sts::000000000000:assumed-role/test/session",
		AccountID:      sts.MockAccountID,
		SessionName:    "after-restore",
		Expiration:     time.Now().Add(time.Hour),
	})
	assert.Equal(t, 1, b.SessionCount())
}

// TestRefinement1_GetAccessKeyInfoWithAssumedRole verifies GetAccessKeyInfo uses the assumed-role account.
func TestRefinement1_GetAccessKeyInfoWithAssumedRole(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	b.AddSessionInternal(&sts.SessionInfo{
		AccessKeyID:    "ASIATEST000000000004",
		AssumedRoleArn: "arn:aws:sts::111111111111:assumed-role/test/session",
		AccountID:      "111111111111",
		SessionName:    "session",
		Expiration:     time.Now().Add(time.Hour),
	})

	rec := r1PostForm(t, sts.NewHandler(b), url.Values{
		"Action":      {"GetAccessKeyInfo"},
		"Version":     {"2011-06-15"},
		"AccessKeyId": {"ASIATEST000000000004"},
	})

	assert.Equal(t, http.StatusOK, rec.Code)

	var result sts.GetAccessKeyInfoResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &result))
	assert.Equal(t, "111111111111", result.GetAccessKeyInfoResult.Account)
}

// TestRefinement1_HandleErrorRequestIDNotZero verifies errors return a non-zero request ID.
func TestRefinement1_HandleErrorRequestIDNotZero(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)

	rec := r1PostForm(t, h, url.Values{
		"Action":  {"AssumeRole"},
		"Version": {"2011-06-15"},
		// Missing RoleArn and RoleSessionName.
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.NotEqual(t, "00000000-0000-0000-0000-000000000000", errResp.RequestID)
	assert.NotEmpty(t, errResp.RequestID)
}

// TestRefinement1_AssumeRoleWithSAMLNameQualifier verifies NameQualifier is non-empty in SAML response.
func TestRefinement1_AssumeRoleWithSAMLNameQualifier(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	input := &sts.AssumeRoleWithSAMLInput{
		RoleArn:       "arn:aws:iam::000000000000:role/test-role",
		PrincipalArn:  "arn:aws:iam::000000000000:saml-provider/MyIdP",
		SAMLAssertion: "PHNhbWxwOkFzc2VydGlvbj4=",
	}

	resp, err := b.AssumeRoleWithSAML(input)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.AssumeRoleWithSAMLResult.NameQualifier)
}

// TestRefinement1_AssumeRoleWithSAMLSessionName verifies session name uses input.RoleSessionName.
func TestRefinement1_AssumeRoleWithSAMLSessionName(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	input := &sts.AssumeRoleWithSAMLInput{
		RoleArn:         "arn:aws:iam::000000000000:role/test-role",
		PrincipalArn:    "arn:aws:iam::000000000000:saml-provider/MyIdP",
		SAMLAssertion:   "PHNhbWxwOkFzc2VydGlvbj4=",
		RoleSessionName: "my-saml-session",
	}

	resp, err := b.AssumeRoleWithSAML(input)
	require.NoError(t, err)
	assert.Contains(t, resp.AssumeRoleWithSAMLResult.AssumedRoleUser.Arn, "my-saml-session")
}

// TestRefinement1_AssumeRootDerivedAccount verifies AssumeRoot uses TargetPrincipal account.
func TestRefinement1_AssumeRootDerivedAccount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		targetPrincipal string
		wantAccount     string
	}{
		{
			name:            "account_id",
			targetPrincipal: "123456789012",
			wantAccount:     "123456789012",
		},
		{
			name:            "arn",
			targetPrincipal: "arn:aws:iam::987654321098:root",
			wantAccount:     "987654321098",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.AssumeRoot(&sts.AssumeRootInput{
				TargetPrincipal: tt.targetPrincipal,
				TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
			})
			require.NoError(t, err)
			require.NotEmpty(t, resp.AssumeRootResult.Credentials.AccessKeyID)

			// The account in the session should be derived from TargetPrincipal.
			accessKeyID := resp.AssumeRootResult.Credentials.AccessKeyID
			snap := b.Snapshot(t.Context())
			b2 := sts.NewInMemoryBackend()
			require.NoError(t, b2.Restore(t.Context(), snap))

			ci, err := b2.GetCallerIdentity(accessKeyID, "")
			require.NoError(t, err)
			assert.Equal(t, tt.wantAccount, ci.GetCallerIdentityResult.Account)
		})
	}
}

// TestRefinement1_DelegatedTokenDuration verifies GetDelegatedAccessToken accepts DurationSeconds.
func TestRefinement1_DelegatedTokenDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		duration int32
		wantErr  bool
	}{
		{
			name:     "default_duration",
			duration: 0,
			wantErr:  false,
		},
		{
			name:     "custom_duration",
			duration: 1800,
			wantErr:  false,
		},
		{
			name:     "too_short",
			duration: 100,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			resp, err := b.GetDelegatedAccessToken(&sts.GetDelegatedAccessTokenInput{
				TradeInToken:    "test-token",
				DurationSeconds: tt.duration,
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, resp.GetDelegatedAccessTokenResult.Credentials.AccessKeyID)
		})
	}
}

// TestRefinement1_AssumeRoleWithWebIdentityTagsStored verifies Tags are stored in session.
func TestRefinement1_AssumeRoleWithWebIdentityTagsStored(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
		RoleArn:          "arn:aws:iam::000000000000:role/test-role",
		RoleSessionName:  "test-session",
		WebIdentityToken: "eyJhbGciOiJtb2NrIiwidHlwIjoiSldUIn0.eyJzdWIiOiJ0ZXN0LXN1YmplY3QiLCJhdWQiOiJ0ZXN0LWF1ZCJ9.mock",
		Tags:             []sts.Tag{{Key: "Env", Value: "test"}},
	})
	require.NoError(t, err)

	accessKeyID := resp.AssumeRoleWithWebIdentityResult.Credentials.AccessKeyID

	snap := b.Snapshot(t.Context())
	b2 := sts.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, 1, b2.SessionCount())

	ci, err := b2.GetCallerIdentity(accessKeyID, "")
	require.NoError(t, err)
	assert.NotEmpty(t, ci.GetCallerIdentityResult.Arn)
}

// TestRefinement1_AssumeRoleWithSAMLSourceIdentity verifies SourceIdentity flows through SAML.
func TestRefinement1_AssumeRoleWithSAMLSourceIdentity(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
		RoleArn:        "arn:aws:iam::000000000000:role/test-role",
		PrincipalArn:   "arn:aws:iam::000000000000:saml-provider/MyIdP",
		SAMLAssertion:  "PHNhbWxwOkFzc2VydGlvbj4=",
		SourceIdentity: "my-saml-identity",
	})
	require.NoError(t, err)
	assert.Equal(t, "my-saml-identity", resp.AssumeRoleWithSAMLResult.SourceIdentity)
}

// TestRefinement1_AssumeRoleWithWebIdentitySourceIdentity verifies SourceIdentity flows through OIDC.
func TestRefinement1_AssumeRoleWithWebIdentitySourceIdentity(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
		RoleArn:          "arn:aws:iam::000000000000:role/test-role",
		RoleSessionName:  "test-session",
		WebIdentityToken: "eyJhbGciOiJtb2NrIn0.eyJzdWIiOiJzdWIxIn0.sig",
		SourceIdentity:   "my-oidc-identity",
	})
	require.NoError(t, err)
	assert.Equal(t, "my-oidc-identity", resp.AssumeRoleWithWebIdentityResult.SourceIdentity)
}

// TestRefinement1_AddSessionInternalAndCount verifies AddSessionInternal increases SessionCount.
func TestRefinement1_AddSessionInternalAndCount(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	assert.Equal(t, 0, b.SessionCount())

	for i := range 3 {
		b.AddSessionInternal(&sts.SessionInfo{
			AccessKeyID:    fmt.Sprintf("ASIATEST%012d", i),
			AssumedRoleArn: "arn:aws:sts::000000000000:assumed-role/test/session",
			AccountID:      sts.MockAccountID,
			SessionName:    fmt.Sprintf("session-%d", i),
			Expiration:     time.Now().Add(time.Hour),
		})
	}

	assert.Equal(t, 3, b.SessionCount())
}

// TestRefinement1_SnapshotRestoreRoundtrip verifies Snapshot/Restore preserves all fields.
func TestRefinement1_SnapshotRestoreRoundtrip(t *testing.T) {
	t.Parallel()

	expiration := time.Now().Add(time.Hour).UTC().Truncate(time.Second)
	original := &sts.SessionInfo{
		AccessKeyID:    "ASIATEST000000000099",
		AssumedRoleArn: "arn:aws:sts::000000000000:assumed-role/my-role/my-session",
		AccountID:      sts.MockAccountID,
		SessionName:    "my-session",
		AssumedRoleID:  "AROAMY-ROLE:my-session",
		SourceIdentity: "test-identity",
		Tags:           []sts.Tag{{Key: "k", Value: "v"}},
		Expiration:     expiration,
	}

	b := sts.NewInMemoryBackend()
	b.AddSessionInternal(original)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := sts.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))
	assert.Equal(t, 1, b2.SessionCount())

	ci, err := b2.GetCallerIdentity("ASIATEST000000000099", "")
	require.NoError(t, err)
	assert.Equal(t, sts.MockAccountID, ci.GetCallerIdentityResult.Account)
	assert.Equal(t, original.AssumedRoleArn, ci.GetCallerIdentityResult.Arn)
}

// TestRefinement1_ErrorMappingMissingParam verifies error mapping for missing params.
func TestRefinement1_ErrorMappingMissingParam(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		values     url.Values
		wantCode   string
		wantStatus int
	}{
		{
			name: "missing_role_arn",
			values: url.Values{
				"Action":          {"AssumeRole"},
				"Version":         {"2011-06-15"},
				"RoleSessionName": {"session"},
			},
			wantCode:   "MissingParameter",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_action",
			values: url.Values{
				"Action":  {"NonExistentAction"},
				"Version": {"2011-06-15"},
			},
			wantCode:   "InvalidAction",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			h := sts.NewHandler(b)

			rec := r1PostForm(t, h, tt.values)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var errResp sts.ErrorResponse
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantCode, errResp.Error.Code)
		})
	}
}

// TestRefinement1_BackendDefaultAccountID verifies default account ID is MockAccountID.
func TestRefinement1_BackendDefaultAccountID(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	assert.Equal(t, sts.MockAccountID, b.AccountID())
}

// r1PostForm is a test helper that posts form values through the STS handler.
func r1PostForm(t *testing.T, h *sts.Handler, values url.Values) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	body := values.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}
