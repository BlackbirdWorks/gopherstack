package sts_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// ── Fix 1-5: GetFederationToken validation improvements ───────────────────────

func TestRefinement3_GetFederationToken_NameCharset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		fedName string
		wantErr bool
	}{
		{name: "valid alphanumeric", fedName: "alice", wantErr: false},
		{name: "valid with allowed chars", fedName: "alice+=,.@-", wantErr: false},
		{name: "invalid colon", fedName: "alice:bob", wantErr: true},
		{name: "invalid space", fedName: "alice bob", wantErr: true},
		{name: "invalid slash", fedName: "alice/bob", wantErr: true},
		{name: "too short", fedName: "a", wantErr: true},
		{name: "too long 33 chars", fedName: strings.Repeat("x", 33), wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.GetFederationToken(&sts.GetFederationTokenInput{
				Name: tc.fedName,
			})

			if tc.wantErr {
				require.ErrorIs(t, err, sts.ErrInvalidFederationName)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRefinement3_GetFederationToken_TagConstraints(t *testing.T) {
	t.Parallel()

	t.Run("aws_prefix_tag_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.GetFederationToken(&sts.GetFederationTokenInput{
			Name: "alice",
			Tags: []sts.Tag{{Key: "aws:reserved", Value: "val"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("duplicate_tag_key_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.GetFederationToken(&sts.GetFederationTokenInput{
			Name: "alice",
			Tags: []sts.Tag{{Key: "MyKey", Value: "v1"}, {Key: "mykey", Value: "v2"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("value_over_256_chars_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.GetFederationToken(&sts.GetFederationTokenInput{
			Name: "alice",
			Tags: []sts.Tag{{Key: "k", Value: strings.Repeat("v", 257)}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagValue)
	})
}

func TestRefinement3_GetFederationToken_PolicyArnsValidation(t *testing.T) {
	t.Parallel()

	t.Run("too_many_policy_arns_rejected", func(t *testing.T) {
		t.Parallel()

		arns := make([]string, 11)
		for i := range arns {
			arns[i] = "arn:aws:iam::aws:policy/P"
		}

		b := sts.NewInMemoryBackend()
		_, err := b.GetFederationToken(&sts.GetFederationTokenInput{
			Name:       "alice",
			PolicyArns: arns,
		})
		require.ErrorIs(t, err, sts.ErrTooManyPolicyArns)
	})

	t.Run("malformed_policy_arn_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.GetFederationToken(&sts.GetFederationTokenInput{
			Name:       "alice",
			PolicyArns: []string{"not-an-arn"},
		})
		require.ErrorIs(t, err, sts.ErrInvalidPolicyArn)
	})
}

func TestRefinement3_GetFederationToken_PolicyValidation(t *testing.T) {
	t.Parallel()

	t.Run("malformed_json_policy_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.GetFederationToken(&sts.GetFederationTokenInput{
			Name:   "alice",
			Policy: "not valid json",
		})
		require.ErrorIs(t, err, sts.ErrMalformedPolicyDocument)
	})

	t.Run("policy_too_large_rejected", func(t *testing.T) {
		t.Parallel()

		bigPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"` +
			strings.Repeat("arn:aws:s3:::bucket/prefix/", 90) + `*"}]}`
		b := sts.NewInMemoryBackend()
		_, err := b.GetFederationToken(&sts.GetFederationTokenInput{
			Name:   "alice",
			Policy: bigPolicy,
		})
		require.ErrorIs(t, err, sts.ErrPackedPolicyTooLarge)
	})

	t.Run("valid_policy_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.GetFederationToken(&sts.GetFederationTokenInput{
			Name:   "alice",
			Policy: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`,
		})
		require.NoError(t, err)
		assert.Positive(t, resp.GetFederationTokenResult.PackedPolicySize)
	})
}

// ── Fix 6: AssumeRoleWithWebIdentity tag validation ───────────────────────────

func TestRefinement3_AssumeRoleWithWebIdentity_TagValidation(t *testing.T) {
	t.Parallel()

	t.Run("aws_prefix_tag_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
			RoleArn:          "arn:aws:iam::123456789012:role/R",
			RoleSessionName:  "session",
			WebIdentityToken: "header.eyJzdWIiOiJ0ZXN0In0.sig",
			Tags:             []sts.Tag{{Key: "aws:bad", Value: "v"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("too_many_tags_rejected", func(t *testing.T) {
		t.Parallel()

		tags := make([]sts.Tag, 51)
		for i := range tags {
			tags[i] = sts.Tag{Key: "key" + strings.Repeat("x", i%5), Value: "val"}
		}
		// deduplicate keys for count test
		for i := range tags {
			tags[i] = sts.Tag{Key: "k" + string(rune('a'+i%26)) + string(rune('0'+i/26)), Value: "v"}
		}

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
			RoleArn:          "arn:aws:iam::123456789012:role/R",
			RoleSessionName:  "session",
			WebIdentityToken: "header.eyJzdWIiOiJ0ZXN0In0.sig",
			Tags:             tags,
		})
		require.ErrorIs(t, err, sts.ErrTooManyTags)
	})
}

// ── Fix 7: validateRoleArn — 12-digit account validation ────────────────────────

func TestRefinement3_ValidateRoleArn_AccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		roleArn string
		wantErr bool
	}{
		{
			name:    "valid 12-digit account",
			roleArn: "arn:aws:iam::123456789012:role/MyRole",
			wantErr: false,
		},
		{
			name:    "short account ID rejected",
			roleArn: "arn:aws:iam::1234567:role/MyRole",
			wantErr: true,
		},
		{
			name:    "non-numeric account rejected",
			roleArn: "arn:aws:iam::abcdefghijkl:role/MyRole",
			wantErr: true,
		},
		{
			name:    "empty account rejected",
			roleArn: "arn:aws:iam:::role/MyRole",
			wantErr: true,
		},
		{
			name:    "resource not starting with role/ rejected",
			roleArn: "arn:aws:iam::123456789012:user/bob",
			wantErr: true,
		},
		{
			name:    "assumed-role resource rejected",
			roleArn: "arn:aws:sts::123456789012:assumed-role/R/s",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         tc.roleArn,
				RoleSessionName: "session",
			})

			if tc.wantErr {
				require.ErrorIs(t, err, sts.ErrInvalidRoleArn)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ── Fix 8: AssumeRoot TargetPrincipal validation ─────────────────────────────

func TestRefinement3_AssumeRoot_TargetPrincipalValidation(t *testing.T) {
	t.Parallel()

	t.Run("valid_12_digit_account_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoot(&sts.AssumeRootInput{
			TargetPrincipal: "123456789012",
			TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
		})
		require.NoError(t, err)
	})

	t.Run("valid_arn_target_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoot(&sts.AssumeRootInput{
			TargetPrincipal: "arn:aws:iam::987654321098:root",
			TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
		})
		require.NoError(t, err)
	})

	t.Run("invalid_account_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoot(&sts.AssumeRootInput{
			TargetPrincipal: "not-an-account",
			TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
		})
		require.ErrorIs(t, err, sts.ErrInvalidTargetPrincipal)
	})

	t.Run("short_account_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoot(&sts.AssumeRootInput{
			TargetPrincipal: "12345",
			TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
		})
		require.ErrorIs(t, err, sts.ErrInvalidTargetPrincipal)
	})
}

// ── Fix 9: GetSessionToken — TokenCode without SerialNumber rejected ──────────

func TestRefinement3_GetSessionToken_TokenCodeWithoutSerial(t *testing.T) {
	t.Parallel()

	t.Run("token_code_without_serial_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.GetSessionToken(&sts.GetSessionTokenInput{
			TokenCode: "123456",
		})
		require.ErrorIs(t, err, sts.ErrTokenCodeWithoutSerial)
	})

	t.Run("serial_without_token_code_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.GetSessionToken(&sts.GetSessionTokenInput{
			SerialNumber: "arn:aws:iam::123456789012:mfa/my-device",
		})
		require.ErrorIs(t, err, sts.ErrMFACodeRequired)
	})

	t.Run("both_provided_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.GetSessionToken(&sts.GetSessionTokenInput{
			SerialNumber: "arn:aws:iam::123456789012:mfa/my-device",
			TokenCode:    "654321",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.GetSessionTokenResult.Credentials.AccessKeyID)
	})

	t.Run("neither_provided_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.GetSessionToken(&sts.GetSessionTokenInput{})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.GetSessionTokenResult.Credentials.AccessKeyID)
	})
}

// ── Fix 10: deriveRoleID — no collision for similar names ─────────────────────

func TestRefinement3_DeriveRoleID_NoDuplicates(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()

	roleARNs := []string{
		"arn:aws:iam::123456789012:role/MyRole",
		"arn:aws:iam::123456789012:role/MyRoleAdmin",
		"arn:aws:iam::123456789012:role/MyRoleReadOnly",
		"arn:aws:iam::123456789012:role/MyRolePowerUser",
	}

	seenIDs := make(map[string]struct{})
	for _, arn := range roleARNs {
		resp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         arn,
			RoleSessionName: "session",
		})
		require.NoError(t, err)

		id := resp.AssumeRoleResult.AssumedRoleUser.AssumedRoleID
		// The AssumedRoleID is "AROAXXX:session"; check the AROA prefix part
		parts := strings.SplitN(id, ":", 2)
		aroaPart := parts[0]
		_, dup := seenIDs[aroaPart]
		assert.False(t, dup, "duplicate role ID %s for ARN %s", aroaPart, arn)
		seenIDs[aroaPart] = struct{}{}
	}
}

// ── Fix: isSessionExpired helper — consistent expiry behavior ─────────────────

func TestRefinement3_SessionExpiry_Consistent(t *testing.T) {
	t.Parallel()

	t.Run("expired_session_rejected_by_validate_credential", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
		})
		require.NoError(t, err)

		creds := resp.AssumeRoleResult.Credentials
		// Force expiry in the past.
		b.SetSessionExpiration(creds.AccessKeyID, time.Now().Add(-time.Minute))

		_, err = b.ValidateSessionCredential(creds.AccessKeyID, creds.SessionToken)
		require.ErrorIs(t, err, sts.ErrSessionNotFound)
	})

	t.Run("expired_session_falls_back_in_get_caller_identity", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
		})
		require.NoError(t, err)

		creds := resp.AssumeRoleResult.Credentials
		b.SetSessionExpiration(creds.AccessKeyID, time.Now().Add(-time.Minute))

		// After expiry GetCallerIdentity returns the default (root) identity.
		ci, err := b.GetCallerIdentity(creds.AccessKeyID, "")
		require.NoError(t, err)
		assert.Equal(t, sts.MockUserArn, ci.GetCallerIdentityResult.Arn)
	})
}

// ── Fix: GetFederationToken PackedPolicySize includes PolicyArns ──────────────

func TestRefinement3_GetFederationToken_PackedPolicySizeWithArns(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	resp, err := b.GetFederationToken(&sts.GetFederationTokenInput{
		Name:       "alice",
		PolicyArns: []string{"arn:aws:iam::aws:policy/ReadOnlyAccess"},
	})
	require.NoError(t, err)
	assert.Positive(t, resp.GetFederationTokenResult.PackedPolicySize)
}
