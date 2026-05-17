package sts_test

import (
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

// ── helpers ──────────────────────────────────────────────────────────────────

func accuracyHandler(t *testing.T) (*sts.Handler, *sts.InMemoryBackend, *echo.Echo) {
	t.Helper()

	b := sts.NewInMemoryBackend()
	h := sts.NewHandler(b)
	e := echo.New()

	return h, b, e
}

func accuracyPost(t *testing.T, h *sts.Handler, e *echo.Echo, form url.Values) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	req = req.WithContext(logger.Save(req.Context(), nil))

	require.NoError(t, h.Handler()(e.NewContext(req, rec)))

	return rec
}

func decodeError(t *testing.T, body []byte) sts.ErrorResponse {
	t.Helper()

	var resp sts.ErrorResponse
	require.NoError(t, xml.Unmarshal(body, &resp))

	return resp
}

// ── Gap #8: GetSessionToken max duration 129600 ───────────────────────────────

func TestAccuracy_Gap8_GetSessionToken_MaxDuration(t *testing.T) {
	t.Parallel()

	t.Run("accepts_129600", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.GetSessionToken(&sts.GetSessionTokenInput{
			DurationSeconds: 129600,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.GetSessionTokenResult.Credentials.AccessKeyID)
	})

	t.Run("rejects_above_129600", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.GetSessionToken(&sts.GetSessionTokenInput{
			DurationSeconds: 129601,
		})
		require.ErrorIs(t, err, sts.ErrInvalidDuration)
	})

	t.Run("rejects_old_max_43200_was_valid_now_still_valid", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.GetSessionToken(&sts.GetSessionTokenInput{
			DurationSeconds: 43200,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.GetSessionTokenResult.Credentials.AccessKeyID)
	})
}

// ── Gap #9: TokenCode and SerialNumber validation ─────────────────────────────

func TestAccuracy_Gap9_MFAValidation(t *testing.T) {
	t.Parallel()

	t.Run("valid_token_code_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.GetSessionToken(&sts.GetSessionTokenInput{
			SerialNumber: "arn:aws:iam::123456789012:mfa/my-device",
			TokenCode:    "123456",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.GetSessionTokenResult.Credentials.AccessKeyID)
	})

	t.Run("non_6_digit_code_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.GetSessionToken(&sts.GetSessionTokenInput{
			SerialNumber: "arn:aws:iam::123456789012:mfa/my-device",
			TokenCode:    "12345",
		})
		require.ErrorIs(t, err, sts.ErrInvalidMFATokenCode)
	})

	t.Run("alpha_token_code_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.GetSessionToken(&sts.GetSessionTokenInput{
			SerialNumber: "arn:aws:iam::123456789012:mfa/my-device",
			TokenCode:    "abc123",
		})
		require.ErrorIs(t, err, sts.ErrInvalidMFATokenCode)
	})

	t.Run("seven_digits_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.GetSessionToken(&sts.GetSessionTokenInput{
			SerialNumber: "arn:aws:iam::123456789012:mfa/my-device",
			TokenCode:    "1234567",
		})
		require.ErrorIs(t, err, sts.ErrInvalidMFATokenCode)
	})

	t.Run("valid_mfa_arn_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.GetSessionToken(&sts.GetSessionTokenInput{
			SerialNumber: "arn:aws:iam::000000000000:mfa/virtual-device",
			TokenCode:    "999999",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.GetSessionTokenResult.Credentials.AccessKeyID)
	})

	t.Run("malformed_serial_number_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.GetSessionToken(&sts.GetSessionTokenInput{
			SerialNumber: "not-a-serial-number",
			TokenCode:    "123456",
		})
		require.ErrorIs(t, err, sts.ErrInvalidMFASerialNumber)
	})
}

// ── Gap #10: GetCallerIdentity X-Amz-Security-Token validation ────────────────

func TestAccuracy_Gap10_GetCallerIdentity_SessionToken(t *testing.T) {
	t.Parallel()

	t.Run("correct_session_token_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		// Issue a session.
		assumeResp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
			RoleSessionName: "my-session",
		})
		require.NoError(t, err)

		accessKeyID := assumeResp.AssumeRoleResult.Credentials.AccessKeyID
		sessionToken := assumeResp.AssumeRoleResult.Credentials.SessionToken

		// GetCallerIdentity with matching session token.
		resp, err := b.GetCallerIdentity(accessKeyID, sessionToken)
		require.NoError(t, err)
		assert.Contains(t, resp.GetCallerIdentityResult.Arn, "assumed-role")
	})

	t.Run("wrong_session_token_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		assumeResp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
			RoleSessionName: "my-session",
		})
		require.NoError(t, err)

		accessKeyID := assumeResp.AssumeRoleResult.Credentials.AccessKeyID

		_, err = b.GetCallerIdentity(accessKeyID, "wrong-token")
		require.ErrorIs(t, err, sts.ErrAccessDenied)
	})

	t.Run("http_request_uses_x_amz_security_token_header", func(t *testing.T) {
		t.Parallel()

		h, b, e := accuracyHandler(t)
		assumeResp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
			RoleSessionName: "session",
		})
		require.NoError(t, err)

		accessKeyID := assumeResp.AssumeRoleResult.Credentials.AccessKeyID
		sessionToken := assumeResp.AssumeRoleResult.Credentials.SessionToken

		form := url.Values{
			"Action":  {"GetCallerIdentity"},
			"Version": {"2011-06-15"},
		}
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set(
			"Authorization",
			"AWS4-HMAC-SHA256 Credential="+accessKeyID+"/20240101/us-east-1/sts/aws4_request",
		)
		req.Header.Set("X-Amz-Security-Token", sessionToken)
		rec := httptest.NewRecorder()
		req = req.WithContext(logger.Save(req.Context(), nil))

		require.NoError(t, h.Handler()(e.NewContext(req, rec)))
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp sts.GetCallerIdentityResponse
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Contains(t, resp.GetCallerIdentityResult.Arn, "assumed-role")
		assert.Contains(t, resp.GetCallerIdentityResult.Arn, "TestRole")
	})

	t.Run("http_request_wrong_x_amz_security_token_returns_403", func(t *testing.T) {
		t.Parallel()

		h, b, e := accuracyHandler(t)
		assumeResp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
			RoleSessionName: "session",
		})
		require.NoError(t, err)

		accessKeyID := assumeResp.AssumeRoleResult.Credentials.AccessKeyID

		form := url.Values{
			"Action":  {"GetCallerIdentity"},
			"Version": {"2011-06-15"},
		}
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.Header.Set(
			"Authorization",
			"AWS4-HMAC-SHA256 Credential="+accessKeyID+"/20240101/us-east-1/sts/aws4_request",
		)
		req.Header.Set("X-Amz-Security-Token", "bad-token")
		rec := httptest.NewRecorder()
		req = req.WithContext(logger.Save(req.Context(), nil))

		require.NoError(t, h.Handler()(e.NewContext(req, rec)))
		assert.Equal(t, http.StatusForbidden, rec.Code)
	})
}

// ── Gap #11: ValidateSessionCredential ───────────────────────────────────────

func TestAccuracy_Gap11_ValidateSessionCredential(t *testing.T) {
	t.Parallel()

	t.Run("valid_credential_returns_session_info", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		assumeResp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
			RoleSessionName: "my-session",
		})
		require.NoError(t, err)

		creds := assumeResp.AssumeRoleResult.Credentials
		info, err := b.ValidateSessionCredential(creds.AccessKeyID, creds.SessionToken)
		require.NoError(t, err)
		assert.Contains(t, info.AssumedRoleArn, "assumed-role")
		assert.Equal(t, "123456789012", info.AccountID)
	})

	t.Run("unknown_key_returns_session_not_found", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.ValidateSessionCredential("ASIANOTEXIST123456789", "any-token")
		require.ErrorIs(t, err, sts.ErrSessionNotFound)
	})

	t.Run("wrong_token_returns_access_denied", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		assumeResp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
			RoleSessionName: "my-session",
		})
		require.NoError(t, err)

		creds := assumeResp.AssumeRoleResult.Credentials
		_, err = b.ValidateSessionCredential(creds.AccessKeyID, "wrong-token")
		require.ErrorIs(t, err, sts.ErrAccessDenied)
	})

	t.Run("expired_session_returns_not_found", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		assumeResp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
			RoleSessionName: "my-session",
		})
		require.NoError(t, err)

		creds := assumeResp.AssumeRoleResult.Credentials
		b.SetSessionExpiration(creds.AccessKeyID, time.Now().Add(-time.Minute))

		_, err = b.ValidateSessionCredential(creds.AccessKeyID, creds.SessionToken)
		require.ErrorIs(t, err, sts.ErrSessionNotFound)
	})

	t.Run("session_stores_secret_access_key", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		assumeResp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/TestRole",
			RoleSessionName: "my-session",
		})
		require.NoError(t, err)

		creds := assumeResp.AssumeRoleResult.Credentials
		info, err := b.ValidateSessionCredential(creds.AccessKeyID, creds.SessionToken)
		require.NoError(t, err)
		assert.Equal(t, creds.SecretAccessKey, info.SecretAccessKey)
		assert.Equal(t, creds.SessionToken, info.SessionToken)
	})
}

// ── Gap #14: GetAccessKeyInfo well-formed key decoding ────────────────────────

func TestAccuracy_Gap14_GetAccessKeyInfo(t *testing.T) {
	t.Parallel()

	wellFormedPrefixes := []string{"AKIA", "ASIA", "AIDA", "AROA", "AGPA"}

	for _, prefix := range wellFormedPrefixes {
		t.Run("well_formed_"+prefix+"_returns_account", func(t *testing.T) {
			t.Parallel()

			h, _, e := accuracyHandler(t)
			key := prefix + "ABCDEFGHIJ123456"
			form := url.Values{
				"Action":      {"GetAccessKeyInfo"},
				"Version":     {"2011-06-15"},
				"AccessKeyId": {key},
			}
			rec := accuracyPost(t, h, e, form)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"GetAccessKeyInfoResponse"`
				Result  struct {
					Account string `xml:"Account"`
				} `xml:"GetAccessKeyInfoResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.Result.Account)
		})
	}

	t.Run("completely_malformed_key_returns_validation_error", func(t *testing.T) {
		t.Parallel()

		h, _, e := accuracyHandler(t)
		form := url.Values{
			"Action":      {"GetAccessKeyInfo"},
			"Version":     {"2011-06-15"},
			"AccessKeyId": {"badkey"},
		}
		rec := accuracyPost(t, h, e, form)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		errResp := decodeError(t, rec.Body.Bytes())
		assert.Equal(t, "ValidationError", errResp.Error.Code)
	})

	t.Run("issued_key_returns_correct_account", func(t *testing.T) {
		t.Parallel()

		h, b, e := accuracyHandler(t)
		assumeResp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::987654321098:role/TestRole",
			RoleSessionName: "sess",
		})
		require.NoError(t, err)

		key := assumeResp.AssumeRoleResult.Credentials.AccessKeyID
		form := url.Values{
			"Action":      {"GetAccessKeyInfo"},
			"Version":     {"2011-06-15"},
			"AccessKeyId": {key},
		}
		rec := accuracyPost(t, h, e, form)
		assert.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			XMLName xml.Name `xml:"GetAccessKeyInfoResponse"`
			Result  struct {
				Account string `xml:"Account"`
			} `xml:"GetAccessKeyInfoResult"`
		}
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Equal(t, "987654321098", resp.Result.Account)
	})
}

// ── Gap #15: AssumeRole default duration clamped to role max ──────────────────

func TestAccuracy_Gap15_AssumeRole_DefaultDurationClamped(t *testing.T) {
	t.Parallel()

	t.Run("default_clamped_when_role_max_is_small", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		b.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{
			MaxSessionDuration: 900,
		}})

		// With DurationSeconds=0, default is clamped to min(3600, 900)=900 — should succeed.
		resp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/SmallMaxRole",
			RoleSessionName: "session",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.AssumeRoleResult.Credentials.AccessKeyID)
	})

	t.Run("explicit_value_exceeding_role_max_still_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		b.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{
			MaxSessionDuration: 900,
		}})

		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/SmallMaxRole",
			RoleSessionName: "session",
			DurationSeconds: 1800,
		})
		require.ErrorIs(t, err, sts.ErrInvalidDuration)
	})
}

// ── Gap #17: SourceIdentity validation ───────────────────────────────────────

func TestAccuracy_Gap17_SourceIdentityValidation(t *testing.T) {
	t.Parallel()

	validIdentities := []string{
		"Alice",
		"user@example.com",
		"user+tag=value",
		"AB",
		"a-b_c.d,e",
	}

	for _, id := range validIdentities {
		t.Run("valid_"+id, func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::123456789012:role/R",
				RoleSessionName: "session",
				SourceIdentity:  id,
			})
			require.NoError(t, err)
		})
	}

	invalidIdentities := []string{
		"a",                     // too short
		strings.Repeat("x", 65), // too long
		"bad spaces",            // spaces not allowed
		"bad/slash",             // slash not allowed
	}

	for _, id := range invalidIdentities {
		t.Run("invalid_source_identity", func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.AssumeRole(&sts.AssumeRoleInput{
				RoleArn:         "arn:aws:iam::123456789012:role/R",
				RoleSessionName: "session",
				SourceIdentity:  id,
			})
			require.ErrorIs(t, err, sts.ErrInvalidSourceIdentity)
		})
	}
}

// ── Gap #18: Session tag constraint validation ────────────────────────────────

func TestAccuracy_Gap18_SessionTagConstraints(t *testing.T) {
	t.Parallel()

	t.Run("aws_prefix_tag_key_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: "aws:reserved", Value: "val"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("AWS_uppercase_prefix_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: "AWS:Tag", Value: "val"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("empty_tag_key_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: "", Value: "val"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("tag_value_over_256_chars_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: "mykey", Value: strings.Repeat("v", 257)}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagValue)
	})

	t.Run("duplicate_tag_key_case_insensitive_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: "MyKey", Value: "v1"}, {Key: "mykey", Value: "v2"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("tag_key_128_chars_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: strings.Repeat("k", 128), Value: "val"}},
		})
		require.NoError(t, err)
	})

	t.Run("tag_key_129_chars_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: strings.Repeat("k", 129), Value: "val"}},
		})
		require.ErrorIs(t, err, sts.ErrInvalidTagKey)
	})

	t.Run("tag_value_256_chars_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Tags:            []sts.Tag{{Key: "mykey", Value: strings.Repeat("v", 256)}},
		})
		require.NoError(t, err)
	})
}

// ── Gap #19: PackedPolicySize includes managed policy ARNs ───────────────────

func TestAccuracy_Gap19_PackedPolicySizeWithArns(t *testing.T) {
	t.Parallel()

	t.Run("managed_policy_arns_contribute_to_size", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		policyArn := "arn:aws:iam::aws:policy/ReadOnlyAccess"
		resp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			PolicyArns:      []string{policyArn},
		})
		require.NoError(t, err)

		// With an ARN but no inline policy, size should be > 0.
		assert.Positive(t, resp.AssumeRoleResult.PackedPolicySize)
	})

	t.Run("size_exceeds_budget_returns_error", func(t *testing.T) {
		t.Parallel()

		// A single valid JSON policy whose total size exceeds 2048 bytes.
		bigPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"` +
			strings.Repeat("arn:aws:s3:::my-bucket/prefix/", 80) + `*"}]}`
		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Policy:          bigPolicy,
		})
		require.ErrorIs(t, err, sts.ErrPackedPolicyTooLarge)
	})

	t.Run("ceiling_rounding_correct", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		smallPolicy := `{"Statement":[]}`
		resp, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			Policy:          smallPolicy,
		})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, resp.AssumeRoleResult.PackedPolicySize, int32(1))
	})
}

// ── Gap #20: Error code mapping ───────────────────────────────────────────────

func TestAccuracy_Gap20_ErrorCodes(t *testing.T) {
	t.Parallel()

	t.Run("malformed_policy_returns_MalformedPolicyDocument", func(t *testing.T) {
		t.Parallel()

		h, _, e := accuracyHandler(t)
		form := url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::123456789012:role/R"},
			"RoleSessionName": {"session"},
			"Policy":          {"not-valid-json"},
		}
		rec := accuracyPost(t, h, e, form)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		errResp := decodeError(t, rec.Body.Bytes())
		assert.Equal(t, "MalformedPolicyDocument", errResp.Error.Code)
	})

	t.Run("policy_too_large_returns_PackedPolicyTooLarge", func(t *testing.T) {
		t.Parallel()

		h, _, e := accuracyHandler(t)
		// Build a valid JSON string that exceeds the 2048-byte session-policy limit.
		bigPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"` +
			strings.Repeat("arn:aws:s3:::my-bucket/prefix/", 80) + `*"}]}`
		form := url.Values{
			"Action":          {"AssumeRole"},
			"Version":         {"2011-06-15"},
			"RoleArn":         {"arn:aws:iam::123456789012:role/R"},
			"RoleSessionName": {"session"},
			"Policy":          {bigPolicy},
		}
		rec := accuracyPost(t, h, e, form)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		errResp := decodeError(t, rec.Body.Bytes())
		assert.Equal(t, "PackedPolicyTooLarge", errResp.Error.Code)
	})

	t.Run("too_many_policy_arns_returns_ValidationError", func(t *testing.T) {
		t.Parallel()

		h, _, e := accuracyHandler(t)
		form := url.Values{
			"Action":                   {"AssumeRole"},
			"Version":                  {"2011-06-15"},
			"RoleArn":                  {"arn:aws:iam::123456789012:role/R"},
			"RoleSessionName":          {"session"},
			"PolicyArns.member.1.arn":  {"arn:aws:iam::aws:policy/A"},
			"PolicyArns.member.2.arn":  {"arn:aws:iam::aws:policy/B"},
			"PolicyArns.member.3.arn":  {"arn:aws:iam::aws:policy/C"},
			"PolicyArns.member.4.arn":  {"arn:aws:iam::aws:policy/D"},
			"PolicyArns.member.5.arn":  {"arn:aws:iam::aws:policy/E"},
			"PolicyArns.member.6.arn":  {"arn:aws:iam::aws:policy/F"},
			"PolicyArns.member.7.arn":  {"arn:aws:iam::aws:policy/G"},
			"PolicyArns.member.8.arn":  {"arn:aws:iam::aws:policy/H"},
			"PolicyArns.member.9.arn":  {"arn:aws:iam::aws:policy/I"},
			"PolicyArns.member.10.arn": {"arn:aws:iam::aws:policy/J"},
			"PolicyArns.member.11.arn": {"arn:aws:iam::aws:policy/K"},
		}
		rec := accuracyPost(t, h, e, form)
		assert.Equal(t, http.StatusBadRequest, rec.Code)

		errResp := decodeError(t, rec.Body.Bytes())
		assert.Equal(t, "ValidationError", errResp.Error.Code)
	})
}

// ── Gap #21: RoleSessionName disallows colon ─────────────────────────────────

func TestAccuracy_Gap21_RoleSessionName_ColonRejected(t *testing.T) {
	t.Parallel()

	b := sts.NewInMemoryBackend()
	_, err := b.AssumeRole(&sts.AssumeRoleInput{
		RoleArn:         "arn:aws:iam::123456789012:role/R",
		RoleSessionName: "bad:name",
	})
	require.ErrorIs(t, err, sts.ErrInvalidSessionName)
}

func TestAccuracy_Gap21_RoleSessionName_ColonRejected_HTTP(t *testing.T) {
	t.Parallel()

	h, _, e := accuracyHandler(t)
	form := url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::123456789012:role/R"},
		"RoleSessionName": {"bad:session:name"},
	}
	rec := accuracyPost(t, h, e, form)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	errResp := decodeError(t, rec.Body.Bytes())
	assert.Equal(t, "ValidationError", errResp.Error.Code)
}

// ── Gap #23: AssumeRoot exact 900s only ──────────────────────────────────────

func TestAccuracy_Gap23_AssumeRoot_ExactDuration(t *testing.T) {
	t.Parallel()

	t.Run("duration_900_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.AssumeRoot(&sts.AssumeRootInput{
			TargetPrincipal: "123456789012",
			TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
			DurationSeconds: 900,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.AssumeRootResult.Credentials.AccessKeyID)
	})

	t.Run("duration_0_defaults_to_900_accepted", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.AssumeRoot(&sts.AssumeRootInput{
			TargetPrincipal: "123456789012",
			TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.AssumeRootResult.Credentials.AccessKeyID)
	})

	t.Run("duration_1800_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoot(&sts.AssumeRootInput{
			TargetPrincipal: "123456789012",
			TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
			DurationSeconds: 1800,
		})
		require.ErrorIs(t, err, sts.ErrInvalidDuration)
	})

	t.Run("duration_899_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoot(&sts.AssumeRootInput{
			TargetPrincipal: "123456789012",
			TaskPolicyArn:   "arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
			DurationSeconds: 899,
		})
		require.ErrorIs(t, err, sts.ErrInvalidDuration)
	})
}

// ── Gap #7: AssumeRoot approved task policy ARN list ─────────────────────────

func TestAccuracy_Gap7_AssumeRoot_ApprovedPolicyArns(t *testing.T) {
	t.Parallel()

	approved := []string{
		"arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials",
		"arn:aws:iam::aws:policy/root-task/IAMCreateRootUserPassword",
		"arn:aws:iam::aws:policy/root-task/IAMDeleteRootUserCredentials",
		"arn:aws:iam::aws:policy/root-task/S3UnlockBucketPolicy",
		"arn:aws:iam::aws:policy/root-task/SQSUnlockQueuePolicy",
	}

	for _, policyArn := range approved {
		t.Run("approved_"+policyArn[len(policyArn)-15:], func(t *testing.T) {
			t.Parallel()

			b := sts.NewInMemoryBackend()
			_, err := b.AssumeRoot(&sts.AssumeRootInput{
				TargetPrincipal: "123456789012",
				TaskPolicyArn:   policyArn,
			})
			require.NoError(t, err)
		})
	}

	t.Run("non_approved_arn_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoot(&sts.AssumeRootInput{
			TargetPrincipal: "123456789012",
			TaskPolicyArn:   "arn:aws:iam::aws:policy/AdministratorAccess",
		})
		require.ErrorIs(t, err, sts.ErrValidation)
	})
}

// ── Gap #4: PolicyArns validation ────────────────────────────────────────────

func TestAccuracy_Gap4_PolicyArnsValidation(t *testing.T) {
	t.Parallel()

	t.Run("too_many_policy_arns_rejected", func(t *testing.T) {
		t.Parallel()

		arns := make([]string, 11)
		for i := range arns {
			arns[i] = "arn:aws:iam::aws:policy/Policy" + string(rune('A'+i))
		}

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			PolicyArns:      arns,
		})
		require.ErrorIs(t, err, sts.ErrTooManyPolicyArns)
	})

	t.Run("malformed_policy_arn_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			PolicyArns:      []string{"not-an-arn"},
		})
		require.ErrorIs(t, err, sts.ErrInvalidPolicyArn)
	})

	t.Run("ten_policy_arns_accepted", func(t *testing.T) {
		t.Parallel()

		arns := make([]string, 10)
		for i := range arns {
			arns[i] = "arn:aws:iam::aws:policy/Policy" + string(rune('A'+i))
		}

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			PolicyArns:      arns,
		})
		require.NoError(t, err)
	})
}

// ── Gap #6: JWT expiry validation ─────────────────────────────────────────────

// buildJWT constructs a minimal unsigned JWT with the given claims for testing.
func buildJWT(t *testing.T, claims map[string]any) string {
	t.Helper()

	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"mock","typ":"JWT"}`))
	payload, err := json.Marshal(claims)
	require.NoError(t, err)

	return header + "." + base64.RawURLEncoding.EncodeToString(payload) + ".fakesig"
}

func TestAccuracy_Gap6_JWTExpiry(t *testing.T) {
	t.Parallel()

	t.Run("expired_jwt_returns_ErrExpiredToken", func(t *testing.T) {
		t.Parallel()

		expiredToken := buildJWT(t, map[string]any{
			"sub": "test-user",
			"iss": "https://example.com",
			"exp": time.Now().Add(-time.Hour).Unix(),
			"aud": "my-client",
		})

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
			RoleArn:          "arn:aws:iam::123456789012:role/R",
			RoleSessionName:  "session",
			WebIdentityToken: expiredToken,
		})
		require.ErrorIs(t, err, sts.ErrExpiredToken)
	})

	t.Run("valid_jwt_exp_accepted", func(t *testing.T) {
		t.Parallel()

		validToken := buildJWT(t, map[string]any{
			"sub": "test-user",
			"iss": "https://example.com",
			"exp": time.Now().Add(time.Hour).Unix(),
			"aud": "my-client",
		})

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
			RoleArn:          "arn:aws:iam::123456789012:role/R",
			RoleSessionName:  "session",
			WebIdentityToken: validToken,
		})
		require.NoError(t, err)
	})
}

// ── Gap #3: ProvidedContexts validation ──────────────────────────────────────

func TestAccuracy_Gap3_ProvidedContextsValidation(t *testing.T) {
	t.Parallel()

	t.Run("too_many_provided_contexts_rejected", func(t *testing.T) {
		t.Parallel()

		ctxs := make([]sts.ProvidedContext, 6)
		for i := range ctxs {
			ctxs[i] = sts.ProvidedContext{
				ProviderArn:      "arn:aws:iam::123456789012:oidc-provider/example.com",
				ContextAssertion: "assertion",
			}
		}

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:          "arn:aws:iam::123456789012:role/R",
			RoleSessionName:  "session",
			ProvidedContexts: ctxs,
		})
		require.ErrorIs(t, err, sts.ErrInvalidProvidedContext)
	})

	t.Run("context_assertion_over_2048_chars_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		_, err := b.AssumeRole(&sts.AssumeRoleInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			RoleSessionName: "session",
			ProvidedContexts: []sts.ProvidedContext{
				{
					ProviderArn:      "arn:aws:iam::123456789012:oidc-provider/example.com",
					ContextAssertion: strings.Repeat("x", 2049),
				},
			},
		})
		require.ErrorIs(t, err, sts.ErrInvalidProvidedContext)
	})
}
