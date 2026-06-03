package sts_test

import (
	"bytes"
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sts"
)

// ── Batch-2 Issue #1: DecodeAuthorizationMessage invalid base64 → HTTP 400 ───

func TestBatch2_DecodeAuthorizationMessage_InvalidBase64_Returns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		encoded string
	}{
		{
			name:    "garbage_string",
			encoded: "this-is-not-base64!!!",
		},
		{
			name:    "truncated_base64",
			encoded: "SGVsbG8=truncated",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _, e := accuracyHandler(t)
			form := url.Values{
				"Action":         {"DecodeAuthorizationMessage"},
				"Version":        {"2011-06-15"},
				"EncodedMessage": {tt.encoded},
			}
			rec := accuracyPost(t, h, e, form)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			errResp := decodeError(t, rec.Body.Bytes())
			assert.Equal(t, "InvalidAuthorizationMessageException", errResp.Error.Code)
		})
	}
}

// ── Batch-2 Issue #2: XML response field ordering — result before metadata ────

// xmlElementOrder parses raw XML and returns the top-level child element names in order.
func xmlElementOrder(t *testing.T, data []byte) []string {
	t.Helper()

	d := xml.NewDecoder(bytes.NewReader(data))
	var order []string
	depth := 0

	for {
		tok, err := d.Token()
		if err != nil {
			break
		}

		switch se := tok.(type) {
		case xml.StartElement:
			depth++
			if depth == 2 {
				order = append(order, se.Name.Local)
			}
		case xml.EndElement:
			depth--
		}
	}

	return order
}

func TestBatch2_AssumeRole_ResultBeforeMetadata(t *testing.T) {
	t.Parallel()

	h, _, e := accuracyHandler(t)
	form := url.Values{
		"Action":          {"AssumeRole"},
		"Version":         {"2011-06-15"},
		"RoleArn":         {"arn:aws:iam::123456789012:role/R"},
		"RoleSessionName": {"session"},
	}
	rec := accuracyPost(t, h, e, form)
	require.Equal(t, http.StatusOK, rec.Code)

	order := xmlElementOrder(t, rec.Body.Bytes())
	require.Len(t, order, 2)
	assert.Equal(t, "AssumeRoleResult", order[0])
	assert.Equal(t, "ResponseMetadata", order[1])
}

func TestBatch2_GetFederationToken_ResultBeforeMetadata(t *testing.T) {
	t.Parallel()

	h, _, e := accuracyHandler(t)
	form := url.Values{
		"Action":  {"GetFederationToken"},
		"Version": {"2011-06-15"},
		"Name":    {"alice"},
	}
	rec := accuracyPost(t, h, e, form)
	require.Equal(t, http.StatusOK, rec.Code)

	order := xmlElementOrder(t, rec.Body.Bytes())
	require.Len(t, order, 2)
	assert.Equal(t, "GetFederationTokenResult", order[0])
	assert.Equal(t, "ResponseMetadata", order[1])
}

func TestBatch2_AssumeRoleWithSAML_ResultBeforeMetadata(t *testing.T) {
	t.Parallel()

	h, _, e := accuracyHandler(t)
	form := url.Values{
		"Action":        {"AssumeRoleWithSAML"},
		"Version":       {"2011-06-15"},
		"RoleArn":       {"arn:aws:iam::123456789012:role/R"},
		"PrincipalArn":  {"arn:aws:iam::123456789012:saml-provider/MyIdP"},
		"SAMLAssertion": {"assertion"},
	}
	rec := accuracyPost(t, h, e, form)
	require.Equal(t, http.StatusOK, rec.Code)

	order := xmlElementOrder(t, rec.Body.Bytes())
	require.Len(t, order, 2)
	assert.Equal(t, "AssumeRoleWithSAMLResult", order[0])
	assert.Equal(t, "ResponseMetadata", order[1])
}

func TestBatch2_AssumeRoot_ResultBeforeMetadata(t *testing.T) {
	t.Parallel()

	h, _, e := accuracyHandler(t)
	form := url.Values{
		"Action":          {"AssumeRoot"},
		"Version":         {"2011-06-15"},
		"TargetPrincipal": {"123456789012"},
		"TaskPolicyArn":   {"arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials"},
	}
	rec := accuracyPost(t, h, e, form)
	require.Equal(t, http.StatusOK, rec.Code)

	order := xmlElementOrder(t, rec.Body.Bytes())
	require.Len(t, order, 2)
	assert.Equal(t, "AssumeRootResult", order[0])
	assert.Equal(t, "ResponseMetadata", order[1])
}

func TestBatch2_GetDelegatedAccessToken_ResultBeforeMetadata(t *testing.T) {
	t.Parallel()

	h, _, e := accuracyHandler(t)
	form := url.Values{
		"Action":       {"GetDelegatedAccessToken"},
		"Version":      {"2011-06-15"},
		"TradeInToken": {"my-token"},
	}
	rec := accuracyPost(t, h, e, form)
	require.Equal(t, http.StatusOK, rec.Code)

	order := xmlElementOrder(t, rec.Body.Bytes())
	require.Len(t, order, 2)
	assert.Equal(t, "GetDelegatedAccessTokenResult", order[0])
	assert.Equal(t, "ResponseMetadata", order[1])
}

func TestBatch2_GetWebIdentityToken_ResultBeforeMetadata(t *testing.T) {
	t.Parallel()

	h, _, e := accuracyHandler(t)
	form := url.Values{
		"Action":            {"GetWebIdentityToken"},
		"Version":           {"2011-06-15"},
		"Audience.member.1": {"https://example.com"},
		"SigningAlgorithm":  {"RS256"},
	}
	rec := accuracyPost(t, h, e, form)
	require.Equal(t, http.StatusOK, rec.Code)

	order := xmlElementOrder(t, rec.Body.Bytes())
	require.Len(t, order, 2)
	assert.Equal(t, "GetWebIdentityTokenResult", order[0])
	assert.Equal(t, "ResponseMetadata", order[1])
}

func TestBatch2_AssumeRoleWithWebIdentity_ResultBeforeMetadata(t *testing.T) {
	t.Parallel()

	h, _, e := accuracyHandler(t)
	form := url.Values{
		"Action":           {"AssumeRoleWithWebIdentity"},
		"Version":          {"2011-06-15"},
		"RoleArn":          {"arn:aws:iam::123456789012:role/R"},
		"RoleSessionName":  {"session"},
		"WebIdentityToken": {"token"},
	}
	rec := accuracyPost(t, h, e, form)
	require.Equal(t, http.StatusOK, rec.Code)

	order := xmlElementOrder(t, rec.Body.Bytes())
	require.Len(t, order, 2)
	assert.Equal(t, "AssumeRoleWithWebIdentityResult", order[0])
	assert.Equal(t, "ResponseMetadata", order[1])
}

// ── Batch-2 Issue #3: AssumeRoleWithWebIdentity/SAML respect role MaxSessionDuration ──

func TestBatch2_AssumeRoleWithWebIdentity_RespectsRoleMaxSessionDuration(t *testing.T) {
	t.Parallel()

	t.Run("default_clamped_to_role_max", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		b.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{MaxSessionDuration: 900}})

		resp, err := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
			RoleArn:          "arn:aws:iam::123456789012:role/SmallMaxRole",
			RoleSessionName:  "session",
			WebIdentityToken: "token",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.AssumeRoleWithWebIdentityResult.Credentials.AccessKeyID)
	})

	t.Run("explicit_duration_exceeding_role_max_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		b.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{MaxSessionDuration: 900}})

		_, err := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
			RoleArn:          "arn:aws:iam::123456789012:role/SmallMaxRole",
			RoleSessionName:  "session",
			WebIdentityToken: "token",
			DurationSeconds:  1800,
		})
		require.ErrorIs(t, err, sts.ErrInvalidDuration)
	})

	t.Run("no_role_lookup_uses_global_max", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.AssumeRoleWithWebIdentity(&sts.AssumeRoleWithWebIdentityInput{
			RoleArn:          "arn:aws:iam::123456789012:role/R",
			RoleSessionName:  "session",
			WebIdentityToken: "token",
			DurationSeconds:  sts.MaxDurationSeconds,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.AssumeRoleWithWebIdentityResult.Credentials.AccessKeyID)
	})
}

func TestBatch2_AssumeRoleWithSAML_RespectsRoleMaxSessionDuration(t *testing.T) {
	t.Parallel()

	t.Run("default_clamped_to_role_max", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		b.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{MaxSessionDuration: 900}})

		resp, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:       "arn:aws:iam::123456789012:role/SmallMaxRole",
			PrincipalArn:  "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion: "assertion",
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.AssumeRoleWithSAMLResult.Credentials.AccessKeyID)
	})

	t.Run("explicit_duration_exceeding_role_max_rejected", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		b.SetRoleLookup(&stubRoleLookup{meta: &sts.RoleMeta{MaxSessionDuration: 900}})

		_, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:         "arn:aws:iam::123456789012:role/SmallMaxRole",
			PrincipalArn:    "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion:   "assertion",
			DurationSeconds: 1800,
		})
		require.ErrorIs(t, err, sts.ErrInvalidDuration)
	})

	t.Run("no_role_lookup_uses_global_max", func(t *testing.T) {
		t.Parallel()

		b := sts.NewInMemoryBackend()
		resp, err := b.AssumeRoleWithSAML(&sts.AssumeRoleWithSAMLInput{
			RoleArn:         "arn:aws:iam::123456789012:role/R",
			PrincipalArn:    "arn:aws:iam::123456789012:saml-provider/MyIdP",
			SAMLAssertion:   "assertion",
			DurationSeconds: sts.MaxDurationSeconds,
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.AssumeRoleWithSAMLResult.Credentials.AccessKeyID)
	})
}

