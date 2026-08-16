package ses_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

func TestHandler_SetIdentityDkimEnabled(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "creates_identity_if_missing",
			body:         "Action=SetIdentityDkimEnabled&Version=2010-12-01&Identity=new@example.com&DkimEnabled=true",
			wantCode:     http.StatusOK,
			wantContains: "SetIdentityDkimEnabledResponse",
		},
		{
			name:         "missing_identity_param",
			body:         "Action=SetIdentityDkimEnabled&Version=2010-12-01&Identity=&DkimEnabled=true",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_VerifyDomainDkim_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "empty_domain",
			body:         "Action=VerifyDomainDkim&Version=2010-12-01&Domain=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "valid_domain",
			body:         "Action=VerifyDomainDkim&Version=2010-12-01&Domain=example.com",
			wantCode:     http.StatusOK,
			wantContains: "VerifyDomainDkimResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestVerifyDomainDkim_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":  {"VerifyDomainDkim"},
		"Version": {"2010-12-01"},
		"Domain":  {"dkim.example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "VerifyDomainDkimResponse")
	assert.Contains(t, rec.Body.String(), "DkimTokens")
}

func TestGetIdentityDkimAttributes_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("dkim@example.com"))
	require.NoError(t, h.Backend.SetIdentityDkimEnabled("dkim@example.com", true))

	rec := postForm(t, h, url.Values{
		"Action":              {"GetIdentityDkimAttributes"},
		"Version":             {"2010-12-01"},
		"Identities.member.1": {"dkim@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetIdentityDkimAttributesResponse")
}

func TestSetIdentityDkimEnabled_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("d@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":      {"SetIdentityDkimEnabled"},
		"Version":     {"2010-12-01"},
		"Identity":    {"d@example.com"},
		"DkimEnabled": {"true"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	attrs := h.Backend.GetIdentityDkimAttributes([]string{"d@example.com"})
	assert.True(t, attrs["d@example.com"].DkimEnabled)
}

func TestSetIdentityDkimEnabled_False(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("d@example.com"))
	require.NoError(t, b.SetIdentityDkimEnabled("d@example.com", true))
	require.NoError(t, b.SetIdentityDkimEnabled("d@example.com", false))

	attrs := b.GetIdentityDkimAttributes([]string{"d@example.com"})
	assert.False(t, attrs["d@example.com"].DkimEnabled)
}

func TestDkimTokens_AfterDomainVerify(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	tokens, err := b.VerifyDomainDkim("tokens.example.com")
	require.NoError(t, err)
	require.Len(t, tokens, 3)
	for _, tok := range tokens {
		assert.NotEmpty(t, tok)
		assert.Len(t, tok, 32)
	}

	attrs := b.GetIdentityDkimAttributes([]string{"tokens.example.com"})
	assert.Equal(t, "Success", attrs["tokens.example.com"].DkimVerificationStatus)
	assert.Equal(t, tokens, attrs["tokens.example.com"].DkimTokens)
}

func TestDkimAttributes_Unknown_NotStarted(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	attrs := b.GetIdentityDkimAttributes([]string{"unknown@example.com"})
	assert.Equal(t, "NotStarted", attrs["unknown@example.com"].DkimVerificationStatus)
	assert.Empty(t, attrs["unknown@example.com"].DkimTokens)
}

func TestVerifyDomainDkim_DeterministicTokens(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()

	tokens1, err := b.VerifyDomainDkim("example.com")
	require.NoError(t, err)
	assert.Len(t, tokens1, 3)

	tokens2, err := b.VerifyDomainDkim("example.com")
	require.NoError(t, err)
	assert.Equal(t, tokens1, tokens2, "tokens must be deterministic")

	tokensOther, err := b.VerifyDomainDkim("other.com")
	require.NoError(t, err)
	assert.NotEqual(t, tokens1, tokensOther, "different domains get different tokens")
}

func TestGetIdentityDkimAttributes_AfterVerify(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	_, err := b.VerifyDomainDkim("example.com")
	require.NoError(t, err)

	attrs := b.GetIdentityDkimAttributes([]string{"example.com", "unknown.com"})

	assert.Equal(t, "Success", attrs["example.com"].DkimVerificationStatus)
	assert.Len(t, attrs["example.com"].DkimTokens, 3)
	assert.Equal(t, "NotStarted", attrs["unknown.com"].DkimVerificationStatus)
}

func TestSetIdentityDkimEnabled_Persists(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("a@example.com"))
	require.NoError(t, b.SetIdentityDkimEnabled("a@example.com", true))

	attrs := b.GetIdentityDkimAttributes([]string{"a@example.com"})
	assert.True(t, attrs["a@example.com"].DkimEnabled)
}

func TestGetIdentityDkimAttributes_EmailIdentityNotStarted(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("a@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":              {"GetIdentityDkimAttributes"},
		"Version":             {"2010-12-01"},
		"Identities.member.1": {"a@example.com"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "GetIdentityDkimAttributesResponse")
	assert.Contains(t, body, "<DkimVerificationStatus>NotStarted</DkimVerificationStatus>",
		"email identity without DKIM setup must have NotStarted status")
}

func TestGetIdentityDkimAttributes_DomainAfterVerifyDkim(t *testing.T) {
	t.Parallel()

	h := newHandler()
	tokens, err := h.Backend.(*ses.InMemoryBackend).VerifyDomainDkim("example.com")
	require.NoError(t, err)
	require.Len(t, tokens, 3, "VerifyDomainDkim must return 3 tokens")

	rec := postForm(t, h, url.Values{
		"Action":              {"GetIdentityDkimAttributes"},
		"Version":             {"2010-12-01"},
		"Identities.member.1": {"example.com"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<DkimVerificationStatus>Success</DkimVerificationStatus>",
		"domain after VerifyDomainDkim must have Success status")
	assert.Contains(t, body, "<DkimTokens>", "DkimTokens must be present")
}
