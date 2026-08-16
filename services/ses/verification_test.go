package ses_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

func TestHandler_ListVerifiedEmailAddresses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(h *ses.Handler)
		wantContains string
		wantCode     int
	}{
		{
			name:         "empty_list",
			wantCode:     http.StatusOK,
			wantContains: "ListVerifiedEmailAddressesResponse",
		},
		{
			name: "with_verified_email",
			setup: func(h *ses.Handler) {
				require.NoError(t, h.Backend.VerifyEmailIdentity("listed@example.com"))
			},
			wantCode:     http.StatusOK,
			wantContains: "listed@example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postForm(t, h, "Action=ListVerifiedEmailAddresses&Version=2010-12-01")
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_VerifyDomainIdentity_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "empty_domain",
			body:         "Action=VerifyDomainIdentity&Version=2010-12-01&Domain=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "valid_domain",
			body:         "Action=VerifyDomainIdentity&Version=2010-12-01&Domain=example.com",
			wantCode:     http.StatusOK,
			wantContains: "VerifyDomainIdentityResponse",
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

func TestHandler_VerifyEmailAddress_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "empty_email",
			body:         "Action=VerifyEmailAddress&Version=2010-12-01&EmailAddress=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "valid_email",
			body:         "Action=VerifyEmailAddress&Version=2010-12-01&EmailAddress=addr@example.com",
			wantCode:     http.StatusOK,
			wantContains: "VerifyEmailAddressResponse",
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

func TestVerifyDomainIdentity(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":  {"VerifyDomainIdentity"},
		"Version": {"2010-12-01"},
		"Domain":  {"example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "VerifyDomainIdentityResponse")
	assert.Contains(t, rec.Body.String(), "VerificationToken")

	attrs := h.Backend.GetIdentityVerificationAttributes([]string{"example.com"})
	assert.Equal(t, "Success", attrs["example.com"])
}

func TestVerifyDomainIdentity_Deterministic(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	tok1, err := b.VerifyDomainIdentity("example.com")
	require.NoError(t, err)
	tok2, err := b.VerifyDomainIdentity("example.com")
	require.NoError(t, err)
	assert.Equal(t, tok1, tok2, "token must be deterministic")

	tok3, err := b.VerifyDomainIdentity("other.com")
	require.NoError(t, err)
	assert.NotEqual(t, tok1, tok3, "different domains get different tokens")
}

func TestVerifyEmailAddress_LegacyAPI(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":       {"VerifyEmailAddress"},
		"Version":      {"2010-12-01"},
		"EmailAddress": {"legacy@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	addrs := h.Backend.ListVerifiedEmailAddresses()
	assert.Contains(t, addrs, "legacy@example.com")
}

func TestDeleteVerifiedEmailAddress(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("del@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":       {"DeleteVerifiedEmailAddress"},
		"Version":      {"2010-12-01"},
		"EmailAddress": {"del@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	addrs := h.Backend.ListVerifiedEmailAddresses()
	assert.NotContains(t, addrs, "del@example.com")
}

func TestListVerifiedEmailAddresses_FiltersDomains(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("email@example.com"))
	_, err := b.VerifyDomainIdentity("example.com")
	require.NoError(t, err)

	addrs := b.ListVerifiedEmailAddresses()
	assert.Contains(t, addrs, "email@example.com")
	assert.NotContains(t, addrs, "example.com")
}

func TestVerifyDomainIdentity_DeterministicToken(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()

	tok1, err := b.VerifyDomainIdentity("example.com")
	require.NoError(t, err)

	tok2, err := b.VerifyDomainIdentity("example.com")
	require.NoError(t, err)
	assert.Equal(t, tok1, tok2)

	tokOther, err := b.VerifyDomainIdentity("other.com")
	require.NoError(t, err)
	assert.NotEqual(t, tok1, tokOther)
}

func TestSESBackend_DomainLevelVerification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		name      string
		identity  string
		from      string
		wantErr   bool
	}{
		{
			name:     "exact_email_verified",
			identity: "user@example.com",
			from:     "user@example.com",
		},
		{
			name:     "domain_verified_allows_subaddress",
			identity: "example.com",
			from:     "user@example.com",
		},
		{
			name:     "domain_verified_allows_any_address",
			identity: "example.com",
			from:     "other@example.com",
		},
		{
			name:      "unverified_email_rejected",
			identity:  "other@example.com",
			from:      "user@example.com",
			wantErr:   true,
			wantErrIs: ses.ErrMessageRejected,
		},
		{
			name:      "unrelated_domain_rejected",
			identity:  "example.org",
			from:      "user@example.com",
			wantErr:   true,
			wantErrIs: ses.ErrMessageRejected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			require.NoError(t, b.VerifyEmailIdentity(tt.identity))

			_, err := b.SendEmail(ses.SendEmailInput{
				From: tt.from, To: []string{"to@example.com"}, Subject: "s", BodyText: "b",
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
