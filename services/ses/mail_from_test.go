package ses_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// TestSetIdentityMailFromDomain_MustBeSubdomain proves gopherstack-nbp rule
// (a): "The MAIL FROM domain must 1) be a subdomain of the verified
// identity" (api_op_SetIdentityMailFromDomain.go doc comment,
// ses@v1.37.4) -- a MailFromDomain equal to, or not under, the identity's
// own domain is rejected with InvalidParameterValue, matching real AWS SES.
func TestSetIdentityMailFromDomain_MustBeSubdomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		identity       string
		mailFromDomain string
		wantErr        bool
	}{
		{name: "valid_subdomain_of_domain_identity", identity: "example.com", mailFromDomain: "mail.example.com"},
		{
			name: "valid_subdomain_of_email_identity", identity: "user@example.com",
			mailFromDomain: "mail.example.com",
		},
		{name: "equal_to_identity_rejected", identity: "example.com", mailFromDomain: "example.com", wantErr: true},
		{
			name: "unrelated_domain_rejected", identity: "example.com",
			mailFromDomain: "mail.other.com", wantErr: true,
		},
		{
			name: "superdomain_of_identity_rejected", identity: "mail.example.com",
			mailFromDomain: "example.com", wantErr: true,
		},
		{
			name: "suffix_but_not_subdomain_rejected", identity: "example.com",
			mailFromDomain: "notexample.com", wantErr: true,
		},
		{name: "empty_clears_and_is_exempt", identity: "example.com", mailFromDomain: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			err := b.SetIdentityMailFromDomain(tt.identity, tt.mailFromDomain, "")

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ses.ErrInvalidParameter)

				return
			}

			assert.NoError(t, err)
		})
	}
}
