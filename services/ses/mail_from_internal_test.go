package ses

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSendEmail_MailFromDomainNotVerified proves gopherstack-nbp:
// checkMailFromLocked (identities.go) really returns
// ErrMailFromDomainNotVerified when an identity's MailFromStatus is not
// Success and BehaviorOnMXFailure is RejectMessage. SetIdentityMailFromDomain
// always sets MailFromStatus to Success immediately (this service's
// instant-verification convention -- see checkMailFromLocked's doc
// comment), so there is no client-reachable way to produce a non-Success
// status; this white-box test (package ses, not ses_test) reaches into
// b.identities directly -- the same internal seam every other backend
// method already uses -- to force one, which is the only way this real
// code path is ever exercised.
func TestSendEmail_MailFromDomainNotVerified(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		mailFromStatus string
		behavior       string
		wantErr        bool
	}{
		{
			name:           "reject_on_non_success",
			mailFromStatus: "Pending",
			behavior:       behaviorOnMXFailureReject,
			wantErr:        true,
		},
		{
			name:           "use_default_on_non_success_is_permissive",
			mailFromStatus: "Pending",
			behavior:       behaviorOnMXFailureUseDefault,
			wantErr:        false,
		},
		{
			name:           "reject_on_success_is_permissive",
			mailFromStatus: identityStatusSuccess,
			behavior:       behaviorOnMXFailureReject,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			_, err := b.VerifyDomainIdentity("example.com")
			require.NoError(t, err)
			require.NoError(t, b.SetIdentityMailFromDomain("example.com", "mail.example.com", tt.behavior))

			rec, ok := b.identities.Get("example.com")
			require.True(t, ok)
			rec.MailFromStatus = tt.mailFromStatus

			_, err = b.SendEmail(SendEmailInput{
				From: "sender@example.com", To: []string{"to@example.com"}, Subject: "s", BodyText: "b",
			})

			if !tt.wantErr {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrMailFromDomainNotVerified)
		})
	}
}
