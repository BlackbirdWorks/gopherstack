package ses_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_SendEmailRequiresDestination verifies that SendEmail rejects
// requests with no destination addresses. Real AWS requires at least one
// To, Cc, or Bcc address; the emulator previously accepted an empty
// Destination and stored the email without any recipients.
func TestParity_SendEmailRequiresDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{
			name: "absent_destination_rejected",
			body: url.Values{
				"Action":                 {"SendEmail"},
				"Version":                {"2010-12-01"},
				"Source":                 {"sender@example.com"},
				"Message.Subject.Data":   {"Hello"},
				"Message.Body.Text.Data": {"body text"},
			}.Encode(),
			wantCode: http.StatusBadRequest,
		},
		{
			name: "with_to_address_accepted",
			body: url.Values{
				"Action":                           {"SendEmail"},
				"Version":                          {"2010-12-01"},
				"Source":                           {"sender@example.com"},
				"Destination.ToAddresses.member.1": {"rcpt@example.com"},
				"Message.Subject.Data":             {"Hello"},
				"Message.Body.Text.Data":           {"body text"},
			}.Encode(),
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.wantCode == http.StatusOK {
				require.NoError(t, h.Backend.VerifyEmailIdentity("sender@example.com"))
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"SendEmail status for case %q", tt.name)
		})
	}
}
