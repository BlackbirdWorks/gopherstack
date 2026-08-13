package ses_test

import (
	"maps"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/ses"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSendRawEmail_Destinations proves gopherstack-x0sl: SendRawEmail's
// Destinations parameter (wire key "Destinations.member.N", confirmed
// against aws-sdk-go-v2/service/ses@v1.37.4 serializers.go:6682-6684 and
// the AddressList "member" array encoding at serializers.go:4982-4990)
// actually reaches the backend and determines delivery, taking precedence
// over the raw message's To/Cc headers -- rather than being silently parsed
// and discarded, which is indistinguishable from Destinations never being
// read at all.
func TestSendRawEmail_Destinations(t *testing.T) {
	t.Parallel()

	rawMsg := strings.Join([]string{
		"From: raw@example.com",
		"To: visible@example.com",
		"Cc: visible-cc@example.com",
		"Subject: raw",
		"",
		"body",
	}, "\r\n")

	tests := []struct {
		form    url.Values
		name    string
		wantTo  []string
		wantCc  []string
		wantBcc []string
	}{
		{
			// A Destinations entry never mentioned in any header is exactly
			// how Bcc works -- the recipient gets the message despite never
			// appearing in the visible headers.
			name: "bcc only recipient present only in destinations",
			form: url.Values{
				"Destinations.member.1": {"visible@example.com"},
				"Destinations.member.2": {"secret-bcc@example.com"},
			},
			wantTo:  []string{"visible@example.com"},
			wantBcc: []string{"secret-bcc@example.com"},
		},
		{
			// A Destinations entry that also appears in the Cc header is
			// classified as Cc, not lumped into Bcc.
			name: "cc recipient in destinations and header",
			form: url.Values{
				"Destinations.member.1": {"visible@example.com"},
				"Destinations.member.2": {"visible-cc@example.com"},
			},
			wantTo: []string{"visible@example.com"},
			wantCc: []string{"visible-cc@example.com"},
		},
		{
			// No Destinations supplied: headers remain the fallback source.
			// Cc is now parsed from the header too (previously only To was),
			// a direct consequence of this fix threading a real headerCc
			// value through for Destinations classification.
			name:   "no destinations falls back to headers",
			form:   url.Values{},
			wantTo: []string{"visible@example.com"},
			wantCc: []string{"visible-cc@example.com"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ses.NewHandler(ses.NewInMemoryBackend())
			require.NoError(t, h.Backend.VerifyEmailIdentity("raw@example.com"))

			body := url.Values{
				"Action":          {"SendRawEmail"},
				"Version":         {"2010-12-01"},
				"RawMessage.Data": {rawMsg},
			}
			maps.Copy(body, tt.form)

			rec := postForm(t, h, body.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
			require.Len(t, emails, 1)
			e := emails[0]

			assert.ElementsMatch(t, tt.wantTo, e.To)
			assert.ElementsMatch(t, tt.wantCc, e.Cc)
			assert.ElementsMatch(t, tt.wantBcc, e.Bcc)
		})
	}
}
