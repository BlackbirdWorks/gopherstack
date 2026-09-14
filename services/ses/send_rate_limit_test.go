package ses_test

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// sendEmailBackend sends one plain email via the backend directly.
func sendEmailBackend(b *ses.InMemoryBackend) error {
	_, err := b.SendEmail(ses.SendEmailInput{
		From: "s@example.com", To: []string{"t@example.com"}, Subject: "s", BodyText: "b",
	})

	return err
}

// TestSendRateLimit_PerSecondWindow proves gopherstack-a6y: GetSendQuota's
// MaxSendRate (1 msg/sec) is now enforced, not just advertised. N sends
// within one second pass, the (N+1)th is throttled, and the window resets
// once the earliest counted send falls more than a second in the past.
func TestSendRateLimit_PerSecondWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		send func(b *ses.InMemoryBackend) error
		name string
	}{
		{
			name: "send_email",
			send: sendEmailBackend,
		},
		{
			name: "send_templated_email",
			send: func(b *ses.InMemoryBackend) error {
				_, err := b.SendTemplatedEmail(ses.SendTemplatedEmailInput{
					From: "s@example.com", To: []string{"t@example.com"}, TemplateName: "tmpl",
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			require.NoError(t, b.VerifyEmailIdentity("s@example.com"))
			require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "tmpl", SubjectPart: "hi"}))

			// 1st send (N == maxSendRate) succeeds.
			require.NoError(t, tc.send(b))

			// (N+1)th send, same second, is throttled.
			err := tc.send(b)
			require.Error(t, err)
			require.ErrorIs(t, err, ses.ErrThrottling)
			assert.Contains(t, err.Error(), "Maximum sending rate exceeded.")

			// Once the counted send falls outside the 1-second window, the
			// next send succeeds again -- no real sleep, no injected clock:
			// the backend derives the window from b.emails' own Timestamp,
			// so backdating the stored email directly simulates elapsed time.
			b.BackdateEmailForTest(0, time.Now().Add(-2*time.Second))
			require.NoError(t, tc.send(b))
		})
	}
}

// TestSendRateLimit_SharedAcrossOps proves the per-second budget is one
// shared account-level counter, not one per operation: exhausting it via
// SendEmail throttles an immediately following SendTemplatedEmail call.
func TestSendRateLimit_SharedAcrossOps(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "tmpl", SubjectPart: "hi"}))

	require.NoError(t, sendEmailBackend(b))

	_, err := b.SendTemplatedEmail(ses.SendTemplatedEmailInput{
		From: "s@example.com", To: []string{"t@example.com"}, TemplateName: "tmpl",
	})
	require.Error(t, err)
	require.ErrorIs(t, err, ses.ErrThrottling)
}

// TestSendRateLimit_BulkCountsAsOneRequest proves SendBulkTemplatedEmail's
// per-second check runs once per call, not once per destination: a single
// call with several destinations must not throttle itself, since MaxSendRate
// bounds the rate of send requests, not the number of recipients within one.
func TestSendRateLimit_BulkCountsAsOneRequest(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "tmpl", SubjectPart: "hi"}))

	msgIDs, err := b.SendBulkTemplatedEmail(ses.SendBulkTemplatedEmailInput{
		Source:       "s@example.com",
		TemplateName: "tmpl",
		Destinations: []ses.BulkEmailDestination{
			{To: []string{"a@example.com"}},
			{To: []string{"b@example.com"}},
			{To: []string{"c@example.com"}},
		},
	})
	require.NoError(t, err)
	assert.Len(t, msgIDs, 3)

	// The bulk call above already consumed this second's one-request budget,
	// so an immediately following SendEmail is throttled.
	err = sendEmailBackend(b)
	require.Error(t, err)
	require.ErrorIs(t, err, ses.ErrThrottling)
}

// TestSendRateLimit_MaxSendRateMatchesGetSendQuota proves the enforced
// threshold and GetSendQuota's advertised MaxSendRate can never drift: both
// read the same maxSendRate constant.
func TestSendRateLimit_MaxSendRateMatchesGetSendQuota(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))

	advertised := b.GetSendQuota().MaxSendRate
	require.InDelta(t, float64(1), advertised, 0)

	for range int(advertised) {
		require.NoError(t, sendEmailBackend(b))
	}

	err := sendEmailBackend(b)
	require.Error(t, err)
	require.ErrorIs(t, err, ses.ErrThrottling)
}

// TestSendRateLimit_HTTPWireShape proves the HTTP-level error shape: real
// AWS SES returns HTTP 400 with error code "Throttling" and message
// "Maximum sending rate exceeded." (no typed exception exists for this in
// the pinned aws-sdk-go-v2/service/ses@v1.37.4/types/errors.go -- confirmed
// absent -- so this is the SES dev guide's documented generic error code:
// https://docs.aws.amazon.com/ses/latest/dg/manage-sending-quotas.html).
func TestSendRateLimit_HTTPWireShape(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))

	sendVals := url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"s@example.com"},
		"Destination.ToAddresses.member.1": {"t@example.com"},
		"Message.Subject.Data":             {"subj"},
		"Message.Body.Text.Data":           {"body"},
	}

	first := postForm(t, h, sendVals.Encode())
	require.Equal(t, http.StatusOK, first.Code)

	second := postForm(t, h, sendVals.Encode())
	assert.Equal(t, http.StatusBadRequest, second.Code)
	assert.Contains(t, second.Body.String(), "<Code>Throttling</Code>")
	assert.Contains(t, second.Body.String(), "Maximum sending rate exceeded.")
}
