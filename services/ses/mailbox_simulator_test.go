package ses_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// TestGetSendStatistics_SimulatorLabels proves the mailbox simulator's
// documented labeling support (bounce+label@simulator.amazonses.com behaves
// identically to bounce@simulator.amazonses.com) is honored, not just exact
// address matches -- see "Using the mailbox simulator manually" ->
// "Important considerations" at
// https://docs.aws.amazon.com/ses/latest/dg/send-an-email-from-console.html#send-email-simulator.
func TestGetSendStatistics_SimulatorLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		recipient      string
		wantBounces    float64
		wantComplaints float64
	}{
		{name: "bounce_labeled", recipient: "bounce+campaign1@simulator.amazonses.com", wantBounces: 1},
		{name: "bounce_labeled_mixed_case_domain", recipient: "bounce+x@Simulator.AmazonSES.com", wantBounces: 1},
		{name: "suppressionlist_labeled", recipient: "suppressionlist+x@simulator.amazonses.com", wantBounces: 1},
		{name: "complaint_labeled", recipient: "complaint+x@simulator.amazonses.com", wantComplaints: 1},
		{name: "success_labeled_is_noop", recipient: "success+x@simulator.amazonses.com"},
		{name: "ooto_labeled_is_noop", recipient: "ooto+x@simulator.amazonses.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			require.NoError(t, b.VerifyEmailIdentity("s@example.com"))

			_, err := b.SendEmail(ses.SendEmailInput{
				From:     "s@example.com",
				To:       []string{tt.recipient},
				Subject:  "test",
				BodyText: "body",
			})
			require.NoError(t, err)

			points := b.GetSendStatistics()
			require.Len(t, points, 1)
			assert.InDelta(t, tt.wantBounces, points[0].Bounces, 0)
			assert.InDelta(t, tt.wantComplaints, points[0].Complaints, 0)
		})
	}
}

// TestSendEmail_SimulatorOnly_ExcludedFrom24HourQuota proves real AWS SES's
// documented mailbox-simulator behavior: "Emails that you send to the
// mailbox simulator ... don't affect your daily sending quota" (same doc as
// above, "Important considerations"). A send whose every recipient is a
// simulator address must not count toward Max24HourSend, and must not itself
// be blocked by an already-exhausted 24-hour quota.
func TestSendEmail_SimulatorOnly_ExcludedFrom24HourQuota(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))

	for i := range 5 {
		_, err := b.SendEmail(ses.SendEmailInput{
			From: "s@example.com", To: []string{"success@simulator.amazonses.com"}, Subject: "s", BodyText: "b",
		})
		require.NoError(t, err, "simulator-only send %d must not be quota-blocked", i)

		// Simulator sends are still rate-limited (1/sec) even though they
		// don't consume the 24h quota -- back each one out of that window so
		// the next iteration isn't throttled by MaxSendRate instead.
		b.BackdateEmailForTest(i, time.Now().Add(-2*time.Second))
	}

	q := b.GetSendQuota()
	assert.InDelta(t, float64(0), q.SentLast24Hours, 0,
		"simulator-only sends must not appear in SentLast24Hours")
}

// TestSendEmail_SimulatorOnly_StillRateLimited proves MaxSendRate (1/sec)
// still applies to a simulator-only send -- unlike the 24-hour quota, the
// doc says simulator sends "are limited by your account's maximum sending
// rate".
func TestSendEmail_SimulatorOnly_StillRateLimited(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))

	send := func() error {
		_, err := b.SendEmail(ses.SendEmailInput{
			From: "s@example.com", To: []string{"success@simulator.amazonses.com"}, Subject: "s", BodyText: "b",
		})

		return err
	}

	require.NoError(t, send())

	err := send()
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrThrottling)
}

// TestSendEmail_MixedSimulatorAndRealRecipients_ConsumesQuota proves a
// message reaching both a simulator address and a real recipient is treated
// as an ordinary (quota-consuming) send -- the doc's quota exemption is
// documented for the simulator address itself, not for any message that
// happens to mention one.
func TestSendEmail_MixedSimulatorAndRealRecipients_ConsumesQuota(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))

	_, err := b.SendEmail(ses.SendEmailInput{
		From:     "s@example.com",
		To:       []string{"success@simulator.amazonses.com", "real@example.com"},
		Subject:  "s",
		BodyText: "b",
	})
	require.NoError(t, err)

	q := b.GetSendQuota()
	assert.InDelta(t, float64(1), q.SentLast24Hours, 0,
		"a send with any non-simulator recipient must consume the 24h quota")
}
