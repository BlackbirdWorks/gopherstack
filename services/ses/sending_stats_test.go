package ses_test

import (
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/ses"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateAccountSendingEnabled_Persists(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	assert.True(t, b.GetAccountSendingEnabled(), "default is enabled")

	b.UpdateAccountSendingEnabled(false)
	assert.False(t, b.GetAccountSendingEnabled())

	b.UpdateAccountSendingEnabled(true)
	assert.True(t, b.GetAccountSendingEnabled())
}

func TestGetAccountSendingEnabled_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=GetAccountSendingEnabled&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetAccountSendingEnabledResponse")
	assert.Contains(t, rec.Body.String(), "true")
}

func TestUpdateAccountSendingEnabled_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":  {"UpdateAccountSendingEnabled"},
		"Version": {"2010-12-01"},
		"Enabled": {"false"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	assert.False(t, h.Backend.(*ses.InMemoryBackend).AccountSendingEnabledState())

	rec2 := postForm(t, h, "Action=GetAccountSendingEnabled&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "false")
}

func TestAccountSendingEnabled_Reset(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	b.UpdateAccountSendingEnabled(false)
	assert.False(t, b.GetAccountSendingEnabled())

	b.Reset()
	assert.True(t, b.GetAccountSendingEnabled(), "reset restores default true")
}

func TestGetSendQuota_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, "Action=GetSendQuota&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetSendQuotaResponse")
	assert.Contains(t, rec.Body.String(), "Max24HourSend")
	assert.Contains(t, rec.Body.String(), "MaxSendRate")
}

func TestGetSendQuota_Values(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	q := b.GetSendQuota()
	assert.Greater(t, q.Max24HourSend, 0.0)
	assert.Greater(t, q.MaxSendRate, 0.0)
	assert.GreaterOrEqual(t, q.SentLast24Hours, 0.0)
}

func TestGetSendStatistics_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	_, err := h.Backend.SendEmail(ses.SendEmailInput{
		From:     "s@example.com",
		To:       []string{"to@example.com"},
		Subject:  "test",
		BodyText: "body",
	})
	require.NoError(t, err)

	rec := postForm(t, h, "Action=GetSendStatistics&Version=2010-12-01")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetSendStatisticsResponse")
}

func TestGetSendQuota_FixedValues(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":  {"GetSendQuota"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<Max24HourSend>200</Max24HourSend>",
		"Max24HourSend must be 200")
	assert.Contains(t, body, "<MaxSendRate>1</MaxSendRate>",
		"MaxSendRate must be 1")
}

func TestGetSendQuota_SentLast24HoursTracked(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    string
		numSend int
	}{
		{name: "zero_sends", numSend: 0, want: "<SentLast24Hours>0</SentLast24Hours>"},
		{name: "one_send", numSend: 1, want: "<SentLast24Hours>1</SentLast24Hours>"},
		{name: "three_sends", numSend: 3, want: "<SentLast24Hours>3</SentLast24Hours>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))

			for i := range tt.numSend {
				rec := postForm(t, h, url.Values{
					"Action":                           {"SendEmail"},
					"Version":                          {"2010-12-01"},
					"Source":                           {"s@example.com"},
					"Destination.ToAddresses.member.1": {"t@example.com"},
					"Message.Subject.Data":             {"subj"},
					"Message.Body.Text.Data":           {"body"},
				}.Encode())
				require.Equal(t, http.StatusOK, rec.Code, "send %d must succeed", i)
			}

			rec := postForm(t, h, url.Values{
				"Action":  {"GetSendQuota"},
				"Version": {"2010-12-01"},
			}.Encode())
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.want)
		})
	}
}

func TestGetSendStatistics_EmptyState(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":  {"GetSendStatistics"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "GetSendStatisticsResponse",
		"response envelope must be GetSendStatisticsResponse")
	assert.Contains(t, body, "SendDataPoints",
		"SendDataPoints element must be present even when empty")
	assert.NotContains(t, body, "<member>",
		"no member elements expected when no emails sent")
}

func TestGetSendStatistics_PopulatedAfterSend(t *testing.T) {
	t.Parallel()

	h := newHandler()
	sesVerifyAndSend(t, h, "s@example.com", "t@example.com")
	sesVerifyAndSend(t, h, "s@example.com", "t2@example.com")

	rec := postForm(t, h, url.Values{
		"Action":  {"GetSendStatistics"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "<member>", "must have at least one SendDataPoints member after sends")
	assert.Contains(t, body, "<DeliveryAttempts>", "member must contain DeliveryAttempts")
}

func TestGetSendStatistics_TimestampRFC3339(t *testing.T) {
	t.Parallel()

	h := newHandler()
	sesVerifyAndSend(t, h, "s@example.com", "t@example.com")

	rec := postForm(t, h, url.Values{
		"Action":  {"GetSendStatistics"},
		"Version": {"2010-12-01"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	ts := xmlText(rec.Body.Bytes(), "Timestamp")
	require.NotEmpty(t, ts, "Timestamp must be present in SendDataPoints member")
	_, err := time.Parse(time.RFC3339, ts)
	assert.NoError(t, err, "Timestamp must be RFC3339: %q", ts)
}

func TestGetSendStatistics_AggregatesIntoHourlyBuckets(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	h := ses.NewHandler(b)
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))

	// Send 3 emails
	for range 3 {
		rec := postForm(t, h, url.Values{
			"Action":                           {"SendEmail"},
			"Version":                          {"2010-12-01"},
			"Source":                           {"s@example.com"},
			"Destination.ToAddresses.member.1": {"t@example.com"},
			"Message.Subject.Data":             {"subj"},
			"Message.Body.Text.Data":           {"body"},
		}.Encode())
		require.Equal(t, http.StatusOK, rec.Code)
	}

	stats := b.GetSendStatistics()
	require.Len(t, stats, 1, "3 emails in same hour must produce exactly 1 hourly bucket")
	assert.InDelta(t, float64(3), stats[0].DeliveryAttempts, 0,
		"hourly bucket DeliveryAttempts must equal number of emails sent")
}

func TestSESBackend_GetSendQuota(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("q@test.com"))

	_, err := b.SendEmail(ses.SendEmailInput{
		From: "q@test.com", To: []string{"to@test.com"}, Subject: "s", BodyText: "b",
	})
	require.NoError(t, err)

	q := b.GetSendQuota()
	assert.InDelta(t, float64(200), q.Max24HourSend, 0)
	assert.InDelta(t, float64(1), q.MaxSendRate, 0)
	assert.InDelta(t, float64(1), q.SentLast24Hours, 0)
}

func TestSESBackend_GetSendStatistics(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@test.com"))

	_, err := b.SendEmail(ses.SendEmailInput{
		From: "s@test.com", To: []string{"to@test.com"}, Subject: "s", BodyText: "b",
	})
	require.NoError(t, err)

	points := b.GetSendStatistics()
	require.Len(t, points, 1)
	assert.InDelta(t, float64(1), points[0].DeliveryAttempts, 0)
}

func TestSESBackend_GetSendStatisticsEmpty(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	points := b.GetSendStatistics()
	assert.Empty(t, points)
}

func TestSESHandler_GetSendQuota(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, url.Values{
		"Action":  {"GetSendQuota"},
		"Version": {"2010-12-01"},
	}.Encode())

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetSendQuotaResponse")
	assert.Contains(t, rec.Body.String(), "Max24HourSend")
}

func TestSESHandler_GetSendStatistics(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("stat@test.com"))

	_, err := h.Backend.SendEmail(ses.SendEmailInput{
		From: "stat@test.com", To: []string{"to@test.com"}, Subject: "s", BodyText: "b",
	})
	require.NoError(t, err)

	rec := postForm(t, h, url.Values{
		"Action":  {"GetSendStatistics"},
		"Version": {"2010-12-01"},
	}.Encode())

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetSendStatisticsResponse")
}

func TestSESBackend_GetSendQuota_CountsOnlyLast24h(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	// Verify that a freshly sent email counts toward the 24h quota.
	require.NoError(t, b.VerifyEmailIdentity("q24@test.com"))

	_, err := b.SendEmail(ses.SendEmailInput{
		From: "q24@test.com", To: []string{"to@test.com"}, Subject: "s", BodyText: "b",
	})
	require.NoError(t, err)

	q := b.GetSendQuota()
	assert.InDelta(t, float64(200), q.Max24HourSend, 0)
	assert.InDelta(t, float64(1), q.MaxSendRate, 0)
	assert.InDelta(t, float64(1), q.SentLast24Hours, 0)
}

func TestSESBackend_GetSendStatistics_14DayWindow(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("stat14@test.com"))

	// A recently sent email should appear in the stats.
	_, err := b.SendEmail(ses.SendEmailInput{
		From: "stat14@test.com", To: []string{"to@test.com"}, Subject: "recent", BodyText: "b",
	})
	require.NoError(t, err)

	points := b.GetSendStatistics()
	require.Len(t, points, 1)
	assert.InDelta(t, float64(1), points[0].DeliveryAttempts, 0)
}

func TestSESHandler_GetAccountSendingEnabled(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, url.Values{
		"Action":  {"GetAccountSendingEnabled"},
		"Version": {"2010-12-01"},
	}.Encode())

	assert.Equal(t, 200, rec.Code)
	assert.Contains(t, rec.Body.String(), "GetAccountSendingEnabledResponse")
	assert.Contains(t, rec.Body.String(), "<Enabled>true</Enabled>")
}

// TestGetSendQuotaTracksSends verifies SentLast24Hours reflects actual sent emails.
func TestGetSendQuotaTracksSends(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		sendCount   int
		wantAtLeast float64
	}{
		{name: "zero_sends", sendCount: 0, wantAtLeast: 0},
		{name: "three_sends", sendCount: 3, wantAtLeast: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := ses.NewInMemoryBackend()
			require.NoError(t, backend.VerifyEmailIdentity("sender@example.com"))

			for range tt.sendCount {
				_, err := backend.SendEmail(ses.SendEmailInput{
					From:     "sender@example.com",
					To:       []string{"to@example.com"},
					Subject:  "test",
					BodyText: "body",
				})
				require.NoError(t, err)
			}

			quota := backend.GetSendQuota()
			assert.GreaterOrEqual(t, quota.SentLast24Hours, tt.wantAtLeast)
		})
	}
}
