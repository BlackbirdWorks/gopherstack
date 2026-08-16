package ses_test

import (
	"encoding/xml"
	"maps"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

func TestSearchEmails_ToFieldMatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		query     string
		wantCount int
	}{
		{
			name:      "match_by_to_field",
			query:     "recipient",
			wantCount: 1,
		},
		{
			name:      "no_match",
			query:     "zzznomatch",
			wantCount: 0,
		},
		{
			name:      "empty_query_returns_all",
			query:     "",
			wantCount: 1,
		},
	}

	b := ses.NewInMemoryBackend()

	// Seed: verify sender and send an email to build up state.
	require.NoError(t, b.VerifyEmailIdentity("sender@example.com"))
	_, err := b.SendEmail(ses.SendEmailInput{
		From:     "sender@example.com",
		To:       []string{"recipient@example.com"},
		Subject:  "hello",
		BodyText: "body",
	})
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			results := b.SearchEmails(tt.query)
			assert.Len(t, results, tt.wantCount)
		})
	}
}

func itoa(n int) string {
	return url.QueryEscape(func() string {
		b := make([]byte, 0, 3)
		if n >= 100 {
			b = append(b, byte('0'+n/100))
		}
		if n >= 10 {
			b = append(b, byte('0'+(n/10)%10))
		}
		b = append(b, byte('0'+n%10))

		return string(b)
	}())
}

func TestSendEmail_BasicHandler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("from@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"from@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Message.Subject.Data":             {"Test Subject"},
		"Message.Body.Text.Data":           {"Hello World"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SendEmailResponse")
	assert.Contains(t, rec.Body.String(), "MessageId")

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "from@example.com", emails[0].From)
	assert.Equal(t, "Test Subject", emails[0].Subject)
}

func TestSendEmail_HTMLBody(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("from@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"from@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Message.Subject.Data":             {"HTML Email"},
		"Message.Body.Html.Data":           {"<h1>Hello</h1>"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "<h1>Hello</h1>", emails[0].BodyHTML)
}

func TestSendEmail_UnverifiedSource_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"unverified@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Message.Subject.Data":             {"Subject"},
		"Message.Body.Text.Data":           {"body"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSendEmail_MultipleTo(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"s@example.com"},
		"Destination.ToAddresses.member.1": {"a@example.com"},
		"Destination.ToAddresses.member.2": {"b@example.com"},
		"Destination.ToAddresses.member.3": {"c@example.com"},
		"Message.Subject.Data":             {"Multi"},
		"Message.Body.Text.Data":           {"body"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Len(t, emails[0].To, 3)
}

func TestSendEmail_ReturnPath(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"s@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Message.Subject.Data":             {"S"},
		"Message.Body.Text.Data":           {"b"},
		"ReturnPath":                       {"bounce@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "bounce@example.com", emails[0].ReturnPath)
}

func TestSendRawEmail_ParsesHeaders(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("raw@example.com"))

	rawMsg := strings.Join([]string{
		"From: raw@example.com",
		"To: dest@example.com",
		"Subject: Raw Test",
		"",
		"Raw body text",
	}, "\r\n")

	rec := postForm(t, h, url.Values{
		"Action":          {"SendRawEmail"},
		"Version":         {"2010-12-01"},
		"RawMessage.Data": {rawMsg},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "raw@example.com", emails[0].From)
	assert.Equal(t, "Raw Test", emails[0].Subject)
	assert.Equal(t, []string{"dest@example.com"}, emails[0].To)
}

func TestSendRawEmail_ExplicitSourceOverridesHeader(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("explicit@example.com"))

	rawMsg := strings.Join([]string{
		"From: header@example.com",
		"To: dest@example.com",
		"Subject: Test",
		"",
		"body",
	}, "\r\n")

	rec := postForm(t, h, url.Values{
		"Action":          {"SendRawEmail"},
		"Version":         {"2010-12-01"},
		"Source":          {"explicit@example.com"},
		"RawMessage.Data": {rawMsg},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "explicit@example.com", emails[0].From)
}

func TestSendRawEmail_EmptyMessage_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := postForm(t, h, url.Values{
		"Action":          {"SendRawEmail"},
		"Version":         {"2010-12-01"},
		"RawMessage.Data": {""},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSendEmail_Tags_ConfigSet(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))
	// AWS SES requires ConfigurationSetName to reference an existing configuration
	// set (ConfigurationSetDoesNotExist otherwise), so create it up front.
	require.NoError(t, b.CreateConfigurationSet("my-cs"))

	tags := []ses.Tag{{Name: "env", Value: "prod"}, {Name: "team", Value: "backend"}}
	msgID, err := b.SendEmail(ses.SendEmailInput{
		From:                 "s@example.com",
		To:                   []string{"to@example.com"},
		Subject:              "tagged",
		BodyText:             "body",
		ConfigurationSetName: "my-cs",
		Tags:                 tags,
	})
	require.NoError(t, err)

	email, err := b.GetEmailByID(msgID)
	require.NoError(t, err)
	assert.Equal(t, "my-cs", email.ConfigurationSetName)
	assert.Equal(t, tags, email.Tags)
}

func TestSendEmail_SearchByFrom(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("alice@example.com"))
	require.NoError(t, b.VerifyEmailIdentity("bob@example.com"))

	_, err := b.SendEmail(ses.SendEmailInput{
		From: "alice@example.com", To: []string{"x@example.com"},
		Subject: "A", BodyText: "a",
	})
	require.NoError(t, err)
	_, err = b.SendEmail(ses.SendEmailInput{
		From: "bob@example.com", To: []string{"x@example.com"},
		Subject: "B", BodyText: "b",
	})
	require.NoError(t, err)

	results := b.SearchEmails("alice")
	assert.Len(t, results, 1)
	assert.Equal(t, "alice@example.com", results[0].From)
}

func TestSendEmail_CcBccReplyTo(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("sender@example.com"))

	msgID, err := b.SendEmail(ses.SendEmailInput{
		From:     "sender@example.com",
		To:       []string{"to@example.com"},
		Cc:       []string{"cc1@example.com", "cc2@example.com"},
		Bcc:      []string{"bcc@example.com"},
		ReplyTo:  []string{"reply@example.com"},
		Subject:  "Test",
		BodyText: "body",
	})
	require.NoError(t, err)

	email, err := b.GetEmailByID(msgID)
	require.NoError(t, err)
	assert.Equal(t, []string{"cc1@example.com", "cc2@example.com"}, email.Cc)
	assert.Equal(t, []string{"bcc@example.com"}, email.Bcc)
	assert.Equal(t, []string{"reply@example.com"}, email.ReplyTo)
}

func TestSendEmail_CcBccReplyTo_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	// AWS SES requires ConfigurationSetName to reference an existing configuration
	// set (ConfigurationSetDoesNotExist otherwise), so create it up front.
	require.NoError(t, h.Backend.CreateConfigurationSet("my-set"))

	body := url.Values{
		"Action":                            {"SendEmail"},
		"Version":                           {"2010-12-01"},
		"Source":                            {"s@example.com"},
		"Destination.ToAddresses.member.1":  {"to@example.com"},
		"Destination.CcAddresses.member.1":  {"cc@example.com"},
		"Destination.BccAddresses.member.1": {"bcc@example.com"},
		"ReplyToAddresses.member.1":         {"reply@example.com"},
		"Message.Subject.Data":              {"Hello"},
		"Message.Body.Text.Data":            {"body"},
		"ConfigurationSetName":              {"my-set"},
		"Tags.member.1.Name":                {"env"},
		"Tags.member.1.Value":               {"prod"},
		"ReturnPath":                        {"bounce@example.com"},
		"SourceArn":                         {"arn:aws:ses:us-east-1:123:identity/s@example.com"},
	}.Encode()

	rec := postForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	e := emails[0]
	assert.Equal(t, []string{"cc@example.com"}, e.Cc)
	assert.Equal(t, []string{"bcc@example.com"}, e.Bcc)
	assert.Equal(t, []string{"reply@example.com"}, e.ReplyTo)
	assert.Equal(t, "my-set", e.ConfigurationSetName)
	assert.Equal(t, "bounce@example.com", e.ReturnPath)
	assert.Equal(t, "arn:aws:ses:us-east-1:123:identity/s@example.com", e.SourceArn)
	require.Len(t, e.Tags, 1)
	assert.Equal(t, "env", e.Tags[0].Name)
	assert.Equal(t, "prod", e.Tags[0].Value)
}

func TestSendRawEmail_RFC2822Headers(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("raw@example.com"))

	rawMsg := strings.Join([]string{
		"From: raw@example.com",
		"To: dest@example.com",
		"Subject: Raw Subject",
		"",
		"Raw body text",
	}, "\r\n")

	body := url.Values{
		"Action":          {"SendRawEmail"},
		"Version":         {"2010-12-01"},
		"RawMessage.Data": {rawMsg},
	}.Encode()

	rec := postForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	e := emails[0]
	assert.Equal(t, "raw@example.com", e.From)
	assert.Equal(t, "Raw Subject", e.Subject)
	assert.Equal(t, []string{"dest@example.com"}, e.To)
}

func TestSendRawEmail_ExplicitSourceOverridesHeader_Backend(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("explicit@example.com"))

	rawMsg := strings.Join([]string{
		"From: header@example.com",
		"To: dest@example.com",
		"Subject: Test",
		"",
		"body",
	}, "\r\n")

	body := url.Values{
		"Action":          {"SendRawEmail"},
		"Version":         {"2010-12-01"},
		"Source":          {"explicit@example.com"},
		"RawMessage.Data": {rawMsg},
	}.Encode()

	rec := postForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "explicit@example.com", emails[0].From)
}

func TestSendEmail_Tags(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))
	// AWS SES requires ConfigurationSetName to reference an existing configuration
	// set (ConfigurationSetDoesNotExist otherwise), so create it up front.
	require.NoError(t, b.CreateConfigurationSet("cs1"))

	tags := []ses.Tag{{Name: "k1", Value: "v1"}, {Name: "k2", Value: "v2"}}
	msgID, err := b.SendEmail(ses.SendEmailInput{
		From: "s@example.com", To: []string{"to@example.com"},
		Subject: "s", BodyText: "b",
		ConfigurationSetName: "cs1",
		Tags:                 tags,
	})
	require.NoError(t, err)

	email, err := b.GetEmailByID(msgID)
	require.NoError(t, err)
	assert.Equal(t, "cs1", email.ConfigurationSetName)
	assert.Equal(t, tags, email.Tags)
}

func TestSendEmail_MessageIDNonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extra  url.Values
		name   string
		action string
	}{
		{
			name:   "SendEmail",
			action: "SendEmail",
			extra: url.Values{
				"Message.Subject.Data":   {"subj"},
				"Message.Body.Text.Data": {"body"},
			},
		},
		{
			name:   "SendRawEmail",
			action: "SendRawEmail",
			extra: url.Values{
				"RawMessage.Data": {"From: s@example.com\r\nTo: t@example.com\r\nSubject: s\r\n\r\nbody"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))

			vals := url.Values{
				"Action":                           {tt.action},
				"Version":                          {"2010-12-01"},
				"Source":                           {"s@example.com"},
				"Destination.ToAddresses.member.1": {"t@example.com"},
			}
			maps.Copy(vals, tt.extra)

			rec := postForm(t, h, vals.Encode())
			require.Equal(t, http.StatusOK, rec.Code)

			body := rec.Body.String()
			assert.Contains(t, body, "<MessageId>", "response must contain MessageId element")

			msgID := xmlText([]byte(body), "MessageId")
			assert.NotEmpty(t, msgID, "MessageId must not be empty")
			assert.True(t, strings.HasPrefix(msgID, "ses-"),
				"MessageId must start with 'ses-' prefix, got: %q", msgID)
		})
	}
}

func TestSendEmail_MessageIDUnique(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))

	seen := make(map[string]bool)
	for i := range 5 {
		vals := url.Values{
			"Action":                           {"SendEmail"},
			"Version":                          {"2010-12-01"},
			"Source":                           {"s@example.com"},
			"Destination.ToAddresses.member.1": {"t@example.com"},
			"Message.Subject.Data":             {"subj"},
			"Message.Body.Text.Data":           {"body"},
		}
		rec := postForm(t, h, vals.Encode())
		require.Equal(t, http.StatusOK, rec.Code, "send %d must succeed", i)

		msgID := xmlText(rec.Body.Bytes(), "MessageId")
		require.NotEmpty(t, msgID)
		assert.False(t, seen[msgID], "MessageId must be unique across sends, got duplicate: %q", msgID)
		seen[msgID] = true
	}
}

func TestSendEmail_DomainVerificationAllowsSubAddresses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sender string
	}{
		{name: "direct_address", sender: "user@example.com"},
		{name: "subdomain_address", sender: "other@example.com"},
		{name: "plus_address", sender: "user+tag@example.com"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			h := ses.NewHandler(b)

			_, err := b.VerifyDomainIdentity("example.com")
			require.NoError(t, err)

			rec := postForm(t, h, url.Values{
				"Action":                           {"SendEmail"},
				"Version":                          {"2010-12-01"},
				"Source":                           {tt.sender},
				"Destination.ToAddresses.member.1": {"dest@other.com"},
				"Message.Subject.Data":             {"hi"},
				"Message.Body.Text.Data":           {"body"},
			}.Encode())
			assert.Equal(t, http.StatusOK, rec.Code,
				"sender %q under verified domain must be allowed to send", tt.sender)
		})
	}
}

func TestSendEmail_UnverifiedSenderRejected(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"unverified@example.com"},
		"Destination.ToAddresses.member.1": {"dest@other.com"},
		"Message.Subject.Data":             {"hi"},
		"Message.Body.Text.Data":           {"body"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code,
		"sending from unverified address must be rejected")
	assert.Contains(t, rec.Body.String(), "MessageRejected",
		"error code must be MessageRejected")
}

func TestSESBackend_EmailsByIDMapSync(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("a@test.com"))

	msgID, err := b.SendEmail(ses.SendEmailInput{
		From: "a@test.com", To: []string{"b@test.com"}, Subject: "s", BodyText: "body",
	})
	require.NoError(t, err)

	assert.Equal(t, b.EmailCount(), b.EmailsByIDCount())

	e, err := b.GetEmailByID(msgID)
	require.NoError(t, err)
	assert.Equal(t, msgID, e.MessageID)
}

func TestSESBackend_EmailsByIDSyncAfterEviction(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("x@test.com"))

	// Append MaxRetainedEmails+5 so the first 5 are evicted. This exercises
	// the eviction/O(1)-map-sync behavior in appendEmailLocked directly via
	// AppendEmailForTest: real SendEmail now enforces the simulated 200/day
	// send quota (matching real AWS SES sandbox default), so a real send loop
	// of this volume would itself be rejected with MessageRejected long
	// before reaching the retention cap.
	var firstIDs []string

	for i := range ses.MaxRetainedEmails + 5 {
		msgID := b.AppendEmailForTest("x@test.com", []string{"y@test.com"})

		if i < 5 {
			firstIDs = append(firstIDs, msgID)
		}
	}

	assert.Equal(t, ses.MaxRetainedEmails, b.EmailCount())
	assert.Equal(t, b.EmailCount(), b.EmailsByIDCount())

	// Evicted IDs must no longer be in the map.
	for _, id := range firstIDs {
		_, err := b.GetEmailByID(id)
		require.Error(t, err)
		assert.ErrorIs(t, err, ses.ErrEmailNotFound)
	}
}

// TestParity_SendEmailRequiresDestination verifies that SendEmail rejects
// requests with no destination addresses. Real AWS requires at least one
// To, Cc, or Bcc address; the emulator previously accepted an empty
// Destination and stored the email without any recipients.
func TestSendEmailRequiresDestination(t *testing.T) {
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

// TestSendEmailErrorUsesConfiguredRegion verifies that the "not verified" error
// message uses the backend's configured region, not a hardcoded "US-EAST-1".
func TestSendEmailErrorUsesConfiguredRegion(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend().WithRegion("eu-central-1")

	_, err := b.SendEmail(ses.SendEmailInput{
		From:     "unverified@example.com",
		To:       []string{"to@example.com"},
		Subject:  "test",
		BodyText: "body",
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "EU-CENTRAL-1",
		"error should mention configured region EU-CENTRAL-1")
	assert.NotContains(t, err.Error(), "US-EAST-1",
		"error should not mention hardcoded US-EAST-1")
}

func TestAccountSendingPaused_BlocksSend(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))

	b.UpdateAccountSendingEnabled(false)

	_, err := b.SendEmail(ses.SendEmailInput{
		From:     "s@example.com",
		To:       []string{"to@example.com"},
		Subject:  "s",
		BodyText: "b",
	})
	require.ErrorIs(t, err, ses.ErrAccountSendingPaused)

	_, err = b.SendTemplatedEmail(ses.SendTemplatedEmailInput{
		From:         "s@example.com",
		To:           []string{"to@example.com"},
		TemplateName: "does-not-matter",
	})
	require.ErrorIs(t, err, ses.ErrAccountSendingPaused)

	// Re-enabling restores sending.
	b.UpdateAccountSendingEnabled(true)

	_, err = b.SendEmail(ses.SendEmailInput{
		From:     "s@example.com",
		To:       []string{"to@example.com"},
		Subject:  "s",
		BodyText: "b",
	})
	require.NoError(t, err)
}

func TestAccountSendingPaused_Handler_ReturnsExactErrorCode(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	h.Backend.UpdateAccountSendingEnabled(false)

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"s@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Message.Subject.Data":             {"hi"},
		"Message.Body.Text.Data":           {"body"},
	}.Encode())

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "AccountSendingPausedException")
}

func TestSendEmail_DailyQuota_Exhaustion(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))

	quota := b.GetSendQuota()
	limit := int(quota.Max24HourSend)
	require.Positive(t, limit)

	// Seed exactly the quota via AppendEmailForTest (bypassing SendEmail so the
	// seeding itself isn't gated by the very quota being tested).
	for range limit {
		b.AppendEmailForTest("s@example.com", []string{"to@example.com"})
	}

	assert.InEpsilon(t, float64(limit), b.GetSendQuota().SentLast24Hours, 0.001)

	_, err := b.SendEmail(ses.SendEmailInput{
		From:     "s@example.com",
		To:       []string{"to@example.com"},
		Subject:  "s",
		BodyText: "b",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrMessageRejected)
}

func TestSendEmail_UnknownConfigurationSet_Rejected(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))

	_, err := b.SendEmail(ses.SendEmailInput{
		From:                 "s@example.com",
		To:                   []string{"to@example.com"},
		Subject:              "s",
		BodyText:             "b",
		ConfigurationSetName: "does-not-exist",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrConfigSetNotFound)
}

// TestSend_RecipientLimit verifies the 50-recipient-per-message cap is enforced
// for both SendEmail and SendTemplatedEmail with a MessageRejected error.
func TestSend_RecipientLimit(t *testing.T) {
	t.Parallel()

	makeAddrs := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = "r@example.com"
		}

		return out
	}

	t.Run("SendEmail over limit rejected", func(t *testing.T) {
		t.Parallel()

		b := ses.NewInMemoryBackend()
		require.NoError(t, b.VerifyEmailIdentity("sender@example.com"))

		_, err := b.SendEmail(ses.SendEmailInput{
			From: "sender@example.com",
			To:   makeAddrs(51),
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ses.ErrMessageRejected, "got %v", err)
	})

	t.Run("SendEmail at limit accepted", func(t *testing.T) {
		t.Parallel()

		b := ses.NewInMemoryBackend()
		require.NoError(t, b.VerifyEmailIdentity("sender@example.com"))

		_, err := b.SendEmail(ses.SendEmailInput{
			From: "sender@example.com",
			To:   makeAddrs(30),
			Cc:   makeAddrs(20),
		})
		require.NoError(t, err)
	})

	t.Run("SendEmail combined To/Cc/Bcc over limit rejected", func(t *testing.T) {
		t.Parallel()

		b := ses.NewInMemoryBackend()
		require.NoError(t, b.VerifyEmailIdentity("sender@example.com"))

		_, err := b.SendEmail(ses.SendEmailInput{
			From: "sender@example.com",
			To:   makeAddrs(20),
			Cc:   makeAddrs(20),
			Bcc:  makeAddrs(11),
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ses.ErrMessageRejected, "got %v", err)
	})

	t.Run("SendTemplatedEmail over limit rejected", func(t *testing.T) {
		t.Parallel()

		b := ses.NewInMemoryBackend()
		require.NoError(t, b.VerifyEmailIdentity("sender@example.com"))
		require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
			TemplateName: "tmpl", SubjectPart: "s", TextPart: "t",
		}))

		_, err := b.SendTemplatedEmail(ses.SendTemplatedEmailInput{
			From:         "sender@example.com",
			To:           makeAddrs(51),
			TemplateName: "tmpl",
		})
		require.Error(t, err)
		assert.ErrorIs(t, err, ses.ErrMessageRejected, "got %v", err)
	})
}

func TestSESHandler_SendEmail(t *testing.T) {
	t.Parallel()

	h := newHandler()

	// Must verify the source identity first.
	postForm(t, h, "Action=VerifyEmailIdentity&Version=2010-12-01&EmailAddress=sender@example.com")

	body := url.Values{
		"Action":                           {"SendEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"sender@example.com"},
		"Destination.ToAddresses.member.1": {"recipient@example.com"},
		"Message.Subject.Data":             {"Hello World"},
		"Message.Body.Text.Data":           {"Test body"},
		"Message.Body.Html.Data":           {"<p>Test body</p>"},
	}

	rec := postForm(t, h, body.Encode())

	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"SendEmailResponse"`
		Result  struct {
			MessageID string `xml:"MessageId"`
		} `xml:"SendEmailResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.Result.MessageID)

	// Verify email was captured.
	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "sender@example.com", emails[0].From)
	assert.Equal(t, []string{"recipient@example.com"}, emails[0].To)
	assert.Equal(t, "Hello World", emails[0].Subject)
}

func TestSESBackend_GetEmailByID(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("find@test.com"))

	msgID, err := b.SendEmail(ses.SendEmailInput{
		From: "find@test.com", To: []string{"to@test.com"}, Subject: "FindMe", BodyText: "body",
	})
	require.NoError(t, err)

	email, err := b.GetEmailByID(msgID)
	require.NoError(t, err)
	assert.Equal(t, "find@test.com", email.From)
	assert.Equal(t, "FindMe", email.Subject)

	// Not found case.
	_, err = b.GetEmailByID("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrEmailNotFound)
}

func TestSESBackend_EmailRetentionLimit(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("sender@test.com"))

	// Append more emails than the cap via AppendEmailForTest, which exercises
	// the same eviction path as SendEmail without being gated by the
	// simulated 200/day send quota that real SendEmail now enforces.
	for range ses.MaxRetainedEmails + 100 {
		b.AppendEmailForTest("sender@test.com", []string{"to@test.com"})
	}

	assert.Equal(t, ses.MaxRetainedEmails, b.EmailCount())
}

func TestSESBackend_SendEmailUnverifiedSource(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()

	_, err := b.SendEmail(ses.SendEmailInput{
		From: "unverified@test.com", To: []string{"to@test.com"}, Subject: "subj", BodyText: "body",
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrMessageRejected)
}
