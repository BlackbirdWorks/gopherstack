package ses_test

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// ---- Email Cc/Bcc/ReplyTo (issue #3) ----

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

// ---- SendTemplatedEmail Cc/Bcc (issue #3) ----

func TestSendTemplatedEmail_CcBcc_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "tmpl", SubjectPart: "subj", TextPart: "text",
	}))

	body := url.Values{
		"Action":                            {"SendTemplatedEmail"},
		"Version":                           {"2010-12-01"},
		"Source":                            {"s@example.com"},
		"Destination.ToAddresses.member.1":  {"to@example.com"},
		"Destination.CcAddresses.member.1":  {"cc@example.com"},
		"Destination.BccAddresses.member.1": {"bcc@example.com"},
		"Template":                          {"tmpl"},
	}.Encode()

	rec := postForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, []string{"cc@example.com"}, emails[0].Cc)
	assert.Equal(t, []string{"bcc@example.com"}, emails[0].Bcc)
}

// ---- SendRawEmail RFC 2822 header parsing (issue #10) ----

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

// ---- Email ConfigurationSetName / Tags (issue #7) ----

func TestSendEmail_Tags(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))

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

// ---- DKIM deterministic tokens (issue #8) ----

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

// ---- MailFromDomain persistence (issue #9) ----

func TestSetIdentityMailFromDomain_Persists(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("a@example.com"))
	require.NoError(t, b.SetIdentityMailFromDomain("a@example.com", "mail.example.com"))

	attrs := b.GetIdentityMailFromDomainAttributes([]string{"a@example.com"})
	assert.Equal(t, "mail.example.com", attrs["a@example.com"].MailFromDomain)
	assert.Equal(t, "Success", attrs["a@example.com"].MailFromDomainStatus)
}

func TestSetIdentityMailFromDomain_Clear(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("a@example.com"))
	require.NoError(t, b.SetIdentityMailFromDomain("a@example.com", "mail.example.com"))
	require.NoError(t, b.SetIdentityMailFromDomain("a@example.com", ""))

	attrs := b.GetIdentityMailFromDomainAttributes([]string{"a@example.com"})
	assert.Empty(t, attrs["a@example.com"].MailFromDomain)
	assert.Empty(t, attrs["a@example.com"].MailFromDomainStatus)
}

// ---- Notification attributes persistence ----

func TestSetIdentityNotificationTopic_Persists(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("a@example.com"))
	require.NoError(t, b.SetIdentityNotificationTopic("a@example.com", "Bounce", "arn:sns:bounce"))
	require.NoError(t, b.SetIdentityNotificationTopic("a@example.com", "Complaint", "arn:sns:complaint"))
	require.NoError(t, b.SetIdentityNotificationTopic("a@example.com", "Delivery", "arn:sns:delivery"))

	attrs := b.GetIdentityNotificationAttributes([]string{"a@example.com"})
	a := attrs["a@example.com"]
	assert.Equal(t, "arn:sns:bounce", a.BounceTopic)
	assert.Equal(t, "arn:sns:complaint", a.ComplaintTopic)
	assert.Equal(t, "arn:sns:delivery", a.DeliveryTopic)
}

func TestSetIdentityFeedbackForwardingEnabled_Persists(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("a@example.com"))
	require.NoError(t, b.SetIdentityFeedbackForwardingEnabled("a@example.com", false))

	attrs := b.GetIdentityNotificationAttributes([]string{"a@example.com"})
	assert.False(t, attrs["a@example.com"].ForwardingEnabled)
}

func TestSetIdentityHeadersInNotificationsEnabled_Persists(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("a@example.com"))
	require.NoError(t, b.SetIdentityHeadersInNotificationsEnabled("a@example.com", "Bounce", true))
	require.NoError(t, b.SetIdentityHeadersInNotificationsEnabled("a@example.com", "Complaint", true))

	attrs := b.GetIdentityNotificationAttributes([]string{"a@example.com"})
	a := attrs["a@example.com"]
	assert.True(t, a.HeadersInBounce)
	assert.True(t, a.HeadersInComplaint)
	assert.False(t, a.HeadersInDelivery)
}

// ---- ConfigurationSet delivery options / sending state (issue #5) ----

func TestPutConfigurationSetDeliveryOptions_Persists(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateConfigurationSet("cs1"))
	require.NoError(t, b.PutConfigurationSetDeliveryOptions("cs1", "Require"))

	desc, err := b.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	require.NotNil(t, desc.DeliveryOptions)
	assert.Equal(t, "Require", desc.DeliveryOptions.TLSPolicy)
}

func TestUpdateConfigurationSetSendingEnabled_Persists(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateConfigurationSet("cs1"))

	desc, err := b.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	assert.True(t, desc.SendingEnabled, "new config set starts with sending enabled")

	require.NoError(t, b.UpdateConfigurationSetSendingEnabled("cs1", false))

	desc, err = b.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	assert.False(t, desc.SendingEnabled)
}

func TestUpdateConfigurationSetReputationMetricsEnabled_Persists(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateConfigurationSet("cs1"))
	require.NoError(t, b.UpdateConfigurationSetReputationMetricsEnabled("cs1", true))

	desc, err := b.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	assert.True(t, desc.ReputationMetricsEnabled)
}

func TestDescribeConfigurationSet_ReturnsDeliveryAndReputation_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateConfigurationSet("cs1"))

	body := url.Values{
		"Action":               {"DescribeConfigurationSet"},
		"Version":              {"2010-12-01"},
		"ConfigurationSetName": {"cs1"},
	}.Encode()

	rec := postForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DescribeConfigurationSetResponse")
	assert.Contains(t, rec.Body.String(), "ReputationOptions")
}

// ---- ReceiptRule Actions (issue #11) ----

func TestReceiptRule_Actions_CreateDescribe(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateReceiptRuleSet("rs"))

	rule := ses.ReceiptRule{
		Name:    "rule1",
		Enabled: true,
		Actions: []ses.ReceiptAction{
			{Type: ses.ReceiptActionTypeSNS, SNSTopicARN: "arn:aws:sns:us-east-1:123:topic"},
			{Type: ses.ReceiptActionTypeAddHeader, HeaderName: "X-Custom", HeaderValue: "val"},
		},
	}
	require.NoError(t, b.CreateReceiptRule("rs", rule, ""))

	described, err := b.DescribeReceiptRule("rs", "rule1")
	require.NoError(t, err)
	require.Len(t, described.Actions, 2)
	assert.Equal(t, ses.ReceiptActionTypeSNS, described.Actions[0].Type)
	assert.Equal(t, "arn:aws:sns:us-east-1:123:topic", described.Actions[0].SNSTopicARN)
	assert.Equal(t, ses.ReceiptActionTypeAddHeader, described.Actions[1].Type)
	assert.Equal(t, "X-Custom", described.Actions[1].HeaderName)
}

func TestReceiptRule_Actions_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.CreateReceiptRuleSet("rs"))

	body := url.Values{
		"Action":       {"CreateReceiptRule"},
		"Version":      {"2010-12-01"},
		"RuleSetName":  {"rs"},
		"Rule.Name":    {"r1"},
		"Rule.Enabled": {"true"},
		"Rule.Actions.member.1.SNSAction.TopicArn": {"arn:aws:sns:us-east-1:123:t"},
	}.Encode()

	rec := postForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec.Code)

	described, err := h.Backend.DescribeReceiptRule("rs", "r1")
	require.NoError(t, err)
	require.Len(t, described.Actions, 1)
	assert.Equal(t, ses.ReceiptActionTypeSNS, described.Actions[0].Type)
}

// ---- SendBulkTemplatedEmail per-destination (issue #4) ----

func TestSendBulkTemplatedEmail_PerDestination(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("sender@example.com"))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
		TemplateName: "t", SubjectPart: "subj", TextPart: "text",
	}))

	destinations := []ses.BulkEmailDestination{
		{To: []string{"a@example.com"}},
		{To: []string{"b@example.com"}, Cc: []string{"cc@example.com"}},
	}

	msgIDs, err := b.SendBulkTemplatedEmail("sender@example.com", "t", destinations)
	require.NoError(t, err)
	assert.Len(t, msgIDs, 2)

	emails := b.ListEmails()
	require.Len(t, emails, 2)
	assert.Equal(t, []string{"a@example.com"}, emails[0].To)
	assert.Equal(t, []string{"b@example.com"}, emails[1].To)
	assert.Equal(t, []string{"cc@example.com"}, emails[1].Cc)
}

func TestSendBulkTemplatedEmail_Handler_ParsesAllDestinations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "t", SubjectPart: "s", TextPart: "body",
	}))

	body := url.Values{
		"Action":   {"SendBulkTemplatedEmail"},
		"Version":  {"2010-12-01"},
		"Source":   {"s@example.com"},
		"Template": {"t"},
		"Destinations.member.1.Destination.ToAddresses.member.1": {"a@example.com"},
		"Destinations.member.1.ReplacementTemplateData":          {`{"name":"Alice"}`},
		"Destinations.member.2.Destination.ToAddresses.member.1": {"b@example.com"},
		"Destinations.member.2.Destination.CcAddresses.member.1": {"cc@example.com"},
	}.Encode()

	rec := postForm(t, h, body)
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 2)
	assert.Equal(t, []string{"a@example.com"}, emails[0].To)
	assert.Equal(t, []string{"b@example.com"}, emails[1].To)
	assert.Equal(t, []string{"cc@example.com"}, emails[1].Cc)
}

// ---- VerifyDomainIdentity deterministic token ----

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

// ---- Identity snapshot/restore roundtrip ----

func TestIdentityRecord_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("a@example.com"))
	require.NoError(t, b.SetIdentityDkimEnabled("a@example.com", true))
	require.NoError(t, b.SetIdentityMailFromDomain("a@example.com", "mail.example.com"))
	require.NoError(t, b.SetIdentityNotificationTopic("a@example.com", "Bounce", "arn:topic"))

	snap := b.Snapshot()
	require.NotNil(t, snap)

	fresh := ses.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(snap))

	dkim := fresh.GetIdentityDkimAttributes([]string{"a@example.com"})
	assert.True(t, dkim["a@example.com"].DkimEnabled)

	mail := fresh.GetIdentityMailFromDomainAttributes([]string{"a@example.com"})
	assert.Equal(t, "mail.example.com", mail["a@example.com"].MailFromDomain)

	notif := fresh.GetIdentityNotificationAttributes([]string{"a@example.com"})
	assert.Equal(t, "arn:topic", notif["a@example.com"].BounceTopic)
}

// ---- ConfigurationSet snapshot/restore roundtrip ----

func TestConfigurationSet_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateConfigurationSet("cs1"))
	require.NoError(t, b.PutConfigurationSetDeliveryOptions("cs1", "Require"))
	require.NoError(t, b.UpdateConfigurationSetSendingEnabled("cs1", false))
	require.NoError(t, b.UpdateConfigurationSetReputationMetricsEnabled("cs1", true))

	snap := b.Snapshot()
	require.NotNil(t, snap)

	fresh := ses.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(snap))

	desc, err := fresh.DescribeConfigurationSet("cs1")
	require.NoError(t, err)
	require.NotNil(t, desc.DeliveryOptions)
	assert.Equal(t, "Require", desc.DeliveryOptions.TLSPolicy)
	assert.False(t, desc.SendingEnabled)
	assert.True(t, desc.ReputationMetricsEnabled)
}
