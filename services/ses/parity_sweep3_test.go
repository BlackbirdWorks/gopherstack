package ses_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// ---------------------------------------------------------------------------
// Void-result operations must wrap an "<Action>Result" element (or omit it
// entirely for AWS-empty-output-shape ops), never a literal "*Result" element.
// A real AWS SDK client calls decoder.GetElement("<Action>Result") before
// parsing the body, so a wrong wrapper causes a client-side
// DeserializationError even though the emulator's backend state mutated
// correctly — see aws-sdk-go-v2/service/ses deserializers.go.
// ---------------------------------------------------------------------------

func Test_VoidResultOps_WireShape(t *testing.T) {
	t.Parallel()

	cases := []struct {
		setup       func(t *testing.T, h *ses.Handler)
		name        string
		body        string
		wantElement string // must be present verbatim
	}{
		{
			name: "PutIdentityPolicy_has_result_wrapper",
			body: url.Values{
				"Action":     {"PutIdentityPolicy"},
				"Version":    {"2010-12-01"},
				"Identity":   {"example.com"},
				"PolicyName": {"p1"},
				"Policy":     {"{}"},
			}.Encode(),
			wantElement: "<PutIdentityPolicyResult></PutIdentityPolicyResult>",
		},
		{
			name: "SetIdentityDkimEnabled_has_result_wrapper",
			body: url.Values{
				"Action":      {"SetIdentityDkimEnabled"},
				"Version":     {"2010-12-01"},
				"Identity":    {"example.com"},
				"DkimEnabled": {"true"},
			}.Encode(),
			wantElement: "<SetIdentityDkimEnabledResult></SetIdentityDkimEnabledResult>",
		},
		{
			name: "VerifyEmailAddress_has_no_result_wrapper",
			body: url.Values{
				"Action":       {"VerifyEmailAddress"},
				"Version":      {"2010-12-01"},
				"EmailAddress": {"a@example.com"},
			}.Encode(),
			// Real SES's VerifyEmailAddress output shape has zero members, so the
			// wire format omits any Result element entirely.
			wantElement: "<VerifyEmailAddressResponse xmlns=\"http://ses.amazonaws.com/doc/2010-12-01/\">" +
				"<ResponseMetadata>",
		},
		{
			name: "UpdateAccountSendingEnabled_has_no_result_wrapper",
			body: url.Values{
				"Action":  {"UpdateAccountSendingEnabled"},
				"Version": {"2010-12-01"},
				"Enabled": {"false"},
			}.Encode(),
			wantElement: "<UpdateAccountSendingEnabledResponse xmlns=\"http://ses.amazonaws.com/doc/2010-12-01/\">" +
				"<ResponseMetadata>",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantElement)
			// The literal broken wrapper must never appear.
			assert.NotContains(t, rec.Body.String(), "*Result")
		})
	}
}

// ---------------------------------------------------------------------------
// AccountSendingPausedException: UpdateAccountSendingEnabled(false) must
// actually block subsequent Send* operations, matching real AWS SES. Before
// this fix the flag was stored/persisted but never enforced.
// ---------------------------------------------------------------------------

func Test_AccountSendingPaused_BlocksSend(t *testing.T) {
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

func Test_AccountSendingPaused_Handler_ReturnsExactErrorCode(t *testing.T) {
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

// ---------------------------------------------------------------------------
// GetSendQuota advertises Max24HourSend (200, matching the real AWS SES
// sandbox default) but it was never enforced against SendEmail/
// SendTemplatedEmail: an account could send unboundedly regardless of the
// quota it reported. Real AWS SES rejects sends past the 24-hour quota with
// MessageRejected.
// ---------------------------------------------------------------------------

func Test_SendEmail_DailyQuota_Exhaustion(t *testing.T) {
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

// ---------------------------------------------------------------------------
// ConfigurationSetName must reference an existing configuration set for
// SendEmail/SendTemplatedEmail/SendBulkTemplatedEmail — real AWS SES returns
// ConfigurationSetDoesNotExist otherwise. Previously the field was stored on
// the Email record but never validated.
// ---------------------------------------------------------------------------

func Test_SendEmail_UnknownConfigurationSet_Rejected(t *testing.T) {
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

func Test_SendBulkTemplatedEmail_UnknownConfigurationSet_Rejected(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t", SubjectPart: "s", TextPart: "b"}))

	dests := []ses.BulkEmailDestination{{To: []string{"a@example.com"}}}

	_, err := b.SendBulkTemplatedEmail("s@example.com", "t", "", "does-not-exist", "", "", nil, dests)
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrConfigSetNotFound)
}

func Test_SendBulkTemplatedEmail_PlumbsMetadataThrough(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, b.CreateConfigurationSet("cs1"))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t", SubjectPart: "s", TextPart: "b"}))

	dests := []ses.BulkEmailDestination{{To: []string{"a@example.com"}}}

	ids, err := b.SendBulkTemplatedEmail(
		"s@example.com", "t", "", "cs1", "return@example.com", "arn:aws:ses:us-east-1:123:identity/s@example.com",
		[]string{"reply@example.com"}, dests,
	)
	require.NoError(t, err)
	require.Len(t, ids, 1)

	email, err := b.GetEmailByID(ids[0])
	require.NoError(t, err)
	assert.Equal(t, "cs1", email.ConfigurationSetName)
	assert.Equal(t, "return@example.com", email.ReturnPath)
	assert.Equal(t, "arn:aws:ses:us-east-1:123:identity/s@example.com", email.SourceArn)
	assert.Equal(t, []string{"reply@example.com"}, email.ReplyTo)
}

// ---------------------------------------------------------------------------
// SendCustomVerificationEmail was a disguised stub: it validated its two
// string params but never checked the template existed, never registered the
// target identity, and returned a fabricated deterministic ID. It must now
// behave like a real AWS SES SendCustomVerificationEmailInput handler.
// ---------------------------------------------------------------------------

func Test_SendCustomVerificationEmail_RegistersIdentity(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
		TemplateName:          "cv",
		FromEmailAddress:      "noreply@example.com",
		TemplateSubject:       "Verify",
		TemplateContent:       "<p>click</p>",
		SuccessRedirectionURL: "https://example.com/ok",
		FailureRedirectionURL: "https://example.com/fail",
	}))

	// Before the call, the identity is unregistered / NotStarted.
	before := b.GetIdentityVerificationAttributes([]string{"user@example.com"})
	require.Equal(t, "NotStarted", before["user@example.com"])

	msgID, err := b.SendCustomVerificationEmail("user@example.com", "cv", "")
	require.NoError(t, err)
	assert.NotEmpty(t, msgID)

	after := b.GetIdentityVerificationAttributes([]string{"user@example.com"})
	assert.Equal(t, "Success", after["user@example.com"])
}

func Test_SendCustomVerificationEmail_UnknownTemplate_Rejected(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()

	_, err := b.SendCustomVerificationEmail("user@example.com", "does-not-exist", "")
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrCustomVerifTemplateNotFound)
}

func Test_SendCustomVerificationEmail_UnknownConfigurationSet_Rejected(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
		TemplateName:          "cv",
		FromEmailAddress:      "noreply@example.com",
		TemplateSubject:       "Verify",
		TemplateContent:       "<p>click</p>",
		SuccessRedirectionURL: "https://example.com/ok",
		FailureRedirectionURL: "https://example.com/fail",
	}))

	_, err := b.SendCustomVerificationEmail("user@example.com", "cv", "missing-cs")
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrConfigSetNotFound)
}

// ---------------------------------------------------------------------------
// SetIdentityMailFromDomain silently dropped the BehaviorOnMXFailure
// parameter (never plumbed to the backend, so GetIdentityMailFromDomainAttributes
// could never reflect the value a real client set). It must now be validated
// and returned in the attributes response, defaulting to "UseDefaultValue"
// when omitted, matching the AWS SES BehaviorOnMXFailure enum semantics.
// ---------------------------------------------------------------------------

func Test_SetIdentityMailFromDomain_BehaviorOnMXFailure(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		behavior      string
		wantPersisted string
		wantErr       bool
	}{
		{name: "default_when_omitted", behavior: "", wantPersisted: "UseDefaultValue"},
		{name: "use_default_value_explicit", behavior: "UseDefaultValue", wantPersisted: "UseDefaultValue"},
		{name: "reject_message", behavior: "RejectMessage", wantPersisted: "RejectMessage"},
		{name: "invalid_value_rejected", behavior: "Bogus", wantErr: true},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			err := b.SetIdentityMailFromDomain("a@example.com", "mail.example.com", tt.behavior)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, ses.ErrInvalidParameter)

				return
			}

			require.NoError(t, err)

			attrs := b.GetIdentityMailFromDomainAttributes([]string{"a@example.com"})
			assert.Equal(t, tt.wantPersisted, attrs["a@example.com"].BehaviorOnMXFailure)
		})
	}
}

func Test_SetIdentityMailFromDomain_Handler_PlumbsBehaviorOnMXFailure(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := postForm(t, h, url.Values{
		"Action":              {"SetIdentityMailFromDomain"},
		"Version":             {"2010-12-01"},
		"Identity":            {"a@example.com"},
		"MailFromDomain":      {"mail.example.com"},
		"BehaviorOnMXFailure": {"RejectMessage"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	attrs := h.Backend.GetIdentityMailFromDomainAttributes([]string{"a@example.com"})
	assert.Equal(t, "RejectMessage", attrs["a@example.com"].BehaviorOnMXFailure)
}
