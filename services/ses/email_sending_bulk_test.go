package ses_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

func TestHandler_SendBounce_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *ses.Handler)
		name         string
		body         string
		wantContains string
		wantCode     int
	}{
		{
			name:         "missing_original_message_id",
			body:         "Action=SendBounce&Version=2010-12-01&OriginalMessageId=",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			// AWS SES models BounceSender as a required SendBounceInput member.
			name: "missing_bounce_sender",
			body: "Action=SendBounce&Version=2010-12-01&OriginalMessageId=msg-123&" +
				"BouncedRecipientInfoList.member.1.Recipient=to@example.com",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			// AWS SES models BouncedRecipientInfoList as a required, non-empty member.
			name:         "missing_recipient_list",
			body:         "Action=SendBounce&Version=2010-12-01&OriginalMessageId=msg-123&BounceSender=bounce@example.com",
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			// BounceSender must be a verified identity, matching SendEmail's rule.
			name: "unverified_bounce_sender",
			body: "Action=SendBounce&Version=2010-12-01&OriginalMessageId=msg-123&BounceSender=bounce@example.com&" +
				"BouncedRecipientInfoList.member.1.Recipient=to@example.com",
			wantCode:     http.StatusBadRequest,
			wantContains: "MessageRejected",
		},
		{
			name: "valid_original_message_id",
			body: "Action=SendBounce&Version=2010-12-01&OriginalMessageId=msg-123&BounceSender=bounce@example.com&" +
				"BouncedRecipientInfoList.member.1.Recipient=to@example.com",
			wantCode:     http.StatusOK,
			wantContains: "SendBounceResponse",
			setup: func(t *testing.T, h *ses.Handler) {
				t.Helper()
				require.NoError(t, h.Backend.VerifyEmailIdentity("bounce@example.com"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_SendBulkTemplatedEmail_TooManyDestinations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		numDests     int
		wantCode     int
	}{
		{
			name:         "exactly_50_ok",
			numDests:     50,
			wantCode:     http.StatusOK,
			wantContains: "SendBulkTemplatedEmailResponse",
		},
		{
			name:         "51_too_many",
			numDests:     51,
			wantCode:     http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()

			// Create template and verify sender.
			require.NoError(t, h.Backend.VerifyEmailIdentity("bulk@example.com"))
			postForm(t, h, url.Values{
				"Action":                {"CreateTemplate"},
				"Version":               {"2010-12-01"},
				"Template.TemplateName": {"BulkTpl"},
				"Template.SubjectPart":  {"subj"},
				"Template.TextPart":     {"body"},
				"Template.HtmlPart":     {"<p>body</p>"},
			}.Encode())

			vals := url.Values{
				"Action":   {"SendBulkTemplatedEmail"},
				"Version":  {"2010-12-01"},
				"Source":   {"bulk@example.com"},
				"Template": {"BulkTpl"},
			}

			for i := 1; i <= tt.numDests; i++ {
				prefix := "Destinations.member." + url.QueryEscape(url.Values{"n": {string(rune('0' + i))}}.Encode())
				_ = prefix
				key := "Destinations.member." + itoa(i) + ".Destination.ToAddresses.member.1"
				vals[key] = []string{"dest" + itoa(i) + "@example.com"}
			}

			rec := postForm(t, h, vals.Encode())
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestSendTemplatedEmail_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "greet",
		SubjectPart:  "Hello",
		TextPart:     "Hi there",
	}))

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendTemplatedEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"s@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Template":                         {"greet"},
		"TemplateData":                     {`{"name":"Alice"}`},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SendTemplatedEmailResponse")
}

func TestSendTemplatedEmail_NonExistentTemplate_Error(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":                           {"SendTemplatedEmail"},
		"Version":                          {"2010-12-01"},
		"Source":                           {"s@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Template":                         {"nonexistent"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestSendBulkTemplatedEmail_MultipleDestinations(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "bulk",
		SubjectPart:  "Bulk",
		TextPart:     "Hi",
	}))

	rec := postForm(t, h, url.Values{
		"Action":   {"SendBulkTemplatedEmail"},
		"Version":  {"2010-12-01"},
		"Source":   {"s@example.com"},
		"Template": {"bulk"},
		"Destinations.member.1.Destination.ToAddresses.member.1": {"a@example.com"},
		"Destinations.member.2.Destination.ToAddresses.member.1": {"b@example.com"},
		"Destinations.member.3.Destination.ToAddresses.member.1": {"c@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	assert.Len(t, emails, 3)
}

func TestSendBulkTemplatedEmail_PerDestinationData(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
		TemplateName: "t",
		SubjectPart:  "Hi",
		TextPart:     "text",
	}))

	dests := []ses.BulkEmailDestination{
		{To: []string{"a@example.com"}, ReplacementTemplateData: `{"name":"Alice"}`},
		{To: []string{"b@example.com"}, Cc: []string{"cc@example.com"}},
	}

	ids, err := b.SendBulkTemplatedEmail(ses.SendBulkTemplatedEmailInput{
		Source:       "s@example.com",
		TemplateName: "t",
		Destinations: dests,
	})
	require.NoError(t, err)
	assert.Len(t, ids, 2)

	emails := b.ListEmails()
	assert.Len(t, emails, 2)
	assert.Equal(t, []string{"cc@example.com"}, emails[1].Cc)
}

func TestSendBounce_Handler(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	msgID, err := h.Backend.SendEmail(ses.SendEmailInput{
		From:     "s@example.com",
		To:       []string{"to@example.com"},
		Subject:  "orig",
		BodyText: "body",
	})
	require.NoError(t, err)

	// AWS SES requires BounceSender to be a verified identity and
	// BouncedRecipientInfoList to contain at least one recipient entry.
	require.NoError(t, h.Backend.VerifyEmailIdentity("mailer-daemon@example.com"))

	rec := postForm(t, h, url.Values{
		"Action":            {"SendBounce"},
		"Version":           {"2010-12-01"},
		"OriginalMessageId": {msgID},
		"BounceSender":      {"mailer-daemon@example.com"},
		"BouncedRecipientInfoList.member.1.Recipient": {"to@example.com"},
		"MessageDsn.ReportingMta":                     {"dns; example.com"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "SendBounceResponse")
}

func TestSendBounce_Handler_RequiresBounceSenderAndRecipients(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	msgID, err := h.Backend.SendEmail(ses.SendEmailInput{
		From:     "s@example.com",
		To:       []string{"to@example.com"},
		Subject:  "orig",
		BodyText: "body",
	})
	require.NoError(t, err)

	// Missing BounceSender entirely: AWS SES models it as a required member.
	rec := postForm(t, h, url.Values{
		"Action":            {"SendBounce"},
		"Version":           {"2010-12-01"},
		"OriginalMessageId": {msgID},
		"BouncedRecipientInfoList.member.1.Recipient": {"to@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterValue")

	// BounceSender present but not a verified identity: MessageRejected.
	rec = postForm(t, h, url.Values{
		"Action":            {"SendBounce"},
		"Version":           {"2010-12-01"},
		"OriginalMessageId": {msgID},
		"BounceSender":      {"unverified@example.com"},
		"BouncedRecipientInfoList.member.1.Recipient": {"to@example.com"},
	}.Encode())
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "MessageRejected")
}

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

func TestSendBulkTemplatedEmail_PerDestination_Backend(t *testing.T) {
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

	msgIDs, err := b.SendBulkTemplatedEmail(ses.SendBulkTemplatedEmailInput{
		Source:       "sender@example.com",
		TemplateName: "t",
		Destinations: destinations,
	})
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

func TestSendBulkTemplatedEmail_PerDestinationStatus(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "t", SubjectPart: "subj", TextPart: "body",
	}))

	rec := postForm(t, h, url.Values{
		"Action":   {"SendBulkTemplatedEmail"},
		"Version":  {"2010-12-01"},
		"Source":   {"s@example.com"},
		"Template": {"t"},
		"Destinations.member.1.Destination.ToAddresses.member.1": {"a@example.com"},
		"Destinations.member.2.Destination.ToAddresses.member.1": {"b@example.com"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "SendBulkTemplatedEmailResponse")
	assert.Contains(t, body, "<Status>", "each destination must have a Status element")
	assert.Contains(t, body, "<MessageId>", "each destination must have a MessageId element")
	assert.Contains(t, body, "Success", "status must be Success for valid sends")
}

func TestSESBackend_SendTemplatedEmail(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		setup     func(b *ses.InMemoryBackend)
		name      string
		from      string
		template  string
		wantErr   bool
	}{
		{
			name: "success",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.VerifyEmailIdentity("s@test.com"))
				require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
					TemplateName: "welcome",
					SubjectPart:  "Welcome!",
					HTMLPart:     "<h1>Hi</h1>",
					TextPart:     "Hi",
				}))
			},
			from:     "s@test.com",
			template: "welcome",
		},
		{
			name: "domain_verified",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.VerifyEmailIdentity("test.com"))
				require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t1"}))
			},
			from:     "user@test.com",
			template: "t1",
		},
		{
			name: "unverified_source",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t2"}))
			},
			from:      "x@test.com",
			template:  "t2",
			wantErr:   true,
			wantErrIs: ses.ErrMessageRejected,
		},
		{
			name: "template_not_found",
			setup: func(b *ses.InMemoryBackend) {
				require.NoError(t, b.VerifyEmailIdentity("s@test.com"))
			},
			from:      "s@test.com",
			template:  "nonexistent",
			wantErr:   true,
			wantErrIs: ses.ErrTemplateNotFound,
		},
		{
			name:      "empty_source",
			setup:     func(_ *ses.InMemoryBackend) {},
			from:      "",
			template:  "t",
			wantErr:   true,
			wantErrIs: ses.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			tt.setup(b)

			msgID, err := b.SendTemplatedEmail(ses.SendTemplatedEmailInput{
				From: tt.from, To: []string{"to@test.com"}, TemplateName: tt.template,
			})

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, msgID)
			// Verify the email was stored.
			assert.Equal(t, 1, b.EmailCount())
			assert.Equal(t, b.EmailCount(), b.EmailsByIDCount())
		})
	}
}

func TestSESHandler_SendTemplatedEmail(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("sender@example.com"))
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "mytemplate",
		SubjectPart:  "Hello",
		HTMLPart:     "<b>Hello</b>",
	}))

	tests := []struct {
		body         url.Values
		name         string
		wantContains string
		wantCode     int
	}{
		{
			name: "success",
			body: url.Values{
				"Action":                           {"SendTemplatedEmail"},
				"Version":                          {"2010-12-01"},
				"Source":                           {"sender@example.com"},
				"Template":                         {"mytemplate"},
				"Destination.ToAddresses.member.1": {"to@example.com"},
			},
			wantCode:     200,
			wantContains: "SendTemplatedEmailResponse",
		},
		{
			name: "template_not_found",
			body: url.Values{
				"Action":                           {"SendTemplatedEmail"},
				"Version":                          {"2010-12-01"},
				"Source":                           {"sender@example.com"},
				"Template":                         {"no-such-template"},
				"Destination.ToAddresses.member.1": {"to@example.com"},
			},
			wantCode:     400,
			wantContains: "TemplateDoesNotExist",
		},
		{
			name: "unverified_source",
			body: url.Values{
				"Action":                           {"SendTemplatedEmail"},
				"Version":                          {"2010-12-01"},
				"Source":                           {"unknown@example.com"},
				"Template":                         {"mytemplate"},
				"Destination.ToAddresses.member.1": {"to@example.com"},
			},
			wantCode:     400,
			wantContains: "MessageRejected",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postForm(t, h, tt.body.Encode())
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantContains)
		})
	}
}

func TestSendBulkTemplatedEmail_UnknownConfigurationSet_Rejected(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t", SubjectPart: "s", TextPart: "b"}))

	dests := []ses.BulkEmailDestination{{To: []string{"a@example.com"}}}

	_, err := b.SendBulkTemplatedEmail(ses.SendBulkTemplatedEmailInput{
		Source:               "s@example.com",
		TemplateName:         "t",
		ConfigurationSetName: "does-not-exist",
		Destinations:         dests,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrConfigSetNotFound)
}

func TestSendBulkTemplatedEmail_PlumbsMetadataThrough(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, b.CreateConfigurationSet("cs1"))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t", SubjectPart: "s", TextPart: "b"}))

	dests := []ses.BulkEmailDestination{{To: []string{"a@example.com"}}}

	ids, err := b.SendBulkTemplatedEmail(ses.SendBulkTemplatedEmailInput{
		Source:               "s@example.com",
		TemplateName:         "t",
		ConfigurationSetName: "cs1",
		ReturnPath:           "return@example.com",
		ReturnPathArn:        "arn:aws:ses:us-east-1:123:identity/return@example.com",
		SourceArn:            "arn:aws:ses:us-east-1:123:identity/s@example.com",
		ReplyTo:              []string{"reply@example.com"},
		DefaultTags:          []ses.Tag{{Name: "campaign", Value: "spring"}},
		Destinations:         dests,
	})
	require.NoError(t, err)
	require.Len(t, ids, 1)

	email, err := b.GetEmailByID(ids[0])
	require.NoError(t, err)
	assert.Equal(t, "cs1", email.ConfigurationSetName)
	assert.Equal(t, "return@example.com", email.ReturnPath)
	assert.Equal(t, "arn:aws:ses:us-east-1:123:identity/return@example.com", email.ReturnPathArn)
	assert.Equal(t, "arn:aws:ses:us-east-1:123:identity/s@example.com", email.SourceArn)
	assert.Equal(t, []string{"reply@example.com"}, email.ReplyTo)
	assert.Equal(t, []ses.Tag{{Name: "campaign", Value: "spring"}}, email.Tags)
}

// TestSendBulkTemplatedEmail_ReplacementTagsOverrideDefault verifies that a
// destination's ReplacementTags, when present, is used in place of the
// request-level DefaultTags for that destination's stored Email record,
// matching SendBulkTemplatedEmailInput.BulkEmailDestination.ReplacementTags.
func TestSendBulkTemplatedEmail_ReplacementTagsOverrideDefault(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{TemplateName: "t", SubjectPart: "s", TextPart: "b"}))

	dests := []ses.BulkEmailDestination{
		{To: []string{"a@example.com"}}, // no override: inherits DefaultTags
		{To: []string{"b@example.com"}, ReplacementTags: []ses.Tag{{Name: "campaign", Value: "override"}}},
	}

	ids, err := b.SendBulkTemplatedEmail(ses.SendBulkTemplatedEmailInput{
		Source:       "s@example.com",
		TemplateName: "t",
		DefaultTags:  []ses.Tag{{Name: "campaign", Value: "default"}},
		Destinations: dests,
	})
	require.NoError(t, err)
	require.Len(t, ids, 2)

	email0, err := b.GetEmailByID(ids[0])
	require.NoError(t, err)
	assert.Equal(t, []ses.Tag{{Name: "campaign", Value: "default"}}, email0.Tags)

	email1, err := b.GetEmailByID(ids[1])
	require.NoError(t, err)
	assert.Equal(t, []ses.Tag{{Name: "campaign", Value: "override"}}, email1.Tags)
}

// TestHandler_SendBulkTemplatedEmail_ReturnPathArnAndTags_WireParsing verifies
// the handler parses ReturnPathArn, DefaultTags, and per-destination
// ReplacementTags from the query-protocol form and threads them through to
// the stored Email record.
func TestHandler_SendBulkTemplatedEmail_ReturnPathArnAndTags_WireParsing(t *testing.T) {
	t.Parallel()

	h := newHandler()
	require.NoError(t, h.Backend.VerifyEmailIdentity("s@example.com"))
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "t", SubjectPart: "s", TextPart: "body",
	}))

	rec := postForm(t, h, url.Values{
		"Action":                     {"SendBulkTemplatedEmail"},
		"Version":                    {"2010-12-01"},
		"Source":                     {"s@example.com"},
		"Template":                   {"t"},
		"ReturnPathArn":              {"arn:aws:ses:us-east-1:123:identity/return@example.com"},
		"DefaultTags.member.1.Name":  {"campaign"},
		"DefaultTags.member.1.Value": {"spring"},
		"Destinations.member.1.Destination.ToAddresses.member.1": {"a@example.com"},
		"Destinations.member.1.ReplacementTags.member.1.Name":    {"campaign"},
		"Destinations.member.1.ReplacementTags.member.1.Value":   {"override"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	emails := h.Backend.(*ses.InMemoryBackend).ListEmails()
	require.Len(t, emails, 1)
	assert.Equal(t, "arn:aws:ses:us-east-1:123:identity/return@example.com", emails[0].ReturnPathArn)
	assert.Equal(t, []ses.Tag{{Name: "campaign", Value: "override"}}, emails[0].Tags)
}

// TestSendTemplatedEmail_VariableSubstitution verifies that SendTemplatedEmail
// substitutes {{var}} placeholders from the JSON TemplateData into the stored
// subject and bodies, matching AWS SES Handlebars-style templating.
func TestSendTemplatedEmail_VariableSubstitution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		subject      string
		text         string
		html         string
		templateData string
		wantSubject  string
		wantText     string
		wantHTML     string
	}{
		{
			name:         "single var",
			subject:      "Hello {{name}}",
			text:         "Welcome {{name}}",
			html:         "<p>Hi {{name}}</p>",
			templateData: `{"name":"Alice"}`,
			wantSubject:  "Hello Alice",
			wantText:     "Welcome Alice",
			wantHTML:     "<p>Hi Alice</p>",
		},
		{
			name:         "multiple vars",
			subject:      "{{greeting}} {{name}}",
			text:         "{{name}} has {{count}} items",
			html:         "<b>{{name}}</b>",
			templateData: `{"greeting":"Hi","name":"Bob","count":3}`,
			wantSubject:  "Hi Bob",
			wantText:     "Bob has 3 items",
			wantHTML:     "<b>Bob</b>",
		},
		{
			name:         "missing var left intact",
			subject:      "Hello {{name}}",
			text:         "x",
			html:         "y",
			templateData: `{"other":"z"}`,
			wantSubject:  "Hello {{name}}",
			wantText:     "x",
			wantHTML:     "y",
		},
		{
			name:         "empty template data leaves placeholders",
			subject:      "Hello {{name}}",
			text:         "{{name}}",
			html:         "{{name}}",
			templateData: "",
			wantSubject:  "Hello {{name}}",
			wantText:     "{{name}}",
			wantHTML:     "{{name}}",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ses.NewInMemoryBackend()
			require.NoError(t, b.VerifyEmailIdentity("sender@example.com"))
			require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
				TemplateName: "tmpl",
				SubjectPart:  tc.subject,
				TextPart:     tc.text,
				HTMLPart:     tc.html,
			}))

			id, err := b.SendTemplatedEmail(ses.SendTemplatedEmailInput{
				From:         "sender@example.com",
				To:           []string{"to@example.com"},
				TemplateName: "tmpl",
				TemplateData: tc.templateData,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, id)

			emails := b.ListEmails()
			require.Len(t, emails, 1)
			assert.Equal(t, tc.wantSubject, emails[0].Subject)
			assert.Equal(t, tc.wantText, emails[0].BodyText)
			assert.Equal(t, tc.wantHTML, emails[0].BodyHTML)
		})
	}
}

// TestSendTemplatedEmail_InvalidTemplateData verifies malformed TemplateData is
// rejected with InvalidParameterValue, matching real SES request validation.
func TestSendTemplatedEmail_InvalidTemplateData(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("sender@example.com"))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
		TemplateName: "tmpl", SubjectPart: "s", TextPart: "t",
	}))

	_, err := b.SendTemplatedEmail(ses.SendTemplatedEmailInput{
		From:         "sender@example.com",
		To:           []string{"to@example.com"},
		TemplateName: "tmpl",
		TemplateData: `{not valid json`,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrInvalidParameter, "want InvalidParameterValue, got %v", err)
}

// TestSendBulkTemplatedEmail_DefaultAndReplacementData verifies that the
// request-level DefaultTemplateData is applied to every destination and that a
// destination's ReplacementTemplateData overrides matching default keys.
func TestSendBulkTemplatedEmail_DefaultAndReplacementData(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("sender@example.com"))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
		TemplateName: "tmpl",
		SubjectPart:  "{{greeting}} {{name}}",
		TextPart:     "from {{company}}",
	}))

	dests := []ses.BulkEmailDestination{
		{To: []string{"a@example.com"}, ReplacementTemplateData: `{"name":"Alice"}`},
		{To: []string{"b@example.com"}, ReplacementTemplateData: `{"name":"Bob","greeting":"Hey"}`},
	}

	ids, err := b.SendBulkTemplatedEmail(ses.SendBulkTemplatedEmailInput{
		Source:              "sender@example.com",
		TemplateName:        "tmpl",
		DefaultTemplateData: `{"greeting":"Hello","company":"Acme"}`,
		Destinations:        dests,
	})
	require.NoError(t, err)
	require.Len(t, ids, 2)

	emails := b.ListEmails()
	require.Len(t, emails, 2)

	// Destination 1: default greeting + company, replacement name.
	assert.Equal(t, "Hello Alice", emails[0].Subject)
	assert.Equal(t, "from Acme", emails[0].BodyText)

	// Destination 2: replacement overrides greeting; default company applies.
	assert.Equal(t, "Hey Bob", emails[1].Subject)
	assert.Equal(t, "from Acme", emails[1].BodyText)
}

// TestSendBulkTemplatedEmail_MissingTemplate verifies the template is validated
// up front so a missing template fails with TemplateDoesNotExist even before any
// destination is processed.
func TestSendBulkTemplatedEmail_MissingTemplate(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("sender@example.com"))

	dests := []ses.BulkEmailDestination{
		{To: []string{"a@example.com"}},
	}

	_, err := b.SendBulkTemplatedEmail(ses.SendBulkTemplatedEmailInput{
		Source:       "sender@example.com",
		TemplateName: "nope",
		Destinations: dests,
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, ses.ErrTemplateNotFound, "want TemplateDoesNotExist, got %v", err)
}

// TestSendBulkTemplatedEmail_InvalidReplacementData verifies malformed
// per-destination ReplacementTemplateData is rejected with InvalidParameterValue.
func TestSendBulkTemplatedEmail_InvalidReplacementData(t *testing.T) {
	t.Parallel()

	b := ses.NewInMemoryBackend()
	require.NoError(t, b.VerifyEmailIdentity("sender@example.com"))
	require.NoError(t, b.CreateTemplate(ses.EmailTemplate{
		TemplateName: "tmpl", SubjectPart: "s", TextPart: "t",
	}))

	dests := []ses.BulkEmailDestination{
		{To: []string{"a@example.com"}, ReplacementTemplateData: `{bad`},
	}

	_, err := b.SendBulkTemplatedEmail(ses.SendBulkTemplatedEmailInput{
		Source:       "sender@example.com",
		TemplateName: "tmpl",
		Destinations: dests,
	})
	require.ErrorIs(t, err, ses.ErrInvalidParameter, "got %v", err)
	assert.Contains(t, err.Error(), "TemplateData", "got %v", err)
}
