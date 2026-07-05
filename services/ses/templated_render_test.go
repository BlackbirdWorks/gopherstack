package ses_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

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

	ids, err := b.SendBulkTemplatedEmail(
		"sender@example.com",
		"tmpl",
		`{"greeting":"Hello","company":"Acme"}`,
		"",
		"",
		"",
		nil,
		dests,
	)
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

	_, err := b.SendBulkTemplatedEmail("sender@example.com", "nope", "", "", "", "", nil, dests)
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

	_, err := b.SendBulkTemplatedEmail("sender@example.com", "tmpl", "", "", "", "", nil, dests)
	require.ErrorIs(t, err, ses.ErrInvalidParameter, "got %v", err)
	assert.Contains(t, err.Error(), "TemplateData", "got %v", err)
}
