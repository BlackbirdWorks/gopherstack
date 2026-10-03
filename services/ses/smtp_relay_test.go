package ses_test

import (
	"context"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"net/mail"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/smtprelay/smtptest"
	"github.com/blackbirdworks/gopherstack/services/ses"
)

var msgIDRe = regexp.MustCompile(`<MessageId>([^<]+)</MessageId>`)

func relayHandler(t *testing.T, srv *smtptest.Server) *ses.Handler {
	t.Helper()

	env := map[string]string{"SMTP_HOST": srv.Addr()}
	p := &ses.Provider{Getenv: func(k string) string { return env[k] }}

	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)

	h, ok := svc.(*ses.Handler)
	require.True(t, ok)
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	require.NoError(t, h.Backend.VerifyEmailIdentity("sender@example.com"))

	return h
}

func nextSession(t *testing.T, srv *smtptest.Server) smtptest.Session {
	t.Helper()

	select {
	case s := <-srv.Sessions():
		return s
	case <-time.After(10 * time.Second):
		require.FailNow(t, "no SMTP session")

		return smtptest.Session{}
	}
}

func TestSMTPRelayDelivery(t *testing.T) {
	t.Parallel()

	rawMsg := "From: sender@example.com\r\nTo: visible@example.com\r\nSubject: raw\r\n\r\nraw body\r\n"

	tests := []struct {
		check func(t *testing.T, id string, sess smtptest.Session, msg *mail.Message)
		form  url.Values
		name  string
	}{
		{
			name: "send email multipart with cc and bcc",
			form: url.Values{
				"Action": {"SendEmail"}, "Source": {"sender@example.com"},
				"Destination.ToAddresses.member.1":  {"to@example.com"},
				"Destination.CcAddresses.member.1":  {"cc@example.com"},
				"Destination.BccAddresses.member.1": {"bcc@example.com"},
				"Message.Subject.Data":              {"Greetings"},
				"Message.Body.Text.Data":            {"plain"},
				"Message.Body.Html.Data":            {"<b>html</b>"},
				"ReturnPath":                        {"bounce@example.com"},
			},
			check: func(t *testing.T, id string, sess smtptest.Session, msg *mail.Message) {
				t.Helper()
				assert.Equal(t, "bounce@example.com", sess.From)
				assert.Equal(t, []string{"to@example.com", "cc@example.com", "bcc@example.com"}, sess.Rcpts)
				assert.Equal(t, "<"+id+"@example.com>", msg.Header.Get("Message-ID"))
				assert.Equal(t, "Greetings", msg.Header.Get("Subject"))
				assert.Equal(t, "cc@example.com", msg.Header.Get("Cc"))
				assert.NotContains(t, sess.Data, "bcc@example.com")

				mt, params, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
				require.NoError(t, err)
				require.Equal(t, "multipart/alternative", mt)

				mr := multipart.NewReader(msg.Body, params["boundary"])
				p1, err := mr.NextPart()
				require.NoError(t, err)
				b1, _ := io.ReadAll(p1)
				assert.Contains(t, string(b1), "plain")
				p2, err := mr.NextPart()
				require.NoError(t, err)
				b2, _ := io.ReadAll(p2)
				assert.Contains(t, string(b2), "<b>html</b>")
			},
		},
		{
			name: "send raw email passthrough with destinations",
			form: url.Values{
				"Action": {"SendRawEmail"}, "RawMessage.Data": {rawMsg},
				"Source":                {"sender@example.com"},
				"Destinations.member.1": {"visible@example.com"},
				"Destinations.member.2": {"secret@example.com"},
			},
			check: func(t *testing.T, _ string, sess smtptest.Session, msg *mail.Message) {
				t.Helper()
				assert.Equal(t, []string{"visible@example.com", "secret@example.com"}, sess.Rcpts)
				assert.Equal(t, "raw", msg.Header.Get("Subject"))
				assert.Empty(t, msg.Header.Get("Message-ID"))
				assert.Contains(t, sess.Data, "raw body")
			},
		},
		{
			name: "send templated email",
			form: url.Values{
				"Action": {"SendTemplatedEmail"}, "Source": {"sender@example.com"}, "Template": {"welcome"},
				"Destination.ToAddresses.member.1": {"to@example.com"},
				"TemplateData":                     {`{"name":"Ada"}`},
			},
			check: func(t *testing.T, _ string, sess smtptest.Session, msg *mail.Message) {
				t.Helper()
				assert.Equal(t, []string{"to@example.com"}, sess.Rcpts)
				assert.Equal(t, "Hi Ada", msg.Header.Get("Subject"))
				assert.Contains(t, msg.Header.Get("Content-Type"), "text/plain")
			},
		},
		{
			name: "simulator recipients are not relayed",
			form: url.Values{
				"Action": {"SendEmail"}, "Source": {"sender@example.com"},
				"Destination.ToAddresses.member.1": {"success@simulator.amazonses.com"},
				"Destination.ToAddresses.member.2": {"real@example.com"},
				"Message.Subject.Data":             {"s"}, "Message.Body.Text.Data": {"b"},
			},
			check: func(t *testing.T, _ string, sess smtptest.Session, _ *mail.Message) {
				t.Helper()
				assert.Equal(t, []string{"real@example.com"}, sess.Rcpts)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := smtptest.Start(t, smtptest.Options{})
			h := relayHandler(t, srv)
			require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
				TemplateName: "welcome", SubjectPart: "Hi {{name}}", TextPart: "Dear {{name}}",
			}))

			tt.form.Set("Version", "2010-12-01")
			rec := postForm(t, h, tt.form.Encode())
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			m := msgIDRe.FindStringSubmatch(rec.Body.String())
			require.Len(t, m, 2)

			sess := nextSession(t, srv)
			msg, err := mail.ReadMessage(strings.NewReader(sess.Data))
			require.NoError(t, err)
			tt.check(t, m[1], sess, msg)
		})
	}
}

func TestSMTPRelayBulkTemplated(t *testing.T) {
	t.Parallel()

	srv := smtptest.Start(t, smtptest.Options{})
	h := relayHandler(t, srv)
	require.NoError(t, h.Backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "welcome", SubjectPart: "Hi {{name}}", TextPart: "Dear {{name}}",
	}))

	form := url.Values{
		"Action": {"SendBulkTemplatedEmail"}, "Version": {"2010-12-01"}, "Source": {"sender@example.com"},
		"Template":            {"welcome"},
		"DefaultTemplateData": {`{"name":"X"}`},
		"Destinations.member.1.Destination.ToAddresses.member.1": {"a@example.com"},
		"Destinations.member.1.ReplacementTemplateData":          {`{"name":"A"}`},
		"Destinations.member.2.Destination.ToAddresses.member.1": {"b@example.com"},
	}
	rec := postForm(t, h, form.Encode())
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	for _, want := range []struct{ rcpt, subject string }{{"a@example.com", "Hi A"}, {"b@example.com", "Hi X"}} {
		sess := nextSession(t, srv)
		assert.Equal(t, []string{want.rcpt}, sess.Rcpts)
		assert.Contains(t, sess.Data, "Subject: "+want.subject)
	}
}

func TestSMTPRelayRespectsRejection(t *testing.T) {
	t.Parallel()

	srv := smtptest.Start(t, smtptest.Options{})
	h := relayHandler(t, srv)

	send := func(src string) int {
		form := url.Values{
			"Action": {"SendEmail"}, "Version": {"2010-12-01"}, "Source": {src},
			"Destination.ToAddresses.member.1": {"to@example.com"},
			"Message.Subject.Data":             {src}, "Message.Body.Text.Data": {"b"},
		}

		return postForm(t, h, form.Encode()).Code
	}

	assert.NotEqual(t, http.StatusOK, send("unverified@example.com"))
	require.Equal(t, http.StatusOK, send("sender@example.com"))

	sess := nextSession(t, srv)
	assert.Contains(t, sess.Data, "Subject: sender@example.com", "rejected send must not be relayed")
}

func TestSMTPRelayFailureDoesNotChangeAPI(t *testing.T) {
	t.Parallel()

	srv := smtptest.Start(t, smtptest.Options{RejectRcpt: true})
	h := relayHandler(t, srv)

	form := url.Values{
		"Action": {"SendEmail"}, "Version": {"2010-12-01"}, "Source": {"sender@example.com"},
		"Destination.ToAddresses.member.1": {"to@example.com"},
		"Message.Subject.Data":             {"s"}, "Message.Body.Text.Data": {"b"},
	}
	rec := postForm(t, h, form.Encode())
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Regexp(t, msgIDRe, rec.Body.String())

	_ = nextSession(t, srv)
	assert.Len(t, h.Backend.ListEmails(), 1)
}
