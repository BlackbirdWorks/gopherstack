package sesv2_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"mime"
	"net/http"
	"net/mail"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/smtprelay/smtptest"
	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

func relayHandler(t *testing.T, srv *smtptest.Server) *sesv2.Handler {
	t.Helper()

	env := map[string]string{"SMTP_HOST": srv.Addr()}
	p := &sesv2.Provider{Getenv: func(k string) string { return env[k] }}

	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)

	h, ok := svc.(*sesv2.Handler)
	require.True(t, ok)
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/identities",
		map[string]any{"EmailIdentity": "sender@example.com"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

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
	dest := map[string]any{
		"ToAddresses":  []string{"to@example.com"},
		"CcAddresses":  []string{"cc@example.com"},
		"BccAddresses": []string{"bcc@example.com"},
	}

	tests := []struct {
		body  map[string]any
		check func(t *testing.T, id string, sess smtptest.Session, msg *mail.Message)
		name  string
	}{
		{
			name: "simple html and text with cc bcc reply-to",
			body: map[string]any{
				"FromEmailAddress": "sender@example.com",
				"Destination":      dest,
				"ReplyToAddresses": []string{"reply@example.com"},
				"Content": map[string]any{"Simple": map[string]any{
					"Subject": map[string]any{"Data": "Hello"},
					"Body": map[string]any{
						"Text": map[string]any{"Data": "plain"},
						"Html": map[string]any{"Data": "<b>html</b>"},
					},
				}},
			},
			check: func(t *testing.T, id string, sess smtptest.Session, msg *mail.Message) {
				t.Helper()
				assert.Equal(t, "sender@example.com", sess.From)
				assert.Equal(t, []string{"to@example.com", "cc@example.com", "bcc@example.com"}, sess.Rcpts)
				assert.Equal(t, "<"+id+"@example.com>", msg.Header.Get("Message-ID"))
				assert.Equal(t, "reply@example.com", msg.Header.Get("Reply-To"))
				assert.Equal(t, "Hello", msg.Header.Get("Subject"))
				assert.NotContains(t, sess.Data, "bcc@example.com")

				mt, _, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
				require.NoError(t, err)
				assert.Equal(t, "multipart/alternative", mt)
			},
		},
		{
			name: "raw passthrough",
			body: map[string]any{
				"FromEmailAddress": "sender@example.com",
				"Destination":      dest,
				"Content": map[string]any{"Raw": map[string]any{
					"Data": base64.StdEncoding.EncodeToString([]byte(rawMsg)),
				}},
			},
			check: func(t *testing.T, _ string, sess smtptest.Session, msg *mail.Message) {
				t.Helper()
				assert.Equal(t, []string{"to@example.com", "cc@example.com", "bcc@example.com"}, sess.Rcpts)
				assert.Equal(t, "raw", msg.Header.Get("Subject"))
				assert.Empty(t, msg.Header.Get("Message-ID"))
			},
		},
		{
			name: "inline template content",
			body: map[string]any{
				"FromEmailAddress": "sender@example.com",
				"Destination":      map[string]any{"ToAddresses": []string{"to@example.com"}},
				"Content": map[string]any{"Template": map[string]any{
					"TemplateContent": map[string]any{"Subject": "Hi {{name}}", "Text": "Dear {{name}}"},
					"TemplateData":    `{"name":"Ada"}`,
				}},
			},
			check: func(t *testing.T, _ string, sess smtptest.Session, msg *mail.Message) {
				t.Helper()
				assert.Equal(t, []string{"to@example.com"}, sess.Rcpts)
				assert.Equal(t, "Hi Ada", msg.Header.Get("Subject"))
				assert.Contains(t, msg.Header.Get("Content-Type"), "text/plain")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := smtptest.Start(t, smtptest.Options{})
			h := relayHandler(t, srv)

			rec := doRequest(t, h, http.MethodPost, "/v2/email/outbound-emails", tt.body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var out map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			sess := nextSession(t, srv)
			msg, err := mail.ReadMessage(strings.NewReader(sess.Data))
			require.NoError(t, err)
			tt.check(t, out["MessageId"], sess, msg)
		})
	}
}

func TestSMTPRelayRespectsRejectionAndFailure(t *testing.T) {
	t.Parallel()

	body := func(from string) map[string]any {
		return map[string]any{
			"FromEmailAddress": from,
			"Destination":      map[string]any{"ToAddresses": []string{"to@example.com"}},
			"Content": map[string]any{"Simple": map[string]any{
				"Subject": map[string]any{"Data": from},
				"Body":    map[string]any{"Text": map[string]any{"Data": "b"}},
			}},
		}
	}

	t.Run("unverified sender is not relayed", func(t *testing.T) {
		t.Parallel()

		srv := smtptest.Start(t, smtptest.Options{})
		h := relayHandler(t, srv)

		rec := doRequest(t, h, http.MethodPost, "/v2/email/outbound-emails", body("unverified@example.com"))
		assert.NotEqual(t, http.StatusOK, rec.Code)

		rec = doRequest(t, h, http.MethodPost, "/v2/email/outbound-emails", body("sender@example.com"))
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, nextSession(t, srv).Data, "Subject: sender@example.com")
	})

	t.Run("failing smtp server leaves API result unchanged", func(t *testing.T) {
		t.Parallel()

		srv := smtptest.Start(t, smtptest.Options{RejectRcpt: true})
		h := relayHandler(t, srv)

		rec := doRequest(t, h, http.MethodPost, "/v2/email/outbound-emails", body("sender@example.com"))
		require.Equal(t, http.StatusOK, rec.Code)

		var out map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.NotEmpty(t, out["MessageId"])

		_ = nextSession(t, srv)
		assert.Len(t, h.Backend.ListEmails(), 1)
	})
}
