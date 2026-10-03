package smtprelay_test

import (
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/mail"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/smtprelay"
	"github.com/blackbirdworks/gopherstack/pkgs/smtprelay/smtptest"
)

const sessionWait = 10 * time.Second

func next(t *testing.T, s *smtptest.Server) smtptest.Session {
	t.Helper()

	select {
	case sess := <-s.Sessions():
		return sess
	case <-time.After(sessionWait):
		require.FailNow(t, "no SMTP session")

		return smtptest.Session{}
	}
}

func TestRelayDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, sess smtptest.Session, msg *mail.Message)
		name  string
		msg   smtprelay.Message
	}{
		{
			name: "text only with bcc",
			msg: smtprelay.Message{
				ID: "ses-1", From: "Sender <a@example.com>", To: []string{"b@example.com"},
				Cc: []string{"c@example.com"}, Bcc: []string{"d@example.com"},
				ReplyTo: []string{"r@example.com"}, Subject: "Hi", Text: "hello",
			},
			check: func(t *testing.T, sess smtptest.Session, msg *mail.Message) {
				t.Helper()
				assert.Equal(t, "a@example.com", sess.From)
				assert.Equal(t, []string{"b@example.com", "c@example.com", "d@example.com"}, sess.Rcpts)
				assert.Equal(t, "Sender <a@example.com>", msg.Header.Get("From"))
				assert.Equal(t, "b@example.com", msg.Header.Get("To"))
				assert.Equal(t, "c@example.com", msg.Header.Get("Cc"))
				assert.Equal(t, "r@example.com", msg.Header.Get("Reply-To"))
				assert.Equal(t, "<ses-1@example.com>", msg.Header.Get("Message-ID"))
				assert.NotContains(t, sess.Data, "d@example.com")
				_, err := msg.Header.Date()
				require.NoError(t, err)
				mt, _, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
				require.NoError(t, err)
				assert.Equal(t, "text/plain", mt)
			},
		},
		{
			name: "html only with return path",
			msg: smtprelay.Message{
				ID: "x", From: "a@example.com", EnvelopeFrom: "bounce@example.com",
				To: []string{"b@example.com"}, Subject: "H", HTML: "<b>x</b>",
			},
			check: func(t *testing.T, sess smtptest.Session, msg *mail.Message) {
				t.Helper()
				assert.Equal(t, "bounce@example.com", sess.From)
				assert.Contains(t, msg.Header.Get("Content-Type"), "text/html")
			},
		},
		{
			name: "multipart alternative",
			msg: smtprelay.Message{
				ID: "m", From: "a@example.com", To: []string{"b@example.com"},
				Subject: "Ünï", Text: "plain é", HTML: "<p>html é</p>",
			},
			check: func(t *testing.T, _ smtptest.Session, msg *mail.Message) {
				t.Helper()
				subj, err := new(mime.WordDecoder).DecodeHeader(msg.Header.Get("Subject"))
				require.NoError(t, err)
				assert.Equal(t, "Ünï", subj)

				mt, params, err := mime.ParseMediaType(msg.Header.Get("Content-Type"))
				require.NoError(t, err)
				require.Equal(t, "multipart/alternative", mt)

				mr := multipart.NewReader(msg.Body, params["boundary"])
				var types []string
				var bodies []string

				for {
					p, perr := mr.NextPart()
					if perr == io.EOF {
						break
					}

					require.NoError(t, perr)
					types = append(types, p.Header.Get("Content-Type"))
					b, rerr := io.ReadAll(p)
					require.NoError(t, rerr)
					bodies = append(bodies, string(b))
				}

				require.Len(t, types, 2)
				assert.Contains(t, types[0], "text/plain")
				assert.Contains(t, types[1], "text/html")
				assert.Contains(t, bodies[0], "plain é")
				assert.Contains(t, bodies[1], "html é")
			},
		},
		{
			name: "raw passthrough",
			msg: smtprelay.Message{
				From: "a@example.com", To: []string{"b@example.com"}, Bcc: []string{"hidden@example.com"},
				Raw: []byte("From: a@example.com\r\nTo: b@example.com\r\nSubject: raw\r\n\r\nbody\r\n"),
			},
			check: func(t *testing.T, sess smtptest.Session, msg *mail.Message) {
				t.Helper()
				assert.Equal(t, []string{"b@example.com", "hidden@example.com"}, sess.Rcpts)
				assert.Equal(t, "raw", msg.Header.Get("Subject"))
				assert.Empty(t, msg.Header.Get("Message-ID"))
				assert.Empty(t, msg.Header.Get("Content-Type"))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := smtptest.Start(t, smtptest.Options{})
			r := smtprelay.New(smtprelay.Config{Host: srv.Addr()})
			defer r.Close()

			require.True(t, r.Enqueue(tt.msg))

			sess := next(t, srv)
			msg, err := mail.ReadMessage(strings.NewReader(sess.Data))
			require.NoError(t, err)
			tt.check(t, sess, msg)
		})
	}
}

func TestRelayAuthAndTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		user     string
		starttls bool
		auth     bool
		wantTLS  bool
		wantAuth bool
	}{
		{name: "starttls then auth", user: "u", starttls: true, auth: true, wantTLS: true, wantAuth: true},
		{name: "starttls without credentials", starttls: true, wantTLS: true},
		{name: "plaintext localhost auth allowed", user: "u", auth: true, wantAuth: true},
		{name: "no credentials plaintext", wantTLS: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := smtptest.Options{AdvertiseAuth: tt.auth}
			ropts := []smtprelay.Option{}

			if tt.starttls {
				server, client := smtptest.TLSConfigs(t)
				opts.TLSConfig = server
				ropts = append(ropts, smtprelay.WithTLSConfig(client))
			}

			srv := smtptest.Start(t, opts)
			r := smtprelay.New(smtprelay.Config{Host: srv.Addr(), User: tt.user, Pass: "secret"}, ropts...)
			defer r.Close()

			require.True(t, r.Enqueue(smtprelay.Message{
				ID: "1", From: "a@example.com", To: []string{"b@example.com"}, Subject: "s", Text: "t",
			}))

			sess := next(t, srv)
			assert.Equal(t, tt.wantTLS, sess.TLS)
			assert.Equal(t, tt.wantAuth, len(sess.AuthLines) > 0)
			assert.Equal(t, []string{"b@example.com"}, sess.Rcpts)
		})
	}
}

func TestRelayFailureThenRecovery(t *testing.T) {
	t.Parallel()

	bad := smtptest.Start(t, smtptest.Options{RejectRcpt: true})
	r := smtprelay.New(smtprelay.Config{Host: bad.Addr()})
	defer r.Close()

	msg := smtprelay.Message{ID: "1", From: "a@example.com", To: []string{"b@example.com"}, Text: "t"}
	require.True(t, r.Enqueue(msg))
	assert.Empty(t, next(t, bad).Rcpts)
	require.True(t, r.Enqueue(msg), "relay keeps accepting after a failed delivery")
	assert.Empty(t, next(t, bad).Rcpts)
}

func TestRelayUnreachableHost(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	r := smtprelay.New(smtprelay.Config{Host: addr})
	require.True(t, r.Enqueue(smtprelay.Message{From: "a@example.com", To: []string{"b@example.com"}}))
	r.Close()
}

func TestRelayQueueBoundAndClose(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer ln.Close()

	accepted := make(chan net.Conn, 1)

	go func() {
		c, aerr := ln.Accept()
		if aerr == nil {
			accepted <- c
		}
	}()

	r := smtprelay.New(smtprelay.Config{Host: ln.Addr().String()}, smtprelay.WithQueueSize(2))
	msg := smtprelay.Message{From: "a@example.com", To: []string{"b@example.com"}}

	require.True(t, r.Enqueue(msg))

	select {
	case c := <-accepted:
		defer c.Close()
	case <-time.After(sessionWait):
		require.FailNow(t, "worker never dialed")
	}

	assert.True(t, r.Enqueue(msg))
	assert.True(t, r.Enqueue(msg))
	assert.False(t, r.Enqueue(msg), "queue is bounded")

	r.Close()
	r.Close()
	assert.False(t, r.Enqueue(msg), "closed relay rejects")
}

func TestHeaderInjectionRejected(t *testing.T) {
	t.Parallel()

	const evil = "\r\nBcc: evil@example.com"

	tests := []struct {
		name string
		msg  smtprelay.Message
	}{
		{name: "to", msg: smtprelay.Message{From: "a@example.com", To: []string{"x@example.com" + evil}}},
		{name: "cc", msg: smtprelay.Message{From: "a@example.com", Cc: []string{"x@example.com" + evil}}},
		{name: "from", msg: smtprelay.Message{From: "a@example.com" + evil, To: []string{"x@example.com"}}},
		{name: "reply_to", msg: smtprelay.Message{
			From: "a@example.com", To: []string{"x@example.com"}, ReplyTo: []string{"r@example.com" + evil},
		}},
		{name: "message_id", msg: smtprelay.Message{
			ID: "id" + evil, From: "a@example.com", To: []string{"x@example.com"},
		}},
		{name: "bare_lf", msg: smtprelay.Message{From: "a@example.com\nBcc: evil@example.com"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.msg.Text = "t"

			_, err := tt.msg.Bytes()
			require.ErrorIs(t, err, smtprelay.ErrHeaderInjection)
		})
	}
}

func TestSubjectInjectionNeutralised(t *testing.T) {
	t.Parallel()

	raw, err := smtprelay.Message{
		From: "a@example.com", To: []string{"x@example.com"}, Text: "t",
		Subject: "hi\r\nBcc: evil@example.com",
	}.Bytes()
	require.NoError(t, err)

	msg, err := mail.ReadMessage(strings.NewReader(string(raw)))
	require.NoError(t, err)
	assert.Empty(t, msg.Header.Get("Bcc"))
}

func TestEnvelopeInjectionNeverAddsRecipients(t *testing.T) {
	t.Parallel()

	rawMsg := []byte("From: a@example.com\r\nTo: b@example.com\r\nBcc: evil@example.com\r\n\r\nbody\r\n")

	tests := []struct {
		name string
		msg  smtprelay.Message
	}{
		{name: "raw", msg: smtprelay.Message{
			From: "a@example.com", Raw: rawMsg,
			To: []string{"b@example.com", "x@example.com>\r\nRCPT TO:<evil@example.com"},
		}},
		{name: "raw_header_bcc_ignored", msg: smtprelay.Message{
			From: "a@example.com", Raw: rawMsg, To: []string{"b@example.com"},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := smtptest.Start(t, smtptest.Options{})
			r := smtprelay.New(smtprelay.Config{Host: srv.Addr()})
			defer r.Close()

			require.True(t, r.Enqueue(tt.msg))

			sess := next(t, srv)
			assert.Equal(t, []string{"b@example.com"}, sess.Rcpts)
		})
	}
}

func TestConfigFromEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		env    map[string]string
		name   string
		want   smtprelay.Config
		wantOK bool
	}{
		{name: "unset", env: map[string]string{}},
		{name: "blank host", env: map[string]string{"SMTP_HOST": "  "}},
		{
			name:   "full",
			env:    map[string]string{"SMTP_HOST": "mail:587", "SMTP_USER": "u", "SMTP_PASS": "p"},
			want:   smtprelay.Config{Host: "mail:587", User: "u", Pass: "p"},
			wantOK: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, ok := smtprelay.ConfigFromEnv(func(k string) string { return tt.env[k] })
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.want, got)
		})
	}
}
