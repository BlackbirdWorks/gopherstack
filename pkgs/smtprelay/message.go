package smtprelay

import (
	"bytes"
	"errors"
	"fmt"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/textproto"
	"strings"
	"time"
)

// ErrHeaderInjection is returned when a header value contains CR or LF.
var ErrHeaderInjection = errors.New("smtprelay: header value contains a line break")

// Message is one outbound email. When Raw is set it is sent verbatim and only the
// envelope fields (EnvelopeFrom, To, Cc, Bcc) are used.
type Message struct {
	Date    time.Time
	ID      string
	From    string
	Subject string
	Text    string
	HTML    string
	// EnvelopeFrom overrides From as the SMTP MAIL FROM (e.g. a SES ReturnPath).
	EnvelopeFrom string
	Raw          []byte
	To           []string
	Cc           []string
	Bcc          []string
	ReplyTo      []string
}

func (m Message) envelopeFrom() string {
	if m.EnvelopeFrom != "" {
		return m.EnvelopeFrom
	}

	return m.From
}

func (m Message) recipients() []string {
	out := make([]string, 0, len(m.To)+len(m.Cc)+len(m.Bcc))

	for _, group := range [][]string{m.To, m.Cc, m.Bcc} {
		for _, a := range group {
			if b, err := bareAddress(a); err == nil {
				out = append(out, b)
			}
		}
	}

	return out
}

// Bytes renders the RFC 5322 message. Bcc recipients never appear in the headers.
func (m Message) Bytes() ([]byte, error) {
	if m.Raw != nil {
		return m.Raw, nil
	}

	var buf bytes.Buffer

	hdr := func(k, v string) error {
		if strings.ContainsAny(v, "\r\n") {
			return fmt.Errorf("%w: %s", ErrHeaderInjection, k)
		}

		if v != "" {
			buf.WriteString(k + ": " + v + "\r\n")
		}

		return nil
	}

	date := m.Date
	if date.IsZero() {
		date = time.Now()
	}

	var msgID string
	if m.ID != "" {
		msgID = "<" + m.ID + "@" + domainOf(m.From) + ">"
	}

	pairs := [][2]string{
		{"From", m.From},
		{"To", strings.Join(m.To, ", ")},
		{"Cc", strings.Join(m.Cc, ", ")},
		{"Reply-To", strings.Join(m.ReplyTo, ", ")},
		{"Subject", mime.QEncoding.Encode("utf-8", m.Subject)},
		{"Date", date.Format(time.RFC1123Z)},
		{"Message-ID", msgID},
		{"MIME-Version", "1.0"},
	}

	for _, p := range pairs {
		if err := hdr(p[0], p[1]); err != nil {
			return nil, err
		}
	}

	if err := writeBody(&buf, m.Text, m.HTML); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func writeBody(buf *bytes.Buffer, text, html string) error {
	switch {
	case html == "":
		buf.WriteString(
			"Content-Type: text/plain; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n",
		)

		return writeQP(buf, text)
	case text == "":
		buf.WriteString("Content-Type: text/html; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n")

		return writeQP(buf, html)
	}

	mw := multipart.NewWriter(buf)
	buf.WriteString("Content-Type: multipart/alternative; boundary=" + mw.Boundary() + "\r\n\r\n")

	for _, part := range [][2]string{{"text/plain", text}, {"text/html", html}} {
		w, err := mw.CreatePart(textproto.MIMEHeader{
			"Content-Type":              {part[0] + "; charset=utf-8"},
			"Content-Transfer-Encoding": {"quoted-printable"},
		})
		if err != nil {
			return fmt.Errorf("create part: %w", err)
		}

		if err = writeQP(w, part[1]); err != nil {
			return err
		}
	}

	return mw.Close()
}

func writeQP(w interface{ Write([]byte) (int, error) }, s string) error {
	qp := quotedprintable.NewWriter(w)

	if _, err := qp.Write([]byte(s)); err != nil {
		return fmt.Errorf("encode body: %w", err)
	}

	return qp.Close()
}

func domainOf(addr string) string {
	if a, err := bareAddress(addr); err == nil {
		addr = a
	}

	if at := strings.LastIndex(addr, "@"); at >= 0 {
		return addr[at+1:]
	}

	return "localhost"
}
