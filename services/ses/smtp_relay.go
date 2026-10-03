package ses

import (
	"os"

	"github.com/blackbirdworks/gopherstack/pkgs/smtprelay"
)

// WithSMTPRelay enables best-effort real delivery of accepted mail via r and returns the backend.
func (b *InMemoryBackend) WithSMTPRelay(r *smtprelay.Relay) *InMemoryBackend {
	b.relay = r

	return b
}

// WithSMTPRelay attaches r to the backend (when it is the in-memory one) and closes it on Shutdown.
func (h *Handler) WithSMTPRelay(r *smtprelay.Relay) *Handler {
	h.relay = r

	if ib, ok := h.Backend.(*InMemoryBackend); ok {
		ib.WithSMTPRelay(r)
	}

	return h
}

// relayFromEnv builds a relay from SMTP_HOST/SMTP_USER/SMTP_PASS, or returns nil when SMTP_HOST is unset.
func relayFromEnv(getenv func(string) string) *smtprelay.Relay {
	if getenv == nil {
		getenv = os.Getenv
	}

	cfg, ok := smtprelay.ConfigFromEnv(getenv)
	if !ok {
		return nil
	}

	return smtprelay.New(cfg)
}

// relayEmail queues e for real delivery; mailbox-simulator recipients are never relayed.
func (b *InMemoryBackend) relayEmail(e Email, raw []byte) {
	if b.relay == nil {
		return
	}

	m := smtprelay.Message{
		ID:           e.MessageID,
		From:         e.From,
		EnvelopeFrom: e.ReturnPath,
		To:           realRecipients(e.To),
		Cc:           realRecipients(e.Cc),
		Bcc:          realRecipients(e.Bcc),
		ReplyTo:      e.ReplyTo,
		Subject:      e.Subject,
		Text:         e.BodyText,
		HTML:         e.BodyHTML,
		Date:         e.Timestamp,
		Raw:          raw,
	}

	if len(raw) == 0 {
		m.Raw = nil
	}

	if len(m.To)+len(m.Cc)+len(m.Bcc) == 0 {
		return
	}

	b.relay.Enqueue(m)
}

func realRecipients(addrs []string) []string {
	var out []string

	for _, a := range addrs {
		if !isSimulatorAddress(a) {
			out = append(out, a)
		}
	}

	return out
}
