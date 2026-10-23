package ses

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const dsnSubject = "Delivery Status Notification (Failure)"

// buildDSN renders an RFC 3464 multipart/report for a bounced inbound message.
func buildDSN(r *inboundRun, a ReceiptAction, code int, boundary string) (string, string) {
	status := a.StatusCode
	if status == "" {
		status = "5.0.0"
	}

	diag := fmt.Sprintf("smtp; %d %s", code, a.Message)
	human := fmt.Sprintf("Your message could not be delivered to %s.\r\n%s\r\n",
		strings.Join(r.matched, ", "), a.Message)

	var sb strings.Builder

	fmt.Fprintf(&sb, "From: %s\r\nTo: %s\r\nSubject: %s\r\nDate: %s\r\nMIME-Version: 1.0\r\n",
		a.Sender, r.env.from, dsnSubject, time.Now().UTC().Format(time.RFC1123Z))
	fmt.Fprintf(&sb, "Content-Type: multipart/report; report-type=delivery-status; boundary=%q\r\n\r\n", boundary)
	fmt.Fprintf(&sb, "--%s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s\r\n", boundary, human)
	fmt.Fprintf(&sb, "--%s\r\nContent-Type: message/delivery-status\r\n\r\n", boundary)
	fmt.Fprintf(&sb, "Reporting-MTA: dns; inbound-smtp.%s.amazonaws.com\r\nArrival-Date: %s\r\n",
		r.plan.region, r.start.UTC().Format(time.RFC1123Z))

	for _, rcpt := range r.matched {
		fmt.Fprintf(&sb, "\r\nFinal-Recipient: rfc822; %s\r\nAction: failed\r\nStatus: %s\r\nDiagnostic-Code: %s\r\n",
			rcpt, status, diag)
	}

	fmt.Fprintf(&sb, "\r\n--%s\r\nContent-Type: text/rfc822-headers\r\n\r\n%s\r\n--%s--\r\n",
		boundary, headerBlock(r.env.data), boundary)

	return sb.String(), human
}

func headerBlock(data []byte) string {
	head, _, _ := strings.Cut(string(data), "\r\n\r\n")

	return head
}

// sendDSN records the bounce notification as a sent message and relays it when an SMTP relay is configured.
func (b *InMemoryBackend) sendDSN(r *inboundRun, a ReceiptAction, code int) {
	if r.env.from == "" || a.Sender == "" {
		return
	}

	raw, human := buildDSN(r, a, code, "dsn-"+uuid.NewString())

	b.mu.Lock("sendDSN")
	e := Email{
		MessageID: "ses-dsn-" + uuid.NewString(),
		From:      a.Sender,
		To:        []string{r.env.from},
		Subject:   dsnSubject,
		BodyText:  human + "\r\n" + raw,
		Timestamp: time.Now().UTC(),
	}
	b.appendEmailLocked(e)
	b.mu.Unlock()

	b.relayEmail(e, []byte(raw))
}
