package sesv2

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// maxRetainedEmails is the maximum number of sent emails retained in memory.
// When the cap is exceeded the oldest entries are dropped FIFO so a long-running
// instance cannot leak memory through repeated SendEmail calls.
const maxRetainedEmails = 10000

// emailCompactionHighWater is the slice length that triggers compaction.
// Compacting only when the slice has grown to twice the cap keeps
// trimming amortized O(1) per SendEmail.
const emailCompactionHighWater = maxRetainedEmails + maxRetainedEmails

// Email captures a sent email for local inspection.
type Email struct {
	Timestamp time.Time `json:"timestamp"`
	From      string    `json:"from"`
	Subject   string    `json:"subject"`
	BodyHTML  string    `json:"bodyHTML"`
	BodyText  string    `json:"bodyText"`
	MessageID string    `json:"messageID"`
	To        []string  `json:"to"`
}

// SendEmail captures an outbound email and returns a message ID.
func (b *InMemoryBackend) SendEmail(
	from string,
	to []string,
	subject, bodyHTML, bodyText string,
) (string, error) {
	if from == "" {
		return "", fmt.Errorf("%w: FromEmailAddress is required", ErrInvalidInput)
	}

	msgID := "sesv2-" + uuid.New().String()

	email := Email{
		MessageID: msgID,
		From:      from,
		To:        to,
		Subject:   subject,
		BodyHTML:  bodyHTML,
		BodyText:  bodyText,
		Timestamp: time.Now(),
	}

	b.mu.Lock("SendEmail")
	defer b.mu.Unlock()

	if err := b.checkFromIdentityLocked(from); err != nil {
		return "", err
	}
	b.emails = append(b.emails, email)
	// Compact only when the slice has grown to twice the cap so trimming is
	// amortized O(1) per send rather than O(maxRetainedEmails) on every send
	// past the cap. The dropped prefix becomes unreachable once the slice
	// header advances and is collected on the next reslice/grow.
	if len(b.emails) >= emailCompactionHighWater {
		// Reslice into a fresh backing array so the dropped tail can be GC'd
		// immediately rather than held by the original (now larger) backing
		// array.
		trimmed := make([]Email, maxRetainedEmails, emailCompactionHighWater)
		copy(trimmed, b.emails[len(b.emails)-maxRetainedEmails:])
		b.emails = trimmed
	}

	return msgID, nil
}

// checkFromIdentityLocked verifies the from address against registered identities.
// It checks exact email match first, then the domain portion as a fallback.
// Must be called with b.mu held for writing or reading.
func (b *InMemoryBackend) checkFromIdentityLocked(from string) error {
	if id, ok := b.identities.Get(from); ok && id.VerifiedForSending {
		return nil
	}
	if at := strings.LastIndex(from, "@"); at >= 0 {
		domain := from[at+1:]
		if id, ok := b.identities.Get(domain); ok && id.VerifiedForSending {
			return nil
		}
	}

	return fmt.Errorf("%w: identity not verified for sending: %s", ErrInvalidInput, from)
}

// ListEmails returns a copy of all captured emails.
func (b *InMemoryBackend) ListEmails() []Email {
	b.mu.RLock("ListEmails")
	defer b.mu.RUnlock()

	out := make([]Email, len(b.emails))
	copy(out, b.emails)

	return out
}

// ---- bulk email ----
//
// Field-diffed against aws-sdk-go-v2/service/sesv2/types' BulkEmailEntry/
// Destination/ReplacementEmailContent/ReplacementTemplate/MessageHeader/
// MessageTag. bulkEmailDestination/messageHeader/messageTag/
// replacementTemplate/replacementEmailContent/bulkEmailEntry replace what
// was previously a []map[string]any parsed with ad-hoc type assertions --
// functionally equivalent but with compile-time field-name safety.

// bulkEmailDestination mirrors types.Destination.
type bulkEmailDestination struct {
	ToAddresses  []string `json:"ToAddresses"`
	CcAddresses  []string `json:"CcAddresses"`
	BccAddresses []string `json:"BccAddresses"`
}

// messageHeader mirrors types.MessageHeader.
type messageHeader struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// messageTag mirrors types.MessageTag.
type messageTag struct {
	Name  string `json:"Name"`
	Value string `json:"Value"`
}

// replacementTemplate mirrors types.ReplacementTemplate.
type replacementTemplate struct {
	ReplacementTemplateData string `json:"ReplacementTemplateData"`
}

// replacementEmailContent mirrors types.ReplacementEmailContent.
type replacementEmailContent struct {
	ReplacementTemplate *replacementTemplate `json:"ReplacementTemplate"`
}

// bulkEmailEntry mirrors types.BulkEmailEntry.
type bulkEmailEntry struct {
	Destination             bulkEmailDestination     `json:"Destination"`
	ReplacementEmailContent *replacementEmailContent `json:"ReplacementEmailContent"`
	ReplacementHeaders      []messageHeader          `json:"ReplacementHeaders"`
	ReplacementTags         []messageTag             `json:"ReplacementTags"`
}

// SendBulkEmail sends bulk emails — records sent emails with actual recipients.
func (b *InMemoryBackend) SendBulkEmail(
	fromEmailAddress string,
	bulkEmailEntries []bulkEmailEntry,
) ([]bulkEmailEntryResultOutput, error) {
	results := make([]bulkEmailEntryResultOutput, 0, len(bulkEmailEntries))

	for _, entry := range bulkEmailEntries {
		msgID, _ := b.SendEmail(fromEmailAddress, entry.Destination.ToAddresses, "", "", "")
		if msgID == "" {
			msgID = "sesv2-bulk-" + uuid.New().String()
		}

		results = append(results, bulkEmailEntryResultOutput{
			MessageID: msgID,
			Status:    keyStatusSuccess,
		})
	}

	return results, nil
}
