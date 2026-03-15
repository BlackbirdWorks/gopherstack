package ses

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// Errors returned by the SES backend.
var (
	ErrIdentityNotFound = errors.New("IdentityNotFound")
	ErrEmailNotFound    = errors.New("EmailNotFound")
	ErrInvalidParameter = errors.New("InvalidParameterValue")
	ErrMessageRejected  = errors.New("MessageRejected")
)

// maxRetainedEmails is the maximum number of sent emails retained in memory.
// Oldest emails are evicted when the limit is exceeded.
const maxRetainedEmails = 10000

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

// InMemoryBackend is an in-memory store for SES emails and verified identities.
type InMemoryBackend struct {
	identities map[string]bool
	mu         *lockmetrics.RWMutex
	emails     []Email
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		identities: make(map[string]bool),
		mu:         lockmetrics.New("ses"),
	}
}

// VerifyEmailIdentity adds an identity (address or domain) and marks it as verified.
func (b *InMemoryBackend) VerifyEmailIdentity(identity string) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: identity is required", ErrInvalidParameter)
	}

	b.mu.Lock("VerifyEmailIdentity")
	defer b.mu.Unlock()

	b.identities[identity] = true

	return nil
}

// DeleteIdentity removes a verified identity.
// This is idempotent — deleting a non-existent identity returns success,
// matching real AWS SES behavior.
func (b *InMemoryBackend) DeleteIdentity(identity string) {
	b.mu.Lock("DeleteIdentity")
	defer b.mu.Unlock()

	delete(b.identities, identity)
}

const sesDefaultMaxItems = 100

// ListIdentities returns a paginated list of registered identities sorted alphabetically.
func (b *InMemoryBackend) ListIdentities(nextToken string, maxItems int) page.Page[string] {
	b.mu.RLock("ListIdentities")
	defer b.mu.RUnlock()

	out := make([]string, 0, len(b.identities))
	for id := range b.identities {
		out = append(out, id)
	}

	sort.Strings(out)

	return page.New(out, nextToken, maxItems, sesDefaultMaxItems)
}

// GetIdentityVerificationAttributes returns verification status for each requested identity.
// All registered identities are auto-verified.
func (b *InMemoryBackend) GetIdentityVerificationAttributes(identities []string) map[string]string {
	b.mu.RLock("GetIdentityVerificationAttributes")
	defer b.mu.RUnlock()

	result := make(map[string]string, len(identities))

	for _, id := range identities {
		if _, ok := b.identities[id]; ok {
			result[id] = "Success"
		} else {
			result[id] = "NotStarted"
		}
	}

	return result
}

// SendEmail captures an outbound email and returns a message ID.
// The source address must be a verified identity (matching real AWS SES behavior).
func (b *InMemoryBackend) SendEmail(from string, to []string, subject, bodyHTML, bodyText string) (string, error) {
	if from == "" {
		return "", fmt.Errorf("%w: Source is required", ErrInvalidParameter)
	}

	b.mu.Lock("SendEmail")
	defer b.mu.Unlock()

	if !b.identities[from] {
		return "", fmt.Errorf(
			"%w: Email address is not verified. The following identities failed the check in region US-EAST-1: %s",
			ErrMessageRejected, from,
		)
	}

	msgID := "ses-" + uuid.New().String()

	email := Email{
		MessageID: msgID,
		From:      from,
		To:        to,
		Subject:   subject,
		BodyHTML:  bodyHTML,
		BodyText:  bodyText,
		Timestamp: time.Now(),
	}

	b.emails = append(b.emails, email)

	if len(b.emails) > maxRetainedEmails {
		b.emails = b.emails[len(b.emails)-maxRetainedEmails:]
	}

	return msgID, nil
}

// ListEmails returns a copy of all captured emails.
func (b *InMemoryBackend) ListEmails() []Email {
	b.mu.RLock("ListEmails")
	defer b.mu.RUnlock()

	out := make([]Email, len(b.emails))
	copy(out, b.emails)

	return out
}

// GetEmailByID returns the email with the given MessageID, or an error if not found.
func (b *InMemoryBackend) GetEmailByID(messageID string) (Email, error) {
	b.mu.RLock("GetEmailByID")
	defer b.mu.RUnlock()

	for _, e := range b.emails {
		if e.MessageID == messageID {
			return e, nil
		}
	}

	return Email{}, fmt.Errorf("%w: %s", ErrEmailNotFound, messageID)
}
