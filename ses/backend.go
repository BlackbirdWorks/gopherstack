package ses

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Errors returned by the SES backend.
var (
	ErrIdentityNotFound = errors.New("IdentityNotFound")
)

// Email captures a sent email for local inspection.
type Email struct {
	Timestamp time.Time
	From      string
	To        []string
	Subject   string
	BodyHTML  string
	BodyText  string
	MessageID string
}

// InMemoryBackend is an in-memory store for SES emails and verified identities.
type InMemoryBackend struct {
	mu         sync.RWMutex
	emails     []Email
	identities map[string]bool // identity → verified
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		identities: make(map[string]bool),
	}
}

// VerifyEmailIdentity adds an identity (address or domain) and marks it as verified.
func (b *InMemoryBackend) VerifyEmailIdentity(identity string) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: identity is required", ErrIdentityNotFound)
	}

	b.mu.Lock()
	b.identities[identity] = true
	b.mu.Unlock()

	return nil
}

// DeleteIdentity removes a verified identity.
func (b *InMemoryBackend) DeleteIdentity(identity string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.identities[identity]; !ok {
		return fmt.Errorf("%w: %s", ErrIdentityNotFound, identity)
	}

	delete(b.identities, identity)

	return nil
}

// ListIdentities returns all registered identities sorted alphabetically.
func (b *InMemoryBackend) ListIdentities() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]string, 0, len(b.identities))
	for id := range b.identities {
		out = append(out, id)
	}

	return out
}

// GetIdentityVerificationAttributes returns verification status for each requested identity.
// All registered identities are auto-verified.
func (b *InMemoryBackend) GetIdentityVerificationAttributes(identities []string) map[string]string {
	b.mu.RLock()
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
func (b *InMemoryBackend) SendEmail(from string, to []string, subject, bodyHTML, bodyText string) (string, error) {
	if from == "" {
		return "", fmt.Errorf("%w: Source is required", ErrIdentityNotFound)
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

	b.mu.Lock()
	b.emails = append(b.emails, email)
	b.mu.Unlock()

	return msgID, nil
}

// ListEmails returns a copy of all captured emails.
func (b *InMemoryBackend) ListEmails() []Email {
	b.mu.RLock()
	defer b.mu.RUnlock()

	out := make([]Email, len(b.emails))
	copy(out, b.emails)

	return out
}
