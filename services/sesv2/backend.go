package sesv2

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// Errors returned by the SES v2 backend.
var (
	ErrIdentityNotFound       = errors.New("NotFoundException")
	ErrIdentityAlreadyExists  = errors.New("AlreadyExistsException")
	ErrConfigSetNotFound      = errors.New("NotFoundException")
	ErrConfigSetAlreadyExists = errors.New("AlreadyExistsException")
	ErrInvalidParameter       = errors.New("BadRequestException")
)

// EmailIdentity represents a verified email address or domain identity.
type EmailIdentity struct {
	Identity           string `json:"identity"`
	IdentityType       string `json:"identityType"` // "EMAIL_ADDRESS" or "DOMAIN"
	VerifiedForSending bool   `json:"verifiedForSending"`
}

// ConfigurationSet represents a SES v2 configuration set.
type ConfigurationSet struct {
	CreatedAt time.Time `json:"createdAt"`
	Name      string    `json:"name"`
}

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

// InMemoryBackend is an in-memory store for SES v2 email identities, emails, and configuration sets.
type InMemoryBackend struct {
	identities        map[string]*EmailIdentity
	configurationSets map[string]*ConfigurationSet
	mu                *lockmetrics.RWMutex
	region            string
	accountID         string
	emails            []Email
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		identities:        make(map[string]*EmailIdentity),
		configurationSets: make(map[string]*ConfigurationSet),
		mu:                lockmetrics.New("sesv2"),
		region:            config.DefaultRegion,
		accountID:         config.DefaultAccountID,
	}
}

// NewInMemoryBackendWithConfig creates a new InMemoryBackend with the given config.
func NewInMemoryBackendWithConfig(cfg config.GlobalConfig) *InMemoryBackend {
	b := NewInMemoryBackend()
	if cfg.Region != "" {
		b.region = cfg.Region
	}

	if cfg.AccountID != "" {
		b.accountID = cfg.AccountID
	}

	return b
}

// Region returns the backend's AWS region.
func (b *InMemoryBackend) Region() string { return b.region }

// identityType determines the identity type from the identity string.
func identityType(identity string) string {
	if strings.Contains(identity, "@") {
		return "EMAIL_ADDRESS"
	}

	return "DOMAIN"
}

// CreateEmailIdentity creates a new email identity and marks it as verified.
func (b *InMemoryBackend) CreateEmailIdentity(identity string) (*EmailIdentity, error) {
	if strings.TrimSpace(identity) == "" {
		return nil, fmt.Errorf("%w: EmailIdentity is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateEmailIdentity")
	defer b.mu.Unlock()

	if _, exists := b.identities[identity]; exists {
		return nil, fmt.Errorf("%w: identity %s already exists", ErrIdentityAlreadyExists, identity)
	}

	ei := &EmailIdentity{
		Identity:           identity,
		IdentityType:       identityType(identity),
		VerifiedForSending: true,
	}
	b.identities[identity] = ei

	return ei, nil
}

// GetEmailIdentity returns the email identity with the given name.
func (b *InMemoryBackend) GetEmailIdentity(identity string) (*EmailIdentity, error) {
	b.mu.RLock("GetEmailIdentity")
	defer b.mu.RUnlock()

	ei, ok := b.identities[identity]
	if !ok {
		return nil, fmt.Errorf("%w: identity %s not found", ErrIdentityNotFound, identity)
	}

	return ei, nil
}

const sesv2DefaultMaxItems = 100

// ListEmailIdentities returns a paginated, sorted list of email identities.
func (b *InMemoryBackend) ListEmailIdentities(nextToken string, pageSize int) page.Page[*EmailIdentity] {
	b.mu.RLock("ListEmailIdentities")
	defer b.mu.RUnlock()

	out := make([]*EmailIdentity, 0, len(b.identities))
	for _, ei := range b.identities {
		out = append(out, ei)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Identity < out[j].Identity
	})

	return page.New(out, nextToken, pageSize, sesv2DefaultMaxItems)
}

// DeleteEmailIdentity removes an email identity.
func (b *InMemoryBackend) DeleteEmailIdentity(identity string) error {
	b.mu.Lock("DeleteEmailIdentity")
	defer b.mu.Unlock()

	if _, ok := b.identities[identity]; !ok {
		return fmt.Errorf("%w: identity %s not found", ErrIdentityNotFound, identity)
	}

	delete(b.identities, identity)

	return nil
}

// CreateConfigurationSet creates a new configuration set.
func (b *InMemoryBackend) CreateConfigurationSet(name string) (*ConfigurationSet, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateConfigurationSet")
	defer b.mu.Unlock()

	if _, exists := b.configurationSets[name]; exists {
		return nil, fmt.Errorf("%w: configuration set %s already exists", ErrConfigSetAlreadyExists, name)
	}

	cs := &ConfigurationSet{
		Name:      name,
		CreatedAt: time.Now(),
	}
	b.configurationSets[name] = cs

	return cs, nil
}

// GetConfigurationSet returns the configuration set with the given name.
func (b *InMemoryBackend) GetConfigurationSet(name string) (*ConfigurationSet, error) {
	b.mu.RLock("GetConfigurationSet")
	defer b.mu.RUnlock()

	cs, ok := b.configurationSets[name]
	if !ok {
		return nil, fmt.Errorf("%w: configuration set %s not found", ErrConfigSetNotFound, name)
	}

	return cs, nil
}

// ListConfigurationSets returns a paginated, sorted list of configuration sets.
func (b *InMemoryBackend) ListConfigurationSets(nextToken string, pageSize int) page.Page[*ConfigurationSet] {
	b.mu.RLock("ListConfigurationSets")
	defer b.mu.RUnlock()

	out := make([]*ConfigurationSet, 0, len(b.configurationSets))
	for _, cs := range b.configurationSets {
		out = append(out, cs)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})

	return page.New(out, nextToken, pageSize, sesv2DefaultMaxItems)
}

// DeleteConfigurationSet removes a configuration set.
func (b *InMemoryBackend) DeleteConfigurationSet(name string) error {
	b.mu.Lock("DeleteConfigurationSet")
	defer b.mu.Unlock()

	if _, ok := b.configurationSets[name]; !ok {
		return fmt.Errorf("%w: configuration set %s not found", ErrConfigSetNotFound, name)
	}

	delete(b.configurationSets, name)

	return nil
}

// SendEmail captures an outbound email and returns a message ID.
func (b *InMemoryBackend) SendEmail(from string, to []string, subject, bodyHTML, bodyText string) (string, error) {
	if from == "" {
		return "", fmt.Errorf("%w: FromEmailAddress is required", ErrInvalidParameter)
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
	b.emails = append(b.emails, email)
	b.mu.Unlock()

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
