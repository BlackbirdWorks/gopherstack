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
	ErrIdentityNotFound  = errors.New("IdentityNotFound")
	ErrEmailNotFound     = errors.New("EmailNotFound")
	ErrInvalidParameter  = errors.New("InvalidParameterValue")
	ErrMessageRejected   = errors.New("MessageRejected")
	ErrTemplateNotFound  = errors.New("TemplateDoesNotExist")
	ErrTemplateExists    = errors.New("AlreadyExists")
	ErrConfigSetNotFound = errors.New("ConfigurationSetDoesNotExist")
	ErrConfigSetExists   = errors.New("ConfigurationSetAlreadyExists")
)

// maxRetainedEmails is the maximum number of sent emails retained in memory.
// Oldest emails are evicted when the limit is exceeded.
const maxRetainedEmails = 10000

// defaultEmailTTL is the default time-to-live for retained emails.
const defaultEmailTTL = 24 * time.Hour

// maxSendQuota24Hours is the simulated 24-hour send quota returned by GetSendQuota.
const maxSendQuota24Hours = 200

// maxSendRate is the simulated max send rate (emails/second) returned by GetSendQuota.
const maxSendRate = 1

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

// EmailTemplate represents a stored SES email template.
type EmailTemplate struct {
	TemplateName string `json:"templateName"`
	SubjectPart  string `json:"subjectPart"`
	TextPart     string `json:"textPart"`
	HTMLPart     string `json:"htmlPart"`
}

// InMemoryBackend is an in-memory store for SES emails, verified identities,
// email templates, and configuration sets.
type InMemoryBackend struct {
	identities map[string]bool
	emailsByID map[string]Email
	templates  map[string]EmailTemplate
	configSets map[string]struct{}
	mu         *lockmetrics.RWMutex
	emails     []Email
	emailTTL   time.Duration
}

// NewInMemoryBackend creates a new InMemoryBackend with the default email TTL.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		identities: make(map[string]bool),
		emailsByID: make(map[string]Email),
		templates:  make(map[string]EmailTemplate),
		configSets: make(map[string]struct{}),
		emailTTL:   defaultEmailTTL,
		mu:         lockmetrics.New("ses"),
	}
}

// Reset clears all in-memory state, restoring the backend to its initial empty state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.identities = make(map[string]bool)
	b.emails = nil
	b.emailsByID = make(map[string]Email)
	b.templates = make(map[string]EmailTemplate)
	b.configSets = make(map[string]struct{})
	b.emailTTL = defaultEmailTTL
}

// ttl returns the current email TTL under a read lock.
func (b *InMemoryBackend) ttl() time.Duration {
	b.mu.RLock("ttl")
	defer b.mu.RUnlock()

	return b.emailTTL
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

// isVerifiedLocked reports whether the sender address is authorised to send.
// It performs an exact-identity match first, then falls back to domain-level
// verification: if example.com is a verified identity, any address @example.com
// is also considered verified — matching real AWS SES behavior.
//
// The caller MUST hold b.mu for reading or writing.
func (b *InMemoryBackend) isVerifiedLocked(from string) bool {
	if b.identities[from] {
		return true
	}

	// Domain-level check: strip the local-part and check the domain.
	if at := strings.LastIndex(from, "@"); at >= 0 {
		return b.identities[from[at+1:]]
	}

	return false
}

// appendEmailLocked appends e to the slice and O(1) map, evicting the oldest
// entries when the cap is exceeded.
//
// The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) appendEmailLocked(e Email) {
	b.emails = append(b.emails, e)
	b.emailsByID[e.MessageID] = e

	if len(b.emails) > maxRetainedEmails {
		evicted := b.emails[:len(b.emails)-maxRetainedEmails]
		for _, ev := range evicted {
			delete(b.emailsByID, ev.MessageID)
		}

		b.emails = b.emails[len(b.emails)-maxRetainedEmails:]
	}
}

// SendEmail captures an outbound email and returns a message ID.
// The source address must be a verified identity or from a verified domain
// (matching real AWS SES behavior).
func (b *InMemoryBackend) SendEmail(from string, to []string, subject, bodyHTML, bodyText string) (string, error) {
	if from == "" {
		return "", fmt.Errorf("%w: Source is required", ErrInvalidParameter)
	}

	b.mu.Lock("SendEmail")
	defer b.mu.Unlock()

	if !b.isVerifiedLocked(from) {
		return "", fmt.Errorf(
			"%w: Email address is not verified. The following identities failed the check in region US-EAST-1: %s",
			ErrMessageRejected, from,
		)
	}

	msgID := "ses-" + uuid.New().String()

	b.appendEmailLocked(Email{
		MessageID: msgID,
		From:      from,
		To:        to,
		Subject:   subject,
		BodyHTML:  bodyHTML,
		BodyText:  bodyText,
		Timestamp: time.Now(),
	})

	return msgID, nil
}

// SendTemplatedEmail sends an email using a stored template and returns the message ID.
// The source address must be a verified identity or from a verified domain.
// The template must already exist; ErrTemplateNotFound is returned otherwise.
func (b *InMemoryBackend) SendTemplatedEmail(from string, to []string, templateName string) (string, error) {
	if from == "" {
		return "", fmt.Errorf("%w: Source is required", ErrInvalidParameter)
	}

	b.mu.Lock("SendTemplatedEmail")
	defer b.mu.Unlock()

	if !b.isVerifiedLocked(from) {
		return "", fmt.Errorf(
			"%w: Email address is not verified. The following identities failed the check in region US-EAST-1: %s",
			ErrMessageRejected, from,
		)
	}

	tmpl, ok := b.templates[templateName]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrTemplateNotFound, templateName)
	}

	msgID := "ses-" + uuid.New().String()

	b.appendEmailLocked(Email{
		MessageID: msgID,
		From:      from,
		To:        to,
		Subject:   tmpl.SubjectPart,
		BodyHTML:  tmpl.HTMLPart,
		BodyText:  tmpl.TextPart,
		Timestamp: time.Now(),
	})

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

// GetEmailByID returns the email with the given MessageID in O(1) time, or an error if not found.
func (b *InMemoryBackend) GetEmailByID(messageID string) (Email, error) {
	b.mu.RLock("GetEmailByID")
	defer b.mu.RUnlock()

	if e, ok := b.emailsByID[messageID]; ok {
		return e, nil
	}

	return Email{}, fmt.Errorf("%w: %s", ErrEmailNotFound, messageID)
}

// sweepExpiredEmails removes emails older than emailTTL. Called by the janitor.
// The caller must NOT hold the lock.
func (b *InMemoryBackend) sweepExpiredEmails(cutoff time.Time) int {
	b.mu.Lock("sweepExpiredEmails")
	defer b.mu.Unlock()

	first := 0

	for first < len(b.emails) && b.emails[first].Timestamp.Before(cutoff) {
		delete(b.emailsByID, b.emails[first].MessageID)
		first++
	}

	if first == 0 {
		return 0
	}

	b.emails = b.emails[first:]

	return first
}

// ---- template operations ----

// CreateTemplate stores a new email template. Returns ErrTemplateExists if the name is taken.
func (b *InMemoryBackend) CreateTemplate(tmpl EmailTemplate) error {
	if strings.TrimSpace(tmpl.TemplateName) == "" {
		return fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateTemplate")
	defer b.mu.Unlock()

	if _, exists := b.templates[tmpl.TemplateName]; exists {
		return fmt.Errorf("%w: template %s already exists", ErrTemplateExists, tmpl.TemplateName)
	}

	b.templates[tmpl.TemplateName] = tmpl

	return nil
}

// UpdateTemplate overwrites an existing template. Returns ErrTemplateNotFound if it does not exist.
func (b *InMemoryBackend) UpdateTemplate(tmpl EmailTemplate) error {
	if strings.TrimSpace(tmpl.TemplateName) == "" {
		return fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	b.mu.Lock("UpdateTemplate")
	defer b.mu.Unlock()

	if _, exists := b.templates[tmpl.TemplateName]; !exists {
		return fmt.Errorf("%w: %s", ErrTemplateNotFound, tmpl.TemplateName)
	}

	b.templates[tmpl.TemplateName] = tmpl

	return nil
}

// GetTemplate returns the named template or ErrTemplateNotFound.
func (b *InMemoryBackend) GetTemplate(name string) (EmailTemplate, error) {
	b.mu.RLock("GetTemplate")
	defer b.mu.RUnlock()

	tmpl, ok := b.templates[name]
	if !ok {
		return EmailTemplate{}, fmt.Errorf("%w: %s", ErrTemplateNotFound, name)
	}

	return tmpl, nil
}

// DeleteTemplate removes the named template. Idempotent — missing template returns success.
func (b *InMemoryBackend) DeleteTemplate(name string) {
	b.mu.Lock("DeleteTemplate")
	defer b.mu.Unlock()

	delete(b.templates, name)
}

// ListTemplates returns template names sorted alphabetically, with pagination.
func (b *InMemoryBackend) ListTemplates(nextToken string, maxItems int) page.Page[string] {
	b.mu.RLock("ListTemplates")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.templates))
	for name := range b.templates {
		names = append(names, name)
	}

	sort.Strings(names)

	return page.New(names, nextToken, maxItems, sesDefaultMaxItems)
}

// ---- configuration set operations ----

// CreateConfigurationSet registers a new configuration set. Returns ErrConfigSetExists if it already exists.
func (b *InMemoryBackend) CreateConfigurationSet(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateConfigurationSet")
	defer b.mu.Unlock()

	if _, exists := b.configSets[name]; exists {
		return fmt.Errorf("%w: configuration set %s already exists", ErrConfigSetExists, name)
	}

	b.configSets[name] = struct{}{}

	return nil
}

// DeleteConfigurationSet removes a configuration set.
// Returns ErrConfigSetNotFound if the set does not exist, matching real AWS SES behavior.
func (b *InMemoryBackend) DeleteConfigurationSet(name string) error {
	b.mu.Lock("DeleteConfigurationSet")
	defer b.mu.Unlock()

	if _, exists := b.configSets[name]; !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, name)
	}

	delete(b.configSets, name)

	return nil
}

// ListConfigurationSets returns configuration set names sorted alphabetically.
func (b *InMemoryBackend) ListConfigurationSets(nextToken string, maxItems int) page.Page[string] {
	b.mu.RLock("ListConfigurationSets")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.configSets))
	for name := range b.configSets {
		names = append(names, name)
	}

	sort.Strings(names)

	return page.New(names, nextToken, maxItems, sesDefaultMaxItems)
}

// ---- send statistics / quota ----

// SendQuota holds the simulated SES sending quota values.
type SendQuota struct {
	Max24HourSend   float64
	MaxSendRate     float64
	SentLast24Hours float64
}

// GetSendQuota returns simulated quota values.
// SentLast24Hours counts only emails sent within the past 24 hours.
func (b *InMemoryBackend) GetSendQuota() SendQuota {
	b.mu.RLock("GetSendQuota")
	defer b.mu.RUnlock()

	cutoff := time.Now().UTC().Add(-24 * time.Hour)
	sent := 0

	for i := len(b.emails) - 1; i >= 0; i-- {
		if b.emails[i].Timestamp.Before(cutoff) {
			break
		}

		sent++
	}

	return SendQuota{
		Max24HourSend:   maxSendQuota24Hours,
		MaxSendRate:     maxSendRate,
		SentLast24Hours: float64(sent),
	}
}

// SendDataPoint represents a single send statistics time bucket.
type SendDataPoint struct {
	Timestamp        time.Time `json:"timestamp"`
	DeliveryAttempts float64   `json:"deliveryAttempts"`
	Bounces          float64   `json:"bounces"`
	Complaints       float64   `json:"complaints"`
	Rejects          float64   `json:"rejects"`
}

// sendStatisticsDays is the number of days of send history returned by GetSendStatistics,
// matching real AWS SES behavior (last 2 weeks / 14 days).
const sendStatisticsDays = 14

// GetSendStatistics returns aggregated send data points (one per hour) for the last 14 days,
// matching real AWS SES behavior.
func (b *InMemoryBackend) GetSendStatistics() []SendDataPoint {
	b.mu.RLock("GetSendStatistics")
	defer b.mu.RUnlock()

	cutoff := time.Now().UTC().Add(-sendStatisticsDays * 24 * time.Hour)

	// Aggregate emails into hourly buckets within the 14-day window.
	buckets := make(map[time.Time]float64)

	for _, e := range b.emails {
		if e.Timestamp.Before(cutoff) {
			continue
		}

		hour := e.Timestamp.UTC().Truncate(time.Hour)
		buckets[hour]++
	}

	result := make([]SendDataPoint, 0, len(buckets))

	for ts, count := range buckets {
		result = append(result, SendDataPoint{
			Timestamp:        ts,
			DeliveryAttempts: count,
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Timestamp.Before(result[j].Timestamp)
	})

	return result
}
