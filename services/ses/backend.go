package ses

import (
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// Tag is an email metadata key-value pair.
type Tag struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// IdentityRecord stores per-identity verification and attribute state.
type IdentityRecord struct {
	DeliveryTopic      string   `json:"deliveryTopic,omitempty"`
	MailFromDomain     string   `json:"mailFromDomain,omitempty"`
	MailFromStatus     string   `json:"mailFromStatus,omitempty"`
	BehaviorOnMXFail   string   `json:"behaviorOnMXFailure,omitempty"`
	BounceTopic        string   `json:"bounceTopic,omitempty"`
	ComplaintTopic     string   `json:"complaintTopic,omitempty"`
	DkimTokens         []string `json:"dkimTokens,omitempty"`
	DkimEnabled        bool     `json:"dkimEnabled"`
	ForwardingEnabled  bool     `json:"forwardingEnabled"`
	HeadersInBounce    bool     `json:"headersInBounce"`
	HeadersInComplaint bool     `json:"headersInComplaint"`
	HeadersInDelivery  bool     `json:"headersInDelivery"`
	Verified           bool     `json:"verified"`
}

// ConfigurationSet stores per-configuration-set state.
type ConfigurationSet struct {
	TLSPolicy         string `json:"tlsPolicy,omitempty"`
	SendingEnabled    bool   `json:"sendingEnabled"`
	ReputationMetrics bool   `json:"reputationMetrics"`
}

// BulkEmailDestination is a single destination entry for SendBulkTemplatedEmail.
type BulkEmailDestination struct {
	ReplacementTemplateData string
	To                      []string
	Cc                      []string
	Bcc                     []string
}

// SendEmailInput contains all parameters for sending an email.
type SendEmailInput struct {
	Tags                 []Tag
	From                 string
	Subject              string
	BodyHTML             string
	BodyText             string
	ConfigurationSetName string
	ReturnPath           string
	SourceArn            string
	To                   []string
	Cc                   []string
	Bcc                  []string
	ReplyTo              []string
}

// SendTemplatedEmailInput contains all parameters for sending a templated email.
type SendTemplatedEmailInput struct {
	Tags                 []Tag
	From                 string
	TemplateName         string
	ConfigurationSetName string
	ReturnPath           string
	SourceArn            string
	To                   []string
	Cc                   []string
	Bcc                  []string
	ReplyTo              []string
}

// Errors returned by the SES backend.
var (
	ErrIdentityNotFound            = errors.New("IdentityNotFound")
	ErrEmailNotFound               = errors.New("EmailNotFound")
	ErrInvalidParameter            = errors.New("InvalidParameterValue")
	ErrMessageRejected             = errors.New("MessageRejected")
	ErrTemplateNotFound            = errors.New("TemplateDoesNotExist")
	ErrTemplateExists              = errors.New("AlreadyExists")
	ErrConfigSetNotFound           = errors.New("ConfigurationSetDoesNotExist")
	ErrConfigSetExists             = errors.New("ConfigurationSetAlreadyExists")
	ErrReceiptRuleSetNotFound      = errors.New("RuleSetDoesNotExist")
	ErrReceiptRuleSetExists        = errors.New("AlreadyExists")
	ErrReceiptRuleNotFound         = errors.New("RuleDoesNotExist")
	ErrReceiptRuleExists           = errors.New("AlreadyExists")
	ErrReceiptFilterNotFound       = errors.New("FilterDoesNotExist")
	ErrReceiptFilterExists         = errors.New("AlreadyExists")
	ErrEventDestinationNotFound    = errors.New("EventDestinationDoesNotExist")
	ErrEventDestinationExists      = errors.New("EventDestinationAlreadyExists")
	ErrTrackingOptionsNotFound     = errors.New("TrackingOptionsDoesNotExist")
	ErrTrackingOptionsExists       = errors.New("TrackingOptionsAlreadyExists")
	ErrCustomVerifTemplateNotFound = errors.New("CustomVerificationEmailTemplateDoesNotExist")
	ErrCustomVerifTemplateExists   = errors.New("CustomVerificationEmailTemplateAlreadyExists")
	ErrValidation                  = errors.New("ValidationError")
)

// maxRetainedEmails is the maximum number of sent emails retained in memory.
// Oldest emails are evicted when the limit is exceeded.
const maxRetainedEmails = 10000

// defaultEmailTTL is the default time-to-live for retained emails.
const defaultEmailTTL = 24 * time.Hour

const (
	FilterPolicyAllow   = "Allow"
	FilterPolicyBlock   = "Block"
	TLSPolicyOptional   = "Optional"
	TLSPolicyRequire    = "Require"
	ruleSetStatusActive = "Active"

	// identityStatusSuccess is the verification status for verified identities.
	identityStatusSuccess = "Success"
	// identityStatusNotStarted is the default status for unverified identities.
	identityStatusNotStarted = "NotStarted"
	// boolTrue / boolFalse are the string literals used in AWS form-encoded params.
	boolTrue  = "true"
	boolFalse = "false"
)

const defaultAccountID = "123456789012"

// maxSendQuota24Hours is the simulated 24-hour send quota returned by GetSendQuota.
const maxSendQuota24Hours = 200

// maxSendRate is the simulated max send rate (emails/second) returned by GetSendQuota.
const maxSendRate = 1

// Email captures a sent email for local inspection.
type Email struct {
	Tags                 []Tag     `json:"tags,omitempty"`
	Timestamp            time.Time `json:"timestamp"`
	From                 string    `json:"from"`
	Subject              string    `json:"subject"`
	BodyHTML             string    `json:"bodyHTML"`
	BodyText             string    `json:"bodyText"`
	MessageID            string    `json:"messageID"`
	ConfigurationSetName string    `json:"configurationSetName,omitempty"`
	ReturnPath           string    `json:"returnPath,omitempty"`
	SourceArn            string    `json:"sourceArn,omitempty"`
	To                   []string  `json:"to"`
	Cc                   []string  `json:"cc,omitempty"`
	Bcc                  []string  `json:"bcc,omitempty"`
	ReplyTo              []string  `json:"replyTo,omitempty"`
}

// EmailTemplate represents a stored SES email template.
type EmailTemplate struct {
	TemplateName string `json:"templateName"`
	SubjectPart  string `json:"subjectPart"`
	TextPart     string `json:"textPart"`
	HTMLPart     string `json:"htmlPart"`
}

// ReceiptRuleSet represents an SES receipt rule set.
type ReceiptRuleSet struct {
	Name      string        `json:"name"`
	CreatedAt time.Time     `json:"createdAt"`
	Rules     []ReceiptRule `json:"rules"`
}

const (
	ReceiptActionTypeS3        = "S3"
	ReceiptActionTypeSNS       = "SNS"
	ReceiptActionTypeLambda    = "Lambda"
	ReceiptActionTypeSQS       = "SQS"
	ReceiptActionTypeAddHeader = "AddHeader"
	ReceiptActionTypeBounce    = "Bounce"
	ReceiptActionTypeStop      = "Stop"
)

// ReceiptAction is a single action within a receipt rule.
// Type identifies which action fields apply: S3, SNS, Lambda, SQS, AddHeader, Bounce, Stop.
type ReceiptAction struct {
	Type              string `json:"type"`
	S3BucketName      string `json:"s3BucketName,omitempty"`
	S3KeyPrefix       string `json:"s3KeyPrefix,omitempty"`
	S3TopicARN        string `json:"s3TopicARN,omitempty"`
	SNSTopicARN       string `json:"snsTopicARN,omitempty"`
	LambdaFunctionARN string `json:"lambdaFunctionARN,omitempty"`
	LambdaTopicARN    string `json:"lambdaTopicARN,omitempty"`
	SQSQueueARN       string `json:"sqsQueueARN,omitempty"`
	SQSTopicARN       string `json:"sqsTopicARN,omitempty"`
	HeaderName        string `json:"headerName,omitempty"`
	HeaderValue       string `json:"headerValue,omitempty"`
	SMTPReplyCode     string `json:"smtpReplyCode,omitempty"`
	StatusCode        string `json:"statusCode,omitempty"`
	Message           string `json:"message,omitempty"`
	Sender            string `json:"sender,omitempty"`
	BounceTopicARN    string `json:"bounceTopicARN,omitempty"`
}

// ReceiptRule represents a single receipt rule within a rule set.
type ReceiptRule struct {
	Name        string          `json:"name"`
	TLSPolicy   string          `json:"tlsPolicy"`
	Recipients  []string        `json:"recipients"`
	Actions     []ReceiptAction `json:"actions,omitempty"`
	Enabled     bool            `json:"enabled"`
	ScanEnabled bool            `json:"scanEnabled"`
}

// ReceiptFilter represents an IP-based receipt filter.
type ReceiptFilter struct {
	Name   string `json:"name"`
	Policy string `json:"policy"`
	CIDR   string `json:"cidr"`
}

// EventDestination represents a configuration set event destination.
type EventDestination struct {
	Name               string   `json:"name"`
	SNSTopicARN        string   `json:"snsTopicARN,omitempty"`
	MatchingEventTypes []string `json:"matchingEventTypes"`
	Enabled            bool     `json:"enabled"`
}

// TrackingOptions represents the tracking options for a configuration set.
type TrackingOptions struct {
	CustomRedirectDomain string `json:"customRedirectDomain"`
}

// CustomVerificationEmailTemplate represents a custom verification email template.
type CustomVerificationEmailTemplate struct {
	TemplateName          string `json:"templateName"`
	FromEmailAddress      string `json:"fromEmailAddress"`
	TemplateSubject       string `json:"templateSubject"`
	TemplateContent       string `json:"templateContent"`
	SuccessRedirectionURL string `json:"successRedirectionURL"`
	FailureRedirectionURL string `json:"failureRedirectionURL"`
}

// InMemoryBackend is an in-memory store for SES emails, verified identities,
// email templates, and configuration sets.
type InMemoryBackend struct {
	identities             map[string]*IdentityRecord
	emailsByID             map[string]Email
	templates              map[string]EmailTemplate
	configSets             map[string]*ConfigurationSet
	receiptRuleSets        map[string]*ReceiptRuleSet
	receiptFilters         map[string]*ReceiptFilter
	eventDestinations      map[string]map[string]*EventDestination
	trackingOptions        map[string]*TrackingOptions
	customVerifTemplates   map[string]*CustomVerificationEmailTemplate
	policies               map[string]map[string]string // identity → policyName → policyDocument
	activeRuleSet          string
	mu                     *lockmetrics.RWMutex
	emails                 []Email
	emailTTL               time.Duration
	configuredEmailTTL     time.Duration
	accountSendingEnabled  bool
}

// NewInMemoryBackend creates a new InMemoryBackend with the default email TTL.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		identities:            make(map[string]*IdentityRecord),
		emailsByID:            make(map[string]Email),
		templates:             make(map[string]EmailTemplate),
		configSets:            make(map[string]*ConfigurationSet),
		receiptRuleSets:       make(map[string]*ReceiptRuleSet),
		receiptFilters:        make(map[string]*ReceiptFilter),
		eventDestinations:     make(map[string]map[string]*EventDestination),
		trackingOptions:       make(map[string]*TrackingOptions),
		customVerifTemplates:  make(map[string]*CustomVerificationEmailTemplate),
		policies:              make(map[string]map[string]string),
		emailTTL:              defaultEmailTTL,
		configuredEmailTTL:    defaultEmailTTL,
		accountSendingEnabled: true,
		mu:                    lockmetrics.New("ses"),
	}
}

// WithEmailTTL sets the TTL for stored sent emails and returns the backend for chaining.
// Zero falls back to the default TTL.
// The configured TTL is preserved across Reset() calls.
func (b *InMemoryBackend) WithEmailTTL(ttl time.Duration) *InMemoryBackend {
	if ttl > 0 {
		b.emailTTL = ttl
		b.configuredEmailTTL = ttl
	}

	return b
}

// Reset clears all in-memory state, restoring the backend to its initial empty state.
// The configured email TTL (set via WithEmailTTL) is preserved.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.identities = make(map[string]*IdentityRecord)
	b.emails = nil
	b.emailsByID = make(map[string]Email)
	b.templates = make(map[string]EmailTemplate)
	b.configSets = make(map[string]*ConfigurationSet)
	b.receiptRuleSets = make(map[string]*ReceiptRuleSet)
	b.receiptFilters = make(map[string]*ReceiptFilter)
	b.eventDestinations = make(map[string]map[string]*EventDestination)
	b.trackingOptions = make(map[string]*TrackingOptions)
	b.customVerifTemplates = make(map[string]*CustomVerificationEmailTemplate)
	b.policies = make(map[string]map[string]string)
	b.activeRuleSet = ""
	b.accountSendingEnabled = true
	b.emailTTL = b.configuredEmailTTL
}

// ttl returns the current email TTL under a read lock.
func (b *InMemoryBackend) ttl() time.Duration {
	b.mu.RLock("ttl")
	defer b.mu.RUnlock()

	return b.emailTTL
}

// VerifyEmailIdentity adds an identity (address or domain) and marks it as verified.
// If the identity already exists its verified flag is updated without clearing other attributes.
func (b *InMemoryBackend) VerifyEmailIdentity(identity string) error {
	if strings.TrimSpace(identity) == "" {
		return fmt.Errorf("%w: identity is required", ErrInvalidParameter)
	}

	b.mu.Lock("VerifyEmailIdentity")
	defer b.mu.Unlock()

	if rec, ok := b.identities[identity]; ok {
		rec.Verified = true
	} else {
		b.identities[identity] = &IdentityRecord{Verified: true, ForwardingEnabled: true}
	}

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

// getOrCreateIdentityLocked returns the IdentityRecord for identity, creating one if absent.
// The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) getOrCreateIdentityLocked(identity string) *IdentityRecord {
	if rec, ok := b.identities[identity]; ok {
		return rec
	}

	rec := &IdentityRecord{ForwardingEnabled: true}
	b.identities[identity] = rec

	return rec
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
// Verified identities return Success; unknown identities return NotStarted.
func (b *InMemoryBackend) GetIdentityVerificationAttributes(identities []string) map[string]string {
	b.mu.RLock("GetIdentityVerificationAttributes")
	defer b.mu.RUnlock()

	result := make(map[string]string, len(identities))

	for _, id := range identities {
		if rec, ok := b.identities[id]; ok && rec.Verified {
			result[id] = identityStatusSuccess
		} else {
			result[id] = identityStatusNotStarted
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
	if rec, ok := b.identities[from]; ok && rec.Verified {
		return true
	}

	// Domain-level check: strip the local-part and check the domain.
	if at := strings.LastIndex(from, "@"); at >= 0 {
		domain := from[at+1:]
		rec, ok := b.identities[domain]

		return ok && rec.Verified
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
func (b *InMemoryBackend) SendEmail(in SendEmailInput) (string, error) {
	if in.From == "" {
		return "", fmt.Errorf("%w: Source is required", ErrInvalidParameter)
	}

	// AWS SES caps a single message at 10 MiB total (subject + body + headers).
	const maxMessageBytes = 10 * 1024 * 1024
	if len(in.Subject)+len(in.BodyHTML)+len(in.BodyText) > maxMessageBytes {
		return "", fmt.Errorf("%w: message exceeds 10 MB", ErrMessageRejected)
	}

	b.mu.Lock("SendEmail")
	defer b.mu.Unlock()

	if !b.isVerifiedLocked(in.From) {
		return "", fmt.Errorf(
			"%w: Email address is not verified. The following identities failed the check in region US-EAST-1: %s",
			ErrMessageRejected, in.From,
		)
	}

	msgID := "ses-" + uuid.New().String()

	b.appendEmailLocked(Email{
		MessageID:            msgID,
		From:                 in.From,
		To:                   in.To,
		Cc:                   in.Cc,
		Bcc:                  in.Bcc,
		ReplyTo:              in.ReplyTo,
		Subject:              in.Subject,
		BodyHTML:             in.BodyHTML,
		BodyText:             in.BodyText,
		ConfigurationSetName: in.ConfigurationSetName,
		Tags:                 in.Tags,
		ReturnPath:           in.ReturnPath,
		SourceArn:            in.SourceArn,
		Timestamp:            time.Now(),
	})

	return msgID, nil
}

// SendTemplatedEmail sends an email using a stored template and returns the message ID.
// The source address must be a verified identity or from a verified domain.
// The template must already exist; ErrTemplateNotFound is returned otherwise.
func (b *InMemoryBackend) SendTemplatedEmail(in SendTemplatedEmailInput) (string, error) {
	if in.From == "" {
		return "", fmt.Errorf("%w: Source is required", ErrInvalidParameter)
	}

	b.mu.Lock("SendTemplatedEmail")
	defer b.mu.Unlock()

	if !b.isVerifiedLocked(in.From) {
		return "", fmt.Errorf(
			"%w: Email address is not verified. The following identities failed the check in region US-EAST-1: %s",
			ErrMessageRejected, in.From,
		)
	}

	tmpl, ok := b.templates[in.TemplateName]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrTemplateNotFound, in.TemplateName)
	}

	msgID := "ses-" + uuid.New().String()

	b.appendEmailLocked(Email{
		MessageID:            msgID,
		From:                 in.From,
		To:                   in.To,
		Cc:                   in.Cc,
		Bcc:                  in.Bcc,
		ReplyTo:              in.ReplyTo,
		Subject:              tmpl.SubjectPart,
		BodyHTML:             tmpl.HTMLPart,
		BodyText:             tmpl.TextPart,
		ConfigurationSetName: in.ConfigurationSetName,
		Tags:                 in.Tags,
		ReturnPath:           in.ReturnPath,
		SourceArn:            in.SourceArn,
		Timestamp:            time.Now(),
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

	b.configSets[name] = &ConfigurationSet{SendingEnabled: true}

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
	delete(b.eventDestinations, name)
	delete(b.trackingOptions, name)

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

	for _, v := range slices.Backward(b.emails) {
		if v.Timestamp.Before(cutoff) {
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

// ---- receipt rule set operations ----

// findRuleIndex returns the index of the rule with the given name, or -1 if not found.
func findRuleIndex(rules []ReceiptRule, name string) int {
	for i, r := range rules {
		if r.Name == name {
			return i
		}
	}

	return -1
}

// cloneReceiptRuleSet returns a deep copy of a ReceiptRuleSet.
func cloneReceiptRuleSet(rs *ReceiptRuleSet) ReceiptRuleSet {
	rules := make([]ReceiptRule, len(rs.Rules))
	for i, r := range rs.Rules {
		recipients := make([]string, len(r.Recipients))
		copy(recipients, r.Recipients)
		actions := make([]ReceiptAction, len(r.Actions))
		copy(actions, r.Actions)
		rules[i] = ReceiptRule{
			Name:        r.Name,
			Enabled:     r.Enabled,
			TLSPolicy:   r.TLSPolicy,
			ScanEnabled: r.ScanEnabled,
			Recipients:  recipients,
			Actions:     actions,
		}
	}

	return ReceiptRuleSet{Name: rs.Name, CreatedAt: rs.CreatedAt, Rules: rules}
}

// CreateReceiptRuleSet creates a new receipt rule set.
// Returns ErrReceiptRuleSetExists if it already exists.
func (b *InMemoryBackend) CreateReceiptRuleSet(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateReceiptRuleSet")
	defer b.mu.Unlock()

	if _, exists := b.receiptRuleSets[name]; exists {
		return fmt.Errorf("%w: receipt rule set %s already exists", ErrReceiptRuleSetExists, name)
	}

	b.receiptRuleSets[name] = &ReceiptRuleSet{
		Name:      name,
		CreatedAt: time.Now().UTC(),
		Rules:     []ReceiptRule{},
	}

	return nil
}

// CloneReceiptRuleSet creates a copy of an existing receipt rule set under a new name.
func (b *InMemoryBackend) CloneReceiptRuleSet(originalName, newName string) error {
	if strings.TrimSpace(originalName) == "" {
		return fmt.Errorf("%w: OriginalRuleSetName is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(newName) == "" {
		return fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CloneReceiptRuleSet")
	defer b.mu.Unlock()

	src, exists := b.receiptRuleSets[originalName]
	if !exists {
		return fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, originalName)
	}

	if _, ok := b.receiptRuleSets[newName]; ok {
		return fmt.Errorf("%w: receipt rule set %s already exists", ErrReceiptRuleSetExists, newName)
	}

	rules := make([]ReceiptRule, len(src.Rules))
	for i, r := range src.Rules {
		recipients := make([]string, len(r.Recipients))
		copy(recipients, r.Recipients)
		actions := make([]ReceiptAction, len(r.Actions))
		copy(actions, r.Actions)

		rules[i] = ReceiptRule{
			Name:        r.Name,
			Enabled:     r.Enabled,
			TLSPolicy:   r.TLSPolicy,
			ScanEnabled: r.ScanEnabled,
			Recipients:  recipients,
			Actions:     actions,
		}
	}

	b.receiptRuleSets[newName] = &ReceiptRuleSet{
		Name:      newName,
		CreatedAt: time.Now().UTC(),
		Rules:     rules,
	}

	return nil
}

// CreateReceiptRule adds a new rule to an existing receipt rule set.
func (b *InMemoryBackend) CreateReceiptRule(ruleSetName string, rule ReceiptRule, after string) error {
	if strings.TrimSpace(ruleSetName) == "" {
		return fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(rule.Name) == "" {
		return fmt.Errorf("%w: Rule.Name is required", ErrInvalidParameter)
	}

	if rule.TLSPolicy != "" && rule.TLSPolicy != TLSPolicyOptional && rule.TLSPolicy != TLSPolicyRequire {
		return fmt.Errorf("%w: TlsPolicy must be Optional or Require", ErrInvalidParameter)
	}

	b.mu.Lock("CreateReceiptRule")
	defer b.mu.Unlock()

	rs, exists := b.receiptRuleSets[ruleSetName]
	if !exists {
		return fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, ruleSetName)
	}

	for _, r := range rs.Rules {
		if r.Name == rule.Name {
			return fmt.Errorf("%w: rule %s already exists in rule set %s", ErrReceiptRuleExists, rule.Name, ruleSetName)
		}
	}

	if after == "" {
		rs.Rules = append([]ReceiptRule{rule}, rs.Rules...)

		return nil
	}

	idx := findRuleIndex(rs.Rules, after)

	if idx < 0 {
		return fmt.Errorf("%w: after rule %s not found", ErrReceiptRuleNotFound, after)
	}

	newRules := make([]ReceiptRule, 0, len(rs.Rules)+1)
	newRules = append(newRules, rs.Rules[:idx+1]...)
	newRules = append(newRules, rule)
	newRules = append(newRules, rs.Rules[idx+1:]...)
	rs.Rules = newRules

	return nil
}

// ---- receipt filter operations ----

// CreateReceiptFilter creates a new IP-based receipt filter.
func (b *InMemoryBackend) CreateReceiptFilter(filter ReceiptFilter) error {
	if strings.TrimSpace(filter.Name) == "" {
		return fmt.Errorf("%w: Filter.Name is required", ErrInvalidParameter)
	}

	if filter.Policy != "" && filter.Policy != FilterPolicyAllow && filter.Policy != FilterPolicyBlock {
		return fmt.Errorf("%w: Policy must be Allow or Block", ErrInvalidParameter)
	}

	b.mu.Lock("CreateReceiptFilter")
	defer b.mu.Unlock()

	if _, exists := b.receiptFilters[filter.Name]; exists {
		return fmt.Errorf("%w: receipt filter %s already exists", ErrReceiptFilterExists, filter.Name)
	}

	f := filter
	b.receiptFilters[filter.Name] = &f

	return nil
}

// ---- configuration set event destination operations ----

// CreateConfigurationSetEventDestination adds an event destination to a configuration set.
func (b *InMemoryBackend) CreateConfigurationSetEventDestination(configSetName string, dest EventDestination) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(dest.Name) == "" {
		return fmt.Errorf("%w: EventDestination.Name is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateConfigurationSetEventDestination")
	defer b.mu.Unlock()

	if _, exists := b.configSets[configSetName]; !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	if b.eventDestinations[configSetName] == nil {
		b.eventDestinations[configSetName] = make(map[string]*EventDestination)
	}

	if _, exists := b.eventDestinations[configSetName][dest.Name]; exists {
		return fmt.Errorf(
			"%w: event destination %s already exists in configuration set %s",
			ErrEventDestinationExists,
			dest.Name,
			configSetName,
		)
	}

	d := dest
	b.eventDestinations[configSetName][dest.Name] = &d

	return nil
}

// DeleteConfigurationSetEventDestination removes an event destination from a configuration set.
func (b *InMemoryBackend) DeleteConfigurationSetEventDestination(configSetName, destName string) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteConfigurationSetEventDestination")
	defer b.mu.Unlock()

	if _, exists := b.configSets[configSetName]; !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	dests := b.eventDestinations[configSetName]
	if dests == nil {
		return fmt.Errorf("%w: %s", ErrEventDestinationNotFound, destName)
	}

	if _, exists := dests[destName]; !exists {
		return fmt.Errorf("%w: %s", ErrEventDestinationNotFound, destName)
	}

	delete(dests, destName)

	return nil
}

// ---- configuration set tracking options operations ----

// CreateConfigurationSetTrackingOptions sets the tracking options for a configuration set.
func (b *InMemoryBackend) CreateConfigurationSetTrackingOptions(configSetName, customRedirectDomain string) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateConfigurationSetTrackingOptions")
	defer b.mu.Unlock()

	if _, exists := b.configSets[configSetName]; !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	if _, exists := b.trackingOptions[configSetName]; exists {
		return fmt.Errorf(
			"%w: tracking options already exist for configuration set %s",
			ErrTrackingOptionsExists,
			configSetName,
		)
	}

	b.trackingOptions[configSetName] = &TrackingOptions{CustomRedirectDomain: customRedirectDomain}

	return nil
}

// DeleteConfigurationSetTrackingOptions removes the tracking options from a configuration set.
func (b *InMemoryBackend) DeleteConfigurationSetTrackingOptions(configSetName string) error {
	if strings.TrimSpace(configSetName) == "" {
		return fmt.Errorf("%w: ConfigurationSetName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteConfigurationSetTrackingOptions")
	defer b.mu.Unlock()

	if _, exists := b.configSets[configSetName]; !exists {
		return fmt.Errorf("%w: %s", ErrConfigSetNotFound, configSetName)
	}

	if _, exists := b.trackingOptions[configSetName]; !exists {
		return fmt.Errorf(
			"%w: tracking options do not exist for configuration set %s",
			ErrTrackingOptionsNotFound,
			configSetName,
		)
	}

	delete(b.trackingOptions, configSetName)

	return nil
}

// ---- custom verification email template operations ----

// CreateCustomVerificationEmailTemplate creates a custom verification email template.
func (b *InMemoryBackend) CreateCustomVerificationEmailTemplate(tmpl CustomVerificationEmailTemplate) error {
	if strings.TrimSpace(tmpl.TemplateName) == "" {
		return fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(tmpl.FromEmailAddress) == "" {
		return fmt.Errorf("%w: FromEmailAddress is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(tmpl.TemplateSubject) == "" {
		return fmt.Errorf("%w: TemplateSubject is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(tmpl.TemplateContent) == "" {
		return fmt.Errorf("%w: TemplateContent is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(tmpl.SuccessRedirectionURL) == "" {
		return fmt.Errorf("%w: SuccessRedirectionURL is required", ErrInvalidParameter)
	}

	if strings.TrimSpace(tmpl.FailureRedirectionURL) == "" {
		return fmt.Errorf("%w: FailureRedirectionURL is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateCustomVerificationEmailTemplate")
	defer b.mu.Unlock()

	if _, exists := b.customVerifTemplates[tmpl.TemplateName]; exists {
		return fmt.Errorf(
			"%w: custom verification email template %s already exists",
			ErrCustomVerifTemplateExists,
			tmpl.TemplateName,
		)
	}

	t := tmpl
	b.customVerifTemplates[tmpl.TemplateName] = &t

	return nil
}

// DeleteCustomVerificationEmailTemplate removes a custom verification email template.
func (b *InMemoryBackend) DeleteCustomVerificationEmailTemplate(templateName string) error {
	if strings.TrimSpace(templateName) == "" {
		return fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteCustomVerificationEmailTemplate")
	defer b.mu.Unlock()

	if _, exists := b.customVerifTemplates[templateName]; !exists {
		return fmt.Errorf("%w: %s", ErrCustomVerifTemplateNotFound, templateName)
	}

	delete(b.customVerifTemplates, templateName)

	return nil
}

// ListReceiptFilters returns a sorted slice of all receipt filters.
func (b *InMemoryBackend) ListReceiptFilters() []ReceiptFilter {
	b.mu.RLock("ListReceiptFilters")
	defer b.mu.RUnlock()

	out := make([]ReceiptFilter, 0, len(b.receiptFilters))
	for _, f := range b.receiptFilters {
		out = append(out, *f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// DeleteReceiptFilter removes a receipt filter by name.
func (b *InMemoryBackend) DeleteReceiptFilter(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: Filter.Name is required", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteReceiptFilter")
	defer b.mu.Unlock()
	if _, exists := b.receiptFilters[name]; !exists {
		return fmt.Errorf("%w: %s", ErrReceiptFilterNotFound, name)
	}
	delete(b.receiptFilters, name)

	return nil
}

// ListReceiptRuleSets returns a sorted slice of all receipt rule sets (name + createdAt only).
func (b *InMemoryBackend) ListReceiptRuleSets() []ReceiptRuleSet {
	b.mu.RLock("ListReceiptRuleSets")
	defer b.mu.RUnlock()

	out := make([]ReceiptRuleSet, 0, len(b.receiptRuleSets))
	for _, rs := range b.receiptRuleSets {
		out = append(out, ReceiptRuleSet{Name: rs.Name, CreatedAt: rs.CreatedAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

// DescribeReceiptRuleSet returns a deep copy of the named rule set.
func (b *InMemoryBackend) DescribeReceiptRuleSet(name string) (ReceiptRuleSet, error) {
	if strings.TrimSpace(name) == "" {
		return ReceiptRuleSet{}, fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}
	b.mu.RLock("DescribeReceiptRuleSet")
	defer b.mu.RUnlock()
	rs, exists := b.receiptRuleSets[name]
	if !exists {
		return ReceiptRuleSet{}, fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, name)
	}

	return cloneReceiptRuleSet(rs), nil
}

// DeleteReceiptRule removes a receipt rule from a rule set.
func (b *InMemoryBackend) DeleteReceiptRule(ruleSetName, ruleName string) error {
	if strings.TrimSpace(ruleSetName) == "" {
		return fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}
	if strings.TrimSpace(ruleName) == "" {
		return fmt.Errorf("%w: Rule.Name is required", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteReceiptRule")
	defer b.mu.Unlock()
	rs, exists := b.receiptRuleSets[ruleSetName]
	if !exists {
		return fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, ruleSetName)
	}
	idx := findRuleIndex(rs.Rules, ruleName)
	if idx < 0 {
		return fmt.Errorf("%w: %s", ErrReceiptRuleNotFound, ruleName)
	}
	rs.Rules = append(rs.Rules[:idx], rs.Rules[idx+1:]...)

	return nil
}

// DeleteReceiptRuleSet removes a receipt rule set and its rules.
func (b *InMemoryBackend) DeleteReceiptRuleSet(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: RuleSetName is required", ErrInvalidParameter)
	}
	b.mu.Lock("DeleteReceiptRuleSet")
	defer b.mu.Unlock()
	if _, exists := b.receiptRuleSets[name]; !exists {
		return fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, name)
	}
	delete(b.receiptRuleSets, name)
	if b.activeRuleSet == name {
		b.activeRuleSet = ""
	}

	return nil
}

// SetActiveReceiptRuleSet sets the named rule set as active.
// Passing an empty name clears the active rule set.
func (b *InMemoryBackend) SetActiveReceiptRuleSet(name string) error {
	b.mu.Lock("SetActiveReceiptRuleSet")
	defer b.mu.Unlock()
	if name != "" {
		if _, exists := b.receiptRuleSets[name]; !exists {
			return fmt.Errorf("%w: %s", ErrReceiptRuleSetNotFound, name)
		}
	}
	b.activeRuleSet = name

	return nil
}

// DescribeActiveReceiptRuleSet returns the active receipt rule set.
// Returns false if none is set.
func (b *InMemoryBackend) DescribeActiveReceiptRuleSet() (ReceiptRuleSet, bool, error) {
	b.mu.RLock("DescribeActiveReceiptRuleSet")
	defer b.mu.RUnlock()
	if b.activeRuleSet == "" {
		return ReceiptRuleSet{}, false, nil
	}
	rs, exists := b.receiptRuleSets[b.activeRuleSet]
	if !exists {
		return ReceiptRuleSet{}, false, nil
	}

	return cloneReceiptRuleSet(rs), true, nil
}

// GetCustomVerificationEmailTemplate returns the named custom verification email template.
func (b *InMemoryBackend) GetCustomVerificationEmailTemplate(
	templateName string,
) (CustomVerificationEmailTemplate, error) {
	if strings.TrimSpace(templateName) == "" {
		return CustomVerificationEmailTemplate{}, fmt.Errorf("%w: TemplateName is required", ErrInvalidParameter)
	}
	b.mu.RLock("GetCustomVerificationEmailTemplate")
	defer b.mu.RUnlock()
	tmpl, exists := b.customVerifTemplates[templateName]
	if !exists {
		return CustomVerificationEmailTemplate{}, fmt.Errorf("%w: %s", ErrCustomVerifTemplateNotFound, templateName)
	}

	return *tmpl, nil
}

// ListCustomVerificationEmailTemplates returns a sorted slice of all custom verification email templates.
func (b *InMemoryBackend) ListCustomVerificationEmailTemplates() []CustomVerificationEmailTemplate {
	b.mu.RLock("ListCustomVerificationEmailTemplates")
	defer b.mu.RUnlock()
	out := make([]CustomVerificationEmailTemplate, 0, len(b.customVerifTemplates))
	for _, t := range b.customVerifTemplates {
		out = append(out, *t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TemplateName < out[j].TemplateName })

	return out
}

// Region returns the AWS region for this backend instance.
func (b *InMemoryBackend) Region() string {
	return config.DefaultRegion
}

// AccountID returns the simulated AWS account ID.
func (b *InMemoryBackend) AccountID() string {
	return defaultAccountID
}
