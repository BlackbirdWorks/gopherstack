package ses

import (
	"time"

	"github.com/google/uuid"
)

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultSESJanitorInterval

// MaxRetainedEmails exposes the email retention cap for testing.
const MaxRetainedEmails = maxRetainedEmails

// DefaultEmailTTL exposes the default email TTL for testing.
const DefaultEmailTTL = defaultEmailTTL

// EmailCount returns the number of stored emails.
func (b *InMemoryBackend) EmailCount() int {
	b.mu.RLock("EmailCount")
	defer b.mu.RUnlock()

	return len(b.emails)
}

// EmailsByIDCount returns the number of entries in the O(1) lookup map.
func (b *InMemoryBackend) EmailsByIDCount() int {
	b.mu.RLock("EmailsByIDCount")
	defer b.mu.RUnlock()

	return len(b.emailsByID)
}

// IdentityCount returns the number of verified identities.
func (b *InMemoryBackend) IdentityCount() int {
	b.mu.RLock("IdentityCount")
	defer b.mu.RUnlock()

	return len(b.identities)
}

// TemplateCount returns the number of stored templates.
func (b *InMemoryBackend) TemplateCount() int {
	b.mu.RLock("TemplateCount")
	defer b.mu.RUnlock()

	return len(b.templates)
}

// ConfigSetCount returns the number of stored configuration sets.
func (b *InMemoryBackend) ConfigSetCount() int {
	b.mu.RLock("ConfigSetCount")
	defer b.mu.RUnlock()

	return len(b.configSets)
}

// ReceiptRuleSetCount returns the number of stored receipt rule sets.
func (b *InMemoryBackend) ReceiptRuleSetCount() int {
	b.mu.RLock("ReceiptRuleSetCount")
	defer b.mu.RUnlock()

	return len(b.receiptRuleSets)
}

// ReceiptFilterCount returns the number of stored receipt filters.
func (b *InMemoryBackend) ReceiptFilterCount() int {
	b.mu.RLock("ReceiptFilterCount")
	defer b.mu.RUnlock()

	return len(b.receiptFilters)
}

// EventDestinationCount returns the total number of stored event destinations across all config sets.
func (b *InMemoryBackend) EventDestinationCount() int {
	b.mu.RLock("EventDestinationCount")
	defer b.mu.RUnlock()

	total := 0
	for _, dests := range b.eventDestinations {
		total += len(dests)
	}

	return total
}

// TrackingOptionsCount returns the number of configuration sets with tracking options.
func (b *InMemoryBackend) TrackingOptionsCount() int {
	b.mu.RLock("TrackingOptionsCount")
	defer b.mu.RUnlock()

	return len(b.trackingOptions)
}

// CustomVerifTemplateCount returns the number of custom verification email templates.
func (b *InMemoryBackend) CustomVerifTemplateCount() int {
	b.mu.RLock("CustomVerifTemplateCount")
	defer b.mu.RUnlock()

	return len(b.customVerifTemplates)
}

// SetEmailTTL overrides the email TTL — useful for tests that need fast expiry.
func (b *InMemoryBackend) SetEmailTTL(d time.Duration) {
	b.mu.Lock("SetEmailTTL")
	defer b.mu.Unlock()

	b.emailTTL = d
}

// GetEmailTTL returns the current email TTL — useful for asserting Reset restores it.
func (b *InMemoryBackend) GetEmailTTL() time.Duration {
	b.mu.RLock("GetEmailTTL")
	defer b.mu.RUnlock()

	return b.emailTTL
}

// GetJanitorInterval returns the Interval configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the interval.
func (h *Handler) GetJanitorInterval() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.Interval
}

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}

// GetEmailTTL returns the email TTL configured on the handler's backend.
// Used in provider tests to verify that the TTL is passed through correctly.
func (h *Handler) GetEmailTTL() time.Duration {
	if ib, ok := h.Backend.(*InMemoryBackend); ok {
		return ib.GetEmailTTL()
	}

	return 0
}

// HandlerOpsLen returns the number of operations in GetSupportedOperations().
func (h *Handler) HandlerOpsLen() int {
	return len(h.GetSupportedOperations())
}

// AddReceiptRuleSetInternal adds a receipt rule set directly for test seeding.
func (b *InMemoryBackend) AddReceiptRuleSetInternal(rs ReceiptRuleSet) {
	b.mu.Lock("AddReceiptRuleSetInternal")
	defer b.mu.Unlock()
	r := rs
	if r.Rules == nil {
		r.Rules = []ReceiptRule{}
	}
	b.receiptRuleSets[rs.Name] = &r
}

// AddReceiptFilterInternal adds a receipt filter directly for test seeding.
func (b *InMemoryBackend) AddReceiptFilterInternal(f ReceiptFilter) {
	b.mu.Lock("AddReceiptFilterInternal")
	defer b.mu.Unlock()
	fc := f
	b.receiptFilters[f.Name] = &fc
}

// AddCustomVerifTemplateInternal adds a custom verification email template for test seeding.
func (b *InMemoryBackend) AddCustomVerifTemplateInternal(t CustomVerificationEmailTemplate) {
	b.mu.Lock("AddCustomVerifTemplateInternal")
	defer b.mu.Unlock()
	tc := t
	b.customVerifTemplates[t.TemplateName] = &tc
}

// ActiveRuleSet returns the name of the currently active rule set.
func (b *InMemoryBackend) ActiveRuleSet() string {
	b.mu.RLock("ActiveRuleSet")
	defer b.mu.RUnlock()

	return b.activeRuleSet
}

// BackdateEmailForTest sets the Timestamp of the email at index i to the given time.
// Used in janitor sweep tests to simulate aged emails.
func (b *InMemoryBackend) BackdateEmailForTest(i int, ts time.Time) {
	b.mu.Lock("BackdateEmailForTest")
	defer b.mu.Unlock()

	b.emails[i].Timestamp = ts
	b.emailsByID[b.emails[i].MessageID] = b.emails[i]
}

// PolicyCount returns the total number of stored identity policies across all identities.
func (b *InMemoryBackend) PolicyCount() int {
	b.mu.RLock("PolicyCount")
	defer b.mu.RUnlock()

	total := 0
	for _, m := range b.policies {
		total += len(m)
	}

	return total
}

// AccountSendingEnabled returns the current account-level sending enabled flag.
func (b *InMemoryBackend) AccountSendingEnabledState() bool {
	b.mu.RLock("AccountSendingEnabledState")
	defer b.mu.RUnlock()

	return b.accountSendingEnabled
}

// AppendEmailForTest appends an email directly via the same appendEmailLocked
// path SendEmail uses internally — including the maxRetainedEmails eviction
// behavior — but bypasses SendEmail's business-rule preconditions (sender
// verification, the simulated 24-hour send quota, account-sending-enabled).
// Used by volume/retention tests that need far more than
// maxSendQuota24Hours (200) sends, which real SendEmail now rejects with
// MessageRejected once exhausted.
func (b *InMemoryBackend) AppendEmailForTest(from string, to []string) string {
	b.mu.Lock("AppendEmailForTest")
	defer b.mu.Unlock()

	msgID := "ses-test-" + uuid.New().String()
	b.appendEmailLocked(Email{
		MessageID: msgID,
		From:      from,
		To:        to,
		Timestamp: time.Now(),
	})

	return msgID
}
