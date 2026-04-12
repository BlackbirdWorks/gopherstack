package ses

import "time"

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
	return h.Backend.GetEmailTTL()
}

// BackdateEmailForTest sets the Timestamp of the email at index i to the given time.
// Used in janitor sweep tests to simulate aged emails.
func (b *InMemoryBackend) BackdateEmailForTest(i int, ts time.Time) {
	b.mu.Lock("BackdateEmailForTest")
	defer b.mu.Unlock()

	b.emails[i].Timestamp = ts
	b.emailsByID[b.emails[i].MessageID] = b.emails[i]
}
