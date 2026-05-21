package ses

import (
	"encoding/json"
	"log/slog"
	"maps"
	"time"
)

type backendSnapshot struct {
	Identities             map[string]*IdentityRecord                  `json:"identities"`
	Templates              map[string]EmailTemplate                    `json:"templates"`
	ConfigSets             map[string]*ConfigurationSet                `json:"configSets"`
	ReceiptRuleSets        map[string]*ReceiptRuleSet                  `json:"receiptRuleSets"`
	ReceiptFilters         map[string]*ReceiptFilter                   `json:"receiptFilters"`
	EventDestinations      map[string]map[string]*EventDestination     `json:"eventDestinations"`
	TrackingOptions        map[string]*TrackingOptions                 `json:"trackingOptions"`
	CustomVerifTemplates   map[string]*CustomVerificationEmailTemplate `json:"customVerifTemplates"`
	Policies               map[string]map[string]string                `json:"policies,omitempty"`
	ActiveRuleSet          string                                      `json:"activeRuleSet,omitempty"`
	Emails                 []Email                                     `json:"emails"`
	AccountSendingEnabled  bool                                        `json:"accountSendingEnabled"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	ids := make(map[string]*IdentityRecord, len(b.identities))
	for k, v := range b.identities {
		c := *v
		ids[k] = &c
	}

	emails := make([]Email, len(b.emails))
	copy(emails, b.emails)

	tmpls := make(map[string]EmailTemplate, len(b.templates))
	maps.Copy(tmpls, b.templates)

	cfgs := make(map[string]*ConfigurationSet, len(b.configSets))
	for k, v := range b.configSets {
		c := *v
		cfgs[k] = &c
	}

	ruleSets := make(map[string]*ReceiptRuleSet, len(b.receiptRuleSets))
	for k, rs := range b.receiptRuleSets {
		clone := cloneReceiptRuleSet(rs)
		ruleSets[k] = &clone
	}

	filters := make(map[string]*ReceiptFilter, len(b.receiptFilters))
	for k, f := range b.receiptFilters {
		fc := *f
		filters[k] = &fc
	}

	evtDests := make(map[string]map[string]*EventDestination, len(b.eventDestinations))
	for csName, dests := range b.eventDestinations {
		destCopy := make(map[string]*EventDestination, len(dests))
		for k, d := range dests {
			dc := *d
			destCopy[k] = &dc
		}
		evtDests[csName] = destCopy
	}

	trackOpts := make(map[string]*TrackingOptions, len(b.trackingOptions))
	for k, t := range b.trackingOptions {
		tc := *t
		trackOpts[k] = &tc
	}

	custTmpls := make(map[string]*CustomVerificationEmailTemplate, len(b.customVerifTemplates))
	for k, t := range b.customVerifTemplates {
		tc := *t
		custTmpls[k] = &tc
	}

	pols := make(map[string]map[string]string, len(b.policies))
	for identity, polMap := range b.policies {
		pm := make(map[string]string, len(polMap))
		maps.Copy(pm, polMap)
		pols[identity] = pm
	}

	snap := backendSnapshot{
		Identities:            ids,
		Emails:                emails,
		Templates:             tmpls,
		ConfigSets:            cfgs,
		ReceiptRuleSets:       ruleSets,
		ReceiptFilters:        filters,
		EventDestinations:     evtDests,
		TrackingOptions:       trackOpts,
		CustomVerifTemplates:  custTmpls,
		Policies:              pols,
		ActiveRuleSet:         b.activeRuleSet,
		AccountSendingEnabled: b.accountSendingEnabled,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("ses: failed to marshal snapshot", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	ensureNonNilMaps(&snap)

	b.identities = snap.Identities
	b.templates = snap.Templates
	b.configSets = snap.ConfigSets
	b.receiptRuleSets = snap.ReceiptRuleSets
	b.receiptFilters = snap.ReceiptFilters
	b.eventDestinations = snap.EventDestinations
	b.trackingOptions = snap.TrackingOptions
	b.customVerifTemplates = snap.CustomVerifTemplates
	b.policies = snap.Policies
	b.activeRuleSet = snap.ActiveRuleSet
	b.accountSendingEnabled = snap.AccountSendingEnabled

	// Drop emails outside the current TTL window and cap to maxRetainedEmails
	// so that memory is bounded immediately after restore.
	cutoff := time.Now().Add(-b.emailTTL)

	start := 0

	for start < len(snap.Emails) && snap.Emails[start].Timestamp.Before(cutoff) {
		start++
	}

	valid := snap.Emails[start:]

	if len(valid) > maxRetainedEmails {
		valid = valid[len(valid)-maxRetainedEmails:]
	}

	b.emails = make([]Email, len(valid))
	copy(b.emails, valid)

	// Rebuild O(1) lookup map from the pruned slice.
	b.emailsByID = make(map[string]Email, len(b.emails))
	for _, e := range b.emails {
		b.emailsByID[e.MessageID] = e
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}

func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.Identities == nil {
		snap.Identities = make(map[string]*IdentityRecord)
	}
	if snap.Emails == nil {
		snap.Emails = []Email{}
	}
	if snap.Templates == nil {
		snap.Templates = make(map[string]EmailTemplate)
	}
	if snap.ConfigSets == nil {
		snap.ConfigSets = make(map[string]*ConfigurationSet)
	}
	if snap.ReceiptRuleSets == nil {
		snap.ReceiptRuleSets = make(map[string]*ReceiptRuleSet)
	}
	if snap.ReceiptFilters == nil {
		snap.ReceiptFilters = make(map[string]*ReceiptFilter)
	}
	if snap.EventDestinations == nil {
		snap.EventDestinations = make(map[string]map[string]*EventDestination)
	}
	if snap.TrackingOptions == nil {
		snap.TrackingOptions = make(map[string]*TrackingOptions)
	}
	if snap.CustomVerifTemplates == nil {
		snap.CustomVerifTemplates = make(map[string]*CustomVerificationEmailTemplate)
	}
	if snap.Policies == nil {
		snap.Policies = make(map[string]map[string]string)
	}
}
