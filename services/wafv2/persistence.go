package wafv2

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	WebACLs            map[string]*WebACL          `json:"webACLs"`
	IPSets             map[string]*IPSet           `json:"ipSets"`
	RegexPatternSets   map[string]*RegexPatternSet `json:"regexPatternSets,omitempty"`
	RuleGroups         map[string]*RuleGroup       `json:"ruleGroups,omitempty"`
	ManagedRuleSets    map[string]*ManagedRuleSet  `json:"managedRuleSets,omitempty"`
	APIKeys            map[string]*APIKey          `json:"apiKeys,omitempty"`
	LoggingConfigs     map[string]json.RawMessage  `json:"loggingConfigs,omitempty"`
	PermissionPolicies map[string]string           `json:"permissionPolicies,omitempty"`
	Associations       map[string]string           `json:"associations,omitempty"`
	AccountID          string                      `json:"accountID"`
	Region             string                      `json:"region"`
}

// Snapshot serializes the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		WebACLs:            b.webACLs,
		IPSets:             b.ipSets,
		RegexPatternSets:   b.regexPatternSets,
		RuleGroups:         b.ruleGroups,
		ManagedRuleSets:    b.managedRuleSets,
		APIKeys:            b.apiKeys,
		LoggingConfigs:     b.loggingConfigs,
		PermissionPolicies: b.permissionPolicies,
		Associations:       b.associations,
		AccountID:          b.accountID,
		Region:             b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		slog.Default().Warn("wafv2: failed to serialize snapshot", "error", err)

		return nil
	}

	return data
}

// ensureNonNilMaps initializes any nil maps in the snapshot so that downstream
// code can unconditionally assign them to the backend fields.
func (snap *backendSnapshot) ensureNonNilMaps() {
	if snap.WebACLs == nil {
		snap.WebACLs = make(map[string]*WebACL)
	}

	if snap.IPSets == nil {
		snap.IPSets = make(map[string]*IPSet)
	}

	if snap.RegexPatternSets == nil {
		snap.RegexPatternSets = make(map[string]*RegexPatternSet)
	}

	if snap.RuleGroups == nil {
		snap.RuleGroups = make(map[string]*RuleGroup)
	}

	if snap.ManagedRuleSets == nil {
		snap.ManagedRuleSets = make(map[string]*ManagedRuleSet)
	}

	if snap.APIKeys == nil {
		snap.APIKeys = make(map[string]*APIKey)
	}

	if snap.LoggingConfigs == nil {
		snap.LoggingConfigs = make(map[string]json.RawMessage)
	}

	if snap.PermissionPolicies == nil {
		snap.PermissionPolicies = make(map[string]string)
	}

	if snap.Associations == nil {
		snap.Associations = make(map[string]string)
	}
}

// rebuildIndexesLocked rebuilds all secondary index maps from the primary data
// in the snapshot. Must be called with b.mu held for writing.
func (b *InMemoryBackend) rebuildIndexesLocked(snap *backendSnapshot) {
	b.webACLByARN = make(map[string]string, len(snap.WebACLs))
	b.ipSetByARN = make(map[string]string, len(snap.IPSets))
	b.regexPatternSetByARN = make(map[string]string, len(snap.RegexPatternSets))
	b.ruleGroupByARN = make(map[string]string, len(snap.RuleGroups))
	b.webACLByNameScope = make(map[string]string, len(snap.WebACLs))
	b.ipSetByNameScope = make(map[string]string, len(snap.IPSets))
	b.regexPatternSetByScope = make(map[string]string, len(snap.RegexPatternSets))
	b.ruleGroupByNameScope = make(map[string]string, len(snap.RuleGroups))

	for _, w := range snap.WebACLs {
		b.webACLByARN[b.WebACLARN(w.Name, w.ID, w.Scope)] = w.ID
		b.webACLByNameScope[nameScope(w.Name, w.Scope)] = w.ID
	}

	for _, s := range snap.IPSets {
		b.ipSetByARN[b.IPSetARN(s.Name, s.ID, s.Scope)] = s.ID
		b.ipSetByNameScope[nameScope(s.Name, s.Scope)] = s.ID
	}

	for _, r := range snap.RegexPatternSets {
		b.regexPatternSetByARN[b.RegexPatternSetARN(r.Name, r.ID, r.Scope)] = r.ID
		b.regexPatternSetByScope[nameScope(r.Name, r.Scope)] = r.ID
	}

	for _, rg := range snap.RuleGroups {
		b.ruleGroupByARN[b.RuleGroupARN(rg.Name, rg.ID, rg.Scope)] = rg.ID
		b.ruleGroupByNameScope[nameScope(rg.Name, rg.Scope)] = rg.ID
	}
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	snap.ensureNonNilMaps()

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.webACLs = snap.WebACLs
	b.ipSets = snap.IPSets
	b.regexPatternSets = snap.RegexPatternSets
	b.ruleGroups = snap.RuleGroups
	b.managedRuleSets = snap.ManagedRuleSets
	b.apiKeys = snap.APIKeys
	b.loggingConfigs = snap.LoggingConfigs
	b.permissionPolicies = snap.PermissionPolicies
	b.associations = snap.Associations
	b.accountID = snap.AccountID
	b.region = snap.Region

	b.rebuildIndexesLocked(&snap)

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
