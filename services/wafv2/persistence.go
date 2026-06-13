package wafv2

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	WebACLs            map[string]map[string]*WebACL          `json:"webACLs"`
	IPSets             map[string]map[string]*IPSet           `json:"ipSets"`
	RegexPatternSets   map[string]map[string]*RegexPatternSet `json:"regexPatternSets,omitempty"`
	RuleGroups         map[string]map[string]*RuleGroup       `json:"ruleGroups,omitempty"`
	ManagedRuleSets    map[string]map[string]*ManagedRuleSet  `json:"managedRuleSets,omitempty"`
	APIKeys            map[string]map[string]*APIKey          `json:"apiKeys,omitempty"`
	LoggingConfigs     map[string]map[string]json.RawMessage  `json:"loggingConfigs,omitempty"`
	PermissionPolicies map[string]map[string]string           `json:"permissionPolicies,omitempty"`
	Associations       map[string]map[string]string           `json:"associations,omitempty"`
	AccountID          string                                 `json:"accountID"`
	Region             string                                 `json:"region"`
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
		snap.WebACLs = make(map[string]map[string]*WebACL)
	}

	if snap.IPSets == nil {
		snap.IPSets = make(map[string]map[string]*IPSet)
	}

	if snap.RegexPatternSets == nil {
		snap.RegexPatternSets = make(map[string]map[string]*RegexPatternSet)
	}

	if snap.RuleGroups == nil {
		snap.RuleGroups = make(map[string]map[string]*RuleGroup)
	}

	if snap.ManagedRuleSets == nil {
		snap.ManagedRuleSets = make(map[string]map[string]*ManagedRuleSet)
	}

	if snap.APIKeys == nil {
		snap.APIKeys = make(map[string]map[string]*APIKey)
	}

	if snap.LoggingConfigs == nil {
		snap.LoggingConfigs = make(map[string]map[string]json.RawMessage)
	}

	if snap.PermissionPolicies == nil {
		snap.PermissionPolicies = make(map[string]map[string]string)
	}

	if snap.Associations == nil {
		snap.Associations = make(map[string]map[string]string)
	}
}

func ensureRegion(m map[string]map[string]string, region string) {
	if m[region] == nil {
		m[region] = make(map[string]string)
	}
}

// rebuildIndexesLocked rebuilds all secondary index maps from the primary data
// in the snapshot. Must be called with b.mu held for writing.
func (b *InMemoryBackend) rebuildIndexesLocked(snap *backendSnapshot) {
	b.webACLByARN = make(map[string]map[string]string)
	b.ipSetByARN = make(map[string]map[string]string)
	b.regexPatternSetByARN = make(map[string]map[string]string)
	b.ruleGroupByARN = make(map[string]map[string]string)
	b.webACLByNameScope = make(map[string]map[string]string)
	b.ipSetByNameScope = make(map[string]map[string]string)
	b.regexPatternSetByScope = make(map[string]map[string]string)
	b.ruleGroupByNameScope = make(map[string]map[string]string)

	for region, regionWebACLs := range snap.WebACLs {
		ensureRegion(b.webACLByARN, region)
		ensureRegion(b.webACLByNameScope, region)

		for _, w := range regionWebACLs {
			b.webACLByARN[region][b.WebACLARN(w.Name, w.ID, w.Scope)] = w.ID
			b.webACLByNameScope[region][nameScope(w.Name, w.Scope)] = w.ID
		}
	}

	for region, regionIPSets := range snap.IPSets {
		ensureRegion(b.ipSetByARN, region)
		ensureRegion(b.ipSetByNameScope, region)

		for _, s := range regionIPSets {
			b.ipSetByARN[region][b.IPSetARN(s.Name, s.ID, s.Scope)] = s.ID
			b.ipSetByNameScope[region][nameScope(s.Name, s.Scope)] = s.ID
		}
	}

	for region, regionRPS := range snap.RegexPatternSets {
		ensureRegion(b.regexPatternSetByARN, region)
		ensureRegion(b.regexPatternSetByScope, region)

		for _, r := range regionRPS {
			b.regexPatternSetByARN[region][b.RegexPatternSetARN(r.Name, r.ID, r.Scope)] = r.ID
			b.regexPatternSetByScope[region][nameScope(r.Name, r.Scope)] = r.ID
		}
	}

	for region, regionRGs := range snap.RuleGroups {
		ensureRegion(b.ruleGroupByARN, region)
		ensureRegion(b.ruleGroupByNameScope, region)

		for _, rg := range regionRGs {
			b.ruleGroupByARN[region][b.RuleGroupARN(rg.Name, rg.ID, rg.Scope)] = rg.ID
			b.ruleGroupByNameScope[region][nameScope(rg.Name, rg.Scope)] = rg.ID
		}
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
