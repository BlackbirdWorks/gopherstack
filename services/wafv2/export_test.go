package wafv2

import "encoding/json"

// CreateWebACLSimple is a test helper that creates a WebACL with minimal parameters,
// using the new extended backend signature. It accepts the old-style positional args
// to ease migration of existing tests.
func CreateWebACLSimple(
	b StorageBackend,
	name, scope, description, defaultAction string,
	tags map[string]string,
) (*WebACL, error) {
	var da json.RawMessage
	if defaultAction == "BLOCK" {
		da = json.RawMessage(`{"Block":{}}`)
	} else {
		da = json.RawMessage(`{"Allow":{}}`)
	}

	return b.CreateWebACL(name, scope, description, da, nil, nil, nil, nil, nil, nil, nil, tags)
}

// WebACLCount returns the number of WebACLs in the backend.
func WebACLCount(b *InMemoryBackend) int {
	b.mu.RLock("WebACLCount")
	defer b.mu.RUnlock()

	return len(b.webACLs)
}

// IPSetCount returns the number of IP sets in the backend.
func IPSetCount(b *InMemoryBackend) int {
	b.mu.RLock("IPSetCount")
	defer b.mu.RUnlock()

	return len(b.ipSets)
}

// RegexPatternSetCount returns the number of regex pattern sets in the backend.
func RegexPatternSetCount(b *InMemoryBackend) int {
	b.mu.RLock("RegexPatternSetCount")
	defer b.mu.RUnlock()

	return len(b.regexPatternSets)
}

// RuleGroupCount returns the number of rule groups in the backend.
func RuleGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("RuleGroupCount")
	defer b.mu.RUnlock()

	return len(b.ruleGroups)
}

// APIKeyCount returns the number of API keys in the backend.
func APIKeyCount(b *InMemoryBackend) int {
	b.mu.RLock("APIKeyCount")
	defer b.mu.RUnlock()

	return len(b.apiKeys)
}

// AssociationCount returns the number of WebACL-to-resource associations.
func AssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("AssociationCount")
	defer b.mu.RUnlock()

	return len(b.associations)
}

// HandlerOpsLen returns the number of supported operations in the handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// AddWebACLInternal inserts a WebACL directly into the backend, bypassing validation.
func AddWebACLInternal(b *InMemoryBackend, w *WebACL) {
	b.mu.Lock("AddWebACLInternal")
	defer b.mu.Unlock()

	if w.Tags == nil {
		w.Tags = make(map[string]string)
	}

	if w.Rules == nil {
		w.Rules = []map[string]any{}
	}

	b.webACLs[w.ID] = w
	b.webACLByARN[b.WebACLARN(w.Name, w.ID, w.Scope)] = w.ID
	b.webACLByNameScope[nameScope(w.Name, w.Scope)] = w.ID
}

// AddIPSetInternal inserts an IPSet directly into the backend, bypassing validation.
func AddIPSetInternal(b *InMemoryBackend, s *IPSet) {
	b.mu.Lock("AddIPSetInternal")
	defer b.mu.Unlock()

	if s.Tags == nil {
		s.Tags = make(map[string]string)
	}

	if s.Addresses == nil {
		s.Addresses = []string{}
	}

	b.ipSets[s.ID] = s
	b.ipSetByARN[b.IPSetARN(s.Name, s.ID, s.Scope)] = s.ID
	b.ipSetByNameScope[nameScope(s.Name, s.Scope)] = s.ID
}

// AddRegexPatternSetInternal inserts a RegexPatternSet directly into the backend.
func AddRegexPatternSetInternal(b *InMemoryBackend, r *RegexPatternSet) {
	b.mu.Lock("AddRegexPatternSetInternal")
	defer b.mu.Unlock()

	if r.Tags == nil {
		r.Tags = make(map[string]string)
	}

	if r.RegularExpressionList == nil {
		r.RegularExpressionList = []RegexEntry{}
	}

	b.regexPatternSets[r.ID] = r
	b.regexPatternSetByARN[b.RegexPatternSetARN(r.Name, r.ID, r.Scope)] = r.ID
	b.regexPatternSetByScope[nameScope(r.Name, r.Scope)] = r.ID
}

// AddRuleGroupInternal inserts a RuleGroup directly into the backend.
func AddRuleGroupInternal(b *InMemoryBackend, rg *RuleGroup) {
	b.mu.Lock("AddRuleGroupInternal")
	defer b.mu.Unlock()

	if rg.Tags == nil {
		rg.Tags = make(map[string]string)
	}

	if rg.Rules == nil {
		rg.Rules = []map[string]any{}
	}

	b.ruleGroups[rg.ID] = rg
	b.ruleGroupByARN[b.RuleGroupARN(rg.Name, rg.ID, rg.Scope)] = rg.ID
	b.ruleGroupByNameScope[nameScope(rg.Name, rg.Scope)] = rg.ID
}
