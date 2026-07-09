package wafv2

import (
	"context"
	"encoding/json"
)

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

	return b.CreateWebACL(context.Background(), name, scope, description, da, nil, nil, nil, nil, nil, nil, nil, tags)
}

// WebACLCount returns the number of WebACLs in the backend (across all regions).
func WebACLCount(b *InMemoryBackend) int {
	b.mu.RLock("WebACLCount")
	defer b.mu.RUnlock()

	return b.webACLs.Len()
}

// IPSetCount returns the number of IP sets in the backend (across all regions).
func IPSetCount(b *InMemoryBackend) int {
	b.mu.RLock("IPSetCount")
	defer b.mu.RUnlock()

	return b.ipSets.Len()
}

// RegexPatternSetCount returns the number of regex pattern sets in the backend (across all regions).
func RegexPatternSetCount(b *InMemoryBackend) int {
	b.mu.RLock("RegexPatternSetCount")
	defer b.mu.RUnlock()

	return b.regexPatternSets.Len()
}

// RuleGroupCount returns the number of rule groups in the backend (across all regions).
func RuleGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("RuleGroupCount")
	defer b.mu.RUnlock()

	return b.ruleGroups.Len()
}

// APIKeyCount returns the number of API keys in the backend (across all regions).
func APIKeyCount(b *InMemoryBackend) int {
	b.mu.RLock("APIKeyCount")
	defer b.mu.RUnlock()

	return b.apiKeys.Len()
}

// AssociationCount returns the number of WebACL-to-resource associations (across all regions).
func AssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("AssociationCount")
	defer b.mu.RUnlock()

	total := 0
	for _, m := range b.associations {
		total += len(m)
	}

	return total
}

// HandlerOpsLen returns the number of supported operations in the handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// HandlerDispatchOpsLen returns the number of entries in the pre-built dispatch table.
func HandlerDispatchOpsLen(h *Handler) int {
	return len(h.ops)
}

// MaxRegions exposes the per-region map cap for test assertions.
const MaxRegions = maxRegions

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

	// ARN is always (re)computed here, mirroring the pre-store.Table behavior
	// where the by-ARN index was populated from a freshly built ARN string
	// regardless of any value the caller may have set on w.ARN. store.Table's
	// primary key and "arn" index are both derived from w.ARN (see
	// store_setup.go), so it must be set for this record to be reachable by ARN.
	w.ARN = b.WebACLARN(w.Name, w.ID, w.Scope)
	b.webACLs.Put(w)
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

	s.ARN = b.IPSetARN(s.Name, s.ID, s.Scope)
	b.ipSets.Put(s)
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

	r.ARN = b.RegexPatternSetARN(r.Name, r.ID, r.Scope)
	b.regexPatternSets.Put(r)
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

	rg.ARN = b.RuleGroupARN(rg.Name, rg.ID, rg.Scope)
	b.ruleGroups.Put(rg)
}
