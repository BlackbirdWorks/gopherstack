package waf

import (
	"fmt"
	"maps"
	"sort"
	"strings"
)

// TagResource adds tags to a resource identified by ARN.
func (b *InMemoryBackend) TagResource(arn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if err := b.requireTaggableLocked(arn); err != nil {
		return err
	}

	if b.tags[arn] == nil {
		b.tags[arn] = make(map[string]string)
	}

	maps.Copy(b.tags[arn], tags)

	return nil
}

// UntagResource removes tags from a resource identified by ARN.
func (b *InMemoryBackend) UntagResource(arn string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if err := b.requireTaggableLocked(arn); err != nil {
		return err
	}

	for _, k := range keys {
		delete(b.tags[arn], k)
	}

	return nil
}

// ListTagsForResource returns the tags for a resource ARN.
func (b *InMemoryBackend) ListTagsForResource(arn string) ([]Tag, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if err := b.requireTaggableLocked(arn); err != nil {
		return nil, err
	}

	tagMap := b.tags[arn]
	result := make([]Tag, 0, len(tagMap))

	for k, v := range tagMap {
		result = append(result, Tag{Key: k, Value: v})
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Key < result[j].Key })

	return result, nil
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every web ACL, rule, rule group and match set plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))
	seen := make(map[string]struct{}, len(b.tags))

	for key, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		seen[key] = struct{}{}
		out = append(out, TaggedEntry{ARN: key, Tags: maps.Clone(t)})
	}

	if withUntagged {
		for _, key := range b.resourceARNsLocked() {
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				out = append(out, TaggedEntry{ARN: key})
			}
		}
	}

	return out
}

func (b *InMemoryBackend) resourceARNsLocked() []string {
	out := b.resourceARNsPart1Locked()
	out = append(out, b.resourceARNsPart2Locked()...)

	return out
}

func (b *InMemoryBackend) resourceARNsPart1Locked() []string {
	out := make(
		[]string,
		0,
		b.webACLs.Len()+b.rules.Len()+b.rateBasedRules.Len()+b.ruleGroups.Len()+b.ipSets.Len()+
			b.byteMatchSets.Len()+b.sizeConstraintSets.Len()+b.sqlInjectionMatchSets.Len(),
	)

	for _, v := range b.webACLs.All() {
		out = append(out, b.webACLARN(v.WebACLId))
	}

	for _, v := range b.rules.All() {
		out = append(out, b.ruleARN(v.RuleId))
	}

	for _, v := range b.rateBasedRules.All() {
		out = append(out, b.rateBasedRuleARN(v.RuleId))
	}

	for _, v := range b.ruleGroups.All() {
		out = append(out, b.ruleGroupARN(v.RuleGroupId))
	}

	for _, v := range b.ipSets.All() {
		out = append(out, b.ipSetARN(v.IPSetId))
	}

	for _, v := range b.byteMatchSets.All() {
		out = append(out, b.byteMatchSetARN(v.ByteMatchSetId))
	}

	for _, v := range b.sizeConstraintSets.All() {
		out = append(out, b.sizeConstraintSetARN(v.SizeConstraintSetId))
	}

	for _, v := range b.sqlInjectionMatchSets.All() {
		out = append(out, b.sqlInjectionMatchSetARN(v.SqlInjectionMatchSetId))
	}

	return out
}

func (b *InMemoryBackend) resourceARNsPart2Locked() []string {
	out := make([]string, 0, b.xssMatchSets.Len()+b.geoMatchSets.Len()+b.regexPatternSets.Len()+b.regexMatchSets.Len())

	for _, v := range b.xssMatchSets.All() {
		out = append(out, b.xssMatchSetARN(v.XssMatchSetId))
	}

	for _, v := range b.geoMatchSets.All() {
		out = append(out, b.geoMatchSetARN(v.GeoMatchSetId))
	}

	for _, v := range b.regexPatternSets.All() {
		out = append(out, b.regexPatternSetARN(v.RegexPatternSetId))
	}

	for _, v := range b.regexMatchSets.All() {
		out = append(out, b.regexMatchSetARN(v.RegexMatchSetId))
	}

	return out
}

const wafARNFields = 6

// requireTaggableLocked rejects a ResourceARN that does not name an existing WAF resource.
func (b *InMemoryBackend) requireTaggableLocked(resourceARN string) error {
	parts := strings.SplitN(resourceARN, ":", wafARNFields)
	if len(parts) == wafARNFields && parts[0] == "arn" && (parts[2] == "waf" || parts[2] == "waf-regional") {
		if kind, id, ok := strings.Cut(parts[5], "/"); ok && b.resourceExistsLocked(kind, id) {
			return nil
		}
	}

	return fmt.Errorf("%w: resource %q does not exist", ErrNotFound, resourceARN)
}

func (b *InMemoryBackend) resourceExistsLocked(kind, id string) bool {
	switch kind {
	case "webacl":
		return b.webACLs.Has(id)
	case "rule":
		return b.rules.Has(id)
	case "ratebasedrule":
		return b.rateBasedRules.Has(id)
	case "rulegroup":
		return b.ruleGroups.Has(id)
	case "ipset":
		return b.ipSets.Has(id)
	case "bytematchset":
		return b.byteMatchSets.Has(id)
	case "sizeconstraintset":
		return b.sizeConstraintSets.Has(id)
	case "sqlinjectionmatchset":
		return b.sqlInjectionMatchSets.Has(id)
	case "xssmatchset":
		return b.xssMatchSets.Has(id)
	case "geomatchset":
		return b.geoMatchSets.Has(id)
	case "regexpatternset":
		return b.regexPatternSets.Has(id)
	case "regexmatchset":
		return b.regexMatchSets.Has(id)
	}

	return false
}
