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

// TaggedResources returns every WAF resource ARN that currently has at least
// one tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for resourceArn, tagMap := range b.tags {
		if len(tagMap) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: resourceArn, Tags: maps.Clone(tagMap)})
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
