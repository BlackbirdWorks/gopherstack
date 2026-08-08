package swf

import (
	"fmt"
	"maps"
	"regexp"
)

// swfARNRegex matches arn:aws:swf:{region}:{account}:/domain/{name}.
var swfARNRegex = regexp.MustCompile(`^arn:aws:swf:[^:]+:[^:]+:/domain/(.+)$`)

// ListTagsForResource returns tags for a resource ARN.
// Returns an error if the ARN is not a valid SWF domain ARN or the domain does not exist.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	if err := b.validateDomainARNLocked(resourceARN); err != nil {
		return nil, err
	}

	tags := b.tags[resourceARN]
	cp := make(map[string]string, len(tags))
	maps.Copy(cp, tags)

	return cp, nil
}

// TagResource adds or updates tags on a resource.
// Validates ARN format, domain existence, tag count and key/value length limits.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	for k, v := range tags {
		if len(k) == 0 || len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key must be 1-%d characters", ErrValidation, maxTagKeyLen)
		}
		if len(v) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value must be 0-%d characters",
				ErrValidation,
				maxTagValueLen,
			)
		}
	}

	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if err := b.validateDomainARNLocked(resourceARN); err != nil {
		return err
	}

	existing := b.tags[resourceARN]
	merged := len(existing)
	for k := range tags {
		if _, alreadyPresent := existing[k]; !alreadyPresent {
			merged++
		}
	}
	if merged > maxTags {
		return fmt.Errorf("%w: resource cannot have more than %d tags", ErrTooManyTags, maxTags)
	}

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}
	maps.Copy(b.tags[resourceARN], tags)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if err := b.validateDomainARNLocked(resourceARN); err != nil {
		return err
	}

	if b.tags[resourceARN] != nil {
		for _, k := range tagKeys {
			delete(b.tags[resourceARN], k)
		}
	}

	return nil
}

// TaggedEntry pairs a resource ARN with its tags.
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every SWF domain ARN that currently has at least one tag
// applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for resourceARN, tags := range b.tags {
		if len(tags) == 0 {
			continue
		}

		result := make(map[string]string, len(tags))
		maps.Copy(result, tags)
		out = append(out, TaggedEntry{ARN: resourceARN, Tags: result})
	}

	return out
}

// validateDomainARNLocked validates a SWF ARN. Caller must hold at least RLock.
func (b *InMemoryBackend) validateDomainARNLocked(arn string) error {
	m := swfARNRegex.FindStringSubmatch(arn)
	if m == nil {
		return fmt.Errorf("%w: invalid SWF domain ARN: %s", ErrValidation, arn)
	}
	domainName := m[1]
	if !b.domains.Has(domainName) {
		return fmt.Errorf("%w: domain %s not found for ARN %s", ErrNotFound, domainName, arn)
	}

	return nil
}
