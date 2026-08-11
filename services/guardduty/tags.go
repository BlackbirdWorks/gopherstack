package guardduty

import (
	"maps"
	"strings"
)

// TagResource sets tags on a resource.
func (b *InMemoryBackend) TagResource(resourceARN string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		b.tags[resourceARN] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceARN], tags)
	b.syncResourceTagsFromARN(resourceARN)

	return nil
}

// UntagResource removes tags from a resource.
func (b *InMemoryBackend) UntagResource(resourceARN string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if b.tags[resourceARN] == nil {
		return nil
	}

	for _, k := range tagKeys {
		delete(b.tags[resourceARN], k)
	}

	b.syncResourceTagsFromARN(resourceARN)

	return nil
}

// syncResourceTagsFromARN propagates b.tags[resourceARN] -- the value
// TagResource/UntagResource/ListTagsForResource treat as authoritative --
// into the owning resource's own Tags field.
//
// Detector/Filter/IPSet/ThreatIntelSet/ThreatEntitySet/TrustedEntitySet/
// MalwareProtectionPlan/PublishingDestination each embed a frozen Tags copy
// set once at creation time and returned by Get*/Describe* (matching the
// real GetDetectorOutput.Tags etc. wire shapes). Without this sync, calling
// TagResource then GetDetector would show the OLD tags while
// ListTagsForResource showed the NEW ones -- real AWS keeps both views
// consistent. Callers must hold b.mu for writing.
func (b *InMemoryBackend) syncResourceTagsFromARN(resourceARN string) {
	segs := strings.Split(arnResourcePart(resourceARN), "/")
	tags := maps.Clone(b.tags[resourceARN])

	const (
		segDetectorPrefix = 2
		segNestedPrefix   = 4
	)

	switch {
	case len(segs) == segDetectorPrefix && segs[0] == pathDetector:
		if d, ok := b.detectors.Get(segs[1]); ok {
			d.Tags = tags
		}

	case len(segs) == segNestedPrefix && segs[0] == pathDetector:
		b.syncNestedResourceTags(segs[1], segs[2], segs[3], tags)

	case len(segs) == segDetectorPrefix && segs[0] == "malware-protection-plan":
		if p, ok := b.malwareProtectionPlans.Get(segs[1]); ok {
			p.Tags = tags
		}
	}
}

// syncNestedResourceTags is syncResourceTagsFromARN's helper for the
// detector/{id}/{collection}/{id} ARN shape shared by Filter, IPSet,
// ThreatIntelSet, ThreatEntitySet, TrustedEntitySet, and
// PublishingDestination.
func (b *InMemoryBackend) syncNestedResourceTags(detectorID, collection, id string, tags map[string]string) {
	key := detectorKey(detectorID, id)

	switch collection {
	case pathFilter:
		if f, ok := b.filters.Get(key); ok {
			f.Tags = tags
		}
	case pathIPSet:
		if s, ok := b.ipSets.Get(key); ok {
			s.Tags = tags
		}
	case pathThreatIntelSet:
		if s, ok := b.threatIntelSets.Get(key); ok {
			s.Tags = tags
		}
	case pathThreatEntitySet:
		if s, ok := b.threatEntitySets.Get(key); ok {
			s.Tags = tags
		}
	case pathTrustedEntitySet:
		if s, ok := b.trustedEntitySets.Get(key); ok {
			s.Tags = tags
		}
	case pathPublishingDestination:
		if d, ok := b.publishingDestinations.Get(key); ok {
			d.Tags = tags
		}
	}
}

// arnResourcePart returns the resource component of an ARN
// (arn:partition:service:region:account:resource), or "" if resourceARN is
// not a well-formed 6-colon-part ARN.
func arnResourcePart(resourceARN string) string {
	parts := strings.SplitN(resourceARN, ":", arnPartCount)
	if len(parts) < arnPartCount {
		return ""
	}

	return parts[arnPartCount-1]
}

// TaggedEntry pairs a resource ARN with its tag map, for cross-service tag
// enumeration by the Resource Groups Tagging API (see cli.go's wireTaggingGuardDuty).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every GuardDuty resource ARN (detectors, filters,
// IP sets, threat intel/entity sets, trusted entity sets, publishing
// destinations, malware protection plans) that currently has at least one
// tag applied via TagResource.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))

	for arn, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		out = append(out, TaggedEntry{ARN: arn, Tags: maps.Clone(t)})
	}

	return out
}

// ListTagsForResource returns tags for a resource.
func (b *InMemoryBackend) ListTagsForResource(resourceARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	t := b.tags[resourceARN]
	if t == nil {
		return map[string]string{}, nil
	}

	return maps.Clone(t), nil
}
