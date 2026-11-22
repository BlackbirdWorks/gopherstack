package guardduty

import (
	"maps"
	"strings"
	"time"
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

// TaggedResources lists every tag-store ARN that carries at least one tag.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	return b.entries(false)
}

// InventoryResources lists every detector, filter, set, destination, malware protection plan and live custom
// detection rule association plus every tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tags))
	seen := make(map[string]struct{}, len(b.tags))

	for arn, t := range b.tags {
		if len(t) == 0 {
			continue
		}

		seen[arn] = struct{}{}
		out = append(out, TaggedEntry{ARN: arn, Tags: maps.Clone(t)})
	}

	if withUntagged {
		for _, arn := range b.resourceARNsLocked() {
			if _, ok := seen[arn]; !ok {
				seen[arn] = struct{}{}
				out = append(out, TaggedEntry{ARN: arn})
			}
		}
	}

	return out
}

func (b *InMemoryBackend) resourceARNsLocked() []string {
	out := make(
		[]string,
		0,
		b.detectors.Len()+b.filters.Len()+b.ipSets.Len()+b.threatIntelSets.Len()+b.threatEntitySets.Len()+
			b.trustedEntitySets.Len()+b.publishingDestinations.Len()+b.malwareProtectionPlans.Len()+
			b.customRuleAssociations.Len(),
	)

	for _, d := range b.detectors.All() {
		out = append(out, b.detectorARN(d.DetectorID))
	}

	for _, f := range b.filters.All() {
		out = append(out, b.filterARN(f.DetectorID, f.Name))
	}

	for _, s := range b.ipSets.All() {
		out = append(out, b.ipSetARN(s.DetectorID, s.IPSetID))
	}

	for _, s := range b.threatIntelSets.All() {
		out = append(out, b.threatIntelSetARN(s.DetectorID, s.ThreatIntelSetID))
	}

	for _, s := range b.threatEntitySets.All() {
		out = append(out, b.threatEntitySetARN(s.DetectorID, s.ThreatEntitySetID))
	}

	for _, s := range b.trustedEntitySets.All() {
		out = append(out, b.trustedEntitySetARN(s.DetectorID, s.TrustedEntitySetID))
	}

	for _, d := range b.publishingDestinations.All() {
		out = append(out, b.publishingDestinationARN(d.DetectorID, d.DestinationID))
	}

	for _, p := range b.malwareProtectionPlans.All() {
		out = append(out, p.Arn)
	}

	now := time.Now()

	for _, a := range b.customRuleAssociations.All() {
		if a.AccountID == b.accountID && !expired(a.ExpiresAt, now) {
			out = append(out, b.customRuleAssociationARN(a.RuleID, a.AssociationID))
		}
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
