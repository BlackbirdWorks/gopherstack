package securityhub

import "maps"

func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	if b.tags[resourceArn] == nil {
		b.tags[resourceArn] = make(map[string]string)
	}

	maps.Copy(b.tags[resourceArn], tags)

	return nil
}

func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	if existing, ok := b.tags[resourceArn]; ok {
		for _, k := range tagKeys {
			delete(existing, k)
		}
	}

	return nil
}

func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	result := make(map[string]string)

	if existing, ok := b.tags[resourceArn]; ok {
		maps.Copy(result, existing)
	}

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

// InventoryResources lists every hub, automation rule, configuration policy, connector, aggregator, insight,
// action target and finding aggregator plus every tagged ARN of the tag store.
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
		b.automationRules.Len()+b.automationRulesV2.Len()+b.configPolicies.Len()+b.cspmConnectors.Len()+
			b.connectorsV2.Len()+b.aggregatorsV2.Len(),
	)

	if b.hub != nil {
		out = append(out, b.hub.HubArn)
	}

	if b.hubV2 != nil {
		out = append(out, b.hubV2.HubV2Arn)
	}

	for _, r := range b.automationRules.All() {
		out = append(out, r.RuleArn)
	}

	for _, r := range b.automationRulesV2.All() {
		out = append(out, r.RuleArn)
	}

	for _, p := range b.configPolicies.All() {
		out = append(out, p.Arn)
	}

	for _, c := range b.cspmConnectors.All() {
		out = append(out, c.ConnectorArn)
	}

	for _, c := range b.connectorsV2.All() {
		out = append(out, c.ConnectorArn)
	}

	for _, a := range b.aggregatorsV2.All() {
		out = append(out, a.AggregatorV2Arn)
	}

	return out
}

func (b *InMemoryBackend) resourceARNsPart2Locked() []string {
	out := make([]string, 0, b.insights.Len()+b.actionTargets.Len()+b.findingAggregators.Len())

	for _, i := range b.insights.All() {
		out = append(out, i.InsightArn)
	}

	for _, t := range b.actionTargets.All() {
		out = append(out, t.ActionTargetArn)
	}

	for _, a := range b.findingAggregators.All() {
		out = append(out, a.FindingAggregatorArn)
	}

	return out
}
