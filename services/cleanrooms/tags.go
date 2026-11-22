package cleanrooms

import "maps"

// resourceARNExists reports whether resourceArn identifies a live resource in
// any taggable collection. TagResource/UntagResource/ListTagsForResource all
// document ResourceNotFoundException for an unknown ARN (see AWS API model),
// but tagsByArn only gains an entry once a resource is actually tagged, so an
// existing-but-never-tagged resource must not be confused with a resource
// that was never created. Callers must hold b.mu (read or write).
func (b *InMemoryBackend) resourceARNExists(resourceArn string) bool {
	found := false
	stop := func(match bool) bool {
		if match {
			found = true
		}

		return !match
	}
	b.collaborations.Range(func(v *Collaboration) bool { return stop(v.Arn == resourceArn) })
	if found {
		return true
	}
	b.memberships.Range(func(v *Membership) bool { return stop(v.Arn == resourceArn) })
	if found {
		return true
	}
	b.configuredTables.Range(func(v *ConfiguredTable) bool { return stop(v.Arn == resourceArn) })
	if found {
		return true
	}
	b.ctAssociations.Range(func(v *ConfiguredTableAssociation) bool { return stop(v.Arn == resourceArn) })
	if found {
		return true
	}
	b.analysisTemplates.Range(func(v *AnalysisTemplate) bool { return stop(v.Arn == resourceArn) })
	if found {
		return true
	}
	b.privacyBudgetTemplates.Range(func(v *PrivacyBudgetTemplate) bool { return stop(v.Arn == resourceArn) })
	if found {
		return true
	}
	b.idMappingTables.Range(func(v *IDMappingTable) bool { return stop(v.Arn == resourceArn) })
	if found {
		return true
	}
	b.idNamespaceAssociations.Range(func(v *IDNamespaceAssociation) bool { return stop(v.Arn == resourceArn) })
	if found {
		return true
	}
	b.camaAssociations.Range(func(v *ConfiguredAudienceModelAssociation) bool { return stop(v.Arn == resourceArn) })
	if found {
		return true
	}
	b.intermediateTables.Range(func(v *IntermediateTable) bool { return stop(v.Arn == resourceArn) })

	return found
}

func (b *InMemoryBackend) ListTagsForResource(resourceArn string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()
	if tags, ok := b.tagsByArn[resourceArn]; ok {
		return maps.Clone(tags), nil
	}
	if !b.resourceARNExists(resourceArn) {
		return nil, ErrNotFound
	}

	return map[string]string{}, nil
}

func (b *InMemoryBackend) TagResource(resourceArn string, tags map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()
	if !b.resourceARNExists(resourceArn) {
		return ErrNotFound
	}
	if b.tagsByArn[resourceArn] == nil {
		b.tagsByArn[resourceArn] = make(map[string]string)
	}
	maps.Copy(b.tagsByArn[resourceArn], tags)

	return nil
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

// InventoryResources lists every collaboration, membership, configured table, association, analysis template,
// privacy budget template, ID mapping table, ID namespace association and intermediate table plus every
// tagged ARN of the tag store.
func (b *InMemoryBackend) InventoryResources() []TaggedEntry {
	return b.entries(true)
}

func (b *InMemoryBackend) entries(withUntagged bool) []TaggedEntry {
	b.mu.RLock("TaggedResources")
	defer b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(b.tagsByArn))
	seen := make(map[string]struct{}, len(b.tagsByArn))

	for key, t := range b.tagsByArn {
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
	out := make(
		[]string,
		0,
		b.collaborations.Len()+b.memberships.Len()+b.configuredTables.Len()+b.ctAssociations.Len()+
			b.analysisTemplates.Len()+b.privacyBudgetTemplates.Len()+b.idMappingTables.Len()+
			b.idNamespaceAssociations.Len()+b.camaAssociations.Len()+b.intermediateTables.Len(),
	)

	for _, c := range b.collaborations.All() {
		out = append(out, c.Arn)
	}

	for _, m := range b.memberships.All() {
		out = append(out, m.Arn)
	}

	for _, c := range b.configuredTables.All() {
		out = append(out, c.Arn)
	}

	for _, a := range b.ctAssociations.All() {
		out = append(out, a.Arn)
	}

	for _, a := range b.analysisTemplates.All() {
		out = append(out, a.Arn)
	}

	for _, p := range b.privacyBudgetTemplates.All() {
		out = append(out, p.Arn)
	}

	for _, t := range b.idMappingTables.All() {
		out = append(out, t.Arn)
	}

	for _, a := range b.idNamespaceAssociations.All() {
		out = append(out, a.Arn)
	}

	for _, a := range b.camaAssociations.All() {
		out = append(out, a.Arn)
	}

	for _, t := range b.intermediateTables.All() {
		out = append(out, t.Arn)
	}

	return out
}

func (b *InMemoryBackend) UntagResource(resourceArn string, tagKeys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()
	if !b.resourceARNExists(resourceArn) {
		return ErrNotFound
	}
	tags := b.tagsByArn[resourceArn]
	for _, k := range tagKeys {
		delete(tags, k)
	}

	return nil
}
