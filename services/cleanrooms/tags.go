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
