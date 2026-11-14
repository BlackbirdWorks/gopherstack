package ram

// ResourceSharedWith reports whether an active resource share associates resourceARN with accountID as a principal.
func (b *InMemoryBackend) ResourceSharedWith(resourceARN, accountID string) bool {
	b.mu.RLock("ResourceSharedWith")
	defer b.mu.RUnlock()

	for _, r := range b.associations {
		if r.AssociationType != associationTypeResource || r.Status != associationStatusAssociated ||
			r.AssociatedEntity != resourceARN {
			continue
		}

		rs, ok := b.resourceShares.Get(r.ResourceShareARN)
		if !ok || rs.Status != statusActive {
			continue
		}

		for _, p := range b.associations {
			if p.AssociationType == associationTypePrincipal && p.Status == associationStatusAssociated &&
				p.ResourceShareARN == r.ResourceShareARN && p.AssociatedEntity == accountID {
				return true
			}
		}
	}

	return false
}
