package ram

import "sort"

// ListPrincipals returns principal associations for shares, filtered by
// resourceOwner ("SELF" or "OTHER-ACCOUNTS") and share ARN. Sorted by associated entity.
func (b *InMemoryBackend) ListPrincipals(
	resourceOwner, shareARN string,
) []*ResourceShareAssociation {
	b.mu.RLock("ListPrincipals")
	defer b.mu.RUnlock()

	result := make([]*ResourceShareAssociation, 0, len(b.associations))

	for _, a := range b.associations {
		if a.AssociationType != associationTypePrincipal {
			continue
		}

		if a.Status == associationStatusDisassociated {
			continue
		}

		if shareARN != "" && a.ResourceShareARN != shareARN {
			continue
		}

		if resourceOwner != "" && !b.ownerMatchesFilter(a.ResourceShareARN, resourceOwner) {
			continue
		}

		result = append(result, cloneAssociation(a))
	}

	sort.Slice(
		result,
		func(i, j int) bool { return result[i].AssociatedEntity < result[j].AssociatedEntity },
	)

	return result
}
