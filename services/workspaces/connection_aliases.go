package workspaces

import "fmt"

// CreateConnectionAlias creates a new connection alias.
func (b *InMemoryBackend) CreateConnectionAlias(
	connectionString string, tags map[string]string,
) (string, error) {
	b.mu.Lock("CreateConnectionAlias")
	defer b.mu.Unlock()

	id := b.nextID("wsca-")
	b.connAliases.Put(&storedConnAlias{
		AliasID:          id,
		ConnectionString: connectionString,
		State:            "CREATED",
		OwnerAccountID:   b.accountID,
		Tags:             cloneTags(tags),
	})

	return id, nil
}

// DescribeConnectionAliases returns connection aliases filtered by IDs or resource.
func (b *InMemoryBackend) DescribeConnectionAliases(
	aliasIDs []string, resourceID string, _ int32, _ string,
) ([]*storedConnAlias, string, error) {
	b.mu.RLock("DescribeConnectionAliases")
	defer b.mu.RUnlock()

	filter := buildFilter(aliasIDs)
	var result []*storedConnAlias

	for _, a := range b.connAliases.All() {
		if !matchesFilter(filter, a.AliasID) {
			continue
		}

		if resourceID != "" && a.AssociatedResource != resourceID {
			continue
		}

		cp := *a
		result = append(result, &cp)
	}

	if result == nil {
		result = []*storedConnAlias{}
	}

	return result, "", nil
}

// DeleteConnectionAlias removes a connection alias.
func (b *InMemoryBackend) DeleteConnectionAlias(aliasID string) error {
	b.mu.Lock("DeleteConnectionAlias")
	defer b.mu.Unlock()

	if !b.connAliases.Has(aliasID) {
		return errConnAliasNotFound
	}

	b.connAliases.Delete(aliasID)

	return nil
}

// AssociateConnectionAlias associates an alias with a resource.
func (b *InMemoryBackend) AssociateConnectionAlias(aliasID, resourceID string) (string, error) {
	b.mu.Lock("AssociateConnectionAlias")
	defer b.mu.Unlock()

	a, ok := b.connAliases.Get(aliasID)
	if !ok {
		return "", errConnAliasNotFound
	}

	a.AssociatedResource = resourceID
	a.ConnectionIdentifier = fmt.Sprintf("wcci-%08x", b.counter)

	return a.ConnectionIdentifier, nil
}

// DisassociateConnectionAlias removes the resource association from an alias.
func (b *InMemoryBackend) DisassociateConnectionAlias(aliasID string) error {
	b.mu.Lock("DisassociateConnectionAlias")
	defer b.mu.Unlock()

	a, ok := b.connAliases.Get(aliasID)
	if !ok {
		return errConnAliasNotFound
	}

	a.AssociatedResource = ""
	a.ConnectionIdentifier = ""

	return nil
}

// DescribeConnectionAliasPermissions returns shared-account permissions for an alias.
func (b *InMemoryBackend) DescribeConnectionAliasPermissions(
	aliasID string, _ int32, _ string,
) (string, []connAliasPermission, string, error) {
	b.mu.RLock("DescribeConnectionAliasPermissions")
	defer b.mu.RUnlock()

	a, ok := b.connAliases.Get(aliasID)
	if !ok {
		return "", nil, "", errConnAliasNotFound
	}

	perms := make([]connAliasPermission, len(a.SharedAccounts))
	copy(perms, a.SharedAccounts)

	return aliasID, perms, "", nil
}

// UpdateConnectionAliasPermission sets the shared-account permission for an alias.
func (b *InMemoryBackend) UpdateConnectionAliasPermission(
	aliasID, accountID string, allowAssociation bool,
) error {
	b.mu.Lock("UpdateConnectionAliasPermission")
	defer b.mu.Unlock()

	a, ok := b.connAliases.Get(aliasID)
	if !ok {
		return errConnAliasNotFound
	}

	for i, p := range a.SharedAccounts {
		if p.AccountID == accountID {
			a.SharedAccounts[i].AllowAssociation = allowAssociation

			return nil
		}
	}

	a.SharedAccounts = append(a.SharedAccounts, connAliasPermission{
		AccountID:        accountID,
		AllowAssociation: allowAssociation,
	})

	return nil
}
