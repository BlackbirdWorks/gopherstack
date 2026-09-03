package workspaces

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// connectionAliasesPageSize and connectionAliasPermissionsPageSize are this
// backend's default page sizes; real AWS doesn't document exact defaults for
// either operation, so these are chosen generously (larger than any
// realistic per-account alias or per-alias shared-account count) so
// pagination only activates when a caller explicitly requests a smaller
// MaxResults/Limit.
const (
	connectionAliasesPageSize          = 100
	connectionAliasPermissionsPageSize = 100
)

// CreateConnectionAlias creates a new connection alias.
func (b *InMemoryBackend) CreateConnectionAlias(
	connectionString string, tags map[string]string,
) (string, error) {
	b.mu.Lock("CreateConnectionAlias")
	defer b.mu.Unlock()

	id := b.nextID("wsca-")
	stored := cloneTags(tags)
	b.connAliases.Put(&storedConnAlias{
		AliasID:          id,
		ConnectionString: connectionString,
		State:            "CREATED",
		OwnerAccountID:   b.accountID,
		Tags:             stored,
	})
	b.tags[id] = stored

	return id, nil
}

// DescribeConnectionAliases returns connection aliases filtered by IDs or resource.
func (b *InMemoryBackend) DescribeConnectionAliases(
	aliasIDs []string, resourceID string, limit int32, nextToken string,
) ([]*storedConnAlias, string, error) {
	b.mu.RLock("DescribeConnectionAliases")
	defer b.mu.RUnlock()

	filter := buildFilter(aliasIDs)
	all := b.connAliases.All()

	sort.Slice(all, func(i, j int) bool { return all[i].AliasID < all[j].AliasID })

	result := make([]*storedConnAlias, 0, len(all))

	for _, a := range all {
		if !matchesFilter(filter, a.AliasID) {
			continue
		}

		if resourceID != "" && a.AssociatedResource != resourceID {
			continue
		}

		cp := *a
		result = append(result, &cp)
	}

	pg := page.New(result, nextToken, int(limit), connectionAliasesPageSize)

	return pg.Data, pg.Next, nil
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

// DescribeConnectionAliasPermissions returns a page of shared-account
// permissions for an alias, in the order they were granted.
func (b *InMemoryBackend) DescribeConnectionAliasPermissions(
	aliasID string, maxResults int32, nextToken string,
) (string, []connAliasPermission, string, error) {
	b.mu.RLock("DescribeConnectionAliasPermissions")
	defer b.mu.RUnlock()

	a, ok := b.connAliases.Get(aliasID)
	if !ok {
		return "", nil, "", errConnAliasNotFound
	}

	all := make([]connAliasPermission, len(a.SharedAccounts))
	copy(all, a.SharedAccounts)

	pg := page.New(all, nextToken, int(maxResults), connectionAliasPermissionsPageSize)

	return aliasID, pg.Data, pg.Next, nil
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
