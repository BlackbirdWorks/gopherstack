package sesv2

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// SuppressedDestination stores a suppressed email address. TenantName is ""
// for the account-level suppression list; a real client targets a specific
// tenant's own, independent suppression list by supplying TenantName.
type SuppressedDestination struct {
	LastUpdateTime time.Time `json:"lastUpdateTime"`
	EmailAddress   string    `json:"emailAddress"`
	Reason         string    `json:"reason"`
	TenantName     string    `json:"tenantName,omitempty"`
}

// suppressedDestinationKey builds the composite key that scopes a suppressed
// address to its account-level ("") or tenant-level suppression list -- the
// same address can be independently suppressed at the account level and at
// any number of tenants' levels.
func suppressedDestinationKey(tenantName, email string) string {
	return tenantName + "\x00" + email
}

// PutSuppressedDestination adds or updates a suppressed destination, scoped
// to tenantName ("" targets the account-level suppression list).
func (b *InMemoryBackend) PutSuppressedDestination(email, reason, tenantName string) error {
	b.mu.Lock("PutSuppressedDestination")
	defer b.mu.Unlock()

	b.suppressedDestinations.Put(&SuppressedDestination{
		EmailAddress:   email,
		Reason:         reason,
		TenantName:     tenantName,
		LastUpdateTime: time.Now(),
	})

	return nil
}

// GetSuppressedDestination retrieves a suppressed destination, scoped to
// tenantName ("" targets the account-level suppression list).
func (b *InMemoryBackend) GetSuppressedDestination(email, tenantName string) (*SuppressedDestination, error) {
	b.mu.RLock("GetSuppressedDestination")
	defer b.mu.RUnlock()

	dest, ok := b.suppressedDestinations.Get(suppressedDestinationKey(tenantName, email))
	if !ok {
		return nil, fmt.Errorf("%w: suppressed destination %s not found", ErrNotFound, email)
	}

	cp := *dest

	return &cp, nil
}

// DeleteSuppressedDestination removes a suppressed destination, scoped to
// tenantName ("" targets the account-level suppression list).
func (b *InMemoryBackend) DeleteSuppressedDestination(email, tenantName string) error {
	b.mu.Lock("DeleteSuppressedDestination")
	defer b.mu.Unlock()

	key := suppressedDestinationKey(tenantName, email)
	if !b.suppressedDestinations.Has(key) {
		return fmt.Errorf("%w: suppressed destination %s not found", ErrNotFound, email)
	}

	b.suppressedDestinations.Delete(key)

	return nil
}

// ListSuppressedDestinations lists suppressed destinations for tenantName's
// suppression list ("" lists the account-level list), optionally filtered by
// reason and/or LastUpdateTime bounds.
func (b *InMemoryBackend) ListSuppressedDestinations(
	reasons []string,
	startDate, endDate *time.Time,
	tenantName, nextToken string,
	pageSize int,
) page.Page[*SuppressedDestination] {
	b.mu.RLock("ListSuppressedDestinations")
	defer b.mu.RUnlock()

	snap := b.suppressedDestinations.Snapshot()

	items := make([]*SuppressedDestination, 0, len(snap))
	for _, d := range snap {
		if d.TenantName != tenantName {
			continue
		}

		if len(reasons) > 0 && !slices.Contains(reasons, d.Reason) {
			continue
		}

		if startDate != nil && d.LastUpdateTime.Before(*startDate) {
			continue
		}

		if endDate != nil && d.LastUpdateTime.After(*endDate) {
			continue
		}

		cp := *d
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}
