package sesv2

import (
	"fmt"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// SuppressedDestination stores a suppressed email address.
type SuppressedDestination struct {
	LastUpdateTime time.Time `json:"lastUpdateTime"`
	EmailAddress   string    `json:"emailAddress"`
	Reason         string    `json:"reason"`
}

// PutSuppressedDestination adds or updates a suppressed destination.
func (b *InMemoryBackend) PutSuppressedDestination(email, reason string) error {
	b.mu.Lock("PutSuppressedDestination")
	defer b.mu.Unlock()

	b.suppressedDestinations.Put(&SuppressedDestination{
		EmailAddress:   email,
		Reason:         reason,
		LastUpdateTime: time.Now(),
	})

	return nil
}

// GetSuppressedDestination retrieves a suppressed destination.
func (b *InMemoryBackend) GetSuppressedDestination(email string) (*SuppressedDestination, error) {
	b.mu.RLock("GetSuppressedDestination")
	defer b.mu.RUnlock()

	dest, ok := b.suppressedDestinations.Get(email)
	if !ok {
		return nil, fmt.Errorf("%w: suppressed destination %s not found", ErrNotFound, email)
	}

	cp := *dest

	return &cp, nil
}

// DeleteSuppressedDestination removes a suppressed destination.
func (b *InMemoryBackend) DeleteSuppressedDestination(email string) error {
	b.mu.Lock("DeleteSuppressedDestination")
	defer b.mu.Unlock()

	if !b.suppressedDestinations.Has(email) {
		return fmt.Errorf("%w: suppressed destination %s not found", ErrNotFound, email)
	}

	b.suppressedDestinations.Delete(email)

	return nil
}

// ListSuppressedDestinations lists suppressed destinations, optionally
// filtered by reason and/or LastUpdateTime bounds. TenantName is not
// honored: SuppressedDestination doesn't track which tenant (if any) added
// it, and there is no separate per-tenant suppression list store.
func (b *InMemoryBackend) ListSuppressedDestinations(
	reasons []string,
	startDate, endDate *time.Time,
	nextToken string,
	pageSize int,
) page.Page[*SuppressedDestination] {
	b.mu.RLock("ListSuppressedDestinations")
	defer b.mu.RUnlock()

	snap := b.suppressedDestinations.Snapshot()

	items := make([]*SuppressedDestination, 0, len(snap))
	for _, d := range snap {
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
