package sesv2

import (
	"fmt"
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

// ListSuppressedDestinations lists all suppressed destinations.
func (b *InMemoryBackend) ListSuppressedDestinations(
	nextToken string,
	pageSize int,
) page.Page[*SuppressedDestination] {
	b.mu.RLock("ListSuppressedDestinations")
	defer b.mu.RUnlock()

	snap := b.suppressedDestinations.Snapshot()

	items := make([]*SuppressedDestination, 0, len(snap))
	for _, d := range snap {
		cp := *d
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}
