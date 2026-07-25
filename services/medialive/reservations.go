package medialive

import (
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Offering operations ---

// ListOfferings returns the seeded offering catalog.
func (b *InMemoryBackend) ListOfferings(
	maxResults int,
	nextToken string,
) ([]*Offering, string, error) {
	b.mu.RLock("ListOfferings")
	defer b.mu.RUnlock()
	pg := page.New(b.offerings, nextToken, maxResults, defaultMaxResults)
	result := make([]*Offering, len(pg.Data))
	copy(result, pg.Data)

	return result, pg.Next, nil
}

// DescribeOffering returns a single offering by ID.
func (b *InMemoryBackend) DescribeOffering(offeringID string) (*Offering, error) {
	b.mu.RLock("DescribeOffering")
	defer b.mu.RUnlock()
	for _, o := range b.offerings {
		if o.OfferingID == offeringID {
			cp := *o

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: offering %s not found", ErrNotFound, offeringID)
}

// --- Reservation operations ---

// PurchaseOffering creates a Reservation from an Offering.
func (b *InMemoryBackend) PurchaseOffering(
	offeringID, name string,
	count int32,
	renewalSettings RenewalSettings,
	tags map[string]string,
) (*Reservation, error) {
	b.mu.Lock("PurchaseOffering")
	defer b.mu.Unlock()
	var off *Offering
	for _, o := range b.offerings {
		if o.OfferingID == offeringID {
			cp := *o
			off = &cp

			break
		}
	}
	if off == nil {
		return nil, fmt.Errorf("%w: offering %s not found", ErrNotFound, offeringID)
	}
	if count <= 0 {
		count = 1
	}
	id := newID()
	r := &storedReservation{
		Tags:                  copyTags(tags),
		ResourceSpecification: off.ResourceSpecification,
		RenewalSettings:       renewalSettings,
		Arn:                   b.reservationARN(id),
		ReservationID:         id,
		Name:                  name,
		OfferingID:            off.OfferingID,
		OfferingDescription:   off.OfferingDescription,
		OfferingType:          off.OfferingType,
		CurrencyCode:          off.CurrencyCode,
		FixedPrice:            off.FixedPrice,
		UsagePrice:            off.UsagePrice,
		Duration:              off.Duration,
		DurationUnits:         off.DurationUnits,
		Start:                 "2024-01-01T00:00:00Z",
		End:                   "2025-01-01T00:00:00Z",
		Region:                b.region,
		State:                 "ACTIVE",
		Count:                 count,
	}
	b.reservations.Put(r)

	return r.toReservation(), nil
}

// ListReservations returns all reservations.
func (b *InMemoryBackend) ListReservations(
	maxResults int,
	nextToken string,
) ([]*Reservation, string, error) {
	b.mu.RLock("ListReservations")
	defer b.mu.RUnlock()
	all := b.reservations.All()
	sort.Slice(all, func(i, j int) bool { return all[i].ReservationID < all[j].ReservationID })
	pg := page.New(all, nextToken, maxResults, defaultMaxResults)
	result := make([]*Reservation, 0, len(pg.Data))
	for _, r := range pg.Data {
		result = append(result, r.toReservation())
	}

	return result, pg.Next, nil
}

// DescribeReservation returns a single reservation.
func (b *InMemoryBackend) DescribeReservation(reservationID string) (*Reservation, error) {
	b.mu.RLock("DescribeReservation")
	defer b.mu.RUnlock()
	r, ok := b.reservations.Get(reservationID)
	if !ok {
		return nil, fmt.Errorf("%w: reservation %s not found", ErrNotFound, reservationID)
	}

	return r.toReservation(), nil
}

// DeleteReservation cancels a reservation.
func (b *InMemoryBackend) DeleteReservation(reservationID string) (*Reservation, error) {
	b.mu.Lock("DeleteReservation")
	defer b.mu.Unlock()
	r, ok := b.reservations.Get(reservationID)
	if !ok {
		return nil, fmt.Errorf("%w: reservation %s not found", ErrNotFound, reservationID)
	}
	r.State = "CANCELED"
	out := r.toReservation()
	b.reservations.Delete(reservationID)
	delete(b.tags, r.Arn)

	return out, nil
}

// UpdateReservation updates a reservation's name and, optionally, its
// renewal settings.
func (b *InMemoryBackend) UpdateReservation(
	reservationID, name string,
	renewalSettings RenewalSettings,
	hasRenewalSettings bool,
) (*Reservation, error) {
	b.mu.Lock("UpdateReservation")
	defer b.mu.Unlock()
	r, ok := b.reservations.Get(reservationID)
	if !ok {
		return nil, fmt.Errorf("%w: reservation %s not found", ErrNotFound, reservationID)
	}
	if name != "" {
		r.Name = name
	}
	if hasRenewalSettings {
		r.RenewalSettings = renewalSettings
	}

	return r.toReservation(), nil
}
