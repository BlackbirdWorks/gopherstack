package ec2

import (
	"errors"
	"fmt"
	"maps"
	"sort"
	"time"

	"github.com/google/uuid"
)

// ErrHostReservationNotFound is returned when a Dedicated Host Reservation is not found.
var ErrHostReservationNotFound = errors.New("InvalidHostReservationID.NotFound")

// ErrHostReservationOfferingNotFound is returned when a Dedicated Host Reservation
// offering ID does not match the static catalog.
var ErrHostReservationOfferingNotFound = errors.New("InvalidOfferingID.NotFound")

// ---- constants ----

const (
	hostReservationCurrencyUSD    = "USD"
	hostReservationDuration1Year  = 31536000
	hostReservationDuration3Years = 94608000
	hostReservationIDPrefixLength = 17

	// hostReservationZeroPrice is the "no cost for this component" price string used
	// by offerings that charge nothing upfront (NoUpfront) or nothing hourly
	// (AllUpfront).
	hostReservationZeroPrice = "0.00"
)

// ---- models ----

// HostOffering describes a purchasable Dedicated Host Reservation offering. The
// catalog is a static, realistic set of offerings (mirroring AWS's own fixed
// catalog); it is not backed by a map since offerings are not created or deleted.
type HostOffering struct {
	OfferingID     string `json:"offeringId,omitempty"`
	InstanceFamily string `json:"instanceFamily,omitempty"`
	PaymentOption  string `json:"paymentOption,omitempty"`
	HourlyPrice    string `json:"hourlyPrice,omitempty"`
	UpfrontPrice   string `json:"upfrontPrice,omitempty"`
	CurrencyCode   string `json:"currencyCode,omitempty"`
	Duration       int32  `json:"duration,omitempty"`
}

// hostReservationOfferingCatalog is the static, realistic set of Dedicated Host
// Reservation offerings this mock exposes via DescribeHostReservationOfferings.
//
//nolint:gochecknoglobals // static read-only catalog, analogous to a lookup table
var hostReservationOfferingCatalog = []HostOffering{
	{
		OfferingID:     "hro-03f1c2a7d6e8b9012",
		InstanceFamily: "m5",
		PaymentOption:  "NoUpfront",
		Duration:       hostReservationDuration1Year,
		HourlyPrice:    "0.238",
		UpfrontPrice:   hostReservationZeroPrice,
		CurrencyCode:   hostReservationCurrencyUSD,
	},
	{
		OfferingID:     "hro-14a2d3b8e7f9c0123",
		InstanceFamily: "m5",
		PaymentOption:  "AllUpfront",
		Duration:       hostReservationDuration3Years,
		HourlyPrice:    hostReservationZeroPrice,
		UpfrontPrice:   "3831.00",
		CurrencyCode:   hostReservationCurrencyUSD,
	},
	{
		OfferingID:     "hro-25b3e4c9f8a0d1234",
		InstanceFamily: "c5",
		PaymentOption:  "PartialUpfront",
		Duration:       hostReservationDuration1Year,
		HourlyPrice:    "0.119",
		UpfrontPrice:   "521.00",
		CurrencyCode:   hostReservationCurrencyUSD,
	},
	{
		OfferingID:     "hro-36c4f5d0a9b1e2345",
		InstanceFamily: "r5",
		PaymentOption:  "NoUpfront",
		Duration:       hostReservationDuration3Years,
		HourlyPrice:    "0.298",
		UpfrontPrice:   hostReservationZeroPrice,
		CurrencyCode:   hostReservationCurrencyUSD,
	},
}

// HostReservationPurchase describes a single line item of a host reservation
// purchase (or preview thereof). HostReservationID is empty for previews, since no
// reservation is created until PurchaseHostReservation is called.
type HostReservationPurchase struct {
	HostReservationID string   `json:"hostReservationId,omitempty"`
	InstanceFamily    string   `json:"instanceFamily,omitempty"`
	PaymentOption     string   `json:"paymentOption,omitempty"`
	CurrencyCode      string   `json:"currencyCode,omitempty"`
	HourlyPrice       string   `json:"hourlyPrice,omitempty"`
	UpfrontPrice      string   `json:"upfrontPrice,omitempty"`
	HostIDSet         []string `json:"hostIdSet,omitempty"`
	Duration          int32    `json:"duration,omitempty"`
}

// HostReservationPurchasePreview is the result of GetHostReservationPurchasePreview.
type HostReservationPurchasePreview struct {
	CurrencyCode      string
	TotalHourlyPrice  string
	TotalUpfrontPrice string
	Purchases         []HostReservationPurchase
}

// HostReservation represents a purchased Dedicated Host Reservation.
type HostReservation struct {
	Start             time.Time         `json:"start"`
	End               time.Time         `json:"end"`
	Tags              map[string]string `json:"tags,omitempty"`
	HostReservationID string            `json:"hostReservationId,omitempty"`
	OfferingID        string            `json:"offeringId,omitempty"`
	InstanceFamily    string            `json:"instanceFamily,omitempty"`
	PaymentOption     string            `json:"paymentOption,omitempty"`
	CurrencyCode      string            `json:"currencyCode,omitempty"`
	HourlyPrice       string            `json:"hourlyPrice,omitempty"`
	UpfrontPrice      string            `json:"upfrontPrice,omitempty"`
	State             string            `json:"state,omitempty"`
	HostIDSet         []string          `json:"hostIdSet,omitempty"`
	Duration          int32             `json:"duration,omitempty"`
	Count             int32             `json:"count,omitempty"`
}

func cloneHostReservation(hr *HostReservation) *HostReservation {
	cp := *hr
	cp.Tags = maps.Clone(hr.Tags)
	cp.HostIDSet = append([]string(nil), hr.HostIDSet...)

	return &cp
}

// findHostOffering returns the catalog entry for offeringID, or
// ErrHostReservationOfferingNotFound if it does not match a known offering.
func findHostOffering(offeringID string) (*HostOffering, error) {
	for i := range hostReservationOfferingCatalog {
		if hostReservationOfferingCatalog[i].OfferingID == offeringID {
			return &hostReservationOfferingCatalog[i], nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrHostReservationOfferingNotFound, offeringID)
}

// DescribeHostReservationOfferings returns the static catalog of purchasable
// Dedicated Host Reservation offerings, optionally filtered by offering ID and/or
// min/max duration in seconds. The catalog is static (no Create/Delete API), so this
// method needs no lock.
func (b *InMemoryBackend) DescribeHostReservationOfferings(
	offeringID string,
	minDuration, maxDuration int32,
) []HostOffering {
	out := make([]HostOffering, 0, len(hostReservationOfferingCatalog))

	for _, o := range hostReservationOfferingCatalog {
		if offeringID != "" && o.OfferingID != offeringID {
			continue
		}

		if minDuration > 0 && o.Duration < minDuration {
			continue
		}

		if maxDuration > 0 && o.Duration > maxDuration {
			continue
		}

		out = append(out, o)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].OfferingID < out[j].OfferingID })

	return out
}

// requireHostsExist validates that every host ID names a real Dedicated Host.
// Must be called with b.mu held.
func (b *InMemoryBackend) requireHostsExist(hostIDs []string) error {
	if len(hostIDs) == 0 {
		return fmt.Errorf("%w: HostIdSet is required", ErrInvalidParameter)
	}

	for _, id := range hostIDs {
		if _, ok := b.dedicatedHosts.Get(id); !ok {
			return fmt.Errorf("%w: %s", ErrHostNotFound, id)
		}
	}

	return nil
}

// GetHostReservationPurchasePreview previews the cost of purchasing a Dedicated Host
// Reservation for the given real Dedicated Hosts and offering, without creating one.
func (b *InMemoryBackend) GetHostReservationPurchasePreview(
	hostIDs []string,
	offeringID string,
) (*HostReservationPurchasePreview, error) {
	offering, err := findHostOffering(offeringID)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetHostReservationPurchasePreview")
	defer b.mu.RUnlock()

	if existsErr := b.requireHostsExist(hostIDs); existsErr != nil {
		return nil, existsErr
	}

	return &HostReservationPurchasePreview{
		CurrencyCode:      offering.CurrencyCode,
		TotalHourlyPrice:  totalHostReservationPrice(offering.HourlyPrice, len(hostIDs)),
		TotalUpfrontPrice: totalHostReservationPrice(offering.UpfrontPrice, len(hostIDs)),
		Purchases: []HostReservationPurchase{
			{
				InstanceFamily: offering.InstanceFamily,
				PaymentOption:  offering.PaymentOption,
				CurrencyCode:   offering.CurrencyCode,
				HourlyPrice:    offering.HourlyPrice,
				UpfrontPrice:   offering.UpfrontPrice,
				Duration:       offering.Duration,
				HostIDSet:      append([]string(nil), hostIDs...),
			},
		},
	}, nil
}

// totalHostReservationPrice multiplies a per-host decimal price string by hostCount,
// formatting the result with two decimal places like the source prices.
func totalHostReservationPrice(perHostPrice string, hostCount int) string {
	var perHost float64
	if _, err := fmt.Sscanf(perHostPrice, "%f", &perHost); err != nil {
		return perHostPrice
	}

	return fmt.Sprintf("%.2f", perHost*float64(hostCount))
}

// PurchaseHostReservation purchases a Dedicated Host Reservation for the given real
// Dedicated Hosts and offering, creating real reservation state.
func (b *InMemoryBackend) PurchaseHostReservation(
	hostIDs []string,
	offeringID string,
	tags map[string]string,
) (*HostReservationPurchasePreview, error) {
	offering, err := findHostOffering(offeringID)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("PurchaseHostReservation")
	defer b.mu.Unlock()

	if existsErr := b.requireHostsExist(hostIDs); existsErr != nil {
		return nil, existsErr
	}

	now := time.Now().UTC()
	hr := &HostReservation{
		HostReservationID: "hr-" + uuid.New().String()[:hostReservationIDPrefixLength],
		OfferingID:        offering.OfferingID,
		InstanceFamily:    offering.InstanceFamily,
		PaymentOption:     offering.PaymentOption,
		CurrencyCode:      offering.CurrencyCode,
		HourlyPrice:       offering.HourlyPrice,
		UpfrontPrice:      offering.UpfrontPrice,
		Duration:          offering.Duration,
		Count:             int32(len(hostIDs)), //nolint:gosec // request-bounded host count, not attacker-controlled
		HostIDSet:         append([]string(nil), hostIDs...),
		State:             stateActive,
		Start:             now,
		End:               now.Add(time.Duration(offering.Duration) * time.Second),
		Tags:              maps.Clone(tags),
	}
	b.hostReservations.Put(hr)

	return &HostReservationPurchasePreview{
		CurrencyCode:      offering.CurrencyCode,
		TotalHourlyPrice:  totalHostReservationPrice(offering.HourlyPrice, len(hostIDs)),
		TotalUpfrontPrice: totalHostReservationPrice(offering.UpfrontPrice, len(hostIDs)),
		Purchases: []HostReservationPurchase{
			{
				HostReservationID: hr.HostReservationID,
				InstanceFamily:    offering.InstanceFamily,
				PaymentOption:     offering.PaymentOption,
				CurrencyCode:      offering.CurrencyCode,
				HourlyPrice:       offering.HourlyPrice,
				UpfrontPrice:      offering.UpfrontPrice,
				Duration:          offering.Duration,
				HostIDSet:         append([]string(nil), hostIDs...),
			},
		},
	}, nil
}

// DescribeHostReservations returns purchased Dedicated Host Reservations, optionally
// filtered by reservation ID.
func (b *InMemoryBackend) DescribeHostReservations(ids []string) []*HostReservation {
	b.mu.RLock("DescribeHostReservations")
	defer b.mu.RUnlock()

	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	out := make([]*HostReservation, 0, b.hostReservations.Len())

	for _, hr := range b.hostReservations.All() {
		if len(idSet) > 0 && !idSet[hr.HostReservationID] {
			continue
		}

		out = append(out, cloneHostReservation(hr))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].HostReservationID < out[j].HostReservationID })

	return out
}

// HostReleaseResult reports the outcome of releasing a single Dedicated Host.
type HostReleaseResult struct {
	HostID  string
	Code    string
	Message string
}

// ReleaseHosts releases real Dedicated Hosts, removing them from backend state.
// Hosts that do not exist are reported as unsuccessful rather than causing the whole
// request to fail, matching AWS's per-resource partial-failure behavior.
func (b *InMemoryBackend) ReleaseHosts(hostIDs []string) ([]string, []HostReleaseResult) {
	b.mu.Lock("ReleaseHosts")
	defer b.mu.Unlock()

	var successful []string

	var unsuccessful []HostReleaseResult

	for _, id := range hostIDs {
		if _, ok := b.dedicatedHosts.Get(id); !ok {
			unsuccessful = append(unsuccessful, HostReleaseResult{
				HostID:  id,
				Code:    ErrHostNotFound.Error(),
				Message: fmt.Sprintf("The Dedicated Host ID %q does not exist", id),
			})

			continue
		}
		b.dedicatedHosts.Delete(id)
		delete(b.tags, id)
		successful = append(successful, id)
	}

	return successful, unsuccessful
}
