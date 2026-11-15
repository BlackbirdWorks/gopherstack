package ce

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
)

const (
	hoursPerMonth = 730.0
	// ResourceRetentionDays is how long resource-level rows are kept, per GetCostAndUsageWithResources.
	ResourceRetentionDays = 14
	tagStatusActive       = "Active"
)

// Usage is one metered usage line. Cost is Quantity times the price list rate for PriceService/PriceMatch/Unit.
type Usage struct {
	Start, End time.Time
	Tags       map[string]string
	// PriceMatch selects the price list product by attribute (for example usagetype and operation).
	PriceMatch       map[string]string
	Service          string
	PriceService     string
	UsageType        string
	Operation        string
	Region           string
	AvailabilityZone string
	InstanceType     string
	ResourceID       string
	ProductFamily    string
	UsageTypeGroup   string
	// Unit is the price list unit the Quantity is expressed in (Hrs, GB-Mo, Requests, ...).
	Unit     string
	Account  string
	Quantity float64
}

// Meter records usage lines into the cost ledger.
type Meter interface {
	RecordUsage(u Usage)
}

// PriceLookup returns the On Demand USD price per unit for a price list product.
type PriceLookup func(serviceCode, region string, match map[string]string, unit string) (float64, bool)

// Holding is a resource held over time; Usage.Quantity is the amount accrued per hour held.
type Holding struct{ Usage Usage }

// HoldingSource lists the resources currently held by a service (running instances, stored bytes, ...).
type HoldingSource func() []Holding

// UsageSource returns usage the service accumulated since the previous call (for example request units).
type UsageSource func() []Usage

// Hourly returns the per-hour quantity for gb gigabytes held under a GB-Mo price.
func Hourly(gb float64) float64 { return gb / hoursPerMonth }

// SetClock replaces the time source used by accrual; call before the backend is shared.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clock = now
	b.lastAccrual = now()
}

// SetPriceLookup installs the price list used to cost metered usage; without one metered rows carry zero cost.
func (b *InMemoryBackend) SetPriceLookup(p PriceLookup) {
	b.mu.Lock("SetPriceLookup")
	defer b.mu.Unlock()

	b.prices = p
}

// AddHoldingSource registers a service whose held resources accrue usage for the time they exist.
func (b *InMemoryBackend) AddHoldingSource(src HoldingSource) {
	b.mu.Lock("AddHoldingSource")
	defer b.mu.Unlock()

	b.holdingSources = append(b.holdingSources, src)
}

// AddUsageSource registers a service whose accumulated usage is drained into the ledger on every accrual.
func (b *InMemoryBackend) AddUsageSource(src UsageSource) {
	b.mu.Lock("AddUsageSource")
	defer b.mu.Unlock()

	b.usageSources = append(b.usageSources, src)
}

// SetResourceLevelData turns on resource-level ledger rows (resource ids and resource tags) for metered usage.
func (b *InMemoryBackend) SetResourceLevelData(on bool) {
	b.mu.Lock("SetResourceLevelData")
	defer b.mu.Unlock()

	b.resourceLevel = on
}

// ResourceLevelData reports whether resource-level ledger rows are enabled.
func (b *InMemoryBackend) ResourceLevelData() bool {
	b.mu.RLock("ResourceLevelData")
	defer b.mu.RUnlock()

	return b.resourceLevel
}

// MeteredEntries returns the metered ledger rows (not the seed catalogue) with start <= date < end.
func (b *InMemoryBackend) MeteredEntries(start, end string) []CostEntry {
	b.Accrue()

	b.mu.RLock("MeteredEntries")
	defer b.mu.RUnlock()

	var out []CostEntry

	for _, e := range b.meteredEntriesLocked() {
		if e.Date >= start && e.Date < end {
			out = append(out, e)
		}
	}

	return out
}

func (b *InMemoryBackend) pruneResourceRowsLocked(now time.Time) {
	cutoff := now.UTC().AddDate(0, 0, -ResourceRetentionDays).Format(dateLayout)
	if b.lastPrune == cutoff {
		return
	}

	b.lastPrune = cutoff

	drop := map[int]struct{}{}

	for _, i := range b.meterIdx {
		if e := &b.costLedger[i]; e.ResourceID != "" && e.Date < cutoff {
			drop[i] = struct{}{}
		}
	}

	if len(drop) == 0 {
		return
	}

	remap := make(map[int]int, len(b.meterIdx))
	keep := make([]CostEntry, 0, len(b.costLedger)-len(drop))

	for i := range b.costLedger {
		if _, gone := drop[i]; gone {
			continue
		}

		remap[i] = len(keep)
		keep = append(keep, b.costLedger[i])
	}

	for k, i := range b.meterIdx {
		if ni, ok := remap[i]; ok {
			b.meterIdx[k] = ni
		} else {
			delete(b.meterIdx, k)
		}
	}

	b.costLedger = keep
}

// Accrue books usage for every held resource over the time since the previous accrual.
func (b *InMemoryBackend) Accrue() {
	b.mu.Lock("Accrue")

	now := b.clock()
	since := b.lastAccrual
	b.pruneResourceRowsLocked(now)
	sources := slices.Clone(b.holdingSources)
	drains := slices.Clone(b.usageSources)

	if !now.After(since) {
		b.mu.Unlock()
		b.drain(drains)

		return
	}

	b.lastAccrual = now
	b.mu.Unlock()

	b.drain(drains)

	hours := now.Sub(since).Hours()

	for _, src := range sources {
		for _, h := range src() {
			u := h.Usage
			u.Quantity *= hours
			u.Start, u.End = since, now
			b.RecordUsage(u)
		}
	}
}

func (b *InMemoryBackend) drain(drains []UsageSource) {
	for _, d := range drains {
		for _, u := range d() {
			b.RecordUsage(u)
		}
	}
}

// RecordUsage adds a usage line to the ledger, split across the UTC days its time range covers.
func (b *InMemoryBackend) RecordUsage(u Usage) {
	if u.Quantity <= 0 || u.Service == "" {
		return
	}

	b.mu.Lock("RecordUsage")
	defer b.mu.Unlock()

	region := u.Region
	if region == "" {
		region = b.region
	}

	rate := 0.0

	if b.prices != nil && u.PriceService != "" {
		rate, _ = b.prices(u.PriceService, region, u.PriceMatch, u.Unit)
	}

	if u.Start.IsZero() {
		u.Start = b.clock()
		u.End = u.Start
	}

	for _, p := range splitByDay(u.Start, u.End, u.Quantity) {
		b.addMeteredLocked(u, region, p.day, p.qty, p.qty*rate)
	}
}

type dayShare struct {
	day string
	qty float64
}

func splitByDay(start, end time.Time, qty float64) []dayShare {
	start, end = start.UTC(), end.UTC()
	if !end.After(start) {
		return []dayShare{{day: start.Format(dateLayout), qty: qty}}
	}

	total := end.Sub(start)

	var out []dayShare

	for cur := start; cur.Before(end); {
		next := time.Date(cur.Year(), cur.Month(), cur.Day()+1, 0, 0, 0, 0, time.UTC)
		if next.After(end) {
			next = end
		}

		out = append(out, dayShare{day: cur.Format(dateLayout), qty: qty * float64(next.Sub(cur)) / float64(total)})
		cur = next
	}

	return out
}

func meterKey(e *CostEntry) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s|%s|%s|%s|%s", e.Date, e.Service, e.UsageType, e.Operation,
		e.Region, e.AvailabilityZone, e.InstanceType, e.ResourceID, e.Account, tagsKey(e.Tags))
}

func tagsKey(tags map[string]string) string {
	if len(tags) == 0 {
		return ""
	}

	keys := slices.Sorted(maps.Keys(tags))

	var sb strings.Builder

	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(tags[k])
		sb.WriteByte(',')
	}

	return sb.String()
}

func (b *InMemoryBackend) aggregateTagsLocked(tags map[string]string) map[string]string {
	var out map[string]string

	for k, v := range tags {
		if t, ok := b.costAllocationTags.Get(k); ok && t.Status == tagStatusActive {
			if out == nil {
				out = map[string]string{}
			}

			out[k] = v
		}
	}

	return out
}

func (b *InMemoryBackend) addMeteredLocked(u Usage, region, day string, qty, cost float64) {
	account := u.Account
	if account == "" {
		account = b.accountID
	}

	row := CostEntry{
		Date: day, Service: u.Service, Region: region, UsageType: u.UsageType, Account: account,
		AvailabilityZone: u.AvailabilityZone, InstanceType: u.InstanceType, Operation: u.Operation,
		PurchaseType: purchaseOnDemand, UsageTypeGroup: u.UsageTypeGroup, ProductFamily: u.ProductFamily,
		ResourceID: u.ResourceID, Tags: maps.Clone(u.Tags),
		BlendedCost: cost, UnblendedCost: cost, UsageQuantity: qty,
	}
	if !b.resourceLevel {
		row.ResourceID, row.Tags = "", b.aggregateTagsLocked(u.Tags)
	}

	key := meterKey(&row)

	if i, ok := b.meterIdx[key]; ok {
		e := &b.costLedger[i]
		e.UsageQuantity += qty
		e.BlendedCost += cost
		e.UnblendedCost += cost

		if len(u.Tags) > 0 {
			e.Tags = row.Tags
		}

		return
	}

	if b.meterIdx == nil {
		b.meterIdx = map[string]int{}
	}

	b.meterIdx[key] = len(b.costLedger)
	b.costLedger = append(b.costLedger, row)
}

func (b *InMemoryBackend) meteredEntriesLocked() []CostEntry {
	out := make([]CostEntry, 0, len(b.meterIdx))
	for _, i := range b.meterIdx {
		out = append(out, b.costLedger[i])
	}

	slices.SortFunc(out, func(x, y CostEntry) int { return strings.Compare(meterKey(&x), meterKey(&y)) })

	return out
}

// restoreMeteredLocked replaces the metered rows with entries, keeping the seed rows.
func (b *InMemoryBackend) restoreMeteredLocked(entries []CostEntry) {
	keep := b.costLedger[:0:0]

	drop := make(map[int]struct{}, len(b.meterIdx))
	for _, i := range b.meterIdx {
		drop[i] = struct{}{}
	}

	for i, e := range b.costLedger {
		if _, ok := drop[i]; !ok {
			keep = append(keep, e)
		}
	}

	b.costLedger = keep
	b.meterIdx = nil

	for i := range entries {
		b.meterIdx = ensureIdx(b.meterIdx)
		b.meterIdx[meterKey(&entries[i])] = len(b.costLedger)
		b.costLedger = append(b.costLedger, entries[i])
	}
}

func ensureIdx(m map[string]int) map[string]int {
	if m == nil {
		return map[string]int{}
	}

	return m
}
