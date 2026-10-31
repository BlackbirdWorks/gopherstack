package cosmosdb

import (
	"sort"
	"time"
)

// SetPartitionLimits overrides the physical partition storage limit (default 50 GB) and sets how long an
// asynchronous throughput scale-up takes (default 0: instant; the service takes "typically 4-6 hours").
func (b *InMemoryBackend) SetPartitionLimits(maxBytes int64, splitDelay time.Duration) {
	b.mu.Lock("SetPartitionLimits")
	defer b.mu.Unlock()

	b.partitionMaxBytes = maxBytes
	b.splitDelay = splitDelay
}

func (b *InMemoryBackend) storageLimit() int64 {
	if b.partitionMaxBytes > 0 {
		return b.partitionMaxBytes
	}

	return partitionMaxBytes
}

// layoutTarget is the partition count the governing offer needs.
func (b *InMemoryBackend) layoutTarget(d *storedDatabase, c *storedContainer) int {
	return partitionsForRU(int(b.budgetOf(d, c.ID).Rate))
}

// layoutView is the container's current layout, the implicit even one when none was materialised.
func (b *InMemoryBackend) layoutView(d *storedDatabase, c *storedContainer) *pkLayout {
	if c.PKLayout != nil {
		return c.PKLayout.clone()
	}

	return evenLayout(b.layoutTarget(d, c))
}

// prepareLayout pins the implicit layout of a container that has an offer, so a later change splits it.
func (b *InMemoryBackend) prepareLayout(d *storedDatabase, c *storedContainer) {
	if c.PKLayout == nil && b.budgetOf(d, c.ID).Rate > 0 {
		c.PKLayout = evenLayout(b.layoutTarget(d, c))
	}
}

// syncLayout brings the container's partitions up to what its offer needs; the lock must be held.
func (b *InMemoryBackend) syncLayout(d *storedDatabase, c *storedContainer) {
	target := b.layoutTarget(d, c)

	if c.PKLayout == nil {
		if target > 1 {
			c.PKLayout = evenLayout(target)
			b.budgetEpoch++
		}

		return
	}

	l := c.PKLayout

	switch {
	case target <= len(l.Spans):
		l.PendingTarget, l.PendingDue = 0, time.Time{}
	case b.splitDelay <= 0:
		l.growTo(target)
		l.PendingTarget, l.PendingDue = 0, time.Time{}
		b.budgetEpoch++
	default:
		if l.PendingDue.IsZero() {
			l.PendingDue = b.now().Add(b.splitDelay)
		}

		l.PendingTarget = target
	}

	b.recomputeNextDue()
}

func (b *InMemoryBackend) recomputeNextDue() {
	b.nextSplitDue = time.Time{}

	for _, d := range b.databases {
		for _, c := range d.Containers {
			l := c.PKLayout
			if l == nil || l.PendingDue.IsZero() {
				continue
			}

			if b.nextSplitDue.IsZero() || l.PendingDue.Before(b.nextSplitDue) {
				b.nextSplitDue = l.PendingDue
			}
		}
	}
}

// settleDue completes every pending scale-up whose time has come.
func (b *InMemoryBackend) settleDue() {
	b.mu.RLock("settleDue")
	due := b.nextSplitDue
	b.mu.RUnlock()

	if due.IsZero() || b.now().Before(due) {
		return
	}

	b.mu.Lock("settleDue")
	defer b.mu.Unlock()

	b.settleLocked()
}

func (b *InMemoryBackend) settleLocked() int {
	now := b.now()
	splits := 0

	for _, d := range b.databases {
		for _, c := range d.Containers {
			l := c.PKLayout
			if l == nil || l.PendingDue.IsZero() || now.Before(l.PendingDue) {
				continue
			}

			splits += l.growTo(max(l.PendingTarget, b.layoutTarget(d, c)))
			l.PendingTarget, l.PendingDue = 0, time.Time{}
			b.budgetEpoch++
		}
	}

	b.recomputeNextDue()

	return splits
}

// docBytes is the stored size of a document: its JSON body.
func docBytes(d *storedDocument) int64 {
	raw, err := jsonMarshal(d.Body)
	if err != nil {
		return 0
	}

	return int64(len(raw))
}

// splitOversized splits every partition holding more than the storage limit, at the median key hash so the
// children share the data; a partition holding one logical partition cannot split. The lock must be held.
func (b *InMemoryBackend) splitOversized(d *storedDatabase, c *storedContainer) int {
	limit := b.storageLimit()
	splits := 0

	for range maxStorageSplitsPerPass {
		l := b.layoutView(d, c)
		sizes := make([]int64, len(l.Spans))
		hashes := make([][]u128, len(l.Spans))

		for _, doc := range c.Documents {
			h := epkHash(doc.PartitionKeyJSON)
			i := l.indexOf(h)
			sizes[i] += docBytes(doc)
			hashes[i] = append(hashes[i], h)
		}

		split := false

		for i := range l.Spans {
			if sizes[i] <= limit {
				continue
			}

			sort.Slice(hashes[i], func(x, y int) bool { return hashes[i][x].cmp(hashes[i][y]) < 0 })

			at := hashes[i][len(hashes[i])/2]
			if hashes[i][0].cmp(hashes[i][len(hashes[i])-1]) == 0 || !l.splitAt(i, at) {
				continue
			}

			c.PKLayout = l
			split = true

			break
		}

		if !split {
			return splits
		}

		splits++
		b.budgetEpoch++
	}

	return splits
}

// SweepPartitions completes due scale-ups and splits partitions over the storage limit; it returns the splits.
func (b *InMemoryBackend) SweepPartitions() int {
	b.mu.Lock("SweepPartitions")
	defer b.mu.Unlock()

	splits := b.settleLocked()

	for _, d := range b.databases {
		for _, c := range d.Containers {
			splits += b.splitOversized(d, c)
		}
	}

	return splits
}

// PartitionKeyRanges is the container's partition key range feed.
type PartitionKeyRanges struct {
	Layout  *pkLayout
	RID     string
	Pending bool
}

// partitionRanges returns the current layout after settling due splits and storage splits.
func (b *InMemoryBackend) partitionRanges(dbID, collID string) (PartitionKeyRanges, error) {
	b.SweepPartitions()

	b.mu.RLock("partitionRanges")
	defer b.mu.RUnlock()

	c, err := b.findContainer(dbID, collID)
	if err != nil {
		return PartitionKeyRanges{}, err
	}

	d := b.databases[dbID]
	l := b.layoutView(d, c)

	return PartitionKeyRanges{Layout: l, RID: b.containerInfo(dbID, c).RID, Pending: l.pending()}, nil
}

// RangeShares reports each partition's share of the container's documents, in layout order; an empty
// container shares evenly.
func (b *InMemoryBackend) RangeShares(dbID, collID string) []float64 {
	b.mu.RLock("RangeShares")
	defer b.mu.RUnlock()

	c, err := b.findContainer(dbID, collID)
	if err != nil {
		return nil
	}

	l := b.layoutView(b.databases[dbID], c)
	counts := make([]float64, len(l.Spans))

	for _, doc := range c.Documents {
		counts[l.indexOf(epkHash(doc.PartitionKeyJSON))]++
	}

	total := float64(len(c.Documents))

	for i := range counts {
		if total == 0 {
			counts[i] = 1 / float64(len(counts))
		} else {
			counts[i] /= total
		}
	}

	return counts
}
