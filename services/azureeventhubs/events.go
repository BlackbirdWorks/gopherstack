package azureeventhubs

import (
	"context"
	"hash/fnv"
	"strconv"
	"time"
)

func (p *partition) wake() {
	close(p.notify)
	p.notify = make(chan struct{})
}

func (p *partition) begin() int64 {
	if p.NextSeq == 0 {
		return -1
	}

	if len(p.Events) > 0 {
		return p.Events[0].Seq
	}

	return p.NextSeq
}

// trimLocked applies retention and the per-partition cap.
func (b *InMemoryBackend) trimLocked(h *hub, now time.Time) {
	cutoff := now.Add(-h.Config.Retention)

	for _, p := range h.Partitions {
		drop := 0
		for drop < len(p.Events) && p.Events[drop].Enqueued.Before(cutoff) {
			drop++
		}

		drop = max(drop, len(p.Events)-maxEventsPerPartition)
		if drop > 0 {
			p.Events = append([]*event(nil), p.Events[drop:]...)
		}
	}
}

// Sweep applies retention to every hub; it returns the number of hubs visited.
func (b *InMemoryBackend) Sweep(now time.Time) int {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	n := 0

	for _, ns := range b.namespaces {
		for _, h := range ns.Hubs {
			b.trimLocked(h, now)

			n++
		}
	}

	return n
}

func keyPartition(key string, count int) int {
	f := fnv.New32a()
	_, _ = f.Write([]byte(key))

	return int(f.Sum32() % uint32(count)) //nolint:gosec // count is bounded by MaxPartitionCount
}

func (h *hub) pick(pid int, key string) int {
	switch {
	case pid >= 0:
		return pid
	case key != "":
		return keyPartition(key, len(h.Partitions))
	}

	n := h.RoundRobin % len(h.Partitions)
	h.RoundRobin = n + 1

	return n
}

// Append stores events in one partition: pid, else the hash of the first
// event's key, else round-robin. It returns the partition used.
func (b *InMemoryBackend) Append(ref HubRef, pid int, evs []NewEvent) (int, error) {
	for _, e := range evs {
		if len(e.Raw) > MaxEventBytes {
			return 0, ErrEventTooLarge
		}
	}

	b.mu.Lock("Append")
	defer b.mu.Unlock()

	_, h, err := b.hubLocked(ref)
	if err != nil {
		return 0, err
	}

	key := ""
	if len(evs) > 0 {
		key = evs[0].Key
	}

	if pid >= len(h.Partitions) {
		return 0, ErrPartitionNotFound
	}

	pid = h.pick(pid, key)
	now := b.now().UTC()
	p := h.Partitions[pid]

	for _, e := range evs {
		p.LastOffset += int64(len(e.Raw)) + 1
		p.Events = append(p.Events, &event{
			Seq: p.NextSeq, Offset: p.LastOffset, Enqueued: now, Key: e.Key, Raw: e.Raw,
		})
		p.NextSeq++
		p.LastTime = now
	}

	b.trimLocked(h, now)
	p.wake()

	return pid, nil
}

// Next blocks until the partition holds an event at or after cursor, ctx ends,
// or stop closes (ErrReceiverStolen is reported through the caller).
func (b *InMemoryBackend) Next(
	ctx context.Context, ref HubRef, pid int, cursor int64, stop <-chan struct{},
) (*event, error) {
	for {
		ev, wait, err := b.peekNext(ref, pid, cursor)
		if err != nil || ev != nil {
			return ev, err
		}

		select {
		case <-wait:
		case <-stop:
			return nil, ErrReceiverStolen
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (b *InMemoryBackend) peekNext(ref HubRef, pid int, cursor int64) (*event, <-chan struct{}, error) {
	b.mu.Lock("Next")
	defer b.mu.Unlock()

	h, p, err := b.partLocked(ref, pid)
	if err != nil {
		return nil, nil, err
	}

	b.trimLocked(h, b.now())

	if len(p.Events) == 0 || cursor >= p.NextSeq {
		return nil, p.notify, nil
	}

	idx := max(cursor-p.Events[0].Seq, 0)

	return p.Events[idx], nil, nil
}

// PartitionInfo returns a partition's runtime state after retention.
func (b *InMemoryBackend) PartitionInfo(ref HubRef, pid int) (PartitionInfo, error) {
	b.mu.Lock("PartitionInfo")
	defer b.mu.Unlock()

	h, p, err := b.partLocked(ref, pid)
	if err != nil {
		return PartitionInfo{}, err
	}

	b.trimLocked(h, b.now())

	info := PartitionInfo{
		Hub: ref.Hub, Partition: strconv.Itoa(pid), BeginSequence: p.begin(), LastSequence: p.NextSeq - 1,
		LastOffset: p.LastOffset, LastEnqueuedTime: p.LastTime, Empty: len(p.Events) == 0,
	}

	if p.LastTime.IsZero() {
		info.LastEnqueuedTime = h.CreatedAt
	}

	if p.NextSeq == 0 {
		info.LastOffset = -1
	}

	return info, nil
}

// Resolve turns a selector into the first sequence number a receiver should get.
func (b *InMemoryBackend) Resolve(ref HubRef, pid int, sel Selector) (int64, error) {
	b.mu.Lock("Resolve")
	defer b.mu.Unlock()

	h, p, err := b.partLocked(ref, pid)
	if err != nil {
		return 0, err
	}

	b.trimLocked(h, b.now())

	return sel.start(p), nil
}

// HubCount returns the number of hubs across namespaces.
func (b *InMemoryBackend) HubCount() int {
	b.mu.RLock("HubCount")
	defer b.mu.RUnlock()

	n := 0
	for _, ns := range b.namespaces {
		n += len(ns.Hubs)
	}

	return n
}
