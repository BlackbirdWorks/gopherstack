package shield

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// DescribeAttack returns the details of a specific attack.
func (b *InMemoryBackend) DescribeAttack(attackID string) (*Attack, error) {
	b.mu.RLock("DescribeAttack")
	defer b.mu.RUnlock()

	a, ok := b.attacks.Get(attackID)
	if !ok {
		return nil, fmt.Errorf("%w: attack %q not found", ErrAttackNotFound, attackID)
	}

	atk := *a

	return &atk, nil
}

// ListAttacks returns all attacks, optionally filtered by resource ARNs (match any).
// start and end are optional Unix epoch seconds (0 = not filtered).
func (b *InMemoryBackend) ListAttacks(resourceARNs []string, startTime, endTime int64) []*Attack {
	b.mu.RLock("ListAttacks")
	defer b.mu.RUnlock()

	arnSet := make(map[string]struct{}, len(resourceARNs))
	for _, a := range resourceARNs {
		arnSet[a] = struct{}{}
	}

	items := b.attacks.All()
	list := make([]*Attack, 0, len(items))

	for _, a := range items {
		if len(arnSet) > 0 {
			if _, ok := arnSet[a.ResourceARN]; !ok {
				continue
			}
		}

		ts := a.StartTime.Unix()
		if startTime > 0 && ts < startTime {
			continue
		}

		// endTime is ToExclusive (types.TimeRange): the boundary itself is
		// excluded, not included.
		if endTime > 0 && ts >= endTime {
			continue
		}

		cp := *a
		cp.AttackVectors = append([]AttackVector(nil), a.AttackVectors...)
		cp.AttackCounters = append([]AttackCounter(nil), a.AttackCounters...)
		cp.Mitigations = append([]Mitigation(nil), a.Mitigations...)
		list = append(list, &cp)
	}

	slices.SortFunc(list, func(a, b *Attack) int {
		if a.StartTime.Before(b.StartTime) {
			return -1
		}

		if a.StartTime.After(b.StartTime) {
			return 1
		}

		return 0
	})

	return list
}

// AddAttackInternal creates an attack record directly (for tests).
func (b *InMemoryBackend) AddAttackInternal(attackID, resourceARN string) *Attack {
	b.mu.Lock("AddAttackInternal")
	defer b.mu.Unlock()

	now := time.Now()
	a := &Attack{
		AttackID:    attackID,
		ResourceARN: resourceARN,
		StartTime:   now.Add(-1 * time.Hour),
		EndTime:     now,
		AttackVectors: []AttackVector{
			{VectorType: "SYN_FLOOD"},
		},
		Mitigations: []Mitigation{
			{MitigationName: "Shield Advanced mitigation"},
		},
	}
	b.attacks.Put(a)

	cp := *a

	return &cp
}

// Representative traffic magnitudes for simulated attack counters.
const (
	simBpsMax  = 1e9
	simBpsAvg  = 5e8
	simBpsSum  = 3e10
	simPpsMax  = 1e6
	simPpsAvg  = 5e5
	simPpsSum  = 3e7
	simSamples = 60
)

// simAttackCounters returns representative traffic counters for a simulated attack.
func simAttackCounters() []AttackCounter {
	return []AttackCounter{
		{Name: "Total bps", Max: simBpsMax, Average: simBpsAvg, Sum: simBpsSum, N: simSamples, Unit: "bits/second"},
		{Name: "Total pps", Max: simPpsMax, Average: simPpsAvg, Sum: simPpsSum, N: simSamples, Unit: "packets/second"},
	}
}

// SimulateAttack creates a synthetic attack record reachable via the API.
// attackVectorTypes may be empty; defaults to SYN_FLOOD if omitted.
func (b *InMemoryBackend) SimulateAttack(resourceARN string, attackVectorTypes []string) (*Attack, error) {
	b.mu.Lock("SimulateAttack")
	defer b.mu.Unlock()

	if matches := b.protectionsByResourceARN.Get(resourceARN); len(matches) == 0 {
		return nil, fmt.Errorf("%w: no protection found for resource %q", ErrProtectionNotFound, resourceARN)
	}

	if len(attackVectorTypes) == 0 {
		attackVectorTypes = []string{"SYN_FLOOD"}
	}

	vectors := make([]AttackVector, 0, len(attackVectorTypes))
	for _, vt := range attackVectorTypes {
		vectors = append(vectors, AttackVector{VectorType: vt})
	}

	id := newShieldID()
	now := time.Now()
	a := &Attack{
		AttackID:       id,
		ResourceARN:    resourceARN,
		StartTime:      now.Add(-5 * time.Minute),
		EndTime:        now,
		AttackVectors:  vectors,
		AttackCounters: simAttackCounters(),
		Mitigations: []Mitigation{
			{MitigationName: "Shield Advanced mitigation"},
		},
	}
	b.attacks.Put(a)

	cp := *a
	cp.AttackVectors = append([]AttackVector(nil), a.AttackVectors...)
	cp.AttackCounters = append([]AttackCounter(nil), a.AttackCounters...)
	cp.Mitigations = append([]Mitigation(nil), a.Mitigations...)

	return &cp, nil
}

// attackStatsBucket holds per-month volume maxima.
type attackStatsBucket struct {
	count  int64
	maxBps float64
	maxPps float64
	maxRps float64
}

// accumulateCounters updates bucket maxima from attack counters.
func accumulateCounters(bk *attackStatsBucket, counters []AttackCounter) {
	for _, c := range counters {
		switch {
		case strings.Contains(c.Name, "bps"):
			if c.Max > bk.maxBps {
				bk.maxBps = c.Max
			}
		case strings.Contains(c.Name, "pps"):
			if c.Max > bk.maxPps {
				bk.maxPps = c.Max
			}
		case strings.Contains(c.Name, "rps"):
			if c.Max > bk.maxRps {
				bk.maxRps = c.Max
			}
		}
	}
}

// buildAttackVolumeFromBucket converts non-zero maxima to an AttackVolume.
func buildAttackVolumeFromBucket(bk *attackStatsBucket) *AttackVolume {
	if bk.maxBps == 0 && bk.maxPps == 0 && bk.maxRps == 0 {
		return nil
	}

	vol := &AttackVolume{}

	if bk.maxBps > 0 {
		vol.BitsPerSecond = &AttackVolumeStatistics{Max: bk.maxBps}
	}

	if bk.maxPps > 0 {
		vol.PacketsPerSecond = &AttackVolumeStatistics{Max: bk.maxPps}
	}

	if bk.maxRps > 0 {
		vol.RequestsPerSecond = &AttackVolumeStatistics{Max: bk.maxRps}
	}

	return vol
}

// DescribeAttackStatistics returns summary statistics about attacks, bucketed by month.
func (b *InMemoryBackend) DescribeAttackStatistics() *AttackStatistics {
	b.mu.RLock("DescribeAttackStatistics")
	defer b.mu.RUnlock()

	now := time.Now()
	from := now.AddDate(-1, 0, 0)

	buckets := make(map[string]*attackStatsBucket)

	for _, a := range b.attacks.All() {
		if a.StartTime.Before(from) || a.StartTime.After(now) {
			continue
		}

		key := a.StartTime.Format("2006-01")
		bk := buckets[key]

		if bk == nil {
			bk = &attackStatsBucket{}
			buckets[key] = bk
		}

		bk.count++
		accumulateCounters(bk, a.AttackCounters)
	}

	keys := make([]string, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	items := make([]AttackStatisticsItem, 0, len(keys))

	for _, k := range keys {
		bk := buckets[k]
		item := AttackStatisticsItem{AttackCount: bk.count, AttackVolume: buildAttackVolumeFromBucket(bk)}
		items = append(items, item)
	}

	if len(items) == 0 {
		items = []AttackStatisticsItem{{AttackCount: 0}}
	}

	return &AttackStatistics{
		TimeRange: AttackTimeRange{
			FromInclusive: from.Unix(),
			ToExclusive:   now.Unix(),
		},
		DataItems: items,
	}
}
