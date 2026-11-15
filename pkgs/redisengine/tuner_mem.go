package redisengine

import (
	"math/rand/v2"
	"strings"
	"time"

	"github.com/alicebob/miniredis/v2/size"
)

const (
	oomMessage  = "OOM command not allowed when used memory > 'maxmemory'."
	lfuInitVal  = 5
	lfuMax      = 255
	keyOverhead = 64
)

type keyMeta struct {
	lfuAt  time.Time
	access int64
	lfu    int
}

// memAccount tracks the approximate size of every key and the recency and frequency data eviction needs.
type memAccount struct {
	sizes map[dbKey]int64
	meta  map[dbKey]*keyMeta
	total int64
	clock int64
	dirty bool
}

func newMemAccount() memAccount {
	return memAccount{sizes: map[dbKey]int64{}, meta: map[dbKey]*keyMeta{}, dirty: true}
}

func (t *tuner) measure(db int, key string) int64 {
	kd, ok := dumpKey(t.mr.DB(db), db, key)
	if !ok {
		return 0
	}

	return int64(size.Of(kd)) + keyOverhead + int64(len(key))
}

// rebuild re-measures every key; callers hold t.mu.
func (t *tuner) rebuild() {
	t.mem.sizes = map[dbKey]int64{}
	t.mem.total = 0

	live := map[dbKey]struct{}{}

	for db := range keyspaceDBs {
		for _, k := range t.mr.DB(db).Keys() {
			dk := dbKey{db: db, key: k}
			n := t.measure(db, k)
			t.mem.sizes[dk] = n
			t.mem.total += n
			live[dk] = struct{}{}

			if _, ok := t.mem.meta[dk]; !ok {
				t.mem.meta[dk] = t.newMeta()
			}
		}
	}

	for dk := range t.mem.meta {
		if _, ok := live[dk]; !ok {
			delete(t.mem.meta, dk)
		}
	}

	t.mem.dirty = false
}

func (t *tuner) newMeta() *keyMeta {
	t.mem.clock++

	return &keyMeta{access: t.mem.clock, lfu: lfuInitVal, lfuAt: t.clock()}
}

// refresh re-measures the given keys, dropping the bookkeeping of those that no longer exist.
func (t *tuner) refresh(db int, keys []string, accessed bool, cfg *Settings) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.mem.dirty {
		return
	}

	for _, k := range keys {
		dk := dbKey{db: db, key: k}
		n := t.measure(db, k)
		t.mem.total += n - t.mem.sizes[dk]

		if n == 0 {
			delete(t.mem.sizes, dk)
			delete(t.mem.meta, dk)

			continue
		}

		t.mem.sizes[dk] = n

		m := t.mem.meta[dk]
		if m == nil {
			m = t.newMeta()
			t.mem.meta[dk] = m
		}

		if accessed {
			t.mem.clock++
			m.access = t.mem.clock
			m.touchLFU(cfg, t.clock())
		}
	}
}

func (m *keyMeta) touchLFU(cfg *Settings, now time.Time) {
	m.decay(cfg, now)

	if m.lfu >= lfuMax {
		return
	}

	base := max(m.lfu-lfuInitVal, 0)
	p := 1.0 / float64(base*cfg.LFULogFactor+1)

	if rand.Float64() < p { //nolint:gosec // probabilistic counter, not security
		m.lfu++
	}
}

func (m *keyMeta) decay(cfg *Settings, now time.Time) {
	if cfg.LFUDecayTime <= 0 {
		return
	}

	period := time.Duration(cfg.LFUDecayTime) * time.Minute
	n := int(now.Sub(m.lfuAt) / period)

	if n > 0 {
		m.lfu = max(m.lfu-n, 0)
		m.lfuAt = m.lfuAt.Add(time.Duration(n) * period)
	}
}

// admit makes room for a write under the memory limit, evicting per the policy; it reports false when the command
// must be refused with OOM.
func (t *tuner) admit(cfg *Settings) bool {
	if cfg.MaxMemory <= 0 {
		return true
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.expireLocked()

	if t.mem.dirty {
		t.rebuild()
	}

	for t.mem.total > cfg.MaxMemory {
		if cfg.Policy == PolicyNoEviction {
			return false
		}

		victim, ok := t.pickVictim(cfg)
		if !ok {
			return false
		}

		t.evictLocked(victim, cfg)
	}

	return true
}

func (t *tuner) evictLocked(v dbKey, cfg *Settings) {
	t.mr.DB(v.db).Del(v.key)
	t.mem.total -= t.mem.sizes[v]
	delete(t.mem.sizes, v)
	delete(t.mem.meta, v)
	delete(t.deadline, v)

	t.evicted++
	t.emitLocked(v.db, v.key, "evicted", eventEvicted, cfg)
}

func (t *tuner) pickVictim(cfg *Settings) (dbKey, bool) {
	var (
		best    dbKey
		found   bool
		bestKey int64
	)

	volatile := strings.HasPrefix(cfg.Policy, "volatile-")
	random := strings.HasSuffix(cfg.Policy, "-random")
	seen := 0

	for dk := range t.mem.sizes {
		ttl := t.mr.DB(dk.db).TTL(dk.key)
		if volatile && ttl <= 0 {
			continue
		}

		seen++

		score := t.score(cfg, dk, ttl)
		if random {
			if rand.IntN(seen) == 0 { //nolint:gosec // reservoir sampling for a random victim
				best, found = dk, true
			}

			continue
		}

		if !found || score < bestKey || score == bestKey && t.mem.meta[dk].access < t.mem.meta[best].access {
			best, bestKey, found = dk, score, true
		}
	}

	return best, found
}

func (t *tuner) score(cfg *Settings, dk dbKey, ttl time.Duration) int64 {
	m := t.mem.meta[dk]

	switch cfg.Policy {
	case PolicyVolatileTTL:
		return int64(ttl)
	case PolicyAllKeysLFU, PolicyVolatileLFU:
		m.decay(cfg, t.clock())

		return int64(m.lfu)
	default:
		return m.access
	}
}
