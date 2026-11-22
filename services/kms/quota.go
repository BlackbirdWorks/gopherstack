package kms

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/quotalimit"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const (
	quotaServiceCode = "kms"
	quotaKeys        = "L-C2F1777E"
	quotaAliasesKey  = "L-340F62FB"
	quotaGrantsKey   = "L-D594A657"
)

// SetQuotaLimiter makes CreateKey, CreateAlias and CreateGrant honour Service Quotas; nil disables it.
func (h *Handler) SetQuotaLimiter(f quotalimit.Func) {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.quotaGate.Set(f)
	}
}

// KeyCount returns the customer managed keys held in region; empty region means the backend's.
func (h *Handler) KeyCount(region string) int {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		return b.keyCount(region)
	}

	return 0
}

// MaxAliasesPerKey returns the most aliases any single key in region has.
func (h *Handler) MaxAliasesPerKey(region string) int {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		return b.maxAliasesPerKey(region)
	}

	return 0
}

// MaxGrantsPerKey returns the most grants any single key in region has.
func (h *Handler) MaxGrantsPerKey(region string) int {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		return b.maxGrantsPerKeyIn(region)
	}

	return 0
}

func (b *InMemoryBackend) regionOr(region string) string {
	if region == "" {
		return b.defaultRegion
	}

	return region
}

func (b *InMemoryBackend) existingTable(region string) *store.Table[Key] {
	b.tableMu.Lock()
	defer b.tableMu.Unlock()

	return b.keys[region]
}

func (b *InMemoryBackend) keyCount(region string) int {
	t := b.existingTable(b.regionOr(region))
	if t == nil {
		return 0
	}

	b.mu.RLock("KeyCount")
	defer b.mu.RUnlock()

	return b.keyCountLocked(t)
}

func (b *InMemoryBackend) keyCountLocked(t *store.Table[Key]) int {
	n := 0

	for _, k := range t.All() {
		if !isAWSManagedKey(k) {
			n++
		}
	}

	return n
}

func (b *InMemoryBackend) maxAliasesPerKey(region string) int {
	b.tableMu.Lock()
	t := b.aliases[b.regionOr(region)]
	b.tableMu.Unlock()

	if t == nil {
		return 0
	}

	b.mu.RLock("MaxAliasesPerKey")
	defer b.mu.RUnlock()

	perKey := map[string]int{}
	most := 0

	for _, a := range t.All() {
		perKey[a.TargetKeyID]++
		most = max(most, perKey[a.TargetKeyID])
	}

	return most
}

func (b *InMemoryBackend) maxGrantsPerKeyIn(region string) int {
	b.tableMu.Lock()
	g := b.grants[b.regionOr(region)]
	b.tableMu.Unlock()

	if g == nil {
		return 0
	}

	b.mu.RLock("MaxGrantsPerKey")
	defer b.mu.RUnlock()

	perKey := map[string]int{}
	most := 0

	for _, gr := range g.table.All() {
		perKey[gr.KeyID]++
		most = max(most, perKey[gr.KeyID])
	}

	return most
}

// grantKeyRegion is the Region the key behind keyID lives in, which owns the grant quota; the request Region
// when the key does not resolve.
func (b *InMemoryBackend) grantKeyRegion(ctx context.Context, keyID string) string {
	b.mu.RLock("GrantKeyRegion")
	defer b.mu.RUnlock()

	if _, region, err := b.resolveKeyAndRegion(ctx, keyID, ErrInvalidArn); err == nil {
		return region
	}

	return getRegion(ctx, b.defaultRegion)
}

func (b *InMemoryBackend) quotaLimit(region, code string) quotalimit.Limit {
	return b.quotaGate.Lookup(b.accountID, region, quotaServiceCode, code)
}

func (b *InMemoryBackend) checkKeyQuotaLocked(region string, limit quotalimit.Limit) error {
	t := b.existingTable(region)
	if t == nil || !limit.Set {
		return nil
	}

	if limit.Reached(b.keyCountLocked(t)) {
		return fmt.Errorf("%w: the maximum number of KMS keys (%.0f) has been reached", ErrLimitExceeded, limit.Value)
	}

	return nil
}

func (b *InMemoryBackend) checkAliasQuotaLocked(region, targetID string, limit quotalimit.Limit) error {
	if !limit.Set {
		return nil
	}

	n := 0

	for _, a := range b.aliasesStore(region).All() {
		if a.TargetKeyID == targetID {
			n++
		}
	}

	if limit.Reached(n) {
		return fmt.Errorf("%w: key %q already has the maximum number of aliases (%.0f)",
			ErrLimitExceeded, targetID, limit.Value)
	}

	return nil
}

func grantsPerKeyLimit(limit quotalimit.Limit) int {
	if limit.Set {
		return int(limit.Value)
	}

	return maxGrantsPerKey
}
