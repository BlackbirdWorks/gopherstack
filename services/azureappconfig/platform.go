package azureappconfig

import (
	"errors"
	"strings"
	"time"
)

// Platform supplies the ARM-held properties of a configuration store that the data plane honours.
type Platform interface {
	StoreSettings(name string) (PlatformSettings, bool)
	ReplicaParent(label string) (string, bool)
}

// PlatformSettings are the ARM properties the data plane reads.
type PlatformSettings struct {
	SKU                 string
	PublicNetworkAccess string
}

// ErrTooManyRequests is returned when a store exhausted its tier's request or storage quota.
var ErrTooManyRequests = errors.New("azureappconfig: request or storage quota exceeded")

// Tier limits from the App Configuration FAQ (tier comparison): requests per window, storage in bytes.
const (
	freeDailyRequests     = 1000
	developerHourlyReqs   = 6000
	standardHourlyReqs    = 30000
	freeStorage           = int64(10 << 20)
	developerStorage      = int64(500 << 20)
	standardStorage       = int64(1 << 30)
	premiumStorage        = int64(4 << 30)
	skuFree               = "free"
	skuDeveloper          = "developer"
	skuPremium            = "premium"
	retryAfterMillisFloor = 1000
	dayLength             = 24 * time.Hour
)

type usageWindow struct {
	start time.Time
	count int
}

// SetPlatform wires the ARM-held settings source; nil leaves stores unmanaged (no request quota).
func (b *InMemoryBackend) SetPlatform(p Platform) {
	b.mu.Lock("SetPlatform")
	defer b.mu.Unlock()

	b.platform = p
}

func (b *InMemoryBackend) settingsLocked(name string) PlatformSettings {
	if b.platform == nil {
		return PlatformSettings{}
	}

	ps, _ := b.platform.StoreSettings(name)
	ps.SKU = strings.ToLower(ps.SKU)

	return ps
}

// ResolveReplica maps a replica endpoint label to the store it replicates.
func (b *InMemoryBackend) ResolveReplica(label string) (string, bool) {
	b.mu.RLock("ResolveReplica")
	defer b.mu.RUnlock()

	if b.platform == nil {
		return "", false
	}

	return b.platform.ReplicaParent(label)
}

func storageLimit(sku string) int64 {
	switch sku {
	case skuFree:
		return freeStorage
	case skuDeveloper:
		return developerStorage
	case skuPremium:
		return premiumStorage
	default:
		return standardStorage
	}
}

// requestWindow returns the quota window start, the window length and the request limit of a tier (limit 0: none).
func requestWindow(sku string, now time.Time) (time.Time, time.Duration, int) {
	now = now.UTC()

	switch sku {
	case skuFree:
		return now.Truncate(dayLength), dayLength, freeDailyRequests
	case skuDeveloper:
		return now.Truncate(time.Hour), time.Hour, developerHourlyReqs
	case "standard":
		return now.Truncate(time.Hour), time.Hour, standardHourlyReqs
	default:
		return time.Time{}, 0, 0
	}
}

// Admit applies the store's network and request-quota rules to one data-plane request. The returned duration is
// the suggested retry delay of a 429.
func (b *InMemoryBackend) Admit(storeName string) (time.Duration, error) {
	b.mu.Lock("Admit")
	defer b.mu.Unlock()

	ps := b.settingsLocked(storeName)
	if strings.EqualFold(ps.PublicNetworkAccess, "Disabled") {
		return 0, ErrPublicAccessDisabled
	}

	now := b.now()
	start, length, limit := requestWindow(ps.SKU, now)

	if limit == 0 {
		return 0, nil
	}

	if b.usage == nil {
		b.usage = map[string]*usageWindow{}
	}

	key := strings.ToLower(storeName)

	u := b.usage[key]
	if u == nil || !u.start.Equal(start) {
		u = &usageWindow{start: start}
		b.usage[key] = u
	}

	if u.count >= limit {
		return max(start.Add(length).Sub(now), retryAfterMillisFloor*time.Millisecond), ErrTooManyRequests
	}

	u.count++

	return 0, nil
}
