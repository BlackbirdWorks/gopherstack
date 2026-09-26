package route53

import (
	"fmt"
	"sort"
)

// EnableHostedZoneDNSSEC enables DNSSEC for a hosted zone, returning the
// wire ID of a newly registered change (see registerChange).
// Requires at least one ACTIVE KSK; returns ErrKeySigningKeyWithActiveStatusNF otherwise.
func (b *InMemoryBackend) EnableHostedZoneDNSSEC(zoneID string) (string, error) {
	b.mu.Lock("EnableHostedZoneDNSSEC")
	defer b.mu.Unlock()

	zd, ok := b.zones.Get(zoneID)
	if !ok {
		return "", fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	hasActiveKSK := false

	for _, ksk := range b.keySigningKeysByZone.Get(zoneID) {
		if ksk.Status == kskStatusActive {
			hasActiveKSK = true

			break
		}
	}

	if !hasActiveKSK {
		return "", fmt.Errorf(
			"%w: hosted zone %s has no ACTIVE key signing key",
			ErrKeySigningKeyWithActiveStatusNF, zoneID,
		)
	}

	zd.dnssecEnabled = true

	return b.registerChange(), nil
}

// DisableHostedZoneDNSSEC disables DNSSEC for a hosted zone, returning the
// wire ID of a newly registered change (see registerChange).
func (b *InMemoryBackend) DisableHostedZoneDNSSEC(zoneID string) (string, error) {
	b.mu.Lock("DisableHostedZoneDNSSEC")
	defer b.mu.Unlock()

	zd, ok := b.zones.Get(zoneID)
	if !ok {
		return "", fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	zd.dnssecEnabled = false

	return b.registerChange(), nil
}

// GetDNSSEC returns the DNSSEC status and key signing keys for a hosted zone.
func (b *InMemoryBackend) GetDNSSEC(zoneID string) (bool, []KeySigningKey, error) {
	b.mu.RLock("GetDNSSEC")
	defer b.mu.RUnlock()

	zd, ok := b.zones.Get(zoneID)
	if !ok {
		return false, nil, fmt.Errorf("%w: hosted zone %s not found", ErrHostedZoneNotFound, zoneID)
	}

	var ksks []KeySigningKey
	for _, ksk := range b.keySigningKeysByZone.Get(zoneID) {
		cp := *ksk
		ksks = append(ksks, cp)
	}

	sort.Slice(ksks, func(i, j int) bool { return ksks[i].Name < ksks[j].Name })

	return zd.dnssecEnabled, ksks, nil
}
