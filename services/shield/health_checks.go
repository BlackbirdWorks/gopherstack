package shield

import (
	"fmt"
	"slices"
	"strings"
)

// healthCheckIDFromARN extracts the bare ID from a Route 53 health check ARN: real Shield's
// Protection.HealthCheckIds holds bare IDs, not ARNs, so storing the full ARN hid every association.
func healthCheckIDFromARN(healthCheckARN string) string {
	parts := strings.Split(healthCheckARN, "/")

	return parts[len(parts)-1]
}

// AssociateHealthCheck associates a Route 53 health check with a protection.
func (b *InMemoryBackend) AssociateHealthCheck(protectionID, healthCheckARN string) error {
	b.mu.Lock("AssociateHealthCheck")
	defer b.mu.Unlock()

	p, ok := b.protections.Get(protectionID)
	if !ok {
		return fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, protectionID)
	}

	id := healthCheckIDFromARN(healthCheckARN)
	if slices.Contains(p.HealthCheckIDs, id) {
		return nil
	}

	p.HealthCheckIDs = append(p.HealthCheckIDs, id)

	return nil
}

// DisassociateHealthCheck removes a Route 53 health check from a protection.
func (b *InMemoryBackend) DisassociateHealthCheck(protectionID, healthCheckARN string) error {
	b.mu.Lock("DisassociateHealthCheck")
	defer b.mu.Unlock()

	p, ok := b.protections.Get(protectionID)
	if !ok {
		return fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, protectionID)
	}

	id := healthCheckIDFromARN(healthCheckARN)

	idx := slices.Index(p.HealthCheckIDs, id)
	if idx < 0 {
		return fmt.Errorf(
			"%w: health check %q not associated with protection %q",
			ErrProtectionNotFound,
			healthCheckARN,
			protectionID,
		)
	}

	p.HealthCheckIDs = slices.Delete(p.HealthCheckIDs, idx, idx+1)

	return nil
}
