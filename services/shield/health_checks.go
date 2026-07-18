package shield

import (
	"fmt"
	"slices"
)

// AssociateHealthCheck associates a Route 53 health check with a protection.
func (b *InMemoryBackend) AssociateHealthCheck(protectionID, healthCheckARN string) error {
	b.mu.Lock("AssociateHealthCheck")
	defer b.mu.Unlock()

	p, ok := b.protections.Get(protectionID)
	if !ok {
		return fmt.Errorf("%w: protection %q not found", ErrProtectionNotFound, protectionID)
	}

	if slices.Contains(p.HealthCheckIDs, healthCheckARN) {
		return nil
	}

	p.HealthCheckIDs = append(p.HealthCheckIDs, healthCheckARN)

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

	idx := slices.Index(p.HealthCheckIDs, healthCheckARN)
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
