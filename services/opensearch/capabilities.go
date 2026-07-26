package opensearch

import (
	"fmt"
	"time"
)

// capabilityKey builds the composite key shared by every registered
// capability (an application may have at most one registration per
// capability name).
func capabilityKey(applicationID, capabilityName string) string {
	return applicationID + "#" + capabilityName
}

func capabilityKeyFn(v *Capability) string {
	return capabilityKey(v.ApplicationID, v.CapabilityName)
}

// resolveCapabilityStatus settles a CREATING capability copy's status to
// ACTIVE once its processing window has elapsed, mirroring
// resolveCollectionStatus's lazy-settle pattern used for serverless
// collections. It never mutates stored state.
func resolveCapabilityStatus(c *Capability, now time.Time) {
	if c.Status == capabilityStatusCreating && !c.StatusUntil.IsZero() && !now.Before(c.StatusUntil) {
		c.Status = capabilityStatusActive
	}
}

// RegisterCapability registers (or re-registers) a capability on an
// OpenSearch UI application. The only capability configuration the SDK
// currently defines (types.AIConfig) has no fields of its own, so there is
// nothing beyond the name/status to validate or store -- see the
// CapabilityConfig doc comment on the [Capability] type.
func (b *InMemoryBackend) RegisterCapability(
	applicationID, capabilityName string,
) (*Capability, error) {
	if applicationID == "" {
		return nil, fmt.Errorf("%w: ApplicationId is required", ErrInvalidParameter)
	}

	if !capabilityNamePattern.MatchString(capabilityName) {
		return nil, fmt.Errorf(
			"%w: CapabilityName must be 3-30 alphanumeric/hyphen characters",
			ErrInvalidParameter,
		)
	}

	b.mu.Lock("RegisterCapability")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationID) {
		return nil, fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, applicationID)
	}

	capa := &Capability{
		ApplicationID:  applicationID,
		CapabilityName: capabilityName,
		Status:         capabilityStatusActive,
	}

	// Real CREATING -> ACTIVE transition, matching the same
	// processingDelay-driven pattern used for serverless collections: with no
	// configured delay this settles immediately, with a delay the capability
	// is observably "creating" until the window elapses.
	if b.processingDelay > 0 {
		capa.Status = capabilityStatusCreating
		capa.StatusUntil = b.clock().Add(b.processingDelay)
	}

	b.capabilities.Put(capa)

	cp := *capa
	resolveCapabilityStatus(&cp, b.clock())

	return &cp, nil
}

// DeregisterCapability removes a registered capability. Its response status
// is always "deleting" per the real API's documented behavior ("Returns
// deleting when the capability is being removed"), regardless of the
// removal's actual synchronicity in this emulator.
func (b *InMemoryBackend) DeregisterCapability(applicationID, capabilityName string) error {
	b.mu.Lock("DeregisterCapability")
	defer b.mu.Unlock()

	key := capabilityKey(applicationID, capabilityName)
	if !b.capabilities.Has(key) {
		return fmt.Errorf(
			"%w: capability %s not found on application %s",
			ErrCapabilityNotFound,
			capabilityName,
			applicationID,
		)
	}

	b.capabilities.Delete(key)

	return nil
}

// GetCapability returns a registered capability's current status.
func (b *InMemoryBackend) GetCapability(applicationID, capabilityName string) (*Capability, error) {
	b.mu.RLock("GetCapability")
	defer b.mu.RUnlock()

	capability, ok := b.capabilities.Get(capabilityKey(applicationID, capabilityName))
	if !ok {
		return nil, fmt.Errorf(
			"%w: capability %s not found on application %s",
			ErrCapabilityNotFound,
			capabilityName,
			applicationID,
		)
	}

	cp := *capability
	resolveCapabilityStatus(&cp, b.clock())

	return &cp, nil
}
