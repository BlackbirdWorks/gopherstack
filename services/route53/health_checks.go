package route53

import (
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// validateHealthCheckConfig enforces AWS type-specific constraints on a HealthCheckConfig.
func validateHealthCheckConfig(cfg HealthCheckConfig) error {
	if cfg.Type == HealthCheckTypeCloudWatchMetric && cfg.AlarmIdentifier == nil {
		return fmt.Errorf(
			"%w: CLOUDWATCH_METRIC health checks require AlarmIdentifier",
			ErrInvalidInput,
		)
	}

	if cfg.Type == HealthCheckTypeRecoveryControl && cfg.RoutingControlArn == "" {
		return fmt.Errorf(
			"%w: RECOVERY_CONTROL health checks require RoutingControlArn",
			ErrInvalidInput,
		)
	}

	if cfg.Type == HealthCheckTypeCalculated && cfg.HealthThreshold > len(cfg.ChildHealthChecks) {
		return fmt.Errorf(
			"%w: HealthThreshold (%d) must not exceed the number of ChildHealthChecks (%d)",
			ErrInvalidInput, cfg.HealthThreshold, len(cfg.ChildHealthChecks),
		)
	}

	if cfg.InsufficientDataHealthStatus != "" {
		switch cfg.InsufficientDataHealthStatus {
		case defaultHealthStatus, healthUnhealthy, "LastKnownStatus":
		default:
			return fmt.Errorf(
				"%w: InsufficientDataHealthStatus must be Healthy, Unhealthy, or LastKnownStatus",
				ErrInvalidInput,
			)
		}
	}

	return nil
}

const (
	healthCheckIDChars  = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	healthCheckIDLength = 36
	defaultHealthStatus = "Healthy"
)

func randomHealthCheckID() string { return randomID(healthCheckIDChars, healthCheckIDLength) }

// CreateHealthCheck creates a new health check.
func (b *InMemoryBackend) CreateHealthCheck(
	callerRef string,
	cfg HealthCheckConfig,
) (*HealthCheck, error) {
	if callerRef == "" {
		return nil, fmt.Errorf("%w: callerReference is required", ErrInvalidInput)
	}

	if cfg.Type == "" {
		return nil, fmt.Errorf("%w: health check type is required", ErrInvalidInput)
	}

	if err := validateHealthCheckConfig(cfg); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateHealthCheck")
	defer b.mu.Unlock()

	// CallerReference idempotency: reusing a CallerReference with the exact
	// same HealthCheckConfig is a safe retry and returns the existing health
	// check. Reusing it with a different config is rejected — real AWS returns
	// HealthCheckAlreadyExists (409) rather than silently returning (or
	// silently creating a second check for) mismatched input.
	for _, existing := range b.healthChecks.All() {
		if existing.CallerReference != callerRef {
			continue
		}

		if reflect.DeepEqual(existing.Config, cfg) {
			cp := *existing

			return &cp, nil
		}

		return nil, fmt.Errorf(
			"%w: a health check already exists for CallerReference %s with a different configuration",
			ErrHealthCheckAlreadyExists,
			callerRef,
		)
	}

	hc := &HealthCheck{
		ID:              randomHealthCheckID(),
		CallerReference: callerRef,
		Config:          cfg,
		Status:          defaultHealthStatus,
		CreatedAt:       time.Now(),
		Version:         1,
	}

	b.healthChecks.Put(hc)

	cp := *hc

	return &cp, nil
}

// GetHealthCheck returns a single health check.
func (b *InMemoryBackend) GetHealthCheck(id string) (*HealthCheck, error) {
	b.mu.RLock("GetHealthCheck")
	defer b.mu.RUnlock()

	hc, ok := b.healthChecks.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: health check %s not found", ErrHealthCheckNotFound, id)
	}

	cp := *hc

	return &cp, nil
}

// ListHealthChecks returns all health checks.
func (b *InMemoryBackend) ListHealthChecks(
	marker string,
	maxItems int,
) (page.Page[HealthCheck], error) {
	b.mu.RLock("ListHealthChecks")
	defer b.mu.RUnlock()

	all := b.healthChecks.All()
	result := make([]HealthCheck, 0, len(all))
	for _, hc := range all {
		cp := *hc
		result = append(result, cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })

	return page.New(result, marker, maxItems, route53DefaultMaxItems), nil
}

// DeleteHealthCheck removes a health check.
func (b *InMemoryBackend) DeleteHealthCheck(id string) error {
	b.mu.Lock("DeleteHealthCheck")
	defer b.mu.Unlock()

	if !b.healthChecks.Has(id) {
		return fmt.Errorf("%w: health check %s not found", ErrHealthCheckNotFound, id)
	}

	b.healthChecks.Delete(id)
	delete(b.tags, id)

	return nil
}

// UpdateHealthCheck updates configuration fields of an existing health check.
// expectedVersion is the caller-supplied HealthCheckVersion, or nil when the
// request omitted it (the field is optional on the wire). When present, it
// must match the health check's current Version or the update is rejected
// with ErrHealthCheckVersionMismatch — this is AWS's optimistic-concurrency
// guard against overwriting an intervening update. Version is incremented by
// 1 on every successful update, matching real AWS's documented behavior.
func (b *InMemoryBackend) UpdateHealthCheck(
	id string,
	cfg HealthCheckConfig,
	expectedVersion *int64,
) (*HealthCheck, error) {
	b.mu.Lock("UpdateHealthCheck")
	defer b.mu.Unlock()

	hc, ok := b.healthChecks.Get(id)
	if !ok {
		return nil, fmt.Errorf("%w: health check %s not found", ErrHealthCheckNotFound, id)
	}

	if expectedVersion != nil && *expectedVersion != hc.Version {
		return nil, fmt.Errorf(
			"%w: health check %s is at version %d, request specified %d",
			ErrHealthCheckVersionMismatch,
			id, hc.Version, *expectedVersion,
		)
	}

	hc.Config = cfg
	hc.Version++

	cp := *hc

	return &cp, nil
}

// GetHealthCheckStatus returns the mocked health status for a health check.
func (b *InMemoryBackend) GetHealthCheckStatus(id string) (string, error) {
	b.mu.RLock("GetHealthCheckStatus")
	defer b.mu.RUnlock()

	hc, ok := b.healthChecks.Get(id)
	if !ok {
		return "", fmt.Errorf("%w: health check %s not found", ErrHealthCheckNotFound, id)
	}

	return hc.Status, nil
}

// SetHealthCheckStatus overrides the mocked health status for a health check.
// This allows tests to simulate failover scenarios.
func (b *InMemoryBackend) SetHealthCheckStatus(id, status string) error {
	b.mu.Lock("SetHealthCheckStatus")
	defer b.mu.Unlock()

	hc, ok := b.healthChecks.Get(id)
	if !ok {
		return fmt.Errorf("%w: health check %s not found", ErrHealthCheckNotFound, id)
	}

	hc.Status = status
	if hc.Observations == nil {
		hc.Observations = []HealthCheckObservation{}
	}
	// Emulate an observation from a checker
	hc.Observations = append(hc.Observations, HealthCheckObservation{
		Region:      defaultRegion,
		IPAddress:   "192.0.2.1",
		Status:      status,
		CheckedTime: time.Now().UTC(),
	})
	// keep last 50
	const maxObservations = 50
	if len(hc.Observations) > maxObservations {
		hc.Observations = hc.Observations[len(hc.Observations)-maxObservations:]
	}

	return nil
}

// AddHealthCheckInternal adds a health check directly into the backend for testing.
func (b *InMemoryBackend) AddHealthCheckInternal(hc HealthCheck) {
	b.mu.Lock("AddHealthCheckInternal")
	defer b.mu.Unlock()
	cp := hc
	b.healthChecks.Put(&cp)
}

// GetHealthCheckCount returns the total number of health checks.
func (b *InMemoryBackend) GetHealthCheckCount() int {
	b.mu.RLock("GetHealthCheckCount")
	defer b.mu.RUnlock()

	return b.healthChecks.Len()
}
