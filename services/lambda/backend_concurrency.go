package lambda

import (
	"fmt"
	"time"
)

// PutFunctionConcurrency sets the reserved concurrent executions for a function.
// Setting ReservedConcurrentExecutions to 0 disables all invocations of the function.
func (b *InMemoryBackend) PutFunctionConcurrency(
	name string,
	reserved int,
) (*FunctionConcurrency, error) {
	b.mu.Lock("PutFunctionConcurrency")
	defer b.mu.Unlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if reserved < 0 {
		return nil, fmt.Errorf(
			"%w: ReservedConcurrentExecutions must be >= 0",
			ErrInvalidParameterValue,
		)
	}

	b.functionConcurrencies[name] = reserved
	fn.ReservedConcurrentExecutions = &reserved

	return &FunctionConcurrency{ReservedConcurrentExecutions: reserved}, nil
}

// GetFunctionConcurrency returns the reserved concurrent executions for a function.
func (b *InMemoryBackend) GetFunctionConcurrency(name string) (*FunctionConcurrency, error) {
	b.mu.RLock("GetFunctionConcurrency")
	defer b.mu.RUnlock()

	if _, ok := b.functions.Get(name); !ok {
		return nil, ErrFunctionNotFound
	}

	reserved, ok := b.functionConcurrencies[name]
	if !ok {
		return nil, ErrFunctionConcurrencyNotFound
	}

	return &FunctionConcurrency{ReservedConcurrentExecutions: reserved}, nil
}

// DeleteFunctionConcurrency removes the reserved concurrency setting for a function,
// restoring it to the account-level default.
func (b *InMemoryBackend) DeleteFunctionConcurrency(name string) error {
	b.mu.Lock("DeleteFunctionConcurrency")
	defer b.mu.Unlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return ErrFunctionNotFound
	}

	delete(b.functionConcurrencies, name)
	fn.ReservedConcurrentExecutions = nil

	return nil
}

// PutProvisionedConcurrencyConfig sets the provisioned concurrency configuration for a function qualifier.
// The qualifier must be a version number or alias name; $LATEST is not supported.
// Status is returned as READY immediately (stub implementation — no actual pre-warming).
func (b *InMemoryBackend) PutProvisionedConcurrencyConfig(
	name, qualifier string,
	requested int,
) (*ProvisionedConcurrencyConfig, error) {
	b.mu.Lock("PutProvisionedConcurrencyConfig")
	defer b.mu.Unlock()

	fn, ok := b.functions.Get(name)
	if !ok {
		return nil, ErrFunctionNotFound
	}

	if requested <= 0 {
		return nil, fmt.Errorf(
			"%w: ProvisionedConcurrentExecutions must be > 0",
			ErrInvalidParameterValue,
		)
	}

	if qualifier == versionLatest {
		return nil, fmt.Errorf(
			"%w: provisioned concurrency is not supported for $LATEST",
			ErrInvalidParameterValue,
		)
	}

	cfg := &ProvisionedConcurrencyConfig{
		AllocatedProvisionedConcurrentExecutions: requested,
		AvailableProvisionedConcurrentExecutions: requested,
		FunctionArn: buildAliasARN(
			b.region,
			b.accountID,
			fn.FunctionName,
			qualifier,
		),
		LastModified:                             time.Now().UTC().Format(time.RFC3339),
		RequestedProvisionedConcurrentExecutions: requested,
		Status:                                   provisionedConcurrencyReady,
	}

	// When a warm-up delay is configured, provisioned concurrency starts
	// IN_PROGRESS with zero available capacity and transitions to READY after the
	// delay, mirroring AWS's real pre-warming window. With no delay (the default)
	// it reports READY immediately.
	if b.pcActivationDelay > 0 {
		cfg.Status = provisionedConcurrencyInProgress
		cfg.AvailableProvisionedConcurrentExecutions = 0
		b.scheduleProvisionedConcurrencyReady(name, qualifier, b.pcActivationDelay)
	}

	b.provisionedConcurrencies.Put(cfg)

	return cfg, nil
}

const provisionedConcurrencyReady = "READY"

const provisionedConcurrencyInProgress = "IN_PROGRESS"

// scheduleProvisionedConcurrencyReady transitions a provisioned concurrency config
// from IN_PROGRESS to READY after the given delay.
func (b *InMemoryBackend) scheduleProvisionedConcurrencyReady(name, qualifier string, delay time.Duration) {
	b.asyncWG.Go(func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-timer.C:
		case <-b.shutdown:
			return
		case <-b.ctx.Done():
			return
		}

		b.mu.Lock("provisionedConcurrencyReady")
		defer b.mu.Unlock()

		key := buildAliasARN(b.region, b.accountID, name, qualifier)

		cfg, ok := b.provisionedConcurrencies.Get(key)
		if !ok {
			return
		}

		// Copy-on-write: replace the table entry with a new config so any caller
		// holding the previous pointer (returned live by Get) never observes a
		// concurrent field write. FunctionArn (the table's key) is preserved
		// unchanged, so Put replaces the same entry in place.
		updated := *cfg
		updated.Status = provisionedConcurrencyReady
		updated.AvailableProvisionedConcurrentExecutions = updated.RequestedProvisionedConcurrentExecutions
		updated.LastModified = time.Now().UTC().Format(time.RFC3339)
		b.provisionedConcurrencies.Put(&updated)
	})
}

// GetProvisionedConcurrencyConfig returns the provisioned concurrency configuration for a function qualifier.
func (b *InMemoryBackend) GetProvisionedConcurrencyConfig(
	name, qualifier string,
) (*ProvisionedConcurrencyConfig, error) {
	b.mu.RLock("GetProvisionedConcurrencyConfig")
	defer b.mu.RUnlock()

	if _, ok := b.functions.Get(name); !ok {
		return nil, ErrFunctionNotFound
	}

	cfg, ok := b.provisionedConcurrencies.Get(buildAliasARN(b.region, b.accountID, name, qualifier))
	if !ok {
		return nil, ErrProvisionedConcurrencyConfigNotFound
	}

	return cfg, nil
}

// DeleteProvisionedConcurrencyConfig removes the provisioned concurrency configuration for a function qualifier.
func (b *InMemoryBackend) DeleteProvisionedConcurrencyConfig(name, qualifier string) error {
	b.mu.Lock("DeleteProvisionedConcurrencyConfig")
	defer b.mu.Unlock()

	if _, ok := b.functions.Get(name); !ok {
		return ErrFunctionNotFound
	}

	if !b.provisionedConcurrencies.Delete(buildAliasARN(b.region, b.accountID, name, qualifier)) {
		return ErrProvisionedConcurrencyConfigNotFound
	}

	return nil
}

// ListProvisionedConcurrencyConfigs returns all provisioned concurrency configurations for a function.
func (b *InMemoryBackend) ListProvisionedConcurrencyConfigs(
	name string,
) ([]*ProvisionedConcurrencyConfig, error) {
	b.mu.RLock("ListProvisionedConcurrencyConfigs")
	defer b.mu.RUnlock()

	if _, ok := b.functions.Get(name); !ok {
		return nil, ErrFunctionNotFound
	}

	configs := b.provisionedConcurrenciesByFunction.Get(name)

	return append([]*ProvisionedConcurrencyConfig(nil), configs...), nil
}
