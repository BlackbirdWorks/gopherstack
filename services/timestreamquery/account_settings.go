package timestreamquery

import (
	"context"
	"fmt"
	"time"
)

const (
	pricingModelBytesScanned = "BYTES_SCANNED"
	pricingModelComputeUnits = "COMPUTE_UNITS"
)

// Compute mode values for QueryCompute.ComputeMode / QueryComputeUpdate.ComputeMode.
const (
	computeModeOnDemand    = "ON_DEMAND"
	computeModeProvisioned = "PROVISIONED"
)

// defaultAccountSettings returns the initial state for a region's account settings.
// Real AWS always returns QueryCompute with ComputeMode ON_DEMAND by default.
func defaultAccountSettings() AccountSettings {
	return AccountSettings{
		QueryPricingModel: pricingModelComputeUnits,
		QueryCompute:      &QueryCompute{ComputeMode: computeModeOnDemand},
	}
}

// accountSettingsFor returns the account settings for region, initialising defaults if absent.
// Callers must hold b.mu.
func (b *InMemoryBackend) accountSettingsFor(region string) AccountSettings {
	if s, ok := b.accountSettings[region]; ok {
		return s
	}

	return defaultAccountSettings()
}

// DescribeAccountSettings returns the current account-level settings for the request region.
func (b *InMemoryBackend) DescribeAccountSettings(ctx context.Context) AccountSettings {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.RLock("DescribeAccountSettings")
	defer b.mu.RUnlock()

	return b.accountSettingsFor(region)
}

// isValidPricingModel reports whether the given pricing model string is recognised.
func isValidPricingModel(model string) bool {
	return model == pricingModelBytesScanned || model == pricingModelComputeUnits
}

// UpdateAccountSettings updates the account-level settings for the request region and returns the new state.
// Only non-empty queryPricingModel, non-nil maxQueryTCU, and non-nil queryCompute
// values are applied; omitted fields preserve their current values.
//
// queryCompute wires UpdateAccountSettingsInput.QueryCompute -- switching the
// account between ON_DEMAND and PROVISIONED compute mode. An earlier version
// of this method accepted only queryPricingModel/maxQueryTCU, silently
// dropping QueryCompute from every request: the account could never actually
// transition away from the ON_DEMAND default even though DescribeAccountSettings
// always echoed a QueryCompute field back.
func (b *InMemoryBackend) UpdateAccountSettings(
	ctx context.Context, queryPricingModel string, maxQueryTCU *int32, queryCompute *QueryComputeUpdate,
) (AccountSettings, error) {
	region := getRegion(ctx, b.defaultRegion)

	b.mu.Lock("UpdateAccountSettings")
	defer b.mu.Unlock()

	settings := b.accountSettingsFor(region)

	if queryPricingModel != "" {
		if !isValidPricingModel(queryPricingModel) {
			return AccountSettings{}, fmt.Errorf(
				"%w: invalid QueryPricingModel %q, must be one of BYTES_SCANNED or COMPUTE_UNITS",
				ErrValidation,
				queryPricingModel,
			)
		}

		settings.QueryPricingModel = queryPricingModel
	}

	if maxQueryTCU != nil {
		if *maxQueryTCU <= 0 {
			return AccountSettings{}, fmt.Errorf("%w: MaxQueryTCU must be a positive integer", ErrValidation)
		}

		settings.MaxQueryTCU = maxQueryTCU
	}

	if queryCompute != nil {
		updated, err := applyQueryComputeUpdate(settings.QueryCompute, queryCompute)
		if err != nil {
			return AccountSettings{}, err
		}

		settings.QueryCompute = updated
	}

	now := time.Now()
	settings.LastUpdatedTime = &now
	b.accountSettings[region] = settings

	return settings, nil
}

// applyQueryComputeUpdate validates a requested QueryComputeUpdate and
// returns the resulting QueryCompute to store. This emulator applies the
// change synchronously (LastUpdate.Status is always SUCCEEDED): switching to
// PROVISIONED requires a positive TargetQueryTCU, which becomes the
// (immediately active) ActiveQueryTCU; switching to ON_DEMAND clears any
// provisioned capacity.
func applyQueryComputeUpdate(_ *QueryCompute, update *QueryComputeUpdate) (*QueryCompute, error) {
	switch update.ComputeMode {
	case computeModeOnDemand:
		return &QueryCompute{ComputeMode: computeModeOnDemand}, nil
	case computeModeProvisioned:
		if update.TargetQueryTCU == nil || *update.TargetQueryTCU <= 0 {
			return nil, fmt.Errorf(
				"%w: QueryCompute.ProvisionedCapacity.TargetQueryTCU is required and must be positive when ComputeMode is %s",
				ErrValidation,
				computeModeProvisioned,
			)
		}

		active := *update.TargetQueryTCU

		return &QueryCompute{
			ComputeMode: computeModeProvisioned,
			ProvisionedCapacity: &ProvisionedCapacity{
				ActiveQueryTCU: &active,
				LastUpdate: &LastUpdate{
					Status:         "SUCCEEDED",
					TargetQueryTCU: &active,
				},
			},
		}, nil
	default:
		return nil, fmt.Errorf(
			"%w: invalid QueryCompute.ComputeMode %q, must be one of %s or %s",
			ErrValidation, update.ComputeMode, computeModeOnDemand, computeModeProvisioned,
		)
	}
}
