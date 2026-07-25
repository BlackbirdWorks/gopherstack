package timestreamquery

import (
	"context"
	"fmt"
)

const (
	pricingModelBytesScanned = "BYTES_SCANNED"
	pricingModelComputeUnits = "COMPUTE_UNITS"
)

// TCU (Timestream Compute Unit) bounds shared by MaxQueryTCU and
// QueryCompute.ProvisionedCapacity.TargetQueryTCU. Real AWS documents: "you
// must set a minimum capacity of 4 TCU. You can set the maximum number of TCU
// in multiples of 4 ... The maximum value supported for MaxQueryTCU is 1000".
const (
	tcuMin      = 4
	tcuMax      = 1000
	tcuMultiple = 4
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

// validateTCU checks a requested TCU value against the documented bounds:
// minimum 4, maximum 1000, and must be a multiple of 4.
func validateTCU(tcu int32, fieldName string) error {
	if tcu < tcuMin || tcu > tcuMax {
		return fmt.Errorf(
			"%w: %s must be between %d and %d, got %d",
			ErrValidation, fieldName, tcuMin, tcuMax, tcu,
		)
	}

	if tcu%tcuMultiple != 0 {
		return fmt.Errorf(
			"%w: %s must be a multiple of %d, got %d",
			ErrValidation, fieldName, tcuMultiple, tcu,
		)
	}

	return nil
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
		if err := validateTCU(*maxQueryTCU, "MaxQueryTCU"); err != nil {
			return AccountSettings{}, err
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

	b.accountSettings[region] = settings

	return settings, nil
}

// applyQueryComputeUpdate validates a requested QueryComputeUpdate and
// returns the resulting QueryCompute to store. This emulator applies the
// change synchronously (LastUpdate.Status is always SUCCEEDED): switching to
// PROVISIONED requires a valid TargetQueryTCU (4-1000, multiple of 4), which
// becomes the (immediately active) ActiveQueryTCU; switching to ON_DEMAND
// clears any provisioned capacity.
func applyQueryComputeUpdate(_ *QueryCompute, update *QueryComputeUpdate) (*QueryCompute, error) {
	switch update.ComputeMode {
	case computeModeOnDemand:
		return &QueryCompute{ComputeMode: computeModeOnDemand}, nil
	case computeModeProvisioned:
		if update.TargetQueryTCU == nil {
			return nil, fmt.Errorf(
				"%w: QueryCompute.ProvisionedCapacity.TargetQueryTCU is required when ComputeMode is %s",
				ErrValidation,
				computeModeProvisioned,
			)
		}

		if err := validateTCU(*update.TargetQueryTCU, "QueryCompute.ProvisionedCapacity.TargetQueryTCU"); err != nil {
			return nil, err
		}

		notif, err := validateAccountSettingsNotificationConfiguration(update.NotificationConfiguration)
		if err != nil {
			return nil, err
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
				NotificationConfiguration: notif,
			},
		}, nil
	default:
		return nil, fmt.Errorf(
			"%w: invalid QueryCompute.ComputeMode %q, must be one of %s or %s",
			ErrValidation, update.ComputeMode, computeModeOnDemand, computeModeProvisioned,
		)
	}
}

// validateAccountSettingsNotificationConfiguration mirrors the real SDK's
// client-side validation (validateAccountSettingsNotificationConfiguration in
// validators.go): when a NotificationConfiguration is supplied, RoleArn is
// required, and a nested SnsConfiguration requires TopicArn. A nil input is
// valid (the field is optional) and returns nil, nil.
func validateAccountSettingsNotificationConfiguration(
	cfg *AccountSettingsNotificationConfiguration,
) (*AccountSettingsNotificationConfiguration, error) {
	if cfg == nil {
		return nil, nil //nolint:nilnil // absent NotificationConfiguration is a valid, non-error state
	}

	if cfg.RoleArn == "" {
		return nil, fmt.Errorf(
			"%w: QueryCompute.ProvisionedCapacity.NotificationConfiguration.RoleArn is required",
			ErrValidation,
		)
	}

	if cfg.SnsConfiguration != nil && cfg.SnsConfiguration.TopicArn == "" {
		return nil, fmt.Errorf(
			"%w: QueryCompute.ProvisionedCapacity.NotificationConfiguration.SnsConfiguration.TopicArn is required",
			ErrValidation,
		)
	}

	return cfg, nil
}
