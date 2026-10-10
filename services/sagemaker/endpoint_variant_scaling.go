package sagemaker

import (
	"fmt"
	"slices"
)

// ManagedScaling mirrors types.ProductionVariantManagedInstanceScaling.
type ManagedScaling struct {
	MinInstanceCount *int32                         `json:"MinInstanceCount,omitempty"`
	MaxInstanceCount *int32                         `json:"MaxInstanceCount,omitempty"`
	ScaleInPolicy    *ManagedInstanceScalingScaleIn `json:"ScaleInPolicy,omitempty"`
	Status           string                         `json:"Status,omitempty"`
}

// ManagedInstanceScalingScaleIn mirrors types.ProductionVariantManagedInstanceScalingScaleInPolicy.
type ManagedInstanceScalingScaleIn struct {
	CooldownInMinutes *int32 `json:"CooldownInMinutes,omitempty"`
	MaximumStepSize   *int32 `json:"MaximumStepSize,omitempty"`
	Strategy          string `json:"Strategy"`
}

// VariantRoutingConfig mirrors types.ProductionVariantRoutingConfig.
type VariantRoutingConfig struct {
	RoutingStrategy string `json:"RoutingStrategy"`
}

// validateVariantScaling checks one variant's scaling, routing, reservation and
// pool settings against the SDK validators (required members) and enums.
func validateVariantScaling(pv ProductionVariant) error {
	if err := validateVariantPools(pv); err != nil {
		return err
	}

	if err := validateManagedScaling(pv.ManagedInstanceScaling); err != nil {
		return err
	}

	if r := pv.RoutingConfig; r != nil {
		if r.RoutingStrategy == "" {
			return fmt.Errorf("%w: RoutingConfig.RoutingStrategy is required", errInvalidRequest)
		}

		if !slices.Contains([]string{"LEAST_OUTSTANDING_REQUESTS", "RANDOM"}, r.RoutingStrategy) {
			return fmt.Errorf("%w: invalid RoutingStrategy %q", errInvalidRequest, r.RoutingStrategy)
		}
	}

	return nil
}

func validateManagedScaling(m *ManagedScaling) error {
	if m == nil {
		return nil
	}

	if m.Status != "" && !slices.Contains([]string{"ENABLED", "DISABLED"}, m.Status) {
		return fmt.Errorf("%w: invalid ManagedInstanceScaling Status %q", errInvalidRequest, m.Status)
	}

	if ptrNegative(m.MinInstanceCount) || ptrNegative(m.MaxInstanceCount) {
		return fmt.Errorf("%w: ManagedInstanceScaling instance counts must be non-negative", errInvalidRequest)
	}

	p := m.ScaleInPolicy
	if p == nil {
		return nil
	}

	if p.Strategy == "" {
		return fmt.Errorf("%w: ScaleInPolicy.Strategy is required", errInvalidRequest)
	}

	if !slices.Contains([]string{"IDLE_RELEASE", "CONSOLIDATION"}, p.Strategy) {
		return fmt.Errorf("%w: invalid ScaleInPolicy Strategy %q", errInvalidRequest, p.Strategy)
	}

	return nil
}

func ptrNegative(p *int32) bool { return p != nil && *p < 0 }

func validateVariantsScaling(pvs ...[]ProductionVariant) error {
	for _, list := range pvs {
		for _, pv := range list {
			if err := validateVariantScaling(pv); err != nil {
				return err
			}
		}
	}

	return nil
}

func cloneManagedInstanceScaling(m *ManagedScaling) *ManagedScaling {
	if m == nil {
		return nil
	}

	cp := *m
	cp.MinInstanceCount = cloneInt32Ptr(m.MinInstanceCount)
	cp.MaxInstanceCount = cloneInt32Ptr(m.MaxInstanceCount)

	if m.ScaleInPolicy != nil {
		p := *m.ScaleInPolicy
		p.CooldownInMinutes = cloneInt32Ptr(m.ScaleInPolicy.CooldownInMinutes)
		p.MaximumStepSize = cloneInt32Ptr(m.ScaleInPolicy.MaximumStepSize)
		cp.ScaleInPolicy = &p
	}

	return &cp
}

func cloneInt32Ptr(p *int32) *int32 {
	if p == nil {
		return nil
	}

	v := *p

	return &v
}

func cloneRoutingConfig(r *VariantRoutingConfig) *VariantRoutingConfig {
	if r == nil {
		return nil
	}

	cp := *r

	return &cp
}

// ReservationConfig carries the config members shared by
// ProductionVariantCapacityReservationConfig and the matching Summary.
type ReservationConfig struct {
	CapacityReservationPreference string `json:"CapacityReservationPreference,omitempty"`
	MlReservationArn              string `json:"MlReservationArn,omitempty"`
}

// InstancePool mirrors types.InstancePool.
type InstancePool struct {
	Priority          *int32 `json:"Priority"`
	InstanceType      string `json:"InstanceType"`
	ModelNameOverride string `json:"ModelNameOverride,omitempty"`
}

// InstancePoolSummary mirrors types.InstancePoolSummary; the live instance
// count is unknown to the emulator and left absent.
type InstancePoolSummary struct {
	CurrentInstanceCount *int32 `json:"CurrentInstanceCount,omitempty"`
	InstanceType         string `json:"InstanceType,omitempty"`
}

func cloneCapacityReservation(c *ReservationConfig) *ReservationConfig {
	if c == nil {
		return nil
	}

	cp := *c

	return &cp
}

func instancePoolSummaries(pools []InstancePool) []InstancePoolSummary {
	if len(pools) == 0 {
		return nil
	}

	out := make([]InstancePoolSummary, len(pools))
	for i, p := range pools {
		out[i] = InstancePoolSummary{InstanceType: p.InstanceType}
	}

	return out
}

func validateVariantPools(pv ProductionVariant) error {
	if c := pv.CapacityReservationConfig; c != nil && c.CapacityReservationPreference != "" &&
		c.CapacityReservationPreference != "capacity-reservations-only" {
		return fmt.Errorf(
			"%w: invalid CapacityReservationPreference %q",
			errInvalidRequest,
			c.CapacityReservationPreference,
		)
	}

	for i, p := range pv.InstancePools {
		if p.InstanceType == "" {
			return fmt.Errorf("%w: InstancePools[%d].InstanceType is required", errInvalidRequest, i)
		}

		if p.Priority == nil {
			return fmt.Errorf("%w: InstancePools[%d].Priority is required", errInvalidRequest, i)
		}
	}

	return nil
}
