package opensearch

import "fmt"

// Documented OpenSearch Serverless account-level OCU quotas (AWS General
// Reference, "OpenSearch Serverless quotas": "Default indexing capacity
// (OCUs): 10", "Default search capacity (OCUs): 10", "Maximum indexing/search
// capacity (OCUs): 1,700", both listed non-adjustable). The minimum of 2 OCUs
// each comes from the same page's UpdateAccountSettings guidance.
const (
	defaultServerlessAccountCapacityOCU = 10
	minServerlessAccountCapacityOCU     = 2
	maxServerlessAccountCapacityOCU     = 1700
)

// ServerlessCapacityLimits mirrors types.CapacityLimits
// (opensearchserverless@v1.34.4 types/types.go:99-108): the account-wide
// (Classic, no collection group) OCU ceiling. Each bound is independently
// optional so UpdateAccountSettings can change one without touching the
// other.
type ServerlessCapacityLimits struct {
	MaxIndexingCapacityInOCU *int32 `json:"maxIndexingCapacityInOCU,omitempty"`
	MaxSearchCapacityInOCU   *int32 `json:"maxSearchCapacityInOCU,omitempty"`
}

// resolveServerlessAccountCapacityDefaults fills any unset bound with the
// documented account-level default (10 OCUs) without mutating the input.
func resolveServerlessAccountCapacityDefaults(cl ServerlessCapacityLimits) ServerlessCapacityLimits {
	out := cl

	if out.MaxIndexingCapacityInOCU == nil {
		v := int32(defaultServerlessAccountCapacityOCU)
		out.MaxIndexingCapacityInOCU = &v
	}

	if out.MaxSearchCapacityInOCU == nil {
		v := int32(defaultServerlessAccountCapacityOCU)
		out.MaxSearchCapacityInOCU = &v
	}

	return out
}

func validateServerlessAccountCapacity(v *int32) error {
	if v == nil {
		return nil
	}

	if *v < minServerlessAccountCapacityOCU || *v > maxServerlessAccountCapacityOCU {
		return fmt.Errorf(
			"%w: capacity must be between %d and %d OCUs",
			ErrInvalidParameter, minServerlessAccountCapacityOCU, maxServerlessAccountCapacityOCU,
		)
	}

	return nil
}

// GetServerlessAccountSettings returns the account's current capacity
// limits, resolving any never-configured bound to the documented default.
func (b *InMemoryBackend) GetServerlessAccountSettings() ServerlessCapacityLimits {
	b.mu.RLock("GetServerlessAccountSettings")
	defer b.mu.RUnlock()

	return resolveServerlessAccountCapacityDefaults(b.accountCapacityLimits)
}

// UpdateServerlessAccountSettings updates whichever capacity bounds are
// non-nil in capacityLimits, leaving the other untouched, and returns the
// resolved settings.
func (b *InMemoryBackend) UpdateServerlessAccountSettings(
	capacityLimits ServerlessCapacityLimits,
) (ServerlessCapacityLimits, error) {
	if err := validateServerlessAccountCapacity(capacityLimits.MaxIndexingCapacityInOCU); err != nil {
		return ServerlessCapacityLimits{}, err
	}

	if err := validateServerlessAccountCapacity(capacityLimits.MaxSearchCapacityInOCU); err != nil {
		return ServerlessCapacityLimits{}, err
	}

	b.mu.Lock("UpdateServerlessAccountSettings")
	defer b.mu.Unlock()

	if capacityLimits.MaxIndexingCapacityInOCU != nil {
		b.accountCapacityLimits.MaxIndexingCapacityInOCU = capacityLimits.MaxIndexingCapacityInOCU
	}

	if capacityLimits.MaxSearchCapacityInOCU != nil {
		b.accountCapacityLimits.MaxSearchCapacityInOCU = capacityLimits.MaxSearchCapacityInOCU
	}

	return resolveServerlessAccountCapacityDefaults(b.accountCapacityLimits), nil
}

// ServerlessPoliciesStats mirrors GetPoliciesStatsOutput's four Stats
// sub-objects plus TotalPolicyCount (api_op_GetPoliciesStats.go).
type ServerlessPoliciesStats struct {
	DataPolicyCount       int64
	RetentionPolicyCount  int64
	SamlConfigCount       int64
	EncryptionPolicyCount int64
	NetworkPolicyCount    int64
}

// Total sums every counted policy/config, matching GetPoliciesStatsOutput's
// TotalPolicyCount ("The total number of OpenSearch Serverless security
// policies and configurations in your account").
func (s ServerlessPoliciesStats) Total() int64 {
	return s.DataPolicyCount + s.RetentionPolicyCount + s.SamlConfigCount +
		s.EncryptionPolicyCount + s.NetworkPolicyCount
}

// GetServerlessPoliciesStats counts every stored policy/config by type.
func (b *InMemoryBackend) GetServerlessPoliciesStats() ServerlessPoliciesStats {
	b.mu.RLock("GetServerlessPoliciesStats")
	defer b.mu.RUnlock()

	var stats ServerlessPoliciesStats

	for _, p := range b.slAccessPolicies.All() {
		if p.Type == slPolicyTypeData {
			stats.DataPolicyCount++
		}
	}

	stats.RetentionPolicyCount = int64(b.slLifecyclePolicies.Len())

	for _, sc := range b.slSecurityConfigs.All() {
		if sc.Type == slSecurityConfigTypeSAML {
			stats.SamlConfigCount++
		}
	}

	stats.EncryptionPolicyCount = int64(b.slEncryptionPolicies.Len())
	stats.NetworkPolicyCount = int64(b.slNetworkPolicies.Len())

	return stats
}
