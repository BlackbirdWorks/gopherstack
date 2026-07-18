package cloudfront

import (
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
)

// continuousDeploymentPolicyARN builds an ARN for a continuous deployment policy.
func (b *InMemoryBackend) continuousDeploymentPolicyARN(id string) string {
	return fmt.Sprintf("arn:aws:cloudfront::%s:continuous-deployment-policy/%s", b.accountID, id)
}

// dnsNamesFromSingle normalises the legacy single-DNS-name parameter into the
// StagingDistributionDnsNames list AWS actually returns, dropping empty values.
func dnsNamesFromSingle(dns string) []string {
	if dns == "" {
		return nil
	}

	return []string{dns}
}

// CreateContinuousDeploymentPolicy creates a new continuous deployment policy with a single
// staging distribution DNS name and no traffic config. It is kept for backward compatibility;
// CreateContinuousDeploymentPolicyWithConfig supports the full AWS request shape.
func (b *InMemoryBackend) CreateContinuousDeploymentPolicy(
	enabled bool,
	stagingDNS string,
) (*ContinuousDeploymentPolicy, error) {
	return b.CreateContinuousDeploymentPolicyWithConfig(
		enabled, dnsNamesFromSingle(stagingDNS), ContinuousDeploymentTrafficConfig{},
	)
}

// CreateContinuousDeploymentPolicyWithConfig creates a new continuous deployment policy with the
// full set of staging distribution DNS names and a traffic config (SingleWeight or SingleHeader).
func (b *InMemoryBackend) CreateContinuousDeploymentPolicyWithConfig(
	enabled bool,
	stagingDNSNames []string,
	traffic ContinuousDeploymentTrafficConfig,
) (*ContinuousDeploymentPolicy, error) {
	b.mu.Lock("CreateContinuousDeploymentPolicy")
	defer b.mu.Unlock()

	id := generateID()
	var singleDNS string
	if len(stagingDNSNames) > 0 {
		singleDNS = stagingDNSNames[0]
	}

	policy := &ContinuousDeploymentPolicy{
		ID:                          id,
		ARN:                         b.continuousDeploymentPolicyARN(id),
		ETag:                        uuid.NewString(),
		LastModifiedTime:            time.Now().UTC().Format(time.RFC3339),
		Enabled:                     enabled,
		StagingDistributionDNS:      singleDNS,
		StagingDistributionDNSNames: append([]string(nil), stagingDNSNames...),
		TrafficConfig:               traffic,
	}
	b.continuousDeploymentPolicies.Put(policy)

	return b.copyContinuousDeploymentPolicy(policy), nil
}

// copyContinuousDeploymentPolicy returns a deep copy of a ContinuousDeploymentPolicy. Must be
// called with the lock held.
func (b *InMemoryBackend) copyContinuousDeploymentPolicy(
	policy *ContinuousDeploymentPolicy,
) *ContinuousDeploymentPolicy {
	cp := *policy
	cp.StagingDistributionDNSNames = append([]string(nil), policy.StagingDistributionDNSNames...)
	if policy.TrafficConfig.SingleWeightConfig != nil {
		swc := *policy.TrafficConfig.SingleWeightConfig
		if policy.TrafficConfig.SingleWeightConfig.SessionStickinessConfig != nil {
			ssc := *policy.TrafficConfig.SingleWeightConfig.SessionStickinessConfig
			swc.SessionStickinessConfig = &ssc
		}
		cp.TrafficConfig.SingleWeightConfig = &swc
	}
	if policy.TrafficConfig.SingleHeaderConfig != nil {
		shc := *policy.TrafficConfig.SingleHeaderConfig
		cp.TrafficConfig.SingleHeaderConfig = &shc
	}

	return &cp
}

// GetContinuousDeploymentPolicy returns a continuous deployment policy by ID.
func (b *InMemoryBackend) GetContinuousDeploymentPolicy(id string) (*ContinuousDeploymentPolicy, error) {
	b.mu.RLock("GetContinuousDeploymentPolicy")
	defer b.mu.RUnlock()

	policy, ok := b.continuousDeploymentPolicies.Get(id)
	if !ok {
		return nil, fmt.Errorf(
			"%w: continuous deployment policy %s not found",
			ErrContinuousDeploymentPolicyNotFound,
			id,
		)
	}

	return b.copyContinuousDeploymentPolicy(policy), nil
}

// ListContinuousDeploymentPolicies returns all continuous deployment policies sorted by ID.
func (b *InMemoryBackend) ListContinuousDeploymentPolicies() []*ContinuousDeploymentPolicy {
	b.mu.RLock("ListContinuousDeploymentPolicies")
	defer b.mu.RUnlock()

	list := make([]*ContinuousDeploymentPolicy, 0, b.continuousDeploymentPolicies.Len())
	for _, policy := range b.continuousDeploymentPolicies.All() {
		list = append(list, b.copyContinuousDeploymentPolicy(policy))
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list
}

// UpdateContinuousDeploymentPolicy updates an existing continuous deployment policy's Enabled
// flag and single staging DNS name, preserving its current traffic config. It is kept for
// backward compatibility; UpdateContinuousDeploymentPolicyWithConfig supports the full AWS
// request shape (multiple DNS names plus a traffic config replacement).
func (b *InMemoryBackend) UpdateContinuousDeploymentPolicy(
	id string,
	enabled bool,
	stagingDNS string,
) (*ContinuousDeploymentPolicy, error) {
	b.mu.Lock("UpdateContinuousDeploymentPolicy")
	defer b.mu.Unlock()

	policy, ok := b.continuousDeploymentPolicies.Get(id)
	if !ok {
		return nil, fmt.Errorf(
			"%w: continuous deployment policy %s not found",
			ErrContinuousDeploymentPolicyNotFound,
			id,
		)
	}

	policy.Enabled = enabled
	policy.StagingDistributionDNS = stagingDNS
	policy.StagingDistributionDNSNames = dnsNamesFromSingle(stagingDNS)
	policy.ETag = uuid.NewString()
	policy.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)

	return b.copyContinuousDeploymentPolicy(policy), nil
}

// UpdateContinuousDeploymentPolicyWithConfig updates an existing continuous deployment policy,
// replacing its Enabled flag, staging distribution DNS names, and traffic config in full
// (mirroring the real UpdateContinuousDeploymentPolicy request, which always replaces the
// entire ContinuousDeploymentPolicyConfig).
func (b *InMemoryBackend) UpdateContinuousDeploymentPolicyWithConfig(
	id string,
	enabled bool,
	stagingDNSNames []string,
	traffic ContinuousDeploymentTrafficConfig,
) (*ContinuousDeploymentPolicy, error) {
	b.mu.Lock("UpdateContinuousDeploymentPolicy")
	defer b.mu.Unlock()

	policy, ok := b.continuousDeploymentPolicies.Get(id)
	if !ok {
		return nil, fmt.Errorf(
			"%w: continuous deployment policy %s not found",
			ErrContinuousDeploymentPolicyNotFound,
			id,
		)
	}

	var singleDNS string
	if len(stagingDNSNames) > 0 {
		singleDNS = stagingDNSNames[0]
	}

	policy.Enabled = enabled
	policy.StagingDistributionDNS = singleDNS
	policy.StagingDistributionDNSNames = append([]string(nil), stagingDNSNames...)
	policy.TrafficConfig = traffic
	policy.ETag = uuid.NewString()
	policy.LastModifiedTime = time.Now().UTC().Format(time.RFC3339)

	return b.copyContinuousDeploymentPolicy(policy), nil
}

// DeleteContinuousDeploymentPolicy deletes a continuous deployment policy by ID.
func (b *InMemoryBackend) DeleteContinuousDeploymentPolicy(id string) error {
	b.mu.Lock("DeleteContinuousDeploymentPolicy")
	defer b.mu.Unlock()

	if _, ok := b.continuousDeploymentPolicies.Get(id); !ok {
		return fmt.Errorf("%w: continuous deployment policy %s not found", ErrContinuousDeploymentPolicyNotFound, id)
	}

	b.continuousDeploymentPolicies.Delete(id)

	return nil
}
