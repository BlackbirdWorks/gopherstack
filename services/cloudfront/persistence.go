package cloudfront

import "encoding/json"

type backendSnapshot struct {
	Distributions                map[string]*Distribution               `json:"distributions"`
	OAIs                         map[string]*OriginAccessIdentity       `json:"oais"`
	Invalidations                map[string][]*Invalidation             `json:"invalidations,omitempty"`
	AnycastIPLists               map[string]*AnycastIPList              `json:"anycastIPLists,omitempty"`
	CachePolicies                map[string]*CachePolicy                `json:"cachePolicies,omitempty"`
	ConnectionFunctions          map[string]*ConnectionFunction         `json:"connectionFunctions,omitempty"`
	ConnectionGroups             map[string]*ConnectionGroup            `json:"connectionGroups,omitempty"`
	ContinuousDeploymentPolicies map[string]*ContinuousDeploymentPolicy `json:"continuousDeploymentPolicies,omitempty"`
	DistributionAliases          map[string][]string                    `json:"distributionAliases,omitempty"`
	DistributionWebACLs          map[string]string                      `json:"distributionWebACLs,omitempty"`
	DistributionTenantWebACLs    map[string]string                      `json:"distributionTenantWebACLs,omitempty"`
	AccountID                    string                                 `json:"accountId"`
	Region                       string                                 `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Distributions:                b.distributions,
		OAIs:                         b.oais,
		Invalidations:                b.invalidations,
		AnycastIPLists:               b.anycastIPLists,
		CachePolicies:                b.cachePolicies,
		ConnectionFunctions:          b.connectionFunctions,
		ConnectionGroups:             b.connectionGroups,
		ContinuousDeploymentPolicies: b.continuousDeploymentPolicies,
		DistributionAliases:          b.distributionAliases,
		DistributionWebACLs:          b.distributionWebACLs,
		DistributionTenantWebACLs:    b.distributionTenantWebACLs,
		AccountID:                    b.accountID,
		Region:                       b.region,
	}

	data, _ := json.Marshal(snap)

	return data
}

// Restore loads backend state from a JSON snapshot and rebuilds the ARN index.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Distributions == nil {
		snap.Distributions = make(map[string]*Distribution)
	}

	if snap.OAIs == nil {
		snap.OAIs = make(map[string]*OriginAccessIdentity)
	}

	if snap.Invalidations == nil {
		snap.Invalidations = make(map[string][]*Invalidation)
	}

	if snap.AnycastIPLists == nil {
		snap.AnycastIPLists = make(map[string]*AnycastIPList)
	}

	if snap.CachePolicies == nil {
		snap.CachePolicies = make(map[string]*CachePolicy)
	}

	if snap.ConnectionFunctions == nil {
		snap.ConnectionFunctions = make(map[string]*ConnectionFunction)
	}

	if snap.ConnectionGroups == nil {
		snap.ConnectionGroups = make(map[string]*ConnectionGroup)
	}

	if snap.ContinuousDeploymentPolicies == nil {
		snap.ContinuousDeploymentPolicies = make(map[string]*ContinuousDeploymentPolicy)
	}

	if snap.DistributionAliases == nil {
		snap.DistributionAliases = make(map[string][]string)
	}

	if snap.DistributionWebACLs == nil {
		snap.DistributionWebACLs = make(map[string]string)
	}

	if snap.DistributionTenantWebACLs == nil {
		snap.DistributionTenantWebACLs = make(map[string]string)
	}

	// Rebuild the ARN-to-ID index after restore so O(1) tag operations remain correct.
	arnIndex := make(map[string]string, len(snap.Distributions))
	for id, d := range snap.Distributions {
		arnIndex[d.ARN] = id
	}

	b.distributions = snap.Distributions
	b.oais = snap.OAIs
	b.invalidations = snap.Invalidations
	b.anycastIPLists = snap.AnycastIPLists
	b.cachePolicies = snap.CachePolicies
	b.connectionFunctions = snap.ConnectionFunctions
	b.connectionGroups = snap.ConnectionGroups
	b.continuousDeploymentPolicies = snap.ContinuousDeploymentPolicies
	b.distributionAliases = snap.DistributionAliases
	b.distributionWebACLs = snap.DistributionWebACLs
	b.distributionTenantWebACLs = snap.DistributionTenantWebACLs
	b.distributionARNs = arnIndex
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
