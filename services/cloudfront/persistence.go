package cloudfront

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Distributions                map[string]*Distribution               `json:"distributions"`
	OAIs                         map[string]*OriginAccessIdentity       `json:"oais"`
	Invalidations                map[string][]*Invalidation             `json:"invalidations,omitempty"`
	AnycastIPLists               map[string]*AnycastIPList              `json:"anycastIPLists,omitempty"`
	CachePolicies                map[string]*CachePolicy                `json:"cachePolicies,omitempty"`
	ConnectionFunctions          map[string]*ConnectionFunction         `json:"connectionFunctions,omitempty"`
	ConnectionGroups             map[string]*ConnectionGroup            `json:"connectionGroups,omitempty"`
	ContinuousDeploymentPolicies map[string]*ContinuousDeploymentPolicy `json:"continuousDeploymentPolicies,omitempty"`
	OriginAccessControls         map[string]*OriginAccessControl        `json:"originAccessControls,omitempty"`
	ResponseHeadersPolicies      map[string]*ResponseHeadersPolicy      `json:"responseHeadersPolicies,omitempty"`
	Functions                    map[string]*Function                   `json:"functions,omitempty"`
	OriginRequestPolicies        map[string]*OriginRequestPolicy        `json:"originRequestPolicies,omitempty"`
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
		OriginAccessControls:         b.originAccessControls,
		ResponseHeadersPolicies:      b.responseHeadersPolicies,
		Functions:                    b.functions,
		OriginRequestPolicies:        b.originRequestPolicies,
		DistributionAliases:          b.distributionAliases,
		DistributionWebACLs:          b.distributionWebACLs,
		DistributionTenantWebACLs:    b.distributionTenantWebACLs,
		AccountID:                    b.accountID,
		Region:                       b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		// Log the marshal failure so operators can detect data-loss scenarios.
		slog.Default().Warn("cloudfront: Snapshot marshal failure", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot and rebuilds derived indexes.
func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	ensureNonNil(&snap)

	// Rebuild the ARN-to-ID index.
	arnIndex := make(map[string]string, len(snap.Distributions))
	// Rebuild the CallerReference-to-ID index.
	callerRefIndex := make(map[string]string, len(snap.Distributions))
	for id, d := range snap.Distributions {
		arnIndex[d.ARN] = id
		if d.CallerReference != "" {
			callerRefIndex[d.CallerReference] = id
		}
	}

	// Rebuild OAI CallerReference index.
	oaiCallerRefIndex := make(map[string]string, len(snap.OAIs))
	for id, oai := range snap.OAIs {
		if oai.CallerReference != "" {
			oaiCallerRefIndex[oai.CallerReference] = id
		}
	}

	// Rebuild cache policy by-name index.
	cachePolicyByName := make(map[string]string, len(snap.CachePolicies))
	for id, cp := range snap.CachePolicies {
		cachePolicyByName[cp.Name] = id
	}

	// Rebuild OAC by-name index.
	oacByName := make(map[string]string, len(snap.OriginAccessControls))
	for id, oac := range snap.OriginAccessControls {
		oacByName[oac.Name] = id
	}

	// Rebuild response headers policy by-name index.
	rhpByName := make(map[string]string, len(snap.ResponseHeadersPolicies))
	for id, p := range snap.ResponseHeadersPolicies {
		rhpByName[p.Name] = id
	}

	// Rebuild origin request policy by-name index.
	orpByName := make(map[string]string, len(snap.OriginRequestPolicies))
	for id, p := range snap.OriginRequestPolicies {
		orpByName[p.Name] = id
	}

	b.distributions = snap.Distributions
	b.oais = snap.OAIs
	b.invalidations = snap.Invalidations
	b.anycastIPLists = snap.AnycastIPLists
	b.cachePolicies = snap.CachePolicies
	b.connectionFunctions = snap.ConnectionFunctions
	b.connectionGroups = snap.ConnectionGroups
	b.continuousDeploymentPolicies = snap.ContinuousDeploymentPolicies
	b.originAccessControls = snap.OriginAccessControls
	b.responseHeadersPolicies = snap.ResponseHeadersPolicies
	b.functions = snap.Functions
	b.originRequestPolicies = snap.OriginRequestPolicies
	b.distributionAliases = snap.DistributionAliases
	b.distributionWebACLs = snap.DistributionWebACLs
	b.distributionTenantWebACLs = snap.DistributionTenantWebACLs
	b.distributionARNs = arnIndex
	b.distributionCallerRefs = callerRefIndex
	b.oaiCallerRefs = oaiCallerRefIndex
	b.cachePolicyByName = cachePolicyByName
	b.originAccessControlByName = oacByName
	b.responseHeadersPolicyByName = rhpByName
	b.originRequestPolicyByName = orpByName
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNil initialises any nil maps in a snapshot to empty maps so that
// the backend never holds nil map references.
func ensureNonNil(snap *backendSnapshot) {
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

	if snap.OriginAccessControls == nil {
		snap.OriginAccessControls = make(map[string]*OriginAccessControl)
	}

	if snap.ResponseHeadersPolicies == nil {
		snap.ResponseHeadersPolicies = make(map[string]*ResponseHeadersPolicy)
	}

	if snap.Functions == nil {
		snap.Functions = make(map[string]*Function)
	}

	if snap.OriginRequestPolicies == nil {
		snap.OriginRequestPolicies = make(map[string]*OriginRequestPolicy)
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
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
