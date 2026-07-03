package cloudfront

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Distributions          map[string]*Distribution          `json:"distributions"`
	OAIs                   map[string]*OriginAccessIdentity  `json:"oais"`
	Invalidations          map[string][]*Invalidation        `json:"invalidations,omitempty"`
	AnycastIPLists         map[string]*AnycastIPList         `json:"anycastIPLists,omitempty"`
	StreamingDistributions map[string]*StreamingDistribution `json:"streamingDistributions,omitempty"`

	CachePolicies       map[string]*CachePolicy        `json:"cachePolicies,omitempty"`
	ConnectionFunctions map[string]*ConnectionFunction `json:"connectionFunctions,omitempty"`
	ConnectionGroups    map[string]*ConnectionGroup    `json:"connectionGroups,omitempty"`

	ContinuousDeploymentPolicies map[string]*ContinuousDeploymentPolicy `json:"continuousDeploymentPolicies,omitempty"`

	OriginAccessControls    map[string]*OriginAccessControl   `json:"originAccessControls,omitempty"`
	ResponseHeadersPolicies map[string]*ResponseHeadersPolicy `json:"responseHeadersPolicies,omitempty"`
	Functions               map[string]*Function              `json:"functions,omitempty"`
	OriginRequestPolicies   map[string]*OriginRequestPolicy   `json:"originRequestPolicies,omitempty"`

	FieldLevelEncryptions        map[string]*FieldLevelEncryption        `json:"fieldLevelEncryptions,omitempty"`
	FieldLevelEncryptionProfiles map[string]*FieldLevelEncryptionProfile `json:"fieldLevelEncryptionProfiles,omitempty"`

	PublicKeys         map[string]*PublicKey         `json:"publicKeys,omitempty"`
	KeyGroups          map[string]*KeyGroup          `json:"keyGroups,omitempty"`
	RealtimeLogConfigs map[string]*RealtimeLogConfig `json:"realtimeLogConfigs,omitempty"`
	KeyValueStores     map[string]*KeyValueStore     `json:"keyValueStores,omitempty"`
	VpcOrigins         map[string]*VpcOrigin         `json:"vpcOrigins,omitempty"`

	DistributionFunctionAssociations map[string][]FunctionAssociation `json:"distributionFunctionAssociations,omitempty"`
	DistributionAliases              map[string][]string              `json:"distributionAliases,omitempty"`
	DistributionWebACLs              map[string]string                `json:"distributionWebACLs,omitempty"`
	DistributionTenantWebACLs        map[string]string                `json:"distributionTenantWebACLs,omitempty"`

	AccountID string `json:"accountId"`
	Region    string `json:"region"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot() []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Distributions:                    b.distributions,
		OAIs:                             b.oais,
		Invalidations:                    b.invalidations,
		AnycastIPLists:                   b.anycastIPLists,
		StreamingDistributions:           b.streamingDistributions,
		CachePolicies:                    b.cachePolicies,
		ConnectionFunctions:              b.connectionFunctions,
		ConnectionGroups:                 b.connectionGroups,
		ContinuousDeploymentPolicies:     b.continuousDeploymentPolicies,
		OriginAccessControls:             b.originAccessControls,
		ResponseHeadersPolicies:          b.responseHeadersPolicies,
		Functions:                        b.functions,
		OriginRequestPolicies:            b.originRequestPolicies,
		FieldLevelEncryptions:            b.fieldLevelEncryptions,
		FieldLevelEncryptionProfiles:     b.fieldLevelEncryptionProfiles,
		PublicKeys:                       b.publicKeys,
		KeyGroups:                        b.keyGroups,
		RealtimeLogConfigs:               b.realtimeLogConfigs,
		KeyValueStores:                   b.keyValueStores,
		VpcOrigins:                       b.vpcOrigins,
		DistributionFunctionAssociations: b.distributionFunctionAssociations,
		DistributionAliases:              b.distributionAliases,
		DistributionWebACLs:              b.distributionWebACLs,
		DistributionTenantWebACLs:        b.distributionTenantWebACLs,
		AccountID:                        b.accountID,
		Region:                           b.region,
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
// backendIndexes holds the derived lookup indexes rebuilt from a snapshot.
type backendIndexes struct {
	distributionARNs                  map[string]string
	distributionCallerRefs            map[string]string
	streamingDistributionARNs         map[string]string
	streamingDistributionCallerRefs   map[string]string
	oaiCallerRefs                     map[string]string
	cachePolicyByName                 map[string]string
	originAccessControlByName         map[string]string
	responseHeadersPolicyByName       map[string]string
	originRequestPolicyByName         map[string]string
	fieldLevelEncryptionByName        map[string]string
	fieldLevelEncryptionProfileByName map[string]string
	publicKeyByName                   map[string]string
	keyGroupByName                    map[string]string
	realtimeLogConfigByName           map[string]string
	keyValueStoreByName               map[string]string
}

// rebuildDistributionIndexes derives the ARN and CallerReference indexes for distributions
// and streaming distributions.
func rebuildDistributionIndexes(
	snap *backendSnapshot,
) (map[string]string, map[string]string, map[string]string, map[string]string) {
	arnIndex := make(map[string]string, len(snap.Distributions))
	callerRefIndex := make(map[string]string, len(snap.Distributions))

	for id, d := range snap.Distributions {
		arnIndex[d.ARN] = id
		if d.CallerReference != "" {
			callerRefIndex[d.CallerReference] = id
		}
	}

	sdARNIndex := make(map[string]string, len(snap.StreamingDistributions))
	sdCallerRefIndex := make(map[string]string, len(snap.StreamingDistributions))

	for id, sd := range snap.StreamingDistributions {
		sdARNIndex[sd.ARN] = id
		if sd.Config.CallerReference != "" {
			sdCallerRefIndex[sd.Config.CallerReference] = id
		}
	}

	return arnIndex, callerRefIndex, sdARNIndex, sdCallerRefIndex
}

// rebuildIndexes derives all secondary indexes from a snapshot.
func rebuildIndexes(snap *backendSnapshot) backendIndexes {
	arnIndex, callerRefIndex, sdARNIndex, sdCallerRefIndex := rebuildDistributionIndexes(snap)

	oaiCallerRefIndex := make(map[string]string, len(snap.OAIs))

	for id, oai := range snap.OAIs {
		if oai.CallerReference != "" {
			oaiCallerRefIndex[oai.CallerReference] = id
		}
	}

	cachePolicyByName := make(map[string]string, len(snap.CachePolicies))

	for id, cp := range snap.CachePolicies {
		cachePolicyByName[cp.Name] = id
	}

	oacByName := make(map[string]string, len(snap.OriginAccessControls))

	for id, oac := range snap.OriginAccessControls {
		oacByName[oac.Name] = id
	}

	rhpByName := make(map[string]string, len(snap.ResponseHeadersPolicies))

	for id, p := range snap.ResponseHeadersPolicies {
		rhpByName[p.Name] = id
	}

	orpByName := make(map[string]string, len(snap.OriginRequestPolicies))

	for id, p := range snap.OriginRequestPolicies {
		orpByName[p.Name] = id
	}

	fleByName := make(map[string]string, len(snap.FieldLevelEncryptions))

	for id, fle := range snap.FieldLevelEncryptions {
		fleByName[fle.Name] = id
	}

	flePByName := make(map[string]string, len(snap.FieldLevelEncryptionProfiles))

	for id, p := range snap.FieldLevelEncryptionProfiles {
		flePByName[p.Name] = id
	}

	pkByName := make(map[string]string, len(snap.PublicKeys))

	for id, pk := range snap.PublicKeys {
		pkByName[pk.Name] = id
	}

	kgByName := make(map[string]string, len(snap.KeyGroups))

	for id, kg := range snap.KeyGroups {
		kgByName[kg.Name] = id
	}

	rlcByName := make(map[string]string, len(snap.RealtimeLogConfigs))

	for arn, rlc := range snap.RealtimeLogConfigs {
		rlcByName[rlc.Name] = arn
	}

	kvsByName := make(map[string]string, len(snap.KeyValueStores))

	for id, kvs := range snap.KeyValueStores {
		kvsByName[kvs.Name] = id
	}

	return backendIndexes{
		distributionARNs:                  arnIndex,
		distributionCallerRefs:            callerRefIndex,
		streamingDistributionARNs:         sdARNIndex,
		streamingDistributionCallerRefs:   sdCallerRefIndex,
		oaiCallerRefs:                     oaiCallerRefIndex,
		cachePolicyByName:                 cachePolicyByName,
		originAccessControlByName:         oacByName,
		responseHeadersPolicyByName:       rhpByName,
		originRequestPolicyByName:         orpByName,
		fieldLevelEncryptionByName:        fleByName,
		fieldLevelEncryptionProfileByName: flePByName,
		publicKeyByName:                   pkByName,
		keyGroupByName:                    kgByName,
		realtimeLogConfigByName:           rlcByName,
		keyValueStoreByName:               kvsByName,
	}
}

func (b *InMemoryBackend) Restore(data []byte) error {
	var snap backendSnapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	ensureNonNil(&snap)

	idx := rebuildIndexes(&snap)

	b.distributions = snap.Distributions
	b.oais = snap.OAIs
	b.invalidations = snap.Invalidations
	b.anycastIPLists = snap.AnycastIPLists
	b.streamingDistributions = snap.StreamingDistributions
	b.cachePolicies = snap.CachePolicies
	b.connectionFunctions = snap.ConnectionFunctions
	b.connectionGroups = snap.ConnectionGroups
	b.continuousDeploymentPolicies = snap.ContinuousDeploymentPolicies
	b.originAccessControls = snap.OriginAccessControls
	b.responseHeadersPolicies = snap.ResponseHeadersPolicies
	b.functions = snap.Functions
	b.originRequestPolicies = snap.OriginRequestPolicies
	b.fieldLevelEncryptions = snap.FieldLevelEncryptions
	b.fieldLevelEncryptionProfiles = snap.FieldLevelEncryptionProfiles
	b.publicKeys = snap.PublicKeys
	b.keyGroups = snap.KeyGroups
	b.realtimeLogConfigs = snap.RealtimeLogConfigs
	b.keyValueStores = snap.KeyValueStores
	b.vpcOrigins = snap.VpcOrigins
	b.distributionFunctionAssociations = snap.DistributionFunctionAssociations
	b.distributionAliases = snap.DistributionAliases
	b.distributionWebACLs = snap.DistributionWebACLs
	b.distributionTenantWebACLs = snap.DistributionTenantWebACLs
	b.distributionARNs = idx.distributionARNs
	b.distributionCallerRefs = idx.distributionCallerRefs
	b.streamingDistributionARNs = idx.streamingDistributionARNs
	b.streamingDistributionCallerRefs = idx.streamingDistributionCallerRefs
	b.oaiCallerRefs = idx.oaiCallerRefs
	b.cachePolicyByName = idx.cachePolicyByName
	b.originAccessControlByName = idx.originAccessControlByName
	b.responseHeadersPolicyByName = idx.responseHeadersPolicyByName
	b.originRequestPolicyByName = idx.originRequestPolicyByName
	b.fieldLevelEncryptionByName = idx.fieldLevelEncryptionByName
	b.fieldLevelEncryptionProfileByName = idx.fieldLevelEncryptionProfileByName
	b.publicKeyByName = idx.publicKeyByName
	b.keyGroupByName = idx.keyGroupByName
	b.realtimeLogConfigByName = idx.realtimeLogConfigByName
	b.keyValueStoreByName = idx.keyValueStoreByName
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// ensureNonNil initialises any nil maps in a snapshot to empty maps so that
// the backend never holds nil map references.
func ensureNonNil(snap *backendSnapshot) {
	ensureNonNilBaseEntities(snap)
	ensureNonNilPolicies(snap)
	ensureNonNilNewResources(snap)
}

func ensureNonNilBaseEntities(snap *backendSnapshot) {
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

	if snap.StreamingDistributions == nil {
		snap.StreamingDistributions = make(map[string]*StreamingDistribution)
	}

	if snap.ConnectionFunctions == nil {
		snap.ConnectionFunctions = make(map[string]*ConnectionFunction)
	}

	if snap.ConnectionGroups == nil {
		snap.ConnectionGroups = make(map[string]*ConnectionGroup)
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

func ensureNonNilPolicies(snap *backendSnapshot) {
	if snap.CachePolicies == nil {
		snap.CachePolicies = make(map[string]*CachePolicy)
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
}

func ensureNonNilNewResources(snap *backendSnapshot) {
	if snap.FieldLevelEncryptions == nil {
		snap.FieldLevelEncryptions = make(map[string]*FieldLevelEncryption)
	}

	if snap.FieldLevelEncryptionProfiles == nil {
		snap.FieldLevelEncryptionProfiles = make(map[string]*FieldLevelEncryptionProfile)
	}

	if snap.PublicKeys == nil {
		snap.PublicKeys = make(map[string]*PublicKey)
	}

	if snap.KeyGroups == nil {
		snap.KeyGroups = make(map[string]*KeyGroup)
	}

	if snap.RealtimeLogConfigs == nil {
		snap.RealtimeLogConfigs = make(map[string]*RealtimeLogConfig)
	}

	if snap.KeyValueStores == nil {
		snap.KeyValueStores = make(map[string]*KeyValueStore)
	}

	if snap.VpcOrigins == nil {
		snap.VpcOrigins = make(map[string]*VpcOrigin)
	}

	if snap.DistributionFunctionAssociations == nil {
		snap.DistributionFunctionAssociations = make(map[string][]FunctionAssociation)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte { return h.Backend.Snapshot() }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error { return h.Backend.Restore(data) }
