package cloudfront

import (
	"encoding/json"
	"log/slog"
)

type backendSnapshot struct {
	Distributions  map[string]*Distribution         `json:"distributions"`
	OAIs           map[string]*OriginAccessIdentity `json:"oais"`
	Invalidations  map[string][]*Invalidation       `json:"invalidations,omitempty"`
	AnycastIPLists map[string]*AnycastIPList        `json:"anycastIPLists,omitempty"`

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
	connectionFunctionByName          map[string]string
}

// rebuildIndexes derives all secondary indexes from a snapshot.
func rebuildIndexes(snap *backendSnapshot) backendIndexes {
	arnIndex, callerRefIndex := rebuildDistributionIndexes(snap)
	oaiCallerRefIndex := rebuildOAIIndex(snap)
	byName := rebuildByNameIndexes(snap)

	return backendIndexes{
		distributionARNs:                  arnIndex,
		distributionCallerRefs:            callerRefIndex,
		oaiCallerRefs:                     oaiCallerRefIndex,
		cachePolicyByName:                 byName["cachePolicy"],
		originAccessControlByName:         byName["oac"],
		responseHeadersPolicyByName:       byName["rhp"],
		originRequestPolicyByName:         byName["orp"],
		fieldLevelEncryptionByName:        byName["fle"],
		fieldLevelEncryptionProfileByName: byName["flep"],
		publicKeyByName:                   byName["pk"],
		keyGroupByName:                    byName["kg"],
		realtimeLogConfigByName:           byName["rlc"],
		keyValueStoreByName:               byName["kvs"],
		connectionFunctionByName:          byName["cfn"],
	}
}

func rebuildDistributionIndexes(snap *backendSnapshot) (map[string]string, map[string]string) {
	arnIndex := make(map[string]string, len(snap.Distributions))
	callerRefIndex := make(map[string]string, len(snap.Distributions))

	for id, d := range snap.Distributions {
		arnIndex[d.ARN] = id
		if d.CallerReference != "" {
			callerRefIndex[d.CallerReference] = id
		}
	}

	return arnIndex, callerRefIndex
}

func rebuildOAIIndex(snap *backendSnapshot) map[string]string {
	oaiCallerRefIndex := make(map[string]string, len(snap.OAIs))

	for id, oai := range snap.OAIs {
		if oai.CallerReference != "" {
			oaiCallerRefIndex[oai.CallerReference] = id
		}
	}

	return oaiCallerRefIndex
}

func rebuildByNameIndexes(snap *backendSnapshot) map[string]map[string]string {
	result := map[string]map[string]string{
		"cachePolicy": make(map[string]string, len(snap.CachePolicies)),
		"oac":         make(map[string]string, len(snap.OriginAccessControls)),
		"rhp":         make(map[string]string, len(snap.ResponseHeadersPolicies)),
		"orp":         make(map[string]string, len(snap.OriginRequestPolicies)),
		"fle":         make(map[string]string, len(snap.FieldLevelEncryptions)),
		"flep":        make(map[string]string, len(snap.FieldLevelEncryptionProfiles)),
		"pk":          make(map[string]string, len(snap.PublicKeys)),
		"kg":          make(map[string]string, len(snap.KeyGroups)),
		"rlc":         make(map[string]string, len(snap.RealtimeLogConfigs)),
		"kvs":         make(map[string]string, len(snap.KeyValueStores)),
		"cfn":         make(map[string]string, len(snap.ConnectionFunctions)),
	}

	for id, cp := range snap.CachePolicies {
		result["cachePolicy"][cp.Name] = id
	}
	for id, oac := range snap.OriginAccessControls {
		result["oac"][oac.Name] = id
	}
	for id, p := range snap.ResponseHeadersPolicies {
		result["rhp"][p.Name] = id
	}
	for id, p := range snap.OriginRequestPolicies {
		result["orp"][p.Name] = id
	}
	for id, fle := range snap.FieldLevelEncryptions {
		result["fle"][fle.Name] = id
	}
	for id, p := range snap.FieldLevelEncryptionProfiles {
		result["flep"][p.Name] = id
	}
	for id, pk := range snap.PublicKeys {
		result["pk"][pk.Name] = id
	}
	for id, kg := range snap.KeyGroups {
		result["kg"][kg.Name] = id
	}
	for arn, rlc := range snap.RealtimeLogConfigs {
		result["rlc"][rlc.Name] = arn
	}
	for id, kvs := range snap.KeyValueStores {
		result["kvs"][kvs.Name] = id
	}
	for id, fn := range snap.ConnectionFunctions {
		result["cfn"][fn.Name] = id
	}

	return result
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
	b.connectionFunctionByName = idx.connectionFunctionByName
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
