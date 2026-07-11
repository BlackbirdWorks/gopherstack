package apigateway

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/ec2 (commit 12e611a4) / services/sqs (commit 0f09d77c)
// conversions this follows.
//
// Several resource families (resources, deployments, stages, authorizers,
// requestValidators, documentationParts, documentationVersions, models,
// usagePlanKeys) were previously nested per-parent maps (map[string]*apiData
// holding its own map[string]*T, or usagePlanID -> keyID -> *UsagePlanKey).
// store.Table has no notion of nesting, so each becomes a single flat table
// keyed by a composite "<parentID>#<childID>" string, with a secondary
// [store.Index] grouping by parent ID for the "all children of parent X"
// lookups the nested maps used to answer directly. This requires the child
// value type to carry its parent's ID as a field purely for identity/keying
// (never on the wire) -- Resource/Stage/Deployment/Model/DocumentationPart/
// DocumentationVersion already carried `RestAPIID string `json:"-"`` for
// exactly this reason; Authorizer, RequestValidator, and UsagePlanKey gained
// the same field (RestAPIID / UsagePlanID) as part of this conversion.
//
// A handful of fields are deliberately NOT registered here and remain plain
// maps -- see the comment block above registerAllTables for the list and why.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func restAPIKeyFn(v *RestAPI) string { return v.ID }

// resourceKey/deploymentKey/... build the composite "<restAPIID>#<childID>"
// key shared by every resource family nested under a REST API. Access sites
// use these directly so the exact same key is used for Put/Get/Delete.
func resourceKey(restAPIID, resourceID string) string { return restAPIID + "#" + resourceID }
func resourceKeyFn(v *Resource) string                { return resourceKey(v.RestAPIID, v.ID) }

func deploymentKey(restAPIID, deploymentID string) string { return restAPIID + "#" + deploymentID }
func deploymentKeyFn(v *Deployment) string                { return deploymentKey(v.RestAPIID, v.ID) }

func stageKey(restAPIID, stageName string) string { return restAPIID + "#" + stageName }
func stageKeyFn(v *Stage) string                  { return stageKey(v.RestAPIID, v.StageName) }

func authorizerKey(restAPIID, authorizerID string) string { return restAPIID + "#" + authorizerID }
func authorizerKeyFn(v *Authorizer) string                { return authorizerKey(v.RestAPIID, v.ID) }

func requestValidatorKey(restAPIID, validatorID string) string {
	return restAPIID + "#" + validatorID
}
func requestValidatorKeyFn(v *RequestValidator) string { return requestValidatorKey(v.RestAPIID, v.ID) }

func documentationPartKey(restAPIID, docPartID string) string { return restAPIID + "#" + docPartID }
func documentationPartKeyFn(v *DocumentationPart) string {
	return documentationPartKey(v.RestAPIID, v.ID)
}

func documentationVersionKey(restAPIID, version string) string { return restAPIID + "#" + version }
func documentationVersionKeyFn(v *DocumentationVersion) string {
	return documentationVersionKey(v.RestAPIID, v.Version)
}

func modelKey(restAPIID, modelID string) string { return restAPIID + "#" + modelID }
func modelKeyFn(v *Model) string                { return modelKey(v.RestAPIID, v.ID) }

func apiKeyKeyFn(v *APIKey) string { return v.ID }

func basePathMappingKey(domainName, basePath string) string { return domainName + "#" + basePath }
func basePathMappingKeyFn(v *BasePathMapping) string {
	return basePathMappingKey(v.DomainName, v.BasePath)
}

func domainNameKeyFn(v *DomainName) string { return v.DomainNameValue }

func domainNameAccessAssociationKeyFn(v *DomainNameAccessAssociation) string {
	return v.DomainNameAccessAssociationARN
}

func usagePlanKeyFn(v *UsagePlan) string { return v.ID }

func usagePlanKeyKey(usagePlanID, keyID string) string { return usagePlanID + "#" + keyID }
func usagePlanKeyKeyFn(v *UsagePlanKey) string         { return usagePlanKeyKey(v.UsagePlanID, v.ID) }

// gatewayResponseKeyFn reuses gatewayResponseKey (defined in backend.go, next
// to the other GatewayResponse helpers) so the table key matches exactly what
// every access site already computes.
func gatewayResponseKeyFn(v *GatewayResponse) string {
	return gatewayResponseKey(v.RestAPIID, v.ResponseType)
}

func clientCertificateKeyFn(v *ClientCertificate) string { return v.ClientCertificateID }

func vpcLinkKeyFn(v *VpcLink) string { return v.ID }

// registerAllTables sets up every converted resource collection exactly once,
// at construction time.
//
// Two groups, split by whether their store.Table can be snapshotted directly:
//
//   - "Clean" tables (restApis, apiKeys, basePathMappings, domainNames,
//     domainNameAccessAssociations, usagePlans, gatewayResponses,
//     clientCertificates, vpcLinks) are registered on b.registry via
//     store.Register, so persistence.go's Snapshot/Restore can drive them
//     through b.registry.SnapshotAll()/RestoreAll() -- their value types
//     marshal to JSON with no information loss.
//   - "Dirty" tables (resources, deployments, stages, authorizers,
//     requestValidators, documentationParts, documentationVersions, models,
//     usagePlanKeys) are built with store.New but deliberately NOT registered
//     on b.registry. Their composite key depends on a field
//     (RestAPIID/UsagePlanID) tagged `json:"-"` on the live type (see
//     models.go), so a direct json.Marshal/Unmarshal round trip through
//     Table.Snapshot/Restore would silently drop that field and corrupt the
//     table's key on restore. persistence.go instead builds a throwaway DTO
//     [store.Registry] purely to get a correctly-tagged JSON encoding (mirrors
//     the services/sqs pilot, commit 0f09d77c), then restores the live tables
//     directly via Table.Restore. Because they aren't registered on
//     b.registry, InMemoryBackend.Reset resets each of them with an explicit
//     Table.Reset() call alongside b.registry.ResetAll().
//
// The following fields are deliberately plain maps, not store.Table at all --
// see registerAllTables's callers for the full reasoning:
//   - resourceVersions (restAPIID -> uint64): an auxiliary mutation counter
//     used by ResourcesForRouting to invalidate the data-plane's cached
//     routing trie, not a resource collection -- the value has no identity of
//     its own to key a Table by.
//   - apiKeysByValue (value -> keyID): a plain string->string secondary
//     lookup index, not a map[string]*T resource collection. Kept as a
//     hand-maintained parallel map (mirroring how it was maintained
//     alongside apiKeys before conversion) rather than a store.Index, since
//     an Index groups into a slice (1:many) where this lookup is 1:1
//     overwrite-on-collision, matching the pre-conversion map semantics
//     exactly.
//   - usageOverrides (usagePlanID -> keyID -> remaining quota int64): value
//     type is a bare int64 with no identity field of its own to key a Table
//     by, same class of exception as EC2's instanceIMDSOptions.
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call (plus any secondary store.AddIndex) to the concrete field and value
// type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.restApis = store.Register(b.registry, "restApis", store.New(restAPIKeyFn))
	},
	// "Dirty" tables: store.New only, NOT store.Register -- see registerAllTables doc.
	func(b *InMemoryBackend) {
		b.resources = store.New(resourceKeyFn)
		b.resourcesByAPI = b.resources.AddIndex("byAPI", func(v *Resource) string { return v.RestAPIID })
	},
	func(b *InMemoryBackend) {
		b.deployments = store.New(deploymentKeyFn)
		b.deploymentsByAPI = b.deployments.AddIndex("byAPI", func(v *Deployment) string { return v.RestAPIID })
	},
	func(b *InMemoryBackend) {
		b.stages = store.New(stageKeyFn)
		b.stagesByAPI = b.stages.AddIndex("byAPI", func(v *Stage) string { return v.RestAPIID })
	},
	func(b *InMemoryBackend) {
		b.authorizers = store.New(authorizerKeyFn)
		b.authorizersByAPI = b.authorizers.AddIndex("byAPI", func(v *Authorizer) string { return v.RestAPIID })
	},
	func(b *InMemoryBackend) {
		b.requestValidators = store.New(requestValidatorKeyFn)
		b.requestValidatorsByAPI = b.requestValidators.AddIndex(
			"byAPI",
			func(v *RequestValidator) string { return v.RestAPIID },
		)
	},
	func(b *InMemoryBackend) {
		b.documentationParts = store.New(documentationPartKeyFn)
		b.documentationPartsByAPI = b.documentationParts.AddIndex(
			"byAPI",
			func(v *DocumentationPart) string { return v.RestAPIID },
		)
	},
	func(b *InMemoryBackend) {
		b.documentationVersions = store.New(documentationVersionKeyFn)
		b.documentationVersionsByAPI = b.documentationVersions.AddIndex(
			"byAPI",
			func(v *DocumentationVersion) string { return v.RestAPIID },
		)
	},
	func(b *InMemoryBackend) {
		b.models = store.New(modelKeyFn)
		b.modelsByAPI = b.models.AddIndex("byAPI", func(v *Model) string { return v.RestAPIID })
	},
	func(b *InMemoryBackend) {
		b.usagePlanKeys = store.New(usagePlanKeyKeyFn)
		b.usagePlanKeysByPlan = b.usagePlanKeys.AddIndex(
			"byPlan",
			func(v *UsagePlanKey) string { return v.UsagePlanID },
		)
	},
	// "Clean" tables: registered on b.registry.
	func(b *InMemoryBackend) {
		b.apiKeys = store.Register(b.registry, "apiKeys", store.New(apiKeyKeyFn))
	},
	func(b *InMemoryBackend) {
		b.basePathMappings = store.Register(b.registry, "basePathMappings", store.New(basePathMappingKeyFn))
	},
	func(b *InMemoryBackend) {
		b.domainNames = store.Register(b.registry, "domainNames", store.New(domainNameKeyFn))
	},
	func(b *InMemoryBackend) {
		b.domainNameAccessAssociations = store.Register(
			b.registry, "domainNameAccessAssociations", store.New(domainNameAccessAssociationKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.usagePlans = store.Register(b.registry, "usagePlans", store.New(usagePlanKeyFn))
	},
	func(b *InMemoryBackend) {
		b.gatewayResponses = store.Register(b.registry, "gatewayResponses", store.New(gatewayResponseKeyFn))
	},
	func(b *InMemoryBackend) {
		b.clientCertificates = store.Register(b.registry, "clientCertificates", store.New(clientCertificateKeyFn))
	},
	func(b *InMemoryBackend) {
		b.vpcLinks = store.Register(b.registry, "vpcLinks", store.New(vpcLinkKeyFn))
	},
}
