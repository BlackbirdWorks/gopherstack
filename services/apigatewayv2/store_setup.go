package apigatewayv2

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/apigateway (commit 6da0334e) / services/appsync (commit
// 870feb90) conversions this follows closely -- apigatewayv2's shape (a set
// of resource families nested under an API ID, plus a few nested under a
// domain name or portal product) is very similar to apigateway's.
//
// # Nested collections
//
// stages, routes, integrations, deployments, authorizers, and models were
// previously nested per-parent maps (map[apiID]*apiData holding its own
// map[childID]*T inside apiData). store.Table has no notion of nesting, so
// each becomes a single flat table keyed by a composite "<apiID>#<childID>"
// string, with a secondary [store.Index] grouping by API ID for the "all
// children of API X" lookups the nested maps used to answer directly. This
// requires the child value type to carry its parent's API ID as a field
// purely for identity/keying (never on the wire) -- Stage, Route,
// Integration, Deployment, Authorizer, and Model already carry
// `APIID string `json:"-"`` for exactly this reason (see models.go).
//
// integrationResponses and routeResponses were nested two levels deep
// (map[apiID]*apiData -> map[integrationID/routeID]map[responseID]*T). Each
// becomes a flat table keyed by the full composite
// "<apiID>#<integrationID/routeID>#<responseID>", with a secondary index
// grouping by the "<apiID>#<parentID>" prefix (reusing integrationKey/
// routeKey) for the "all responses of integration/route X" lookups.
//
// apiMappings and routingRules were nested per-domain maps
// (map[domainName]map[id]*T); each becomes a flat table keyed by
// "<domainName>#<id>" with a secondary index grouping by domain name.
//
// productPages and productREPages were map[portalProductID][]*T (an
// append-only slice per parent). Each element already carries its own ID
// (ProductPageID/ProductRestEndpointPageID) and its parent ID
// (`PortalProductID string `json:"-"``), so -- exactly like the families
// above -- these become flat composite-key tables with a secondary index
// grouping by portal product ID, rather than being left as raw slice-valued
// maps: unlike EC2's/apigateway's genuinely identity-less slice exceptions,
// every element here already has the identity fields a Table/Index needs.
//
// # What is NOT converted here, and why
//
//   - portalProductSharingPolicies (portalProductID -> policy document
//     string): the value is a bare string with no identity field of its own
//     to key a Table by, same exception class as EC2's instanceIMDSOptions
//     and apigateway's usageOverrides. Left as a plain map, reset by hand in
//     InMemoryBackend.Reset.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func apiKeyFn(v *API) string { return v.APIID }

// stageKey/routeKey/... build the composite "<apiID>#<childID>" key shared by
// every resource family nested under an API. Access sites use these directly
// so the exact same key is used for Put/Get/Delete.
func stageKey(apiID, stageName string) string { return apiID + "#" + stageName }
func stageKeyFn(v *Stage) string              { return stageKey(v.APIID, v.StageName) }

func routeKey(apiID, routeID string) string { return apiID + "#" + routeID }
func routeKeyFn(v *Route) string            { return routeKey(v.APIID, v.RouteID) }

func integrationKey(apiID, integrationID string) string { return apiID + "#" + integrationID }
func integrationKeyFn(v *Integration) string            { return integrationKey(v.APIID, v.IntegrationID) }

func deploymentKey(apiID, deploymentID string) string { return apiID + "#" + deploymentID }
func deploymentKeyFn(v *Deployment) string            { return deploymentKey(v.APIID, v.DeploymentID) }

func authorizerKey(apiID, authorizerID string) string { return apiID + "#" + authorizerID }
func authorizerKeyFn(v *Authorizer) string            { return authorizerKey(v.APIID, v.AuthorizerID) }

func modelKey(apiID, modelID string) string { return apiID + "#" + modelID }
func modelKeyFn(v *Model) string            { return modelKey(v.APIID, v.ModelID) }

// integrationResponseKey builds the composite "<apiID>#<integrationID>#<responseID>"
// key for the integrationResponses table; integrationKey builds the
// "<apiID>#<integrationID>" prefix reused as the secondary index's group key.
func integrationResponseKey(apiID, integrationID, responseID string) string {
	return integrationKey(apiID, integrationID) + "#" + responseID
}
func integrationResponseKeyFn(v *IntegrationResponse) string {
	return integrationResponseKey(v.APIID, v.IntegrationID, v.IntegrationResponseID)
}
func integrationResponseIntegrationIndexKeyFn(v *IntegrationResponse) string {
	return integrationKey(v.APIID, v.IntegrationID)
}

// routeResponseKey builds the composite "<apiID>#<routeID>#<responseID>" key
// for the routeResponses table; routeKey builds the "<apiID>#<routeID>"
// prefix reused as the secondary index's group key.
func routeResponseKey(apiID, routeID, responseID string) string {
	return routeKey(apiID, routeID) + "#" + responseID
}
func routeResponseKeyFn(v *RouteResponse) string {
	return routeResponseKey(v.APIID, v.RouteID, v.RouteResponseID)
}
func routeResponseRouteIndexKeyFn(v *RouteResponse) string {
	return routeKey(v.APIID, v.RouteID)
}

func domainNameKeyFn(v *DomainName) string { return v.DomainNameValue }

// apiMappingKey builds the composite "<domainName>#<mappingID>" key shared by
// every API mapping access site.
func apiMappingKey(domainName, mappingID string) string { return domainName + "#" + mappingID }
func apiMappingKeyFn(v *APIMapping) string              { return apiMappingKey(v.DomainName, v.APIMappingID) }
func apiMappingDomainIndexKeyFn(v *APIMapping) string   { return v.DomainName }

func portalKeyFn(v *Portal) string               { return v.PortalID }
func portalProductKeyFn(v *PortalProduct) string { return v.PortalProductID }

// productPageKey builds the composite "<portalProductID>#<pageID>" key shared
// by every product page access site.
func productPageKey(portalProductID, pageID string) string { return portalProductID + "#" + pageID }
func productPageKeyFn(v *ProductPage) string {
	return productPageKey(v.PortalProductID, v.ProductPageID)
}
func productPagePortalProductIndexKeyFn(v *ProductPage) string { return v.PortalProductID }

func productREPageKey(portalProductID, pageID string) string { return portalProductID + "#" + pageID }
func productREPageKeyFn(v *ProductRestEndpointPage) string {
	return productREPageKey(v.PortalProductID, v.ProductRestEndpointPageID)
}
func productREPagePortalProductIndexKeyFn(v *ProductRestEndpointPage) string {
	return v.PortalProductID
}

func vpcLinkKeyFn(v *VpcLink) string { return v.VpcLinkID }

// routingRuleKey builds the composite "<domainName>#<routingRuleID>" key
// shared by every routing rule access site.
func routingRuleKey(domainName, routingRuleID string) string {
	return domainName + "#" + routingRuleID
}
func routingRuleKeyFn(v *RoutingRule) string            { return routingRuleKey(v.DomainName, v.RoutingRuleID) }
func routingRuleDomainIndexKeyFn(v *RoutingRule) string { return v.DomainName }

// registerAllTables sets up every converted resource collection exactly once,
// at construction time.
//
// Two groups, split by whether their store.Table can be snapshotted directly:
//
//   - "Clean" tables (apis, domainNames, portals, portalProducts, vpcLinks)
//     are registered on b.registry via store.Register, so persistence.go's
//     Snapshot/Restore can drive them through
//     b.registry.SnapshotAll()/RestoreAll() -- their value types marshal to
//     JSON with no information loss.
//   - "Dirty" tables (stages, routes, integrations, deployments,
//     authorizers, models, integrationResponses, routeResponses,
//     apiMappings, productPages, productREPages, routingRules) are built
//     with store.New but deliberately NOT registered on b.registry. Their
//     composite key depends on a field (APIID/DomainName/PortalProductID)
//     tagged `json:"-"` on the live type (see models.go), so a direct
//     json.Marshal/Unmarshal round trip through Table.Snapshot/Restore would
//     silently drop that field and corrupt the table's key on restore.
//     persistence.go instead builds a throwaway DTO [store.Registry] purely
//     to get a correctly-tagged JSON encoding (mirrors services/apigateway,
//     commit 6da0334e), then restores the live tables directly via
//     Table.Restore. Because they aren't registered on b.registry,
//     InMemoryBackend.Reset resets each of them with an explicit
//     Table.Reset() call alongside b.registry.ResetAll().
//
// The following field is deliberately a plain map, not a store.Table at all
// -- see this file's doc comment for the reasoning:
//   - portalProductSharingPolicies (portalProductID -> policy document
//     string): a bare string value with no identity field of its own to key
//     a Table by.
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
	// "Clean" tables: registered on b.registry.
	func(b *InMemoryBackend) {
		b.apis = store.Register(b.registry, "apis", store.New(apiKeyFn))
	},
	func(b *InMemoryBackend) {
		b.domainNames = store.Register(b.registry, "domainNames", store.New(domainNameKeyFn))
	},
	func(b *InMemoryBackend) {
		b.portals = store.Register(b.registry, "portals", store.New(portalKeyFn))
	},
	func(b *InMemoryBackend) {
		b.portalProducts = store.Register(b.registry, "portalProducts", store.New(portalProductKeyFn))
	},
	func(b *InMemoryBackend) {
		b.vpcLinks = store.Register(b.registry, "vpcLinks", store.New(vpcLinkKeyFn))
	},
	// "Dirty" tables: store.New only, NOT store.Register -- see
	// registerAllTables's doc above.
	func(b *InMemoryBackend) {
		b.stages = store.New(stageKeyFn)
		b.stagesByAPI = b.stages.AddIndex("byAPI", func(v *Stage) string { return v.APIID })
	},
	func(b *InMemoryBackend) {
		b.routes = store.New(routeKeyFn)
		b.routesByAPI = b.routes.AddIndex("byAPI", func(v *Route) string { return v.APIID })
	},
	func(b *InMemoryBackend) {
		b.integrations = store.New(integrationKeyFn)
		b.integrationsByAPI = b.integrations.AddIndex("byAPI", func(v *Integration) string { return v.APIID })
	},
	func(b *InMemoryBackend) {
		b.deployments = store.New(deploymentKeyFn)
		b.deploymentsByAPI = b.deployments.AddIndex("byAPI", func(v *Deployment) string { return v.APIID })
	},
	func(b *InMemoryBackend) {
		b.authorizers = store.New(authorizerKeyFn)
		b.authorizersByAPI = b.authorizers.AddIndex("byAPI", func(v *Authorizer) string { return v.APIID })
	},
	func(b *InMemoryBackend) {
		b.models = store.New(modelKeyFn)
		b.modelsByAPI = b.models.AddIndex("byAPI", func(v *Model) string { return v.APIID })
	},
	func(b *InMemoryBackend) {
		b.integrationResponses = store.New(integrationResponseKeyFn)
		b.integrationResponsesByIntegration = b.integrationResponses.AddIndex(
			"byIntegration", integrationResponseIntegrationIndexKeyFn,
		)
	},
	func(b *InMemoryBackend) {
		b.routeResponses = store.New(routeResponseKeyFn)
		b.routeResponsesByRoute = b.routeResponses.AddIndex("byRoute", routeResponseRouteIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.apiMappings = store.New(apiMappingKeyFn)
		b.apiMappingsByDomain = b.apiMappings.AddIndex("byDomain", apiMappingDomainIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.productPages = store.New(productPageKeyFn)
		b.productPagesByPortalProduct = b.productPages.AddIndex("byPortalProduct", productPagePortalProductIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.productREPages = store.New(productREPageKeyFn)
		b.productREPagesByPortalProduct = b.productREPages.AddIndex(
			"byPortalProduct", productREPagePortalProductIndexKeyFn,
		)
	},
	func(b *InMemoryBackend) {
		b.routingRules = store.New(routingRuleKeyFn)
		b.routingRulesByDomain = b.routingRules.AddIndex("byDomain", routingRuleDomainIndexKeyFn)
	},
}
