package cloudformation

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// single-level map[string]*T resource field on InMemoryBackend is registered
// exactly once, here, as a *store.Table[T] on b.registry. See pkgs/store's
// package doc and the services/sqs pilot (commit 0f09d77c) / services/ec2
// conversion (commit 12e611a4) for the pattern this follows.
//
// The following resource fields are deliberately NOT registered here and
// remain plain maps, because store.Table requires a single *V per key and a
// pure key function derived from V's own fields -- neither holds for a
// nested map[string]map[string]V or a one-to-many map[string][]V:
//   - stackIDIndex: reverse index (stackID ARN -> stackName); the map value
//     is a bare string with no field to derive the ARN key from
//   - events: map[string][]StackEvent -- one-to-many, not a single *V per key
//   - resources: map[string]map[string]*StackResource -- nested (stackID ->
//     logicalID -> resource)
//   - changeSets: map[string]map[string]*ChangeSet -- nested (stackName ->
//     changeSetName -> change set)
//   - stackPolicies: map[string]string -- bare string value, no identity field
//   - stackInstances: map[string][]StackInstance -- one-to-many
//   - stackSetOperations: map[string]map[string]*StackSetOperation -- nested
//     (stackSetName -> operationID -> op)
//   - typeConfigs: map[string]string -- bare string value
//   - handlerProgress: map[string]string -- bare string value
//   - signals: map[string][]SignalRecord -- one-to-many, and SignalRecord
//     carries no stackName/logicalID field to key by
//   - stackSetOpResults: map[string]map[string][]StackSetOperationResult --
//     doubly-nested one-to-many
//   - typeVersions: map[string][]*RegisteredTypeVersion -- one-to-many, and
//     order-dependent (RegisterType/SetTypeDefaultVersion walk insertion
//     order to flip IsDefault on prior versions)
//   - resourceScanItems: map[string][]ScannedResource -- one-to-many, and
//     ScannedResource carries no scanID field to key by
//   - resourceDriftStatus: map[string]map[string]string -- nested, bare
//     string value
//   - resourceDriftDetail: map[string]map[string]StackResourceDrift -- nested,
//     and stored by value rather than by pointer
//   - driftByStackID: map[string][]string -- one-to-many reverse index
//
// This mirrors the ec2 conversion, which likewise left every nested
// map[string]map[string]*T (e.g. tgwRTPropagations) and every map[string][]T
// (e.g. ipamPoolCidrs) as a plain field rather than force-fitting it into a
// Table.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func stackKeyFn(s *Stack) string                             { return s.StackName }
func exportKeyFn(e *Export) string                           { return e.Name }
func driftDetectionKeyFn(d *DriftDetectionStatus) string     { return d.StackDriftDetectionID }
func stackSetKeyFn(s *StackSet) string                       { return s.StackSetName }
func generatedTemplateKeyFn(g *GeneratedTemplate) string     { return g.GeneratedTemplateID }
func resourceScanKeyFn(r *ResourceScan) string               { return r.ResourceScanID }
func registeredTypeKeyFn(t *RegisteredType) string           { return t.TypeArn }
func typeRegistrationKeyFn(r *TypeRegistrationRecord) string { return r.Token }
func publisherKeyFn(p *Publisher) string                     { return p.PublisherID }
func stackRefactorKeyFn(r *StackRefactor) string             { return r.RefactorID }
func hookResultKeyFn(h *HookResult) string                   { return h.Token }

// registerAllTables registers every converted resource map on b.registry
// exactly once. It must be called during construction only (immediately after
// b.registry is created), never on every reset -- store.Register panics on a
// duplicate name, so a runtime reset goes through registry.ResetAll() instead
// (see Restore's version-mismatch branch in persistence.go).
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call to the concrete field and value type. Mirrors the ec2 conversion's
// tableRegistrations slice (services/ec2/store_setup.go).
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.stacks = store.Register(b.registry, "stacks", store.New(stackKeyFn))
	},
	func(b *InMemoryBackend) {
		b.exports = store.Register(b.registry, "exports", store.New(exportKeyFn))
	},
	func(b *InMemoryBackend) {
		b.driftDetections = store.Register(b.registry, "driftDetections", store.New(driftDetectionKeyFn))
	},
	func(b *InMemoryBackend) {
		b.stackSets = store.Register(b.registry, "stackSets", store.New(stackSetKeyFn))
	},
	func(b *InMemoryBackend) {
		b.generatedTemplates = store.Register(
			b.registry, "generatedTemplates", store.New(generatedTemplateKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.resourceScans = store.Register(b.registry, "resourceScans", store.New(resourceScanKeyFn))
	},
	func(b *InMemoryBackend) {
		b.typeRegistry = store.Register(b.registry, "typeRegistry", store.New(registeredTypeKeyFn))
	},
	func(b *InMemoryBackend) {
		b.typeRegistrations = store.Register(
			b.registry, "typeRegistrations", store.New(typeRegistrationKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.publishers = store.Register(b.registry, "publishers", store.New(publisherKeyFn))
	},
	func(b *InMemoryBackend) {
		b.stackRefactors = store.Register(b.registry, "stackRefactors", store.New(stackRefactorKeyFn))
	},
	func(b *InMemoryBackend) {
		b.hookResults = store.Register(b.registry, "hookResults", store.New(hookResultKeyFn))
	},
}
