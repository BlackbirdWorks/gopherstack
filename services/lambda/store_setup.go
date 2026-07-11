package lambda

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend that has a pure,
// self-contained identity is registered exactly once, here, as a
// *store.Table[T]. See pkgs/store's package doc and the services/sqs pilot
// (commit 0f09d77c) plus the services/ec2 conversion (commit 12e611a4) for
// the pattern this follows.
//
// Two registries, not one
//
// Lambda has a wrinkle the ec2/sqs/dynamodb conversions did not: several
// resource maps that DO have a pure key function were never wired into
// backendSnapshot (codeSigningConfigs, capacityProviders,
// provisionedConcurrencies -- see persistence.go's backendSnapshot for the
// exact set that WAS already persisted before this refactor). Registering
// those three into the same registry Snapshot()/Restore() use would silently
// start persisting state that never survived a restart before, which is a
// behavior change the mechanical-swap rule forbids. They are therefore
// registered on b.ephemeralRegistry: swept by Reset() exactly like every
// other table, but never touched by Snapshot()/Restore() (only b.registry
// feeds backendSnapshot.Tables). This preserves every field's persisted/
// not-persisted status exactly as it was before the conversion.
//
// permissions: a Table, but not on either registry
//
// b.permissions IS a *store.Table[FunctionPermission] (its key -- function
// name + qualifier + statement ID -- is a pure function of the value) but it
// is registered on NEITHER registry. FunctionPermission.FunctionName and
// .Qualifier are tagged `json:"-"` (they are internal bookkeeping, not part
// of any AWS response shape), so a generic store.Table[FunctionPermission]
// round-tripped through Registry.SnapshotAll()/RestoreAll()'s JSON encoding
// would silently lose exactly the fields permissionKeyFn needs, corrupting
// every key on Restore. persistence.go instead converts b.permissions to/from
// a small DTO with real json tags for those fields (the "DTO-registry
// pattern for dirty structs" the services/sqs pilot, commit 0f09d77c,
// established) and calls b.permissions.Restore/.Reset directly. See
// Reset/Snapshot/Restore for the mechanics.
//
// Fields deliberately left as plain maps (NOT registered anywhere) and why:
//
//   - eventInvokeConfigs (map[string]*FunctionEventInvokeConfig): the value's
//     only identity field, FunctionArn, is copied from the owning function's
//     FunctionConfiguration.FunctionArn at Put time -- but a large share of
//     this package's own test fixtures construct FunctionConfiguration
//     without ever setting FunctionArn (it is optional unless the
//     FunctionURL/EventInvokeConfig surface is what's under test), so it is
//     not a reliably-populated identity the way e.g. functionURLConfigs'
//     FunctionArn is (that one is always built internally by
//     CreateFunctionURLConfig itself via buildURLARN, never echoed from the
//     caller). Keying a Table off it would silently mis-key every such config
//     to "". WAS persisted before this refactor and remains a raw field on
//     backendSnapshot.
//   - versions (map[string][]*FunctionVersion), layers (map[string][]*LayerVersion):
//     one-to-many by slice, not a single V per key -- store.Table's shape is
//     map[string]*V. Matches ec2's precedent of leaving analogous
//     slice-valued fields (e.g. ipamPoolCidrs, spotFleetHistory) raw. Both
//     WERE persisted before and remain raw fields on backendSnapshot.
//   - versionCounters (map[string]int), functionConcurrencies (map[string]int),
//     layerVersionCounters (map[string]int64): the value is a bare
//     counter/scalar with no identity of its own -- keyFn cannot be derived
//     from the value, only from the (externally supplied) map key. Mirrors
//     dynamodb's txnTokens/txnPending exclusion. All three WERE persisted
//     before and remain raw fields on backendSnapshot.
//   - layerPolicies (map[string]map[int64]map[string]*LayerVersionStatement):
//     triple-nested; the innermost value (LayerVersionStatement) carries only
//     a Sid, no LayerName or Version field, so neither outer key is
//     recoverable from the value. WAS persisted before and remains a raw
//     field on backendSnapshot.
//   - activeConcurrencies (map[string]int), fnCodeSigningConfigs
//     (map[string]string): same no-identity-in-value reasoning as above.
//     NEITHER was persisted before; both remain raw, unpersisted fields.
//   - fisFaults (map[string]*FISInvocationFault): FISInvocationFault
//     (Expiry/ErrorProbability/AddDelayMs) carries no identity field --
//     keyed externally by function name, exactly like ec2's
//     instanceIMDSOptions/verifiedAccessGroupPolicies exclusions. NOT
//     persisted before; remains raw and unpersisted.
//   - runtimeManagementConfigs, functionScalingConfigs: both structs declare
//     a FunctionArn field, but GetRuntimeManagementConfig/PutRuntimeManagementConfig
//     and GetFunctionScalingConfig/PutFunctionScalingConfig only ever populate
//     FunctionArn on the value RETURNED to the caller -- the copy actually
//     stored in the map always has a zero-value FunctionArn. There is no
//     reliable identity on the stored value, so a store.Table keyFn cannot be
//     built from it. NOT persisted before; remain raw and unpersisted.
//   - functionRecursionConfigs (map[string]*FunctionRecursionConfig): the
//     value (RecursiveLoop only) carries no identity field at all. NOT
//     persisted before; remains raw and unpersisted.
//   - versionIndex, esmByFunctionARN: derived reverse indexes rebuilt from
//     b.versions / b.eventSourceMappings on Restore (see persistence.go) --
//     never a primary source of truth, so they are neither Tables nor
//     persisted directly, exactly as before this refactor.
//   - runtimes (map[string]*functionRuntime), functionURLServers
//     (map[string]*functionURLServer): live, non-serializable runtime state
//     (docker container handles / OS listeners and *http.Server). Both
//     structs are ALL-unexported-field, so json.Marshal would silently
//     produce "{}" and Restore would rehydrate a broken, half-initialized
//     entry (e.g. a nil rt.mu) that was never a valid runtime -- exactly the
//     "must NOT be serialized" case called out for lambda. Kept as plain maps
//     so they can never flow through Table/Registry's JSON-oriented
//     Snapshot/Restore path. Neither was persisted before; both remain raw.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// functionsKeyFn is the store.Table key function for b.functions.
// FunctionName is set once at CreateFunction and never renamed afterward
// (Lambda has no rename API), so it is a stable identity for the table's
// lifetime.
func functionsKeyFn(v *FunctionConfiguration) string { return v.FunctionName }

// functionURLConfigsKeyFn is the store.Table key function for
// b.functionURLConfigs. FunctionArn is built once at CreateFunctionURLConfig
// (buildURLARN, an unqualified "function:name" ARN) and never mutated, so
// parsing the function name back out of it is a pure, stable derivation.
func functionURLConfigsKeyFn(v *FunctionURLConfig) string {
	name, _ := functionNameAndQualifierFromARN(v.FunctionArn)

	return name
}

// eventSourceMappingsKeyFn is the store.Table key function for
// b.eventSourceMappings.
func eventSourceMappingsKeyFn(v *EventSourceMapping) string { return v.UUID }

// aliasKey builds the composite primary key used by b.aliases: a function
// name and an alias name are both required to identify an alias (the same
// alias name may exist under different functions).
func aliasKey(functionName, aliasName string) string { return functionName + "|" + aliasName }

// aliasKeyFn is the store.Table key function for b.aliases. AliasArn
// (buildAliasARN: "function:name:aliasName") and Name are both set once at
// CreateAlias and never mutated afterward (UpdateAlias only ever touches
// FunctionVersion/Description/RoutingConfig/RevisionID), so deriving the
// composite key from them is pure and stable.
func aliasKeyFn(v *FunctionAlias) string {
	name, _ := functionNameAndQualifierFromARN(v.AliasArn)

	return aliasKey(name, v.Name)
}

// aliasFunctionKeyFn is the store.Index key function for b.aliasesByFunction,
// grouping aliases by owning function name -- the hot path ListAliases and
// the function-delete cascade both need ("every alias for function X").
func aliasFunctionKeyFn(v *FunctionAlias) string {
	name, _ := functionNameAndQualifierFromARN(v.AliasArn)

	return name
}

// permissionKeyFn is the store.Table key function for b.permissions.
// FunctionName, Qualifier, and StatementID are all set once at AddPermission
// and never mutated afterward (there is no UpdatePermission op -- only
// Add/Remove), so the composite key is pure and stable.
func permissionKeyFn(v *FunctionPermission) string {
	return permissionMapKey(v.FunctionName, v.Qualifier) + "|" + v.StatementID
}

// permissionTargetKeyFn is the store.Index key function for
// b.permissionsByTarget, grouping statements by their function+qualifier
// target -- the hot path GetPolicy needs ("every statement for function X
// qualifier Y").
func permissionTargetKeyFn(v *FunctionPermission) string {
	return permissionMapKey(v.FunctionName, v.Qualifier)
}

// codeSigningConfigKeyFn is the store.Table key function for
// b.codeSigningConfigs.
func codeSigningConfigKeyFn(v *CodeSigningConfig) string { return v.CodeSigningConfigArn }

// capacityProviderKeyFn is the store.Table key function for
// b.capacityProviders.
func capacityProviderKeyFn(v *CapacityProvider) string { return v.Name }

// provisionedConcurrencyKeyFn is the store.Table key function for
// b.provisionedConcurrencies. FunctionArn is built via buildAliasARN
// (region/account/functionName/qualifier) once at
// PutProvisionedConcurrencyConfig and never mutated afterward --
// scheduleProvisionedConcurrencyReady replaces the whole value via
// copy-on-write but always preserves FunctionArn unchanged -- so it is a
// pure, stable composite identity for the function+qualifier pair.
func provisionedConcurrencyKeyFn(v *ProvisionedConcurrencyConfig) string { return v.FunctionArn }

// provisionedConcurrencyFunctionKeyFn is the store.Index key function for
// b.provisionedConcurrenciesByFunction, grouping configs by owning function
// name -- the hot path ListProvisionedConcurrencyConfigs and the
// function-delete cascade both need ("every qualifier for function X").
func provisionedConcurrencyFunctionKeyFn(v *ProvisionedConcurrencyConfig) string {
	name, _ := functionNameAndQualifierFromARN(v.FunctionArn)

	return name
}

// registerAllTables registers every converted resource map on b.registry (the
// tables that were already persisted before this refactor) and
// b.ephemeralRegistry (tables with a pure key function that were never
// persisted -- see the package-level doc above) exactly once. It must be
// called during construction only (immediately after both registries are
// created), never on every Reset() -- store.Register panics on a duplicate
// name, so runtime resets go through registry.ResetAll() on both registries
// instead (see InMemoryBackend.Reset). b.permissions is constructed here too
// but deliberately left off both registries -- see the package doc above.
func registerAllTables(b *InMemoryBackend) {
	b.functions = store.Register(b.registry, "functions", store.New(functionsKeyFn))
	b.functionURLConfigs = store.Register(b.registry, "functionURLConfigs", store.New(functionURLConfigsKeyFn))
	b.eventSourceMappings = store.Register(b.registry, "eventSourceMappings", store.New(eventSourceMappingsKeyFn))

	b.aliases = store.Register(b.registry, "aliases", store.New(aliasKeyFn))
	b.aliasesByFunction = b.aliases.AddIndex("function", aliasFunctionKeyFn)

	b.permissions = store.New(permissionKeyFn)
	b.permissionsByTarget = b.permissions.AddIndex("target", permissionTargetKeyFn)

	b.codeSigningConfigs = store.Register(b.ephemeralRegistry, "codeSigningConfigs", store.New(codeSigningConfigKeyFn))
	b.capacityProviders = store.Register(b.ephemeralRegistry, "capacityProviders", store.New(capacityProviderKeyFn))

	b.provisionedConcurrencies = store.Register(
		b.ephemeralRegistry, "provisionedConcurrencies", store.New(provisionedConcurrencyKeyFn),
	)
	b.provisionedConcurrenciesByFunction = b.provisionedConcurrencies.AddIndex(
		"function", provisionedConcurrencyFunctionKeyFn,
	)
}

// deleteAliasesForFunctionLocked removes every alias belonging to
// functionName from b.aliases. It copies the index group's keys before
// deleting so the loop is unaffected by store.Index.remove mutating the same
// backing slice as Table.Delete walks each alias's indexes. Caller must hold
// b.mu.
func (b *InMemoryBackend) deleteAliasesForFunctionLocked(functionName string) {
	matches := b.aliasesByFunction.Get(functionName)
	keys := make([]string, len(matches))

	for i, a := range matches {
		keys[i] = aliasKeyFn(a)
	}

	for _, k := range keys {
		b.aliases.Delete(k)
	}
}

// deletePermissionsForFunctionLocked removes every permission statement
// belonging to functionName from b.permissions, across every qualifier
// (unqualified and every "name:qualifier" scope) -- mirroring the pre-Table
// deletePermissionsForFunctionLocked, which scanned every b.permissions outer
// key for a "name" or "name:"-prefixed match. b.permissionsByTarget cannot
// answer this directly since it groups by the full function+qualifier target,
// not by function name alone, so this does the equivalent linear scan over
// the (typically small) permissions table. Caller must hold b.mu.
func (b *InMemoryBackend) deletePermissionsForFunctionLocked(functionName string) {
	var keys []string

	b.permissions.Range(func(p *FunctionPermission) bool {
		if p.FunctionName == functionName {
			keys = append(keys, permissionKeyFn(p))
		}

		return true
	})

	for _, k := range keys {
		b.permissions.Delete(k)
	}
}

// deleteProvisionedConcurrenciesForFunctionLocked removes every provisioned
// concurrency config (every qualifier) belonging to functionName from
// b.provisionedConcurrencies. Caller must hold b.mu.
func (b *InMemoryBackend) deleteProvisionedConcurrenciesForFunctionLocked(functionName string) {
	matches := b.provisionedConcurrenciesByFunction.Get(functionName)
	keys := make([]string, len(matches))

	for i, cfg := range matches {
		keys[i] = cfg.FunctionArn
	}

	for _, k := range keys {
		b.provisionedConcurrencies.Delete(k)
	}
}

// permissionsForTarget returns every permission statement scoped to the exact
// function+qualifier target (the precise "permissionMapKey(name, qualifier)"
// scope GetPolicy/RemovePermission key off), via an O(1)
// b.permissionsByTarget lookup instead of a full-table scan.
func (b *InMemoryBackend) permissionsForTarget(target string) []*FunctionPermission {
	return b.permissionsByTarget.Get(target)
}
