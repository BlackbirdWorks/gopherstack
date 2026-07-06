package elbv2

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend that carries its own
// identity (an ARN field) is registered exactly once, here, as a
// *store.Table[T] on b.registry. See pkgs/store's package doc and the
// services/ec2 (commit 12e611a4) / services/sqs (commit 0f09d77c) pilots for
// the pattern this follows.
//
// Two fields are deliberately NOT registered here and remain plain maps --
// see the comment above registerAllTables for the list and why.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func loadBalancersKeyFn(v *LoadBalancer) string { return v.LoadBalancerArn }
func targetGroupsKeyFn(v *TargetGroup) string   { return v.TargetGroupArn }
func listenersKeyFn(v *Listener) string         { return v.ListenerArn }
func rulesKeyFn(v *Rule) string                 { return v.RuleArn }
func trustStoresKeyFn(v *TrustStore) string     { return v.TrustStoreArn }

// listenerLBIndexKeyFn groups listeners by their owning load balancer ARN,
// backing the b.listenersByLB secondary index used for LB-scoped listener
// scans (DescribeListeners with an lbArn filter, cascading LB delete,
// duplicate-port checks, and target-group-to-LB resolution).
func listenerLBIndexKeyFn(v *Listener) string { return v.LoadBalancerArn }

// ruleListenerIndexKeyFn groups rules by their owning listener ARN, backing
// the b.rulesByListener secondary index used for listener-scoped rule scans
// (DescribeRules with a listenerArn filter, cascading listener delete,
// default-rule sync, and rule-priority collision checks).
func ruleListenerIndexKeyFn(v *Rule) string { return v.ListenerArn }

// registerAllTables registers every identity-carrying resource map on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created), never on every reset --
// store.Register panics on a duplicate name, so runtime resets go through
// registry.ResetAll() instead.
//
// The following resource fields are deliberately left as plain maps (not
// registered here) because their value carries no identity field of its own
// (store.Table requires the key to be a pure function of the stored value):
//   - resourcePolicies: value type is a bare string (the policy document),
//     keyed externally by resource ARN.
//   - targetReadyAt / targetDrainingUntil: doubly-nested
//     map[string]map[string]time.Time (target-group ARN -> target key ->
//     timestamp) -- not a map[string]*V shape at all, let alone one with a
//     value that knows its own key.
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call (plus any secondary index) to the concrete field and value type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.loadBalancers = store.Register(b.registry, "loadBalancers", store.New(loadBalancersKeyFn))
	},
	func(b *InMemoryBackend) {
		b.targetGroups = store.Register(b.registry, "targetGroups", store.New(targetGroupsKeyFn))
	},
	func(b *InMemoryBackend) {
		listeners := store.Register(b.registry, "listeners", store.New(listenersKeyFn))
		b.listeners = listeners
		b.listenersByLB = listeners.AddIndex("loadBalancer", listenerLBIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		rules := store.Register(b.registry, "rules", store.New(rulesKeyFn))
		b.rules = rules
		b.rulesByListener = rules.AddIndex("listener", ruleListenerIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.trustStores = store.Register(b.registry, "trustStores", store.New(trustStoresKeyFn))
	},
}
