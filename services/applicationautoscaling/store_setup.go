package applicationautoscaling

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 (all
// three resources here already carry their own identity as a real,
// non-json:"-" field, so -- like sesv2 and unlike services/ses -- none of
// them need a "dirty" DTO-registry treatment). See pkgs/store's package doc
// for the underlying primitive.
//
// ScalableTarget is keyed by the (serviceNamespace,resourceId,
// scalableDimension) composite string scalableTargetKey already built from
// its own ServiceNamespace/ResourceID/ScalableDimension fields -- the exact
// key the pre-refactor map used. ScalingPolicy and ScheduledAction are each
// keyed directly by their own ARN field, exactly as the pre-refactor maps
// were.
//
// The pre-refactor reverse-lookup maps (targetARNIndex, policyNameIndex,
// actionNameIndex) are replaced by secondary store.Index values grouping the
// corresponding table by ARN or by the (serviceNamespace,resourceId,
// scalableDimension[,name]) composite key -- see the field comments on
// InMemoryBackend in backend.go.
//
// scalingActivities is left as a raw, unconverted slice: DescribeScalingActivities
// depends on its append order (most-recent-first via slices.Backward), and
// store.Table defines no insertion order.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func scalableTargetKeyFn(v *ScalableTarget) string {
	return scalableTargetKey(v.ServiceNamespace, v.ResourceID, v.ScalableDimension)
}

func scalingPolicyKeyFn(v *ScalingPolicy) string { return v.ARN }

func scheduledActionKeyFn(v *ScheduledAction) string { return v.ARN }

// registerAllTables constructs and registers every store.Table-backed
// resource field (and its secondary indexes) exactly once, at construction
// time. It must be called during construction only, never on every Reset():
// store.Register panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.scalableTargets = store.Register(b.registry, "scalableTargets", store.New(scalableTargetKeyFn))
	b.targetsByARN = b.scalableTargets.AddIndex("byARN", func(v *ScalableTarget) string { return v.ARN })

	b.scalingPolicies = store.Register(b.registry, "scalingPolicies", store.New(scalingPolicyKeyFn))
	b.policiesByName = b.scalingPolicies.AddIndex("byName", func(v *ScalingPolicy) string {
		return policyNameKey(v.ServiceNamespace, v.ResourceID, v.ScalableDimension, v.PolicyName)
	})

	b.scheduledActions = store.Register(b.registry, "scheduledActions", store.New(scheduledActionKeyFn))
	b.actionsByName = b.scheduledActions.AddIndex("byName", func(v *ScheduledAction) string {
		return actionNameKey(v.ServiceNamespace, v.ResourceID, v.ScalableDimension, v.ScheduledActionName)
	})
}
