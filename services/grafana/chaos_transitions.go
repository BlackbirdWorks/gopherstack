package grafana

// chaosTransitionOp is the pseudo-operation name used to target a chaos
// fault rule at this backend's async lifecycle transitions (CREATING ->
// ACTIVE, UPDATING -> ACTIVE, etc.). It is distinct from every real
// operation name this handler routes (see handler.go's
// GetSupportedOperations), so a rule scoped to it never collides with
// pkgs/chaos's HTTP middleware, which only ever matches real dispatched
// operation names against the synchronous request/response cycle -- it has
// no way to reach a transition that fires later, off a timer, with no
// request in flight.
const chaosTransitionOp = "WorkspaceTransition"

// chaosDegradedCode is the chaos FaultError.Code convention that steers an
// injected transition to DEGRADED instead of the operation's own *_FAILED
// status.
const chaosDegradedCode = "DEGRADED"

// workspaceFailureStatus maps each transitional WorkspaceStatus to the
// *_FAILED status its async transition resolves to when chaos-injected.
//
//nolint:gochecknoglobals // static lookup table, never mutated after init
var workspaceFailureStatus = map[string]string{
	StatusCreating:        StatusCreationFailed,
	StatusUpdating:        StatusUpdateFailed,
	StatusUpgrading:       StatusUpgradeFailed,
	StatusVersionUpdating: StatusVersionUpdateFailed,
}

// injectedTransitionOutcome consults the chaos fault store (if wired) for a
// rule targeting service "grafana" operation chaosTransitionOp. When one
// matches and fires, it reports the workspace-lifecycle failure it should
// produce instead of the ordinary transition to ACTIVE.
func (b *InMemoryBackend) injectedTransitionOutcome() (string, bool) {
	fs, ok := b.chaosFaultStore()
	if !ok {
		return "", false
	}

	rule, matched := fs.Match(grafanaService, chaosTransitionOp, b.region)
	if !matched || !rule.ShouldTrigger() {
		return "", false
	}

	fe := rule.EffectiveError()
	if fe.Code == chaosDegradedCode {
		return "chaos-injected degradation", true
	}

	return "chaos-injected transition failure: " + fe.Code, false
}
