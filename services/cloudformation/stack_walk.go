package cloudformation

import (
	"context"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"
)

const (
	phaseReplaced = "replaced"
	phaseStale    = "stale"
	phaseSettle   = "settle"

	reasonCreateCancelled = "Resource creation cancelled"
	reasonUpdateCancelled = "Resource update cancelled"
	reasonUpdateSkipped   = "Resource update skipped by ContinueUpdateRollback"
)

// replacedRecord is the old physical resource of a replaced logical ID, kept until cleanup.
type replacedRecord struct {
	Policy string        `json:"policy,omitempty"`
	Old    StackResource `json:"old"`
}

// walkRuntime caches what a walk re-derives from the stack when it resumes.
type walkRuntime struct {
	tmpl   *Template
	uc     *updateContext
	params map[string]string
}

func (b *InMemoryBackend) addStackEvent(stack *Stack, status, reason string) {
	b.addEvent(stack.StackID, stack.StackName, stack.StackName, stack.StackID, cfnStackType, status, reason)
}

func (b *InMemoryBackend) addResourceEvent(stack *Stack, id, phys, typ, status, reason string) {
	b.addEvent(stack.StackID, stack.StackName, id, phys, typ, status, reason)
}

// stageWalk hands op's next step to the scheduler when inside an async step;
// false means the caller keeps walking inline.
func (b *InMemoryBackend) stageWalk(ctx context.Context, stack *Stack, op *pendingStackOp) bool {
	return b.stepAsync && b.scheduleLocked(ctx, stack.StackID, op)
}

// nextWave returns the sorted members of remaining that can progress now: those
// whose in-set dependencies are all done (leaves false) or that no other member
// depends on (leaves true). IDs absent from res depend on nothing.
func nextWave(res map[string]TemplateResource, remaining []string, leaves bool) []string {
	all := make(map[string]struct{}, len(res))
	for id := range res {
		all[id] = struct{}{}
	}

	left := make(map[string]struct{}, len(remaining))
	for _, id := range remaining {
		left[id] = struct{}{}
	}

	blocked := make(map[string]bool, len(remaining))

	for _, id := range remaining {
		r, ok := res[id]
		if !ok {
			continue
		}

		for _, dep := range resourceDependencies(id, r, all) {
			if _, in := left[dep]; !in {
				continue
			}

			if leaves {
				blocked[dep] = true
			} else {
				blocked[id] = true
			}
		}
	}

	out := make([]string, 0, len(remaining))

	for _, id := range remaining {
		if !blocked[id] {
			out = append(out, id)
		}
	}

	if len(out) == 0 {
		out = slices.Clone(remaining)
	}

	sort.Strings(out)

	return out
}

func without(list []string, drop ...string) []string {
	out := make([]string, 0, len(list))

	for _, id := range list {
		if !slices.Contains(drop, id) {
			out = append(out, id)
		}
	}

	return out
}

func appendUnique(list []string, id string) []string {
	if slices.Contains(list, id) {
		return list
	}

	return append(list, id)
}

// legacyInFlight maps the keys of a snapshot written before in-flight tracking
// ("logical|STATUS") onto the in-flight set.
func (op *pendingStackOp) legacyInFlight() {
	if len(op.InFlight) > 0 {
		return
	}

	for _, key := range op.Announced {
		if id, status, ok := strings.Cut(key, "|"); ok {
			if op.InFlight == nil {
				op.InFlight = make(map[string]string)
			}

			op.InFlight[id] = status
		}
	}

	op.Announced = nil
}

// reverseWalk describes one backwards phase (deletion, rollback, cleanup): the
// resources of the stack leave one dependency wave per step.
type reverseWalk struct {
	deps     map[string]TemplateResource
	target   func(id string) (typ, phys string, ok bool)
	progress func(id string) string
	apply    func(ctx context.Context, id string) (status, reason string)
}

// walkReverseLocked runs the walk until op.Remaining is empty (false) or the next
// wave is deferred to a later step (true).
func (b *InMemoryBackend) walkReverseLocked(
	ctx context.Context, stack *Stack, op *pendingStackOp, w reverseWalk,
) bool {
	for {
		if len(op.InFlight) > 0 {
			b.executeReverseWave(ctx, stack, op, w)
		}

		if !b.announceReverseWave(stack, op, w) {
			return false
		}

		if b.stageWalk(ctx, stack, op) {
			return true
		}
	}
}

func (b *InMemoryBackend) announceReverseWave(stack *Stack, op *pendingStackOp, w reverseWalk) bool {
	for len(op.Remaining) > 0 {
		wave := nextWave(w.deps, op.Remaining, true)
		op.Remaining = without(op.Remaining, wave...)

		for _, id := range wave {
			typ, phys, ok := w.target(id)
			if !ok {
				continue
			}

			status := w.progress(id)
			if op.InFlight == nil {
				op.InFlight = make(map[string]string, len(wave))
			}

			op.InFlight[id] = status
			b.addResourceEvent(stack, id, phys, typ, status, "")
		}

		if len(op.InFlight) > 0 {
			return true
		}
	}

	return false
}

func (b *InMemoryBackend) executeReverseWave(
	ctx context.Context, stack *Stack, op *pendingStackOp, w reverseWalk,
) {
	ids := sortedKeys(op.InFlight)
	op.InFlight = nil

	for _, id := range ids {
		typ, phys, ok := w.target(id)
		if !ok {
			continue
		}

		status, reason := w.apply(ctx, id)
		b.addResourceEvent(stack, id, phys, typ, status, reason)

		if isFailedResourceStatus(status) {
			if op.FailedReasons == nil {
				op.FailedReasons = make(map[string]string)
			}

			op.FailedReasons[id] = reason
		}
	}
}

// failedSummary lists the failures of a walk as "id: reason", ordered by ID.
func failedSummary(reasons map[string]string) string {
	parts := make([]string, 0, len(reasons))

	for _, id := range sortedKeys(reasons) {
		parts = append(parts, id+": "+reasons[id])
	}

	return strings.Join(parts, "; ")
}

// liveResourcesLocked returns the stack's resources with the status of those an
// operation has announced but not yet finished.
func (b *InMemoryBackend) liveResourcesLocked(stack *Stack) map[string]*StackResource {
	live := b.resources[stack.StackID]
	op := b.pendingOps[stack.StackID]

	if op == nil || len(op.InFlight) == 0 {
		return live
	}

	out := make(map[string]*StackResource, len(live)+len(op.InFlight))
	maps.Copy(out, live)

	var types map[string]TemplateResource

	for id, status := range op.InFlight {
		if res, ok := live[id]; ok {
			cp := *res
			cp.Status = status
			out[id] = &cp

			continue
		}

		if types == nil {
			types = b.inFlightTypes(stack)
		}

		out[id] = &StackResource{
			Timestamp: time.Now(), LogicalID: id, Type: types[id].Type, Status: status,
			StackID: stack.StackID, StackName: stack.StackName,
		}
	}

	return out
}

func (b *InMemoryBackend) inFlightTypes(stack *Stack) map[string]TemplateResource {
	tmpl, err := ParseTemplate(stack.TemplateBody)
	if err != nil {
		return nil
	}

	return tmpl.Resources
}
