package redshift

import (
	"fmt"
	"regexp"
	"time"
)

const (
	slStatusCreating  = "CREATING"
	slStatusModifying = "MODIFYING"
	slStatusDeleting  = "DELETING"

	slKindNamespace = "namespace"
	slKindWorkgroup = "workgroup"

	slMinNameLen    = 3
	slMaxNameLen    = 64
	slMinRPU        = 8
	slMaxRPU        = 5632
	slRPUStep       = 8
	slSmallBaseRPU  = 4
	slPortLowStart  = 5431
	slPortLowEnd    = 5455
	slPortHighStart = 8191
	slPortHighEnd   = 8215
)

var slNameRe = regexp.MustCompile(`^[a-z0-9-]+$`)

// slTransition is a pending status change for a namespace or workgroup. It is
// never persisted: a restored non-AVAILABLE resource with no pending change
// settles on first access (see settleServerlessLocked).
type slTransition struct {
	at     time.Time
	final  string
	remove bool
}

func slPendingKey(kind, name string) string { return kind + "|" + name }

// SetServerlessLifecycleDelay sets how long namespaces and workgroups dwell in
// MODIFYING, DELETING (and CREATING for workgroups); zero (default) is instant.
func (b *InMemoryBackend) SetServerlessLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetServerlessLifecycleDelay")
	defer b.mu.Unlock()
	b.slLifecycleDelay = d
}

// slBeginLocked parks a resource in interim for the lifecycle delay and returns
// the status it should now carry; with no delay it returns final immediately
// (or interim for a removal, which the caller reports and deletes at once).
func (b *InMemoryBackend) slBeginLocked(kind, name, interim, final string, remove bool) string {
	if b.slLifecycleDelay <= 0 {
		return final
	}

	b.slPending[slPendingKey(kind, name)] = &slTransition{
		at: time.Now().Add(b.slLifecycleDelay), final: final, remove: remove,
	}

	return interim
}

// settleServerlessLocked applies every due transition and heals orphaned
// transitional statuses. Caller must hold b.mu for writing.
func (b *InMemoryBackend) settleServerlessLocked(now time.Time) {
	for _, wg := range b.slWorkgroups.All() {
		if b.workgroupStartingLocked(wg.WorkgroupName) {
			continue
		}

		if b.settleOneLocked(slKindWorkgroup, wg.WorkgroupName, &wg.Status, now) {
			b.slWorkgroups.Delete(wg.WorkgroupName)
			b.slWorkgroupIdx.remove(wg.WorkgroupName)
		}
	}

	for _, ns := range b.slNamespaces.All() {
		if b.settleOneLocked(slKindNamespace, ns.NamespaceName, &ns.Status, now) {
			b.slNamespaces.Delete(ns.NamespaceName)
			b.slNamespaceIdx.remove(ns.NamespaceName)
		}
	}
}

// settleOneLocked settles one resource's status and reports whether it must now be removed.
func (b *InMemoryBackend) settleOneLocked(kind, name string, status *string, now time.Time) bool {
	key := slPendingKey(kind, name)
	tr := b.slPending[key]

	if tr == nil && *status == slStatusAvailable {
		return false
	}

	if tr != nil && tr.at.After(now) {
		return false
	}

	delete(b.slPending, key)

	if tr != nil && tr.remove || tr == nil && *status == slStatusDeleting {
		return true
	}

	*status = slStatusAvailable

	return false
}

func slBusy(kind, name string) error {
	return fmt.Errorf("%w: there is an operation running on the existing %s %q", ErrServerlessConflict, kind, name)
}

// WorkgroupStatus returns a workgroup's current status, settling pending transitions first.
func (b *InMemoryBackend) WorkgroupStatus(name string) (string, bool) {
	b.mu.Lock("WorkgroupStatus")
	defer b.mu.Unlock()
	b.settleServerlessLocked(time.Now())

	wg, ok := b.slWorkgroups.Get(name)
	if !ok {
		return "", false
	}

	return wg.Status, true
}

func slValidName(member, v string) error {
	if len(v) < slMinNameLen || len(v) > slMaxNameLen || !slNameRe.MatchString(v) {
		return fmt.Errorf(
			"%w: %s must be %d-%d characters of lowercase letters, digits and hyphens",
			ErrServerlessValidation, member, slMinNameLen, slMaxNameLen,
		)
	}

	return nil
}

func slValidRPU(v int) bool { return v >= slMinRPU && v <= slMaxRPU && v%slRPUStep == 0 }

// slValidateWorkgroupParams applies docs-derived (not SDK-stated) bounds to the non-zero fields of p.
func slValidateWorkgroupParams(p WorkgroupParams) error {
	if p.BaseCapacity != 0 && p.BaseCapacity != slSmallBaseRPU && !slValidRPU(p.BaseCapacity) {
		return fmt.Errorf(
			"%w: baseCapacity must be %d or a multiple of %d between %d and %d",
			ErrServerlessValidation, slSmallBaseRPU, slRPUStep, slMinRPU, slMaxRPU,
		)
	}

	if p.MaxCapacity != 0 && !slValidRPU(p.MaxCapacity) {
		return fmt.Errorf(
			"%w: maxCapacity must be a multiple of %d between %d and %d",
			ErrServerlessValidation, slRPUStep, slMinRPU, slMaxRPU,
		)
	}

	if p.BaseCapacity != 0 && p.MaxCapacity != 0 && p.MaxCapacity < p.BaseCapacity {
		return fmt.Errorf("%w: maxCapacity must be greater than or equal to baseCapacity", ErrServerlessValidation)
	}

	if p.Port != 0 && !slValidPort(p.Port) {
		return fmt.Errorf("%w: port must be in the range 5431-5455 or 8191-8215", ErrServerlessValidation)
	}

	return nil
}

// WorkgroupStatus returns a workgroup's status in region; redshiftdata uses it to validate WorkgroupName.
func (h *ServerlessHandler) WorkgroupStatus(region, name string) (string, bool) {
	return h.BackendFor(region).WorkgroupStatus(name)
}

func slValidPort(p int) bool {
	return (p >= slPortLowStart && p <= slPortLowEnd) || (p >= slPortHighStart && p <= slPortHighEnd)
}
