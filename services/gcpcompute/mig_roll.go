package gcpcompute

import (
	"math"
	"sort"
	"time"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	replaceRecreate   = "RECREATE"
	replaceSubstitute = "SUBSTITUTE"
	minActionReplace  = "REPLACE"
	minActionRestart  = "RESTART"
)

func fixedOrPercent(v *computeapi.FixedOrPercent, target int) int {
	switch {
	case v == nil:
		return 0
	case v.Percent > 0:
		return int(math.Ceil(float64(target) * float64(v.Percent) / maxPercent))
	}

	return int(v.Fixed)
}

func outdatedMembers(rec *migRec) []*memberRec {
	targets := map[string]bool{}
	for _, v := range rec.Mgr.Versions {
		targets[v.InstanceTemplate] = true
	}

	var out []*memberRec

	for _, m := range liveMembers(rec) {
		if !targets[m.Template] {
			out = append(out, m)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}

func (b *InMemoryBackend) memberAvailableLocked(rec *migRec, m *memberRec, now time.Time) bool {
	r, ok := b.instances[key(rec.Project, m.Zone, m.Name)]
	if !ok || !instRunning(r, now, b.timing) {
		return false
	}

	return !needsVerify(rec, m)
}

func (b *InMemoryBackend) availableCountLocked(rec *migRec, now time.Time) int {
	n := 0

	for _, m := range liveMembers(rec) {
		if b.memberAvailableLocked(rec, m, now) {
			n++
		}
	}

	return n
}

// rollStepLocked advances a PROACTIVE rolling update as far as maxSurge and maxUnavailable allow right now;
// later ticks continue as the replacements become available.
func (b *InMemoryBackend) rollStepLocked(project string, rec *migRec) {
	up := rec.Mgr.UpdatePolicy
	if up == nil || up.Type != updateProactive {
		return
	}

	switch up.MostDisruptiveAllowedAction {
	case "NONE", "REFRESH", minActionRestart:
		return
	}

	target := migTargetSize(rec.Mgr.TargetSize)
	surge := fixedOrPercent(up.MaxSurge, target)
	unavail := fixedOrPercent(up.MaxUnavailable, target)

	for progressed := true; progressed; {
		outdated := outdatedMembers(rec)
		if len(outdated) == 0 {
			return
		}

		now := b.now().UTC()
		progressed = false

		if up.ReplacementMethod == replaceRecreate {
			progressed = b.recreateStepLocked(project, rec, outdated, unavail, now)

			continue
		}

		live := len(liveMembers(rec))
		if live-len(outdated) < target && live < target+surge && b.growLocked(project, rec, 1) == nil {
			progressed = true

			continue
		}

		avail := b.availableCountLocked(rec, now)

		for _, o := range outdated {
			after := avail
			if b.memberAvailableLocked(rec, o, now) {
				after--
			}

			if after >= target-unavail {
				b.markDeletingLocked(project, o)

				progressed = true

				break
			}
		}
	}
}

func (b *InMemoryBackend) recreateStepLocked(
	project string, rec *migRec, outdated []*memberRec, unavail int, now time.Time,
) bool {
	down := len(liveMembers(rec)) - b.availableCountLocked(rec, now)

	for _, o := range outdated {
		if !b.memberAvailableLocked(rec, o, now) {
			if o.Recreating {
				continue
			}
		} else if down+1 > unavail {
			continue
		}

		if err := b.recreateMemberLocked(project, rec, o); err != nil {
			return false
		}

		return true
	}

	return false
}
