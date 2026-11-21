package gcpcompute

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	computeapi "google.golang.org/api/compute/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	maxSchedSnaps  = 1000
	snapNameStem   = 28
	snapNameStamp  = "20060102-150405"
	snapLookbackHr = 7 * hoursPerDay
	keyParts       = 3
	hhmmParts      = 2
)

type schedSnap struct {
	Created time.Time `json:"created"`
	Name    string    `json:"name"`
}

// schedRun is the progress of one resource policy on one instance or disk.
type schedRun struct {
	Checked time.Time   `json:"checked"`
	Fired   time.Time   `json:"fired,omitzero"`
	Policy  string      `json:"policy"`
	Target  string      `json:"target"`
	Project string      `json:"project"`
	Snaps   []schedSnap `json:"snapshots,omitempty"`
}

// schedKey is led by the policy link; both parts are length-prefixed so no pair of links can collide.
func schedKey(policy, target string) string {
	return strconv.Itoa(len(policy)) + ":" + policy + strconv.Itoa(len(target)) + ":" + target
}

func (b *InMemoryBackend) policiesBySelfLocked() map[string]*computeapi.ResourcePolicy {
	out := make(map[string]*computeapi.ResourcePolicy, len(b.resPolicies))
	for _, p := range b.resPolicies {
		out[p.SelfLink] = p
	}

	return out
}

func (b *InMemoryBackend) schedStateLocked(project, policy, target string, now time.Time) *schedRun {
	k := schedKey(policy, target)

	st, ok := b.schedRuns[k]
	if !ok {
		st = &schedRun{Checked: now, Policy: policy, Target: target, Project: project}
		b.schedRuns[k] = st
	}

	return st
}

// TickSchedules runs the resource policies due on the backend clock: instance start and stop schedules and
// snapshot schedules with their retention. It only changes backend state, so it holds the lock throughout.
func (b *InMemoryBackend) TickSchedules(ctx context.Context) {
	b.mu.Lock("TickSchedules")
	defer b.mu.Unlock()

	if len(b.resPolicies) == 0 && len(b.schedRuns) == 0 {
		return
	}

	now := b.now().UTC()
	pols := b.policiesBySelfLocked()
	seen := map[string]bool{}

	for _, k := range sortedKeys(b.instances) {
		r := b.instances[k]
		project, zone := splitInstKey(k)

		for _, self := range r.Inst.ResourcePolicies {
			pol := pols[self]
			if pol == nil || pol.InstanceSchedulePolicy == nil {
				continue
			}

			seen[schedKey(self, r.Inst.SelfLink)] = true
			st := b.schedStateLocked(project, self, r.Inst.SelfLink, now)
			b.runInstanceScheduleLocked(ctx, project, zone, r, pol, st, now)
		}
	}

	for _, k := range sortedKeys(b.disks) {
		d := b.disks[k]
		project, loc := splitInstKey(k)

		for _, self := range d.ResourcePolicies {
			pol := pols[self]
			if pol == nil || pol.SnapshotSchedulePolicy == nil {
				continue
			}

			seen[schedKey(self, d.SelfLink)] = true
			st := b.schedStateLocked(project, self, d.SelfLink, now)
			b.runSnapshotScheduleLocked(ctx, project, loc, d, pol, st, now)
		}
	}

	b.sweepSchedRunsLocked(seen, pols, now)
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}

func splitInstKey(k string) (string, string) {
	parts := strings.SplitN(k, "/", keyParts)

	if len(parts) < keyParts {
		return k, ""
	}

	return parts[0], parts[1]
}

func scheduleLocation(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		return time.UTC
	}

	return loc
}

func parseStamp(v string) (time.Time, bool) {
	if v == "" {
		return time.Time{}, false
	}

	t, err := time.Parse(time.RFC3339, v)

	return t.UTC(), err == nil
}

func scheduleCron(s *computeapi.ResourcePolicyInstanceSchedulePolicySchedule) (cronSpec, bool) {
	if s == nil {
		return cronSpec{}, false
	}

	c, err := parseCron(s.Schedule)

	return c, err == nil
}

type scheduledAction struct {
	at   time.Time
	stop bool
}

func (b *InMemoryBackend) runInstanceScheduleLocked(
	ctx context.Context, project, zone string, r *instRec, pol *computeapi.ResourcePolicy, st *schedRun, now time.Time,
) {
	from := st.Checked
	st.Checked = now

	is := pol.InstanceSchedulePolicy
	loc := scheduleLocation(is.TimeZone)
	upTo := now

	if start, ok := parseStamp(is.StartTime); ok {
		if now.Before(start) {
			return
		}

		if from.Before(start) {
			from = start.Add(-time.Nanosecond)
		}
	}

	if exp, ok := parseStamp(is.ExpirationTime); ok && exp.Before(upTo) {
		upTo = exp
	}

	var latest *scheduledAction

	for _, a := range []struct {
		s    *computeapi.ResourcePolicyInstanceSchedulePolicySchedule
		stop bool
	}{{is.VmStartSchedule, false}, {is.VmStopSchedule, true}} {
		c, ok := scheduleCron(a.s)
		if !ok {
			continue
		}

		if at, hit := c.latestFire(loc, from, upTo); hit && (latest == nil || !at.Before(latest.at)) {
			latest = &scheduledAction{at: at, stop: a.stop}
		}
	}

	if latest == nil {
		return
	}

	b.applyInstanceActionLocked(ctx, project, zone, r, *latest, now)
	st.Fired = latest.at
}

func (b *InMemoryBackend) applyInstanceActionLocked(
	ctx context.Context, project, zone string, r *instRec, a scheduledAction, now time.Time,
) {
	state := r.status(now, b.timing)

	switch {
	case a.stop && state != stTerminated && state != stStopping && state != stSuspended && state != stSuspending:
		b.stopLocked(r)
	case !a.stop && state == stTerminated:
		if _, err := b.startLocked(project, zone, r); err != nil {
			logger.Load(ctx).WarnContext(ctx, "gcpcompute: scheduled start failed",
				"instance", r.Inst.Name, "error", err.Error())
		}
	}
}

func (b *InMemoryBackend) runSnapshotScheduleLocked(
	ctx context.Context, project, loc string, d *computeapi.Disk, pol *computeapi.ResourcePolicy, st *schedRun,
	now time.Time,
) {
	from := st.Checked
	st.Checked = now
	sp := pol.SnapshotSchedulePolicy

	created, _ := time.Parse(timeLayout, pol.CreationTimestamp)
	if due, ok := snapshotDue(sp.Schedule, created, from, now); ok {
		b.takeScheduledSnapshotLocked(ctx, project, loc, d, sp, st, due)
	}

	b.expireScheduledSnapshotsLocked(project, sp, st, now)
}

func (b *InMemoryBackend) takeScheduledSnapshotLocked(
	ctx context.Context, project, loc string, d *computeapi.Disk,
	sp *computeapi.ResourcePolicySnapshotSchedulePolicy, st *schedRun, due time.Time,
) {
	in := &computeapi.Snapshot{Name: autoSnapshotName(d.Name, due)}
	if props := sp.SnapshotProperties; props != nil {
		in.Labels, in.StorageLocations = maps.Clone(props.Labels), slices.Clone(props.StorageLocations)
	}

	if _, err := b.snapshotDiskLocked(project, loc, d, in, true); err != nil {
		logger.Load(ctx).WarnContext(ctx, "gcpcompute: scheduled snapshot failed",
			"disk", d.Name, "error", err.Error())

		return
	}

	st.Fired = due
	st.Snaps = append(st.Snaps, schedSnap{Name: in.Name, Created: b.now().UTC()})

	for len(st.Snaps) > maxSchedSnaps {
		b.dropScheduledSnapshotLocked(project, st.Snaps[0].Name)
		st.Snaps = st.Snaps[1:]
	}
}

func autoSnapshotName(disk string, at time.Time) string {
	var suffix [2]byte

	_, _ = rand.Read(suffix[:])

	stem := disk
	if len(stem) > snapNameStem {
		stem = strings.TrimRight(stem[:snapNameStem], "-")
	}

	return stem + "-" + at.UTC().Format(snapNameStamp) + "-" + hex.EncodeToString(suffix[:])
}

func (b *InMemoryBackend) dropScheduledSnapshotLocked(project, name string) {
	if s, ok := b.snapshots[key(project, name)]; ok && s.AutoCreated {
		delete(b.snapshots, key(project, name))
	}
}

// expireScheduledSnapshotsLocked deletes the snapshots a schedule took once they are older than its retention.
func (b *InMemoryBackend) expireScheduledSnapshotsLocked(
	project string, sp *computeapi.ResourcePolicySnapshotSchedulePolicy, st *schedRun, now time.Time,
) {
	if sp.RetentionPolicy == nil || sp.RetentionPolicy.MaxRetentionDays < 1 {
		return
	}

	maxAge := time.Duration(sp.RetentionPolicy.MaxRetentionDays) * hoursPerDay * time.Hour
	kept := st.Snaps[:0:0]

	for _, sn := range st.Snaps {
		if now.Sub(sn.Created) >= maxAge {
			b.dropScheduledSnapshotLocked(project, sn.Name)

			continue
		}

		kept = append(kept, sn)
	}

	st.Snaps = kept
}

func (b *InMemoryBackend) sweepSchedRunsLocked(
	seen map[string]bool, pols map[string]*computeapi.ResourcePolicy, now time.Time,
) {
	for k, st := range b.schedRuns {
		if seen[k] {
			continue
		}

		pol := pols[st.Policy]
		if pol == nil || pol.SnapshotSchedulePolicy == nil || len(st.Snaps) == 0 ||
			pol.SnapshotSchedulePolicy.RetentionPolicy == nil ||
			pol.SnapshotSchedulePolicy.RetentionPolicy.OnSourceDiskDelete != "APPLY_RETENTION_POLICY" {
			delete(b.schedRuns, k)

			continue
		}

		b.expireScheduledSnapshotsLocked(st.Project, pol.SnapshotSchedulePolicy, st, now)

		if len(st.Snaps) == 0 {
			delete(b.schedRuns, k)
		}
	}
}

// snapshotDue returns the latest scheduled snapshot time in (after, upTo], looking back at most a week.
func snapshotDue(
	sc *computeapi.ResourcePolicySnapshotSchedulePolicySchedule, created, after, upTo time.Time,
) (time.Time, bool) {
	if sc == nil {
		return time.Time{}, false
	}

	var latest time.Time

	t := after.UTC().Truncate(time.Hour).Add(time.Hour)
	if floor := upTo.UTC().Truncate(time.Hour).Add(-snapLookbackHr * time.Hour); t.Before(floor) {
		t = floor
	}

	for ; !t.After(upTo); t = t.Add(time.Hour) {
		if t.After(after) && !t.Before(created.Truncate(time.Hour)) && snapshotSlot(sc, created, t) {
			latest = t
		}
	}

	return latest, !latest.IsZero()
}

func hourOf(hhmm string) int {
	h, _ := strconv.Atoi(strings.SplitN(hhmm, ":", hhmmParts)[0])

	return h
}

func snapshotSlot(sc *computeapi.ResourcePolicySnapshotSchedulePolicySchedule, created, t time.Time) bool {
	switch {
	case sc.HourlySchedule != nil:
		hs := sc.HourlySchedule
		if hs.HoursInCycle < 1 {
			return false
		}

		diff := (t.Hour() - hourOf(hs.StartTime) + hoursPerDay) % hoursPerDay

		return int64(diff)%hs.HoursInCycle == 0
	case sc.DailySchedule != nil:
		ds := sc.DailySchedule
		if t.Hour() != hourOf(ds.StartTime) || ds.DaysInCycle < 1 {
			return false
		}

		first := time.Date(created.Year(), created.Month(), created.Day(), hourOf(ds.StartTime), 0, 0, 0, time.UTC)
		if first.Before(created) {
			first = first.AddDate(0, 0, 1)
		}

		el := t.Sub(first)

		return el >= 0 && el%(time.Duration(ds.DaysInCycle)*hoursPerDay*time.Hour) == 0
	case sc.WeeklySchedule != nil:
		for _, d := range sc.WeeklySchedule.DayOfWeeks {
			if strings.EqualFold(d.Day, t.Weekday().String()) && t.Hour() == hourOf(d.StartTime) {
				return true
			}
		}
	}

	return false
}

// schedStatusLocked fills resourceStatus of an instance schedule policy from the schedule and what it has run.
func (b *InMemoryBackend) schedStatusLocked(p *computeapi.ResourcePolicy) {
	is := p.InstanceSchedulePolicy
	if is == nil {
		return
	}

	st := &computeapi.ResourcePolicyResourceStatusInstanceSchedulePolicyStatus{}
	p.ResourceStatus = &computeapi.ResourcePolicyResourceStatus{InstanceSchedulePolicy: st}

	var last time.Time

	for _, run := range b.schedRuns {
		if run.Policy == p.SelfLink && run.Fired.After(last) {
			last = run.Fired
		}
	}

	if !last.IsZero() {
		st.LastRunStartTime = last.UTC().Format(time.RFC3339)
	}

	if next, ok := b.nextScheduledRun(is); ok {
		st.NextRunStartTime = next.UTC().Format(time.RFC3339)
	}
}

func (b *InMemoryBackend) nextScheduledRun(is *computeapi.ResourcePolicyInstanceSchedulePolicy) (time.Time, bool) {
	now := b.now().UTC()
	from := now

	if start, ok := parseStamp(is.StartTime); ok && start.After(from) {
		from = start.Add(-time.Nanosecond)
	}

	loc := scheduleLocation(is.TimeZone)

	var best time.Time

	for _, s := range []*computeapi.ResourcePolicyInstanceSchedulePolicySchedule{is.VmStartSchedule, is.VmStopSchedule} {
		c, ok := scheduleCron(s)
		if !ok {
			continue
		}

		if at, hit := c.nextFire(loc, from); hit && (best.IsZero() || at.Before(best)) {
			best = at
		}
	}

	if exp, ok := parseStamp(is.ExpirationTime); ok && !best.IsZero() && !best.Before(exp) {
		return time.Time{}, false
	}

	return best, !best.IsZero()
}
