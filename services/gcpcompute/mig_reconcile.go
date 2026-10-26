package gcpcompute

import (
	"crypto/rand"
	"math"
	"sort"

	computeapi "google.golang.org/api/compute/v1"
)

const suffixLen = 4

const suffixAlphabet = "bcdfghjklmnpqrstvwxz0123456789"

func liveMembers(rec *migRec) []*memberRec {
	var out []*memberRec

	for _, m := range rec.Members {
		if !m.Deleting {
			out = append(out, m)
		}
	}

	return out
}

func (b *InMemoryBackend) templateByLinkLocked(link string) *computeapi.InstanceTemplate {
	for _, t := range b.templates {
		if t.SelfLink == link {
			return t
		}
	}

	return nil
}

func (b *InMemoryBackend) newMemberNameLocked(project string, rec *migRec, zone string) string {
	for {
		var buf [suffixLen]byte

		_, _ = rand.Read(buf[:])

		for i := range buf {
			buf[i] = suffixAlphabet[int(buf[i])%len(suffixAlphabet)]
		}

		name := rec.Mgr.BaseInstanceName + "-" + string(buf[:])
		if _, taken := b.instances[key(project, zone, name)]; taken {
			continue
		}

		for _, m := range rec.Members {
			if m.Name == name {
				name = ""
			}
		}

		if name != "" {
			return name
		}
	}
}

func (b *InMemoryBackend) createInstanceForLocked(project string, rec *migRec, zone, name, tmpl string) *APIError {
	t := b.templateByLinkLocked(tmpl)
	if t == nil {
		return errNotFound(trimURL(tmpl))
	}

	in := instanceFromProps(t.Properties, name)
	if in.Metadata == nil {
		in.Metadata = &computeapi.Metadata{}
	}

	in.Metadata.Items = append(in.Metadata.Items,
		&computeapi.MetadataItems{Key: "created-by", Value: &rec.Mgr.SelfLink},
		&computeapi.MetadataItems{Key: "instance-template", Value: &tmpl},
	)

	_, err := b.insertInstanceLocked(project, zone, in)

	return err
}

func (b *InMemoryBackend) removeMemberInstanceLocked(project string, rec *migRec, m *memberRec) {
	if r, ok := b.instances[key(project, m.Zone, m.Name)]; ok {
		b.removeInstanceLocked(project, m.Zone, r)
	}
}

func (b *InMemoryBackend) migZonesLocked(rec *migRec) []string {
	if rec.Mgr.Region == "" {
		return []string{lastSegment(rec.Mgr.Zone)}
	}

	if dp := rec.Mgr.DistributionPolicy; dp != nil && len(dp.Zones) > 0 {
		out := make([]string, 0, len(dp.Zones))
		for _, z := range dp.Zones {
			out = append(out, lastSegment(z.Zone))
		}

		sort.Strings(out)

		return out
	}

	return zonesInRegion(lastSegment(rec.Mgr.Region))
}

func zoneCounts(rec *migRec) map[string]int {
	out := map[string]int{}
	for _, m := range liveMembers(rec) {
		out[m.Zone]++
	}

	return out
}

func (b *InMemoryBackend) pickZoneLocked(rec *migRec, counts map[string]int) string {
	zones := b.migZonesLocked(rec)
	best := zones[0]

	for _, z := range zones[1:] {
		if counts[z] < counts[best] {
			best = z
		}
	}

	return best
}

// versionAlloc splits n instances across the versions: fixed first, then percent (rounded up), the rest to the
// version without a target size (or the first one).
func versionAlloc(vs []*computeapi.InstanceGroupManagerVersion, n int) []int {
	out := make([]int, len(vs))
	left, rest := n, -1

	for i, v := range vs {
		switch {
		case v.TargetSize == nil:
			rest = i
		case v.TargetSize.Percent > 0:
			out[i] = min(left, int(math.Ceil(float64(n)*float64(v.TargetSize.Percent)/maxPercent)))
		default:
			out[i] = min(left, int(v.TargetSize.Fixed))
		}

		left -= out[i]
	}

	if rest < 0 {
		rest = 0
	}

	out[rest] += left

	return out
}

func templateCounts(members []*memberRec) map[string]int {
	out := map[string]int{}
	for _, m := range members {
		out[m.Template]++
	}

	return out
}

func (b *InMemoryBackend) pickVersion(rec *migRec, counts map[string]int) string {
	vs := rec.Mgr.Versions
	alloc := versionAlloc(vs, int(rec.Mgr.TargetSize))
	best, bestDef := 0, math.MinInt

	for i, v := range vs {
		if d := alloc[i] - counts[v.InstanceTemplate]; d > bestDef {
			best, bestDef = i, d
		}
	}

	return vs[best].InstanceTemplate
}

func (b *InMemoryBackend) growLocked(project string, rec *migRec, n int) *APIError {
	live := liveMembers(rec)
	tc, zc := templateCounts(live), zoneCounts(rec)
	added := make([]*memberRec, 0, n)

	for range n {
		tmpl := b.pickVersion(rec, tc)
		zone := b.pickZoneLocked(rec, zc)
		name := b.newMemberNameLocked(project, rec, zone)

		if err := b.createInstanceForLocked(project, rec, zone, name, tmpl); err != nil {
			for _, m := range added {
				b.removeMemberInstanceLocked(project, rec, m)
				rec.Members = dropMember(rec.Members, m)
			}

			return err
		}

		m := &memberRec{Name: name, Zone: zone, Template: tmpl}
		rec.Members = append(rec.Members, m)
		added = append(added, m)
		tc[tmpl]++
		zc[zone]++
	}

	return nil
}

func dropMember(ms []*memberRec, drop *memberRec) []*memberRec {
	out := ms[:0:0]

	for _, m := range ms {
		if m != drop {
			out = append(out, m)
		}
	}

	return out
}

func (b *InMemoryBackend) markDeletingLocked(project string, rec *migRec, m *memberRec) {
	m.Deleting, m.DeleteAt = true, b.now().UTC().Add(b.timing.Stop)
	b.removeMemberInstanceLocked(project, rec, m)
}

func (b *InMemoryBackend) shrinkLocked(project string, rec *migRec, n int) {
	for range n {
		live := liveMembers(rec)
		if len(live) == 0 {
			return
		}

		tc, zc := templateCounts(live), zoneCounts(rec)
		want := map[string]int{}
		alloc := versionAlloc(rec.Mgr.Versions, int(rec.Mgr.TargetSize))

		for i, v := range rec.Mgr.Versions {
			want[v.InstanceTemplate] += alloc[i]
		}

		now := b.now().UTC()
		score := func(m *memberRec) [3]int {
			var s [3]int

			if r, ok := b.instances[key(project, m.Zone, m.Name)]; !ok || !instRunning(r, now, b.timing) {
				s[0] = 1
			}

			if tc[m.Template] > want[m.Template] {
				s[1] = 1
			}

			s[2] = zc[m.Zone]

			return s
		}

		sort.SliceStable(live, func(i, j int) bool {
			a, c := score(live[i]), score(live[j])
			for k := range a {
				if a[k] != c[k] {
					return a[k] > c[k]
				}
			}

			return live[i].Name > live[j].Name
		})

		b.markDeletingLocked(project, rec, live[0])
	}
}

func (b *InMemoryBackend) healVanishedLocked(project string, rec *migRec) {
	for _, m := range rec.Members {
		if m.Deleting {
			continue
		}

		if _, ok := b.instances[key(project, m.Zone, m.Name)]; ok {
			continue
		}

		if err := b.createInstanceForLocked(project, rec, m.Zone, m.Name, m.Template); err != nil {
			rec.Members = dropMember(rec.Members, m)
		}
	}
}

func (b *InMemoryBackend) reconcileLocked(rec *migRec) *APIError {
	project := rec.Project
	b.healVanishedLocked(project, rec)

	live := len(liveMembers(rec))
	want := int(rec.Mgr.TargetSize)

	switch {
	case live > want:
		b.shrinkLocked(project, rec, live-want)
	case live < want:
		return b.growLocked(project, rec, want-live)
	}

	return nil
}

func (b *InMemoryBackend) sweepGroupsLocked() {
	now := b.now().UTC()

	for _, rec := range b.migs {
		kept := rec.Members[:0:0]

		for _, m := range rec.Members {
			if !m.Deleting || now.Before(m.DeleteAt) {
				kept = append(kept, m)
			}
		}

		rec.Members = kept
		b.healVanishedLocked(rec.Project, rec)
	}
}

// rollOutdatedLocked recreates members whose template is not one of the group's target versions.
func (b *InMemoryBackend) rollOutdatedLocked(project string, rec *migRec) {
	targets := map[string]bool{}
	for _, v := range rec.Mgr.Versions {
		targets[v.InstanceTemplate] = true
	}

	for _, m := range liveMembers(rec) {
		if !targets[m.Template] {
			_ = b.recreateMemberLocked(project, rec, m)
		}
	}
}

func (b *InMemoryBackend) recreateMemberLocked(project string, rec *migRec, m *memberRec) *APIError {
	others := make([]*memberRec, 0, len(rec.Members))
	for _, o := range liveMembers(rec) {
		if o != m {
			others = append(others, o)
		}
	}

	tmpl := b.pickVersion(rec, templateCounts(others))
	b.removeMemberInstanceLocked(project, rec, m)

	if err := b.createInstanceForLocked(project, rec, m.Zone, m.Name, tmpl); err != nil {
		rec.Members = dropMember(rec.Members, m)

		return err
	}

	m.Template, m.Recreating = tmpl, true

	return nil
}

type memberAction struct {
	action string
	inst   *instRec
}

func (b *InMemoryBackend) memberActionLocked(project string, m *memberRec) (memberAction, bool) {
	now := b.now().UTC()
	if m.Deleting {
		if !now.Before(m.DeleteAt) {
			return memberAction{}, false
		}

		return memberAction{action: actDeleting}, true
	}

	r, ok := b.instances[key(project, m.Zone, m.Name)]
	if !ok {
		return memberAction{action: actCreating}, true
	}

	switch r.status(now, b.timing) {
	case stProvision, stStaging:
		if m.Recreating {
			return memberAction{action: actRecreating, inst: r}, true
		}

		return memberAction{action: actCreating, inst: r}, true
	}

	return memberAction{action: actNone, inst: r}, true
}

func (b *InMemoryBackend) migViewLocked(rec *migRec) *computeapi.InstanceGroupManager {
	out := clone(rec.Mgr)
	sum := &computeapi.InstanceGroupManagerActionsSummary{}
	stable, versionOK := true, true
	tc := map[string]int{}

	for _, m := range rec.Members {
		a, visible := b.memberActionLocked(rec.Project, m)
		if !visible {
			continue
		}

		switch a.action {
		case actNone:
			sum.None++
		case actCreating:
			sum.Creating++
		case actDeleting:
			sum.Deleting++
		case actRecreating:
			sum.Recreating++
		}

		if a.action != actNone {
			stable = false
		} else {
			tc[m.Template]++
		}
	}

	alloc := versionAlloc(out.Versions, int(out.TargetSize))
	for i, v := range out.Versions {
		calc := &computeapi.FixedOrPercent{Calculated: int64(alloc[i])}
		if v.TargetSize != nil {
			calc.Fixed, calc.Percent = v.TargetSize.Fixed, v.TargetSize.Percent
		}

		v.TargetSize = calc

		if tc[v.InstanceTemplate] != alloc[i] {
			versionOK = false
		}
	}

	live := len(liveMembers(rec))
	out.CurrentActions = sum
	out.Status = &computeapi.InstanceGroupManagerStatus{
		IsStable:      stable && versionOK && live == int(out.TargetSize),
		VersionTarget: &computeapi.InstanceGroupManagerStatusVersionTarget{IsReached: versionOK},
	}

	for _, a := range b.scalers {
		if a.Target == out.SelfLink {
			out.Status.Autoscaler = a.SelfLink
		}
	}

	out.ForceSendFields = []string{"TargetSize"}

	return out
}

func (b *InMemoryBackend) managedInstancesLocked(rec *migRec) []*computeapi.ManagedInstance {
	var out []*computeapi.ManagedInstance

	for _, m := range rec.Members {
		a, visible := b.memberActionLocked(rec.Project, m)
		if !visible {
			continue
		}

		mi := &computeapi.ManagedInstance{
			Name: m.Name, Instance: zoneLink(rec.Project, m.Zone, "instances", m.Name), CurrentAction: a.action,
			Version: &computeapi.ManagedInstanceVersion{InstanceTemplate: m.Template},
		}

		if a.inst != nil {
			mi.Id = a.inst.Inst.Id
			mi.InstanceStatus = a.inst.status(b.now().UTC(), b.timing)
		}

		out = append(out, mi)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	return out
}
