package dlm

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	policyTypeImage      = "IMAGE_MANAGEMENT"
	resourceTypeVolume   = "VOLUME"
	resourceTypeInstance = "INSTANCE"
	defaultScheduleName  = "Default"

	tagPolicyID     = "aws:dlm:lifecycle-policy-id"
	tagScheduleName = "aws:dlm:lifecycle-schedule-name"
	tagTimestamp    = "aws:dlm:timestamp"
	tagManaged      = "dlm:managed"
)

// RunSchedules creates and expires snapshots and AMIs for every ENABLED policy whose schedules fell due in
// (last evaluation, now]. DISABLED policies are skipped but their evaluation baseline still advances.
func (b *InMemoryBackend) RunSchedules(ctx context.Context, now time.Time) {
	b.mu.Lock("RunSchedules")
	defer b.mu.Unlock()

	if b.compute == nil {
		return
	}

	policies := b.policies.All()
	sort.Slice(policies, func(i, j int) bool { return policies[i].PolicyID < policies[j].PolicyID })

	for _, p := range policies {
		b.runPolicy(ctx, p, now.UTC())
	}
}

func (b *InMemoryBackend) runPolicy(ctx context.Context, p *storedPolicy, now time.Time) {
	if p.LastEvaluated == nil {
		p.LastEvaluated = map[string]time.Time{}
	}

	enabled := p.State == stateEnabled

	for _, sched := range policySchedules(p.PolicyDetails) {
		name, _ := sched["Name"].(string)
		if name == "" {
			name = defaultScheduleName
		}

		baseline, seen := p.LastEvaluated[name]
		if !seen {
			baseline = p.DateCreated.Add(-time.Nanosecond)
		}

		p.LastEvaluated[name] = now

		if !enabled {
			continue
		}

		b.pruneMissing(p.PolicyID, name)

		cr, _ := sched["CreateRule"].(map[string]any)
		if _, due := createRuleDue(cr, p.DateCreated, baseline, now); due {
			b.createForSchedule(ctx, p, sched, name, now)
		}

		if rr, ok := sched["RetainRule"].(map[string]any); ok {
			b.expire(ctx, p.PolicyID, name, rr, sched["ArchiveRule"] != nil, now)
		}

		b.maintain(ctx, p.PolicyID, name, sched, now)
	}
}

func policySchedules(details map[string]any) []map[string]any {
	if policyDetailsIsDefaultPolicy(details) {
		create, _ := numberOf(details["CreateInterval"])
		retain, _ := numberOf(details["RetainInterval"])
		copyTags, _ := details["CopyTags"].(bool)

		return []map[string]any{{
			"Name":       defaultScheduleName,
			"CopyTags":   copyTags,
			"CreateRule": map[string]any{"Interval": create, "IntervalUnit": unitDays},
			"RetainRule": map[string]any{"Interval": retain, "IntervalUnit": unitDays},
		}}
	}

	var out []map[string]any

	for _, s := range asSlice(details["Schedules"]) {
		if m, ok := s.(map[string]any); ok {
			out = append(out, m)
		}
	}

	return out
}

func (b *InMemoryBackend) pruneMissing(policyID, schedule string) {
	for _, m := range b.managed.All() {
		if m.PolicyID == policyID && m.Schedule == schedule && !b.compute.Exists(m.regionOr(b.region), m.Kind, m.ID) {
			b.managed.Delete(m.ID)
		}
	}
}

func (b *InMemoryBackend) createForSchedule(
	ctx context.Context, p *storedPolicy, sched map[string]any, name string, now time.Time,
) {
	images := policyCreatesImages(p.PolicyDetails)

	for _, rt := range policyResourceTypes(p.PolicyDetails) {
		for _, t := range b.matchingTargets(p.PolicyDetails, rt) {
			tags := scheduleTags(p.PolicyID, name, sched, t, now)
			desc := fmt.Sprintf("Created for policy: %s schedule: %s", p.PolicyID, name)
			cc := createCtx{sched: sched, tags: tags, desc: desc}

			switch {
			case images && rt == resourceTypeInstance:
				imgName := fmt.Sprintf("%s-%s", p.PolicyID, now.Format("20060102T150405Z"))
				b.record(ctx, p.PolicyID, name, kindImage, t.ID, now, cc,
					func() (string, error) { return b.compute.CreateImage(b.region, t.ID, imgName, desc, tags) })
			case rt == resourceTypeInstance:
				for _, vid := range t.VolumeIDs {
					b.record(ctx, p.PolicyID, name, kindSnapshot, t.ID, now, cc,
						func() (string, error) { return b.compute.CreateSnapshot(b.region, vid, desc, tags) })
				}
			default:
				b.record(ctx, p.PolicyID, name, kindSnapshot, t.ID, now, cc,
					func() (string, error) { return b.compute.CreateSnapshot(b.region, t.ID, desc, tags) })
			}
		}
	}
}

func (b *InMemoryBackend) record(
	ctx context.Context, policyID, schedule, kind, targetID string, now time.Time, cc createCtx,
	create func() (string, error),
) {
	id, err := create()
	if id != "" {
		m := &managedResource{
			ID: id, Kind: kind, PolicyID: policyID, Schedule: schedule, TargetID: targetID, CreatedAt: now,
		}
		b.managed.Put(m)
		b.applyCreateRules(ctx, cc, m)
	}

	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "dlm: create failed", "policy", policyID, "target", targetID, "error", err)
	}
}

func policyCreatesImages(details map[string]any) bool {
	if policyDetailsIsDefaultPolicy(details) {
		rt, _ := details["ResourceType"].(string)

		return strings.EqualFold(rt, resourceTypeInstance)
	}

	pt, _ := details["PolicyType"].(string)

	return pt == policyTypeImage
}

func policyResourceTypes(details map[string]any) []string {
	if policyDetailsIsDefaultPolicy(details) {
		rt, _ := details["ResourceType"].(string)

		return []string{strings.ToUpper(rt)}
	}

	return policyDetailsStringSlice(details, "ResourceTypes")
}

func (b *InMemoryBackend) matchingTargets(details map[string]any, resourceType string) []Target {
	var out []Target

	targets := b.compute.Targets(b.region, resourceType)
	slices.SortFunc(targets, func(a, c Target) int { return cmp.Compare(a.ID, c.ID) })

	if policyDetailsIsDefaultPolicy(details) {
		for _, t := range targets {
			if !excludedByDefaultPolicy(details, t) {
				out = append(out, t)
			}
		}

		return out
	}

	want := policyDetailsTagPairs(details, "TargetTags")

	for _, t := range targets {
		for _, w := range want {
			if v, ok := t.Tags[w.Key]; ok && v == w.Value {
				out = append(out, t)

				break
			}
		}
	}

	return out
}

func excludedByDefaultPolicy(details map[string]any, t Target) bool {
	ex, _ := details["Exclusions"].(map[string]any)

	if slices.Contains(policyDetailsStringSlice(ex, "ExcludeVolumeTypes"), t.VolumeType) && t.VolumeType != "" {
		return true
	}

	for _, p := range policyDetailsTagPairs(ex, "ExcludeTags") {
		if v, ok := t.Tags[p.Key]; ok && v == p.Value {
			return true
		}
	}

	return false
}

func scheduleTags(policyID, schedule string, sched map[string]any, t Target, now time.Time) map[string]string {
	tags := map[string]string{}
	stamp := now.Format(time.RFC3339)

	if cp, _ := sched["CopyTags"].(bool); cp {
		for k, v := range t.Tags {
			if !strings.HasPrefix(k, "aws:") {
				tags[k] = v
			}
		}
	}

	for _, p := range policyDetailsTagPairs(sched, "TagsToAdd") {
		tags[p.Key] = p.Value
	}

	instanceID := t.InstanceID
	if instanceID == "" && len(t.VolumeIDs) > 0 {
		instanceID = t.ID
	}

	repl := strings.NewReplacer("$(instance-id)", instanceID, "$(timestamp)", stamp)

	for _, p := range policyDetailsTagPairs(sched, "VariableTags") {
		tags[p.Key] = repl.Replace(p.Value)
	}

	tags[tagPolicyID] = policyID
	tags[tagScheduleName] = schedule
	tags[tagTimestamp] = stamp
	tags[tagManaged] = valueTrue

	return tags
}

type resourceSet struct {
	createdAt time.Time
	members   []*managedResource
}

// standardTierSets groups the schedule's own live standard-tier resources by target and creation time.
func (b *InMemoryBackend) standardTierSets(policyID, schedule string) map[string]map[time.Time]*resourceSet {
	byTarget := map[string]map[time.Time]*resourceSet{}

	for _, m := range b.managed.All() {
		if m.PolicyID != policyID || m.Schedule != schedule || m.CopyOf != "" || !m.ArchivedAt.IsZero() {
			continue
		}

		sets := byTarget[m.TargetID]
		if sets == nil {
			sets = map[time.Time]*resourceSet{}
			byTarget[m.TargetID] = sets
		}

		s := sets[m.CreatedAt]
		if s == nil {
			s = &resourceSet{createdAt: m.CreatedAt}
			sets[m.CreatedAt] = s
		}

		s.members = append(s.members, m)
	}

	return byTarget
}

func (b *InMemoryBackend) expire(
	ctx context.Context, policyID, schedule string, rule map[string]any, archive bool, now time.Time,
) {
	byTarget := b.standardTierSets(policyID, schedule)
	count, byCount := numberOf(rule["Count"])
	cutoff, byAge := retainCutoff(rule, now)

	for _, sets := range byTarget {
		ordered := make([]*resourceSet, 0, len(sets))
		for _, s := range sets {
			ordered = append(ordered, s)
		}

		sort.Slice(ordered, func(i, j int) bool { return ordered[i].createdAt.After(ordered[j].createdAt) })

		for i, s := range ordered {
			if (byCount && i >= int(count)) || (byAge && !byCount && !s.createdAt.After(cutoff)) {
				if archive {
					b.archiveSet(ctx, s, now)
				} else {
					b.deleteSet(ctx, s)
				}
			}
		}
	}
}

func (b *InMemoryBackend) deleteSet(ctx context.Context, s *resourceSet) {
	for _, m := range s.members {
		b.dropFastRestore(ctx, m)

		err := b.compute.Delete(m.regionOr(b.region), m.Kind, m.ID)
		if err != nil && !errors.Is(err, ErrResourceNotFound) {
			logger.Load(ctx).WarnContext(ctx, "dlm: expire failed", "policy", m.PolicyID, "id", m.ID, "error", err)

			continue
		}

		b.managed.Delete(m.ID)
	}
}
