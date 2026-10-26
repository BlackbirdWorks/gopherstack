package dlm

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// createCtx carries the schedule and tags a freshly created snapshot or AMI was made with.
type createCtx struct {
	sched map[string]any
	tags  map[string]string
	desc  string
}

func ruleMap(sched map[string]any, key string) map[string]any {
	m, _ := sched[key].(map[string]any)

	return m
}

// addInterval returns base advanced by n units (DAYS, WEEKS, MONTHS, YEARS); ok is false when n is absent or negative.
func addInterval(n any, unit any, base time.Time) (time.Time, bool) {
	f, ok := numberOf(n)
	if !ok || f < 0 {
		return time.Time{}, false
	}

	count := int(f)
	u, _ := unit.(string)

	switch strings.ToUpper(u) {
	case "WEEKS":
		return base.AddDate(0, 0, count*daysPerWeek), true
	case "MONTHS":
		return base.AddDate(0, count, 0), true
	case "YEARS":
		return base.AddDate(count, 0, 0), true
	default:
		return base.AddDate(0, 0, count), true
	}
}

func (b *InMemoryBackend) warn(ctx context.Context, msg string, m *managedResource, err error) {
	logger.Load(ctx).WarnContext(ctx, "dlm: "+msg, "policy", m.PolicyID, "id", m.ID, "error", err)
}

// applyCreateRules runs the schedule's FastRestore, Share, CrossRegionCopy and Deprecate rules on a new resource.
func (b *InMemoryBackend) applyCreateRules(ctx context.Context, cc createCtx, m *managedResource) {
	if m.Kind == kindSnapshot {
		b.enableFastRestore(ctx, cc.sched, m)
		b.shareSnapshot(ctx, cc.sched, m)
	} else {
		b.deprecateAtInterval(ctx, cc.sched, m)
	}

	b.crossRegionCopies(ctx, cc, m)
	b.managed.Put(m)
}

func (b *InMemoryBackend) enableFastRestore(ctx context.Context, sched map[string]any, m *managedResource) {
	rule := ruleMap(sched, "FastRestoreRule")
	if rule == nil {
		return
	}

	zones := policyDetailsStringSlice(rule, "AvailabilityZones")
	if len(zones) == 0 {
		return
	}

	if err := b.compute.SetFastRestore(b.region, m.ID, zones, true); err != nil {
		b.warn(ctx, "enable fast snapshot restore failed", m, err)

		return
	}

	m.FastRestore = zones
}

func (b *InMemoryBackend) dropFastRestore(ctx context.Context, m *managedResource) {
	if len(m.FastRestore) == 0 || m.Kind != kindSnapshot {
		return
	}

	if err := b.compute.SetFastRestore(m.regionOr(b.region), m.ID, m.FastRestore, false); err != nil {
		b.warn(ctx, "disable fast snapshot restore failed", m, err)

		return
	}

	m.FastRestore = nil
	b.managed.Put(m)
}

func (b *InMemoryBackend) shareSnapshot(ctx context.Context, sched map[string]any, m *managedResource) {
	for _, r := range asSlice(sched["ShareRules"]) {
		rule, _ := r.(map[string]any)

		accounts := policyDetailsStringSlice(rule, "TargetAccounts")
		if len(accounts) == 0 {
			continue
		}

		if err := b.compute.SetSnapshotSharing(b.region, m.ID, accounts, true); err != nil {
			b.warn(ctx, "share snapshot failed", m, err)

			continue
		}

		g := shareGrant{Accounts: accounts}
		if at, ok := addInterval(rule["UnshareInterval"], rule["UnshareIntervalUnit"], m.CreatedAt); ok {
			g.UnshareAt = at
		}

		m.Shares = append(m.Shares, g)
	}
}

func (b *InMemoryBackend) unshare(ctx context.Context, m *managedResource, due func(shareGrant) bool) {
	kept := m.Shares[:0:0]

	for _, g := range m.Shares {
		if !due(g) {
			kept = append(kept, g)

			continue
		}

		if err := b.compute.SetSnapshotSharing(m.regionOr(b.region), m.ID, g.Accounts, false); err != nil {
			b.warn(ctx, "unshare snapshot failed", m, err)
			kept = append(kept, g)
		}
	}

	m.Shares = kept
	b.managed.Put(m)
}

func (b *InMemoryBackend) deprecateAtInterval(ctx context.Context, sched map[string]any, m *managedResource) {
	rule := ruleMap(sched, "DeprecateRule")
	if rule == nil {
		return
	}

	at, ok := addInterval(rule["Interval"], rule["IntervalUnit"], m.CreatedAt)
	if !ok {
		return
	}

	if err := b.compute.DeprecateImage(b.region, m.ID, at); err != nil {
		b.warn(ctx, "deprecate image failed", m, err)

		return
	}

	m.Deprecated = true
}

func (b *InMemoryBackend) crossRegionCopies(ctx context.Context, cc createCtx, m *managedResource) {
	for _, r := range asSlice(cc.sched["CrossRegionCopyRules"]) {
		rule, _ := r.(map[string]any)

		dst, _ := rule["TargetRegion"].(string)
		if dst == "" {
			dst, _ = rule["Target"].(string)
		}

		if dst == "" || strings.HasPrefix(dst, "arn:") {
			continue
		}

		b.copyTo(ctx, cc, m, rule, dst)
	}
}

func (b *InMemoryBackend) copyTo(ctx context.Context, cc createCtx, m *managedResource, rule map[string]any, dst string) {
	tags := copyTags(cc, rule, m.PolicyID, m.Schedule, m.CreatedAt)
	encrypted, _ := rule["Encrypted"].(bool)
	cmk, _ := rule["CmkArn"].(string)

	var (
		id  string
		err error
	)

	if m.Kind == kindImage {
		id, err = b.compute.CopyImage(b.region, dst, m.ID, m.PolicyID+"-"+dst, cc.desc, encrypted, cmk, tags)
	} else {
		id, err = b.compute.CopySnapshot(b.region, dst, m.ID, cc.desc, encrypted, cmk, tags)
	}

	if id == "" {
		b.warn(ctx, "cross-region copy failed", m, err)

		return
	}

	cp := &managedResource{
		ID: id, Kind: m.Kind, PolicyID: m.PolicyID, Schedule: m.Schedule, TargetID: m.TargetID,
		CreatedAt: m.CreatedAt, Region: dst, CopyOf: m.ID,
	}

	if rr := ruleMap(rule, "RetainRule"); rr != nil {
		if at, ok := addInterval(rr["Interval"], rr["IntervalUnit"], m.CreatedAt); ok {
			cp.ExpiresAt = at
		}
	}

	if dr := ruleMap(rule, "DeprecateRule"); dr != nil && m.Kind == kindImage {
		if at, ok := addInterval(dr["Interval"], dr["IntervalUnit"], m.CreatedAt); ok {
			if derr := b.compute.DeprecateImage(dst, id, at); derr != nil {
				b.warn(ctx, "deprecate image copy failed", cp, derr)
			} else {
				cp.Deprecated = true
			}
		}
	}

	if err != nil {
		b.warn(ctx, "tag cross-region copy failed", cp, err)
	}

	b.managed.Put(cp)
}

// copyTags are the DLM system tags plus, when the rule asks for it, the user-defined tags without variable tags.
func copyTags(cc createCtx, rule map[string]any, policyID, schedule string, now time.Time) map[string]string {
	out := map[string]string{
		tagPolicyID: policyID, tagScheduleName: schedule, tagTimestamp: now.Format(time.RFC3339), tagManaged: "true",
	}

	if cp, _ := rule["CopyTags"].(bool); !cp {
		return out
	}

	variable := map[string]bool{}
	for _, p := range policyDetailsTagPairs(cc.sched, "VariableTags") {
		variable[p.Key] = true
	}

	for k, v := range cc.tags {
		if !strings.HasPrefix(k, "aws:") && !variable[k] {
			out[k] = v
		}
	}

	return out
}

// archiveSet moves a due snapshot set into the archive tier instead of deleting it.
func (b *InMemoryBackend) archiveSet(ctx context.Context, s *resourceSet, now time.Time) {
	for _, m := range s.members {
		if m.Kind != kindSnapshot {
			continue
		}

		if err := b.compute.ArchiveSnapshot(m.regionOr(b.region), m.ID); err != nil {
			b.warn(ctx, "archive snapshot failed", m, err)

			continue
		}

		b.dropFastRestore(ctx, m)
		b.unshare(ctx, m, func(shareGrant) bool { return true })

		m.ArchivedAt = now
		b.managed.Put(m)
	}
}

// maintain applies the time-driven parts of a schedule's rules to everything it manages.
func (b *InMemoryBackend) maintain(ctx context.Context, policyID, schedule string, sched map[string]any, now time.Time) {
	var members []*managedResource

	for _, m := range b.managed.All() {
		if m.PolicyID == policyID && m.Schedule == schedule {
			members = append(members, m)
		}
	}

	sort.Slice(members, func(i, j int) bool { return members[i].CreatedAt.After(members[j].CreatedAt) })

	b.trimFastRestore(ctx, ruleMap(sched, "FastRestoreRule"), members, now)
	b.expireShares(ctx, members, now)
	b.expireCopies(ctx, members, now)
	b.deprecateByCount(ctx, ruleMap(sched, "DeprecateRule"), members, now)
	b.expireArchived(ctx, ruleMap(ruleMap(ruleMap(sched, "ArchiveRule"), "RetainRule"), "RetentionArchiveTier"), members, now)
}

func ownedByTarget(members []*managedResource, kind string) map[string][]*managedResource {
	out := map[string][]*managedResource{}

	for _, m := range members {
		if m.Kind == kind && m.CopyOf == "" && m.ArchivedAt.IsZero() {
			out[m.TargetID] = append(out[m.TargetID], m)
		}
	}

	return out
}

func (b *InMemoryBackend) trimFastRestore(ctx context.Context, rule map[string]any, members []*managedResource, now time.Time) {
	if rule == nil {
		return
	}

	count, byCount := numberOf(rule["Count"])
	_, byAge := numberOf(rule["Interval"])

	for _, list := range ownedByTarget(members, kindSnapshot) {
		for i, m := range list {
			if len(m.FastRestore) == 0 {
				continue
			}

			expired := false
			if at, ok := addInterval(rule["Interval"], rule["IntervalUnit"], m.CreatedAt); ok && byAge && !byCount {
				expired = !at.After(now)
			}

			if (byCount && i >= int(count)) || expired {
				b.dropFastRestore(ctx, m)
			}
		}
	}
}

func (b *InMemoryBackend) expireShares(ctx context.Context, members []*managedResource, now time.Time) {
	for _, m := range members {
		if len(m.Shares) == 0 {
			continue
		}

		b.unshare(ctx, m, func(g shareGrant) bool { return !g.UnshareAt.IsZero() && !g.UnshareAt.After(now) })
	}
}

func (b *InMemoryBackend) expireCopies(ctx context.Context, members []*managedResource, now time.Time) {
	for _, m := range members {
		if m.CopyOf == "" || m.ExpiresAt.IsZero() || m.ExpiresAt.After(now) {
			continue
		}

		b.deleteSet(ctx, &resourceSet{createdAt: m.CreatedAt, members: []*managedResource{m}})
	}
}

func (b *InMemoryBackend) deprecateByCount(ctx context.Context, rule map[string]any, members []*managedResource, now time.Time) {
	count, ok := numberOf(rule["Count"])
	if rule == nil || !ok {
		return
	}

	for _, list := range ownedByTarget(members, kindImage) {
		live := slices.DeleteFunc(slices.Clone(list), func(m *managedResource) bool { return m.Deprecated })

		for i := int(count); i < len(live); i++ {
			if err := b.compute.DeprecateImage(b.region, live[i].ID, now); err != nil {
				b.warn(ctx, "deprecate image failed", live[i], err)

				continue
			}

			live[i].Deprecated = true
			b.managed.Put(live[i])
		}
	}
}

func (b *InMemoryBackend) expireArchived(ctx context.Context, tier map[string]any, members []*managedResource, now time.Time) {
	if tier == nil {
		return
	}

	count, byCount := numberOf(tier["Count"])
	byTarget := map[string][]*managedResource{}

	for _, m := range members {
		if m.Kind == kindSnapshot && !m.ArchivedAt.IsZero() {
			byTarget[m.TargetID] = append(byTarget[m.TargetID], m)
		}
	}

	for _, list := range byTarget {
		sort.Slice(list, func(i, j int) bool { return list[i].ArchivedAt.After(list[j].ArchivedAt) })

		for i, m := range list {
			at, byAge := addInterval(tier["Interval"], tier["IntervalUnit"], m.ArchivedAt)
			if (byCount && i >= int(count)) || (!byCount && byAge && !at.After(now)) {
				b.deleteSet(ctx, &resourceSet{createdAt: m.CreatedAt, members: []*managedResource{m}})
			}
		}
	}
}
