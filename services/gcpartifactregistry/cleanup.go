package gcpartifactregistry

import (
	"context"
	"slices"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const cleanupInterval = 24 * time.Hour

// validateCleanupPolicies enforces the documented policy shapes: every policy has an action, a delete policy
// has a condition, a keep policy has either a condition or mostRecentVersions, and keep_count is positive.
func validateCleanupPolicies(r *artifactregistrypb.Repository) error {
	if len(r.GetCleanupPolicies()) > 0 && r.GetMode() == artifactregistrypb.Repository_VIRTUAL_REPOSITORY {
		return errInvalid("Cleanup policies do not apply to virtual repositories.")
	}

	for id, p := range r.GetCleanupPolicies() {
		if p.GetAction() == artifactregistrypb.CleanupPolicy_ACTION_UNSPECIFIED {
			return errInvalid("Cleanup policy %q needs an action.", id)
		}

		mrv, cond := p.GetMostRecentVersions(), p.GetCondition()

		switch {
		case mrv == nil && cond == nil:
			return errInvalid("Cleanup policy %q needs a condition or mostRecentVersions.", id)
		case mrv != nil && p.GetAction() != artifactregistrypb.CleanupPolicy_KEEP:
			return errInvalid("Cleanup policy %q: mostRecentVersions is only valid in a KEEP policy.", id)
		case mrv != nil && mrv.KeepCount != nil && mrv.GetKeepCount() < 0:
			return errInvalid("Cleanup policy %q: keep_count must not be negative.", id)
		}
	}

	return nil
}

// cleanupVersion is one version as the cleanup engine sees it.
type cleanupVersion struct {
	created time.Time
	leaf    leafRef
	pkg     string
	version string
	tags    []string
}

func hasPrefixIn(s string, prefixes []string) bool {
	return len(prefixes) == 0 || slices.ContainsFunc(prefixes, func(p string) bool { return strings.HasPrefix(s, p) })
}

func (v cleanupVersion) matches(c *artifactregistrypb.CleanupPolicyCondition, now time.Time) bool {
	tagged := len(v.tags) > 0

	switch c.GetTagState() {
	case artifactregistrypb.CleanupPolicyCondition_TAGGED:
		if !tagged {
			return false
		}
	case artifactregistrypb.CleanupPolicyCondition_UNTAGGED:
		if tagged {
			return false
		}
	case artifactregistrypb.CleanupPolicyCondition_TAG_STATE_UNSPECIFIED, artifactregistrypb.CleanupPolicyCondition_ANY:
	}

	if len(c.GetTagPrefixes()) > 0 &&
		!slices.ContainsFunc(v.tags, func(t string) bool { return hasPrefixIn(t, c.GetTagPrefixes()) }) {
		return false
	}

	if !hasPrefixIn(v.version, c.GetVersionNamePrefixes()) || !hasPrefixIn(v.pkg, c.GetPackageNamePrefixes()) {
		return false
	}

	age := now.Sub(v.created)

	return (c.GetOlderThan() == nil || age > c.GetOlderThan().AsDuration()) &&
		(c.GetNewerThan() == nil || age < c.GetNewerThan().AsDuration())
}

// mostRecentKept returns the versions the repository's mostRecentVersions keep policies protect.
func mostRecentKept(versions []cleanupVersion, policies []*artifactregistrypb.CleanupPolicy) map[string]bool {
	kept := map[string]bool{}

	for _, p := range policies {
		mrv := p.GetMostRecentVersions()
		if mrv == nil || p.GetAction() != artifactregistrypb.CleanupPolicy_KEEP {
			continue
		}

		byPkg := map[string][]cleanupVersion{}

		for _, v := range versions {
			if hasPrefixIn(v.pkg, mrv.GetPackageNamePrefixes()) {
				byPkg[v.pkg] = append(byPkg[v.pkg], v)
			}
		}

		for _, vs := range byPkg {
			sort.SliceStable(vs, func(i, j int) bool { return vs[i].created.After(vs[j].created) })

			for i := 0; i < len(vs) && int32(i) < mrv.GetKeepCount(); i++ {
				kept[vs[i].leaf.pkg.full()+"/versions/"+vs[i].version] = true
			}
		}
	}

	return kept
}

// selectForDeletion applies the documented rules: a version goes when a DELETE policy matches it and no KEEP
// policy does.
func selectForDeletion(
	versions []cleanupVersion, policies []*artifactregistrypb.CleanupPolicy, now time.Time, immutable bool,
) []cleanupVersion {
	kept := mostRecentKept(versions, policies)

	var out []cleanupVersion

	for _, v := range versions {
		if immutable && len(v.tags) > 0 || kept[v.leaf.pkg.full()+"/versions/"+v.version] {
			continue
		}

		del, keep := false, false

		for _, p := range policies {
			c := p.GetCondition()
			if c == nil || !v.matches(c, now) {
				continue
			}

			if p.GetAction() == artifactregistrypb.CleanupPolicy_KEEP {
				keep = true
			} else {
				del = true
			}
		}

		if del && !keep {
			out = append(out, v)
		}
	}

	return out
}

// CleanupResult is what one policy run did to one repository.
type CleanupResult struct {
	Repository string
	Versions   []string
	DryRun     bool
}

func (b *InMemoryBackend) cleanupCandidates(
	ctx context.Context,
	ref repoRef,
	format artifactregistrypb.Repository_Format,
) []cleanupVersion {
	if format == artifactregistrypb.Repository_DOCKER {
		var out []cleanupVersion

		for _, s := range b.imageSnaps(ref, "") {
			for d, m := range s.manifests {
				out = append(out, cleanupVersion{
					created: m.Created, pkg: s.image, version: d, tags: s.tagsOf(d),
					leaf: leafRef{pkg: pkgRef{repo: ref, image: s.image}, leaf: d},
				})
			}
		}

		return out
	}

	var out []cleanupVersion

	for _, id := range b.packageIDs(ref) {
		for _, v := range b.versionsOfPackage(ref, id) {
			out = append(out, cleanupVersion{
				created: v.created, pkg: id, version: v.version,
				tags: b.tagsOf(ref.packageName(id) + "/versions/" + v.version),
				leaf: leafRef{pkg: pkgRef{repo: ref, image: id}, leaf: v.version},
			})
		}
	}

	_ = ctx

	return out
}

func (b *InMemoryBackend) deleteForCleanup(ctx context.Context, v cleanupVersion, docker bool) error {
	if docker {
		return b.deleteVersion(ctx, v.leaf, true)
	}

	return b.genDeleteVersions([]string{v.leaf.pkg.full() + "/versions/" + v.version}, false)
}

// RunCleanup applies every standard repository's cleanup policies at the backend clock's current time. With
// cleanup_policy_dry_run set nothing is deleted and the result lists what would have been.
func (b *InMemoryBackend) RunCleanup(ctx context.Context) []CleanupResult {
	type target struct {
		ref      repoRef
		policies []*artifactregistrypb.CleanupPolicy
		format   artifactregistrypb.Repository_Format
		dryRun   bool
		immut    bool
	}

	var targets []target

	b.mu.RLock("RunCleanup")

	for name, rec := range b.repos {
		pb := rec.pb
		if len(pb.GetCleanupPolicies()) == 0 || pb.GetMode() == artifactregistrypb.Repository_VIRTUAL_REPOSITORY {
			continue
		}

		ref, err := parseRepo(name)
		if err != nil {
			continue
		}

		t := target{
			ref:    ref,
			format: pb.GetFormat(),
			dryRun: pb.GetCleanupPolicyDryRun(),
			immut:  pb.GetDockerConfig().GetImmutableTags(),
		}
		for _, p := range pb.GetCleanupPolicies() {
			t.policies = append(t.policies, p)
		}

		sort.Slice(t.policies, func(i, j int) bool { return t.policies[i].GetId() < t.policies[j].GetId() })

		targets = append(targets, t)
	}

	now := b.now()
	b.mu.RUnlock()

	sort.Slice(targets, func(i, j int) bool { return targets[i].ref.full() < targets[j].ref.full() })

	results := make([]CleanupResult, 0, len(targets))

	for _, t := range targets {
		docker := t.format == artifactregistrypb.Repository_DOCKER
		doomed := selectForDeletion(b.cleanupCandidates(ctx, t.ref, t.format), t.policies, now, t.immut)
		res := CleanupResult{Repository: t.ref.full(), DryRun: t.dryRun}

		for _, v := range doomed {
			name := v.leaf.pkg.full() + "/versions/" + v.version

			if !t.dryRun {
				if err := b.deleteForCleanup(ctx, v, docker); err != nil {
					logger.Load(ctx).
						WarnContext(ctx, "gcpartifactregistry: cleanup delete failed", "version", name, "error", err)

					continue
				}
			}

			res.Versions = append(res.Versions, name)
		}

		sort.Strings(res.Versions)
		results = append(results, res)
	}

	return results
}

// SetClock replaces the clock used for timestamps and cleanup ages; nil restores time.Now.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	if now == nil {
		now = time.Now
	}

	b.now = now
}

// TickerFunc starts a ticker whose channel fires every d; stop releases it.
type TickerFunc func(d time.Duration) (<-chan time.Time, func())

func realTicker(d time.Duration) (<-chan time.Time, func()) {
	t := time.NewTicker(d)

	return t.C, t.Stop
}

// SetCleanupTicker replaces the daily ticker the cleanup worker waits on.
func (b *InMemoryBackend) SetCleanupTicker(f TickerFunc) {
	b.mu.Lock("SetCleanupTicker")
	defer b.mu.Unlock()

	b.ticker = f
}

// CleanupLoop runs RunCleanup on every tick until ctx ends. Google runs cleanup policies about once a day.
func (b *InMemoryBackend) CleanupLoop(ctx context.Context) {
	b.mu.RLock("CleanupLoop")
	mk := b.ticker
	b.mu.RUnlock()

	if mk == nil {
		mk = realTicker
	}

	ch, stop := mk(cleanupInterval)
	defer stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ch:
			b.RunCleanup(ctx)
		}
	}
}
