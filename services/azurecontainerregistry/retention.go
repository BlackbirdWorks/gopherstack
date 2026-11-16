package azurecontainerregistry

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	dockerManifestV2   = "application/vnd.docker.distribution.manifest.v2+json"
	dockerManifestList = "application/vnd.docker.distribution.manifest.list.v2+json"
	hoursPerDay        = 24
)

// RetentionSource reports whether the untagged-manifest retention policy of a registry is enabled and its period
// in days.
type RetentionSource func(registry string) (days int, enabled bool)

// SetRetentionSource lets registries delete untagged manifests after their retention period.
func (b *InMemoryBackend) SetRetentionSource(s RetentionSource) {
	b.mu.Lock("SetRetentionSource")
	defer b.mu.Unlock()

	b.retention = s
}

func (b *InMemoryBackend) retentionPolicy(reg string) (int, bool) {
	b.mu.RLock("retentionPolicy")
	src := b.retention
	b.mu.RUnlock()

	if src == nil {
		return 0, false
	}

	return src(reg)
}

func (r *registry) tagged(repo, digest string) bool {
	rm, ok := r.Repos[repo]
	if !ok {
		return false
	}

	for _, t := range rm.Tags {
		if t.Digest == digest {
			return true
		}
	}

	return false
}

func (r *registry) schedule(repo, digest string, due time.Time) {
	if rm, ok := r.Repos[repo]; !ok || rm.Manifests[digest] == nil || r.tagged(repo, digest) {
		return
	}

	if r.RetentionDue == nil {
		r.RetentionDue = map[string]map[string]time.Time{}
	}

	if r.RetentionDue[repo] == nil {
		r.RetentionDue[repo] = map[string]time.Time{}
	}

	r.RetentionDue[repo][digest] = due
}

func (r *registry) unschedule(repo, digest string) {
	delete(r.RetentionDue[repo], digest)

	if len(r.RetentionDue[repo]) == 0 {
		delete(r.RetentionDue, repo)
	}
}

func retentionDue(now time.Time, days int) time.Time {
	return now.Add(time.Duration(days) * hoursPerDay * time.Hour)
}

// hasDueRetention reports whether any untagged manifest of reg has passed its deletion date.
func (b *InMemoryBackend) hasDueRetention(reg string) bool {
	b.mu.RLock("hasDueRetention")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return false
	}

	now := b.now()

	for _, byDigest := range r.RetentionDue {
		for _, due := range byDigest {
			if !due.After(now) {
				return true
			}
		}
	}

	return false
}

type dueManifest struct{ repo, digest string }

// takeDue returns the manifests due for deletion, dropping every schedule when the policy was disabled since.
func (b *InMemoryBackend) takeDue(reg string, enabled bool) []dueManifest {
	b.mu.Lock("takeDue")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return nil
	}

	if !enabled {
		r.RetentionDue = nil

		return nil
	}

	now := b.now()

	var out []dueManifest

	for repo, byDigest := range r.RetentionDue {
		for digest, due := range byDigest {
			switch {
			case due.After(now):
			case r.tagged(repo, digest) || !r.Repos.Allowed(repo, digest, ociregistry.OpDelete):
				r.unschedule(repo, digest)
			default:
				out = append(out, dueManifest{repo: repo, digest: digest})
			}
		}
	}

	return out
}

// indexChildren returns the digests the index manifests of repo reference.
func (b *InMemoryBackend) indexChildren(ctx context.Context, c ociregistry.Client, reg, repo string) map[string]bool {
	b.mu.RLock("indexChildren")

	var digests []string

	if r, ok := b.regs[strs.Fold(reg)]; ok && r.Repos[repo] != nil {
		for d := range r.Repos[repo].Manifests {
			digests = append(digests, d)
		}
	}

	b.mu.RUnlock()

	out := map[string]bool{}

	for _, d := range digests {
		m, err := c.GetManifest(ctx, repo, d)
		if err != nil {
			continue
		}

		info := c.Describe(ctx, repo, m)
		for _, child := range info.Children {
			out[child.Digest] = true
		}
	}

	return out
}

// SweepRetention deletes the untagged manifests of reg whose retention period has elapsed on the backend clock.
// Only Docker manifests and manifest lists are deleted, and never a manifest an index references.
func (b *InMemoryBackend) SweepRetention(ctx context.Context, reg string) {
	if !b.hasDueRetention(reg) {
		return
	}

	_, enabled := b.retentionPolicy(reg)

	due := b.takeDue(reg, enabled)
	if len(due) == 0 {
		return
	}

	app, ok := b.app(reg)
	if !ok {
		return
	}

	c := ociregistry.NewClient(app)
	children := map[string]map[string]bool{}

	for _, d := range due {
		if children[d.repo] == nil {
			children[d.repo] = b.indexChildren(ctx, c, reg, d.repo)
		}

		b.deleteRetained(ctx, c, reg, d, children[d.repo][d.digest])
	}
}

func (b *InMemoryBackend) deleteRetained(
	ctx context.Context, c ociregistry.Client, reg string, d dueManifest, referenced bool,
) {
	m, err := c.GetManifest(ctx, d.repo, d.digest)
	if err != nil {
		b.RecordManifestDelete(reg, d.repo, d.digest)

		return
	}

	media := c.Describe(ctx, d.repo, m).MediaType

	if referenced || (media != dockerManifestV2 && media != dockerManifestList) {
		b.mu.Lock("deleteRetained")
		if r, ok := b.regs[strs.Fold(reg)]; ok {
			r.unschedule(d.repo, d.digest)
		}
		b.mu.Unlock()

		return
	}

	if err = c.DeleteManifest(ctx, d.repo, d.digest); err == nil {
		b.RecordManifestDelete(reg, d.repo, d.digest)
	}
}

// SetClock replaces the clock that dates ledger entries, retention deadlines and token expiry.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}
