package azurecontainerregistry

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	// blobGrace is how long a blob an unfinished push uploaded, mounted or checked stays safe from collection.
	blobGrace   = 10 * time.Minute
	maxTouched  = 50000
	blobsMarker = "/blobs/"
)

// blobDigestOf returns the blob digest a request writes, mounts or checks, "" for any other request.
func blobDigestOf(r *http.Request) string {
	if r.Method == http.MethodGet {
		return ""
	}

	i := strings.LastIndex(r.URL.Path, blobsMarker)
	if i < 0 {
		return ""
	}

	q := r.URL.Query()

	for _, d := range []string{q.Get("digest"), q.Get("mount"), r.URL.Path[i+len(blobsMarker):]} {
		if ociregistry.IsDigest(d) {
			return d
		}
	}

	return ""
}

// touchBlob notes that an unfinished push is using digest.
func (b *InMemoryBackend) touchBlob(reg, digest string) {
	b.mu.Lock("touchBlob")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return
	}

	now := b.now()

	if len(r.touched) >= maxTouched {
		for d, at := range r.touched {
			if !at.Add(blobGrace).After(now) {
				delete(r.touched, d)
			}
		}

		if len(r.touched) >= maxTouched {
			return
		}
	}

	if r.touched == nil {
		r.touched = map[string]time.Time{}
	}

	r.touched[digest] = now
}

// settleBlobs forgets the touches of the blobs a stored manifest now references.
func (b *InMemoryBackend) settleBlobs(reg, digest string) {
	b.mu.RLock("settleBlobs")

	r, ok := b.regs[strs.Fold(reg)]
	if !ok || r.driver == nil || len(r.touched) == 0 {
		b.mu.RUnlock()

		return
	}

	drv, ctx := r.driver, b.ctx
	b.mu.RUnlock()

	refs, found := referencedBlobs(ctx, drv, digest)
	if !found {
		return
	}

	b.mu.Lock("settleBlobs")
	defer b.mu.Unlock()

	if r = b.regs[strs.Fold(reg)]; r != nil {
		for _, d := range refs {
			delete(r.touched, d)
		}
	}
}

// blobHeld reports whether an unfinished push touched digest within blobGrace, and until when it is held.
func (b *InMemoryBackend) blobHeld(reg, digest string) (time.Time, bool) {
	b.mu.RLock("blobHeld")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return time.Time{}, false
	}

	until := r.touched[digest].Add(blobGrace)

	return until, until.After(b.now())
}

func (b *InMemoryBackend) deferGarbage(reg, repo, digest string, until time.Time) {
	b.mu.Lock("deferGarbage")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return
	}

	if r.GarbageDue == nil {
		r.GarbageDue = map[string]map[string]time.Time{}
	}

	if r.GarbageDue[repo] == nil {
		r.GarbageDue[repo] = map[string]time.Time{}
	}

	r.GarbageDue[repo][digest] = until
}

func (b *InMemoryBackend) hasDueGarbage(reg string) bool {
	b.mu.RLock("hasDueGarbage")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return false
	}

	now := b.now()

	for _, byDigest := range r.GarbageDue {
		for _, due := range byDigest {
			if !due.After(now) {
				return true
			}
		}
	}

	return false
}

type dueBlob struct{ repo, digest string }

func (b *InMemoryBackend) takeDueGarbage(reg string) []dueBlob {
	b.mu.Lock("takeDueGarbage")
	defer b.mu.Unlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok {
		return nil
	}

	now := b.now()

	var out []dueBlob

	for repo, byDigest := range r.GarbageDue {
		for digest, due := range byDigest {
			if due.After(now) {
				continue
			}

			out = append(out, dueBlob{repo: repo, digest: digest})

			delete(byDigest, digest)
		}

		if len(byDigest) == 0 {
			delete(r.GarbageDue, repo)
		}
	}

	return out
}

// sweepGarbage removes the blobs collection deferred once their grace has passed and no manifest references them.
func (b *InMemoryBackend) sweepGarbage(ctx context.Context, reg string) {
	if !b.hasDueGarbage(reg) {
		return
	}

	defer b.deleteGate(reg)()

	due := b.takeDueGarbage(reg)

	drv, live := b.liveManifests(reg)
	if drv == nil || len(due) == 0 {
		return
	}

	keep := keepSet(ctx, drv, live)

	for _, d := range due {
		switch until, held := b.blobHeld(reg, d.digest); {
		case keep[d.digest]:
		case held:
			b.deferGarbage(reg, d.repo, d.digest, until)
		default:
			removeBlob(ctx, drv, d.repo, d.digest)
		}
	}
}
