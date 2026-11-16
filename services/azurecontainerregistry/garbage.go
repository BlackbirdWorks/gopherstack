package azurecontainerregistry

import (
	"context"
	"encoding/json/v2"
	"strings"

	"github.com/distribution/distribution/v3/registry/storage/driver/filesystem"
	"github.com/distribution/distribution/v3/registry/storage/driver/inmemory"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	storageRoot    = "/docker/registry/v2"
	digestAlgoPref = "sha256:"
	maxStoredDoc   = 4 << 20
)

func newDriver(dir string) storagedriver.StorageDriver {
	if dir == "" {
		return inmemory.New()
	}

	d, err := filesystem.FromParameters(map[string]any{"rootdirectory": dir})
	if err != nil {
		return inmemory.New()
	}

	return d
}

func blobPath(digest string) (string, bool) {
	hex, ok := strings.CutPrefix(digest, digestAlgoPref)
	if !ok || len(hex) < 2 || strings.ContainsAny(hex, "/.\\") {
		return "", false
	}

	return storageRoot + "/blobs/sha256/" + hex[:2] + "/" + hex, true
}

type storedManifest struct {
	Config struct {
		Digest string `json:"digest"`
	} `json:"config"`
	Layers []struct {
		Digest string `json:"digest"`
	} `json:"layers"`
}

// referencedBlobs returns the config and layer digests of the manifest stored under digest.
func referencedBlobs(ctx context.Context, drv storagedriver.StorageDriver, digest string) ([]string, bool) {
	p, ok := blobPath(digest)
	if !ok {
		return nil, false
	}

	raw, err := drv.GetContent(ctx, p+"/data")
	if err != nil || len(raw) > maxStoredDoc {
		return nil, false
	}

	var doc storedManifest
	if json.Unmarshal(raw, &doc, jsonx.V1()) != nil {
		return nil, false
	}

	var out []string
	if doc.Config.Digest != "" {
		out = append(out, doc.Config.Digest)
	}

	for _, l := range doc.Layers {
		out = append(out, l.Digest)
	}

	return out, true
}

type liveManifest struct{ repo, digest string }

func (b *InMemoryBackend) liveManifests(reg string) (storagedriver.StorageDriver, []liveManifest) {
	b.mu.RLock("liveManifests")
	defer b.mu.RUnlock()

	r, ok := b.regs[strs.Fold(reg)]
	if !ok || r.driver == nil {
		return nil, nil
	}

	var out []liveManifest

	for repo, rm := range r.Repos {
		for d := range rm.Manifests {
			out = append(out, liveManifest{repo: repo, digest: d})
		}
	}

	return r.driver, out
}

// collectGarbage removes the manifest blob and the config and layer blobs of the deleted manifest digest that no
// remaining manifest of the registry references, and unlinks them from repo. The caller holds the delete gate.
func (b *InMemoryBackend) collectGarbage(ctx context.Context, reg, repo, digest string) {
	drv, live := b.liveManifests(reg)
	if drv == nil || !ociregistry.IsDigest(digest) {
		return
	}

	candidates, ok := referencedBlobs(ctx, drv, digest)
	if !ok {
		return
	}

	keep := map[string]bool{}

	for _, m := range live {
		keep[m.digest] = true

		if refs, found := referencedBlobs(ctx, drv, m.digest); found {
			for _, ref := range refs {
				keep[ref] = true
			}
		}
	}

	if b.onPlanned != nil {
		b.onPlanned()
	}

	for _, c := range append(candidates, digest) {
		if keep[c] {
			continue
		}

		removeBlob(ctx, drv, repo, c)
	}
}

func removeBlob(ctx context.Context, drv storagedriver.StorageDriver, repo, digest string) {
	p, ok := blobPath(digest)
	if !ok {
		return
	}

	hex := strings.TrimPrefix(digest, digestAlgoPref)

	for _, path := range []string{
		p, storageRoot + "/repositories/" + repo + "/_layers/sha256/" + hex,
		storageRoot + "/repositories/" + repo + "/_manifests/revisions/sha256/" + hex,
	} {
		_ = drv.Delete(ctx, path)
	}
}

func (b *InMemoryBackend) baseContext() context.Context {
	b.mu.RLock("baseContext")
	defer b.mu.RUnlock()

	return b.ctx
}
