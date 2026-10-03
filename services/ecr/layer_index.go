package ecr

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

const sha256DigestPrefix = "sha256:"

// layerRefIndex maps repository -> layer digest -> referencing image digests.
// Derived from manifests, never persisted; caller holds the backend lock.
type layerRefIndex struct {
	refs map[string]map[string]map[string]struct{}
}

func newLayerRefIndex() *layerRefIndex {
	return &layerRefIndex{refs: make(map[string]map[string]map[string]struct{})}
}

// manifestDigests returns every full sha256 digest literally present in manifest.
func manifestDigests(manifest string) []string {
	var out []string

	rest := manifest

	for {
		i := strings.Index(rest, sha256DigestPrefix)
		if i < 0 {
			return out
		}

		rest = rest[i:]

		if len(rest) >= len(sha256DigestPrefix)+64 && isFullSHA256Digest(rest[:len(sha256DigestPrefix)+64]) {
			out = append(out, rest[:len(sha256DigestPrefix)+64])
		}

		rest = rest[len(sha256DigestPrefix):]
	}
}

func (x *layerRefIndex) add(img *Image) {
	for _, d := range manifestDigests(img.ImageManifest) {
		byLayer := x.refs[img.RepositoryName]
		if byLayer == nil {
			byLayer = make(map[string]map[string]struct{})
			x.refs[img.RepositoryName] = byLayer
		}

		set := byLayer[d]
		if set == nil {
			set = make(map[string]struct{})
			byLayer[d] = set
		}

		set[img.ImageDigest] = struct{}{}
	}
}

func (x *layerRefIndex) remove(img *Image) {
	byLayer := x.refs[img.RepositoryName]

	for _, d := range manifestDigests(img.ImageManifest) {
		set := byLayer[d]
		delete(set, img.ImageDigest)

		if len(set) == 0 {
			delete(byLayer, d)
		}
	}

	if len(byLayer) == 0 {
		delete(x.refs, img.RepositoryName)
	}
}

func (x *layerRefIndex) rebuild(images *store.Table[Image]) {
	x.refs = make(map[string]map[string]map[string]struct{})

	images.Range(func(img *Image) bool {
		x.add(img)

		return true
	})
}

// putImageLocked stores img, keeping the layer index in step with any image it replaces.
func (b *InMemoryBackend) putImageLocked(img *Image) {
	if old, ok := b.images.Get(imageKeyFn(img)); ok {
		b.layerRefs.remove(old)
	}

	b.images.Put(img)
	b.layerRefs.add(img)
}
