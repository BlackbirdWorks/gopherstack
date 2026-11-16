package ociartifacts

import (
	"slices"
	"strings"
)

// ImageRef identifies a stored container image for other emulated services.
type ImageRef struct {
	ID     string
	Digest string
}

// SignatureRef is a stored image signature as other emulated services verify it.
type SignatureRef struct {
	KeyID        string
	KeyVersionID string
	Message      string
	Signature    string
	Algorithm    string
}

// registryPath drops a leading registry host (`iad.ocir.io/`) from an image reference.
func registryPath(ref string) string {
	host, rest, ok := strings.Cut(ref, "/")
	if ok && (strings.ContainsAny(host, ".:") || host == "localhost") {
		return rest
	}

	return ref
}

// ResolveImage finds the image a `[host/]namespace/repo[:tag|@digest]` reference names. owned is false when the
// reference is outside this tenancy's namespace, so the caller cannot judge it.
func (b *InMemoryBackend) ResolveImage(ref string) (ImageRef, bool, bool) {
	path := registryPath(ref)

	b.mu.RLock("ResolveImage")
	ns := b.namespace
	b.mu.RUnlock()

	if !strings.HasPrefix(path, ns+"/") {
		return ImageRef{}, false, false
	}

	w, _, err := b.LookupContainerImageByURI(path)
	if err != nil {
		return ImageRef{}, false, true
	}

	return ImageRef{ID: w.ID, Digest: w.Digest}, true, true
}

// ImageSignatures lists the signatures stored for an image.
func (b *InMemoryBackend) ImageSignatures(imageID string) []SignatureRef {
	b.mu.RLock("ImageSignatures")
	defer b.mu.RUnlock()

	var out []*Signature

	for _, s := range b.sigs {
		if s.ImageID == imageID {
			out = append(out, s)
		}
	}

	slices.SortFunc(out, func(a, c *Signature) int { return int(a.Seq - c.Seq) })

	refs := make([]SignatureRef, 0, len(out))
	for _, s := range out {
		refs = append(refs, SignatureRef{
			KeyID: s.KmsKeyID, KeyVersionID: s.KmsKeyVersionID, Message: s.Message, Signature: s.Signature,
			Algorithm: s.Algorithm,
		})
	}

	return refs
}
