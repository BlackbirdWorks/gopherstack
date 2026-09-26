package ecrpublic

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

func sha256Digest(data []byte) string {
	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

// isFullSHA256Digest returns true when s is a properly-formed "sha256:<64 hex>" digest.
func isFullSHA256Digest(s string) bool {
	const prefix = "sha256:"
	if len(s) != len(prefix)+sha256.Size*2 {
		return false
	}

	if s[:len(prefix)] != prefix {
		return false
	}

	for _, c := range s[len(prefix):] {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}

	return true
}

// manifestLayerDigests is the minimal subset of an OCI/Docker image manifest
// needed to verify that every layer (and the config blob) a PutImage call
// references was actually uploaded first. A manifest this loose parser
// cannot decode (e.g. a manifest list / OCI index for multi-arch images) is
// treated as having no checkable layer references, since a real manifest
// list references per-platform manifests rather than layer digests directly.
type manifestLayers struct {
	Config struct {
		Digest string `json:"digest"`
	} `json:"config"`
	Layers []struct {
		Digest string `json:"digest"`
	} `json:"layers"`
}

// referencedDigests returns every layer/config digest manifest references,
// or nil if manifest is not a shape this parser understands.
func referencedDigests(manifest string) []string {
	var m manifestLayers
	if err := json.Unmarshal([]byte(manifest), &m); err != nil {
		return nil
	}

	var out []string
	if m.Config.Digest != "" {
		out = append(out, m.Config.Digest)
	}

	for _, l := range m.Layers {
		if l.Digest != "" {
			out = append(out, l.Digest)
		}
	}

	return out
}
