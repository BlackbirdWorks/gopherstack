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

// manifestDoc is the subset of an OCI/Docker image manifest or OCI index
// PutImage inspects: referenced layers/config, or referenced child manifests.
type manifestDoc struct {
	MediaType    string `json:"mediaType"`
	ArtifactType string `json:"artifactType"`
	Config       struct {
		Digest    string `json:"digest"`
		MediaType string `json:"mediaType"`
	} `json:"config"`
	Layers []struct {
		Digest string `json:"digest"`
	} `json:"layers"`
	Manifests []struct {
		Digest string `json:"digest"`
	} `json:"manifests"`
}

func parseManifest(manifest string) (manifestDoc, bool) {
	var m manifestDoc

	return m, json.Unmarshal([]byte(manifest), &m) == nil
}

// referencedDigests returns every layer/config digest an image manifest
// references; an OCI index or undecodable manifest yields none.
func referencedDigests(m manifestDoc) []string {
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

// referencedManifests returns the child manifest digests of an OCI index or
// Docker manifest list.
func referencedManifests(m manifestDoc) []string {
	out := make([]string, 0, len(m.Manifests))

	for _, c := range m.Manifests {
		if c.Digest != "" {
			out = append(out, c.Digest)
		}
	}

	return out
}

// artifactMediaType is the OCI artifactType, else the config blob's media type.
func (m manifestDoc) artifactMediaType() string {
	if m.ArtifactType != "" {
		return m.ArtifactType
	}

	return m.Config.MediaType
}
