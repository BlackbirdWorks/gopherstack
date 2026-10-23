package ociregistry

import (
	"context"
	"encoding/json"
	"strings"
)

const maxConfigBytes = 1 << 20

// Descriptor is the part of a manifest entry used for sizing and platform lookup.
type Descriptor struct {
	Digest    string `json:"digest"`
	MediaType string `json:"mediaType"`
	Size      int64  `json:"size"`
}

type manifestDoc struct {
	Config    Descriptor   `json:"config"`
	MediaType string       `json:"mediaType"`
	Layers    []Descriptor `json:"layers"`
	Manifests []Descriptor `json:"manifests"`
}

// Info summarises a manifest.
type Info struct {
	Digest          string
	MediaType       string
	ConfigMediaType string
	Architecture    string
	OS              string
	Children        []Descriptor
	Size            int64
	Index           bool
}

// IsIndexType reports whether mediaType is a manifest list or OCI index.
func IsIndexType(mediaType string) bool {
	return strings.Contains(mediaType, "manifest.list") || strings.Contains(mediaType, "image.index")
}

// Describe parses a manifest and, for an image manifest, reads its config blob for the platform.
func (c Client) Describe(ctx context.Context, repo string, m Manifest) Info {
	info := Info{Digest: m.Digest, MediaType: m.MediaType}

	var doc manifestDoc
	if json.Unmarshal(m.Body, &doc) != nil {
		return info
	}

	if doc.MediaType != "" {
		info.MediaType = doc.MediaType
	}

	if IsIndexType(info.MediaType) {
		info.Index, info.Children = true, doc.Manifests

		return info
	}

	info.ConfigMediaType = doc.Config.MediaType
	info.Size = doc.Config.Size

	for _, l := range doc.Layers {
		info.Size += l.Size
	}

	info.Architecture, info.OS = c.platform(ctx, repo, doc.Config.Digest)

	return info
}

func (c Client) platform(ctx context.Context, repo, configDigest string) (string, string) {
	if configDigest == "" {
		return "", ""
	}

	raw, err := c.GetBlob(ctx, repo, configDigest, maxConfigBytes)
	if err != nil {
		return "", ""
	}

	var cfg struct {
		Architecture string `json:"architecture"`
		OS           string `json:"os"`
	}

	if json.Unmarshal(raw, &cfg) != nil {
		return "", ""
	}

	return cfg.Architecture, cfg.OS
}
