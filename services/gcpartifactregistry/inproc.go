package gcpartifactregistry

import "cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"

// API exposes the Artifact Registry service for in-process callers (dashboard proxy).
func (h *Handler) API() artifactregistrypb.ArtifactRegistryServer { return &server{b: h.Backend} }
