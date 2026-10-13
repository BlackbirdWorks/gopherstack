package ecr

import (
	"encoding/json"
	"regexp"
	"strings"
)

const (
	mediaTypeDockerManifestV2 = "application/vnd.docker.distribution.manifest.v2+json"
	mediaTypeOCIManifest      = "application/vnd.oci.image.manifest.v1+json"
	mediaTypeOCIIndex         = "application/vnd.oci.image.index.v1+json"
	mediaTypeDockerList       = "application/vnd.docker.distribution.manifest.list.v2+json"
)

var imageTagPattern = regexp.MustCompile(`^[\w][\w.-]{0,127}$`)

func validateImageTag(tag string) error {
	if tag == "" || imageTagPattern.MatchString(tag) {
		return nil
	}

	return invalidParam("Invalid parameter at 'imageTag' failed to satisfy constraint: "+
		"'must match %s'", imageTagPattern.String())
}

type manifestMeta struct {
	Config struct {
		MediaType string `json:"mediaType"`
		Size      int64  `json:"size"`
	} `json:"config"`
	MediaType string `json:"mediaType"`
	Layers    []struct {
		Size int64 `json:"size"`
	} `json:"layers"`
	Manifests []json.RawMessage `json:"manifests"`
}

// manifestMediaTypeAndSize reports the manifest's media type (declared, else
// inferred from its shape) and its compressed size: config plus layer sizes.
func manifestMediaTypeAndSize(manifest string) (string, int64) {
	var m manifestMeta
	if err := json.Unmarshal([]byte(manifest), &m); err != nil {
		return "", 0
	}

	var size int64

	if m.Config.Size > 0 {
		size += m.Config.Size
	}

	for _, l := range m.Layers {
		if l.Size > 0 {
			size += l.Size
		}
	}

	mt := m.MediaType

	switch {
	case mt != "":
	case len(m.Manifests) > 0:
		mt = mediaTypeOCIIndex
	case strings.HasPrefix(m.Config.MediaType, "application/vnd.oci."):
		mt = mediaTypeOCIManifest
	case m.Config.MediaType != "":
		mt = mediaTypeDockerManifestV2
	}

	return mt, size
}

// prepareImageMeta validates the tag and fills media type and size from the manifest.
func prepareImageMeta(image *Image) error {
	if err := validateImageTag(image.ImageID.ImageTag); err != nil {
		return err
	}

	mt, size := manifestMediaTypeAndSize(image.ImageManifest)
	if image.ImageManifestMediaType == "" {
		image.ImageManifestMediaType = mt
	}

	if image.ImageSizeInBytes == 0 {
		image.ImageSizeInBytes = size
	}

	return nil
}

// scanOnPushLocked records scan findings for a freshly pushed image when the
// repository's effective scan frequency is not MANUAL. Caller holds b.mu.
func (b *InMemoryBackend) scanOnPushLocked(repo *Repository, img *Image) {
	freq, _ := b.repoEffectiveScanFrequency(repo.RepositoryName, repo.ScanOnPush)
	if freq == "MANUAL" {
		return
	}

	b.imageScanFindings.Put(generateMockScanFindings(
		img.ImageDigest, repo.RepositoryName, b.accountID, img.ImageID, b.effectiveScanTypeLocked(),
	))
}
