package ecrpublic

import (
	"fmt"
	"sort"
	"time"
)

// tagsForDigestLocked returns every tag in repositoryName currently bound to
// digest, sorted for stable output. Caller must hold b.mu.
func (b *InMemoryBackend) tagsForDigestLocked(repositoryName, digest string) []string {
	var tags []string

	for tag, binding := range b.tagIndex[repositoryName] {
		if binding.Digest == digest {
			tags = append(tags, tag)
		}
	}

	sort.Strings(tags)

	return tags
}

func toImageDetail(img *Image, tags []string) ImageDetail {
	return ImageDetail{
		ArtifactMediaType:      img.ArtifactMediaType,
		ImageDigest:            img.ImageDigest,
		ImageManifestMediaType: img.ImageManifestMediaType,
		ImagePushedAt:          img.ImagePushedAt,
		ImageSizeInBytes:       img.ImageSizeInBytes,
		ImageTags:              tags,
		RegistryID:             img.RegistryID,
		RepositoryName:         img.RepositoryName,
	}
}

// resolveImageLocked finds an image in repositoryName by digest or tag.
// Caller must hold b.mu.
func (b *InMemoryBackend) resolveImageLocked(repositoryName string, id ImageIdentifier) (*Image, bool) {
	if id.ImageDigest != "" {
		return b.images.Get(imageTableKey(repositoryName, id.ImageDigest))
	}

	if id.ImageTag != "" {
		binding, ok := b.tagIndex[repositoryName][id.ImageTag]
		if !ok {
			return nil, false
		}

		return b.images.Get(imageTableKey(repositoryName, binding.Digest))
	}

	return nil, false
}

// DescribeImages returns image details for a repository, optionally filtered
// by digest or tag.
func (b *InMemoryBackend) DescribeImages(
	registryID, repositoryName string, imageIDs []ImageIdentifier,
) ([]ImageDetail, error) {
	b.mu.RLock("DescribeImages")
	defer b.mu.RUnlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return nil, err
	}

	if !b.repos.Has(repositoryName) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	if len(imageIDs) == 0 {
		imgs := b.imagesByRepo.Get(repositoryName)
		out := make([]ImageDetail, 0, len(imgs))

		for _, img := range imgs {
			out = append(out, toImageDetail(img, b.tagsForDigestLocked(repositoryName, img.ImageDigest)))
		}

		sort.Slice(out, func(i, j int) bool { return out[i].ImageDigest < out[j].ImageDigest })

		return out, nil
	}

	out := make([]ImageDetail, 0, len(imageIDs))

	for _, id := range imageIDs {
		img, ok := b.resolveImageLocked(repositoryName, id)
		if !ok {
			return nil, fmt.Errorf("%w: image not found in %s", ErrImageNotFound, repositoryName)
		}

		out = append(out, toImageDetail(img, b.tagsForDigestLocked(repositoryName, img.ImageDigest)))
	}

	return out, nil
}

// DescribeImageTags returns one ImageTagDetail per tag currently bound in the repository.
func (b *InMemoryBackend) DescribeImageTags(registryID, repositoryName string) ([]ImageTagDetail, error) {
	b.mu.RLock("DescribeImageTags")
	defer b.mu.RUnlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return nil, err
	}

	if !b.repos.Has(repositoryName) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	tagIdx := b.tagIndex[repositoryName]
	out := make([]ImageTagDetail, 0, len(tagIdx))

	for tag, binding := range tagIdx {
		img, ok := b.images.Get(imageTableKey(repositoryName, binding.Digest))
		if !ok {
			continue
		}

		out = append(out, ImageTagDetail{
			ArtifactMediaType:      img.ArtifactMediaType,
			CreatedAt:              binding.CreatedAt,
			ImageDigest:            img.ImageDigest,
			ImageManifestMediaType: img.ImageManifestMediaType,
			ImagePushedAt:          img.ImagePushedAt,
			ImageSizeInBytes:       img.ImageSizeInBytes,
			ImageTag:               tag,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ImageTag < out[j].ImageTag })

	return out, nil
}

// PutImage creates or replaces an image manifest, verifying that every layer
// (and config blob) it references was already uploaded via
// BatchCheckLayerAvailability/CompleteLayerUpload.
func (b *InMemoryBackend) PutImage(registryID, repositoryName string, req PutImageRequest) (*Image, error) {
	b.mu.Lock("PutImage")
	defer b.mu.Unlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return nil, err
	}

	if !b.repos.Has(repositoryName) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	digest, err := resolveImageDigest(req)
	if err != nil {
		return nil, err
	}

	if missing := b.missingLayersLocked(repositoryName, req.ImageManifest); len(missing) > 0 {
		return nil, fmt.Errorf("%w: %v", ErrLayersNotFound, missing)
	}

	if req.ImageTag != "" {
		if existing, ok := b.tagIndex[repositoryName][req.ImageTag]; ok {
			if existing.Digest == digest {
				return nil, fmt.Errorf("%w: tag %s in %s", ErrImageAlreadyExists, req.ImageTag, repositoryName)
			}

			return nil, fmt.Errorf("%w: tag %s in %s", ErrImageTagAlreadyExists, req.ImageTag, repositoryName)
		}
	}

	img := &Image{
		ArtifactMediaType:      "",
		ImageDigest:            digest,
		ImageManifest:          req.ImageManifest,
		ImageManifestMediaType: req.ImageManifestMediaType,
		ImagePushedAt:          time.Now(),
		ImageSizeInBytes:       int64(len(req.ImageManifest)),
		RegistryID:             b.accountID,
		RepositoryName:         repositoryName,
	}

	b.images.Put(img)

	if req.ImageTag != "" {
		if b.tagIndex[repositoryName] == nil {
			b.tagIndex[repositoryName] = make(map[string]tagBinding)
		}

		b.tagIndex[repositoryName][req.ImageTag] = tagBinding{Digest: digest, CreatedAt: time.Now()}
	}

	cp := *img

	return &cp, nil
}

func resolveImageDigest(req PutImageRequest) (string, error) {
	computed := sha256Digest([]byte(req.ImageManifest))

	if req.ImageDigest == "" {
		return computed, nil
	}

	if isFullSHA256Digest(req.ImageDigest) && req.ImageDigest != computed {
		return "", fmt.Errorf("%w: got %s, want %s", ErrImageDigestDoesNotMatch, req.ImageDigest, computed)
	}

	return computed, nil
}

// missingLayersLocked returns every layer/config digest manifest references
// that was never uploaded to repositoryName. Caller must hold b.mu.
func (b *InMemoryBackend) missingLayersLocked(repositoryName, manifest string) []string {
	uploaded := b.uploadedLayers[repositoryName]

	var missing []string

	for _, digest := range referencedDigests(manifest) {
		if _, ok := uploaded[digest]; !ok {
			missing = append(missing, digest)
		}
	}

	return missing
}

// BatchDeleteImage deletes images by digest (removing every tag bound to it)
// or by tag (removing only that binding; the image survives if another tag
// still references it).
func (b *InMemoryBackend) BatchDeleteImage(
	registryID, repositoryName string, imageIDs []ImageIdentifier,
) ([]ImageIdentifier, []ImageFailure, error) {
	b.mu.Lock("BatchDeleteImage")
	defer b.mu.Unlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return nil, nil, err
	}

	if !b.repos.Has(repositoryName) {
		return nil, nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	deleted := make([]ImageIdentifier, 0, len(imageIDs))
	failures := make([]ImageFailure, 0, len(imageIDs))

	for _, id := range imageIDs {
		if ok := b.deleteImageIdentifierLocked(repositoryName, id); ok {
			deleted = append(deleted, id)
		} else {
			failures = append(failures, ImageFailure{
				ImageID:       id,
				FailureCode:   "ImageNotFound",
				FailureReason: "requested image not found",
			})
		}
	}

	return deleted, failures, nil
}

func (b *InMemoryBackend) deleteImageIdentifierLocked(repositoryName string, id ImageIdentifier) bool {
	if id.ImageDigest != "" {
		key := imageTableKey(repositoryName, id.ImageDigest)
		if !b.images.Has(key) {
			return false
		}

		for tag, binding := range b.tagIndex[repositoryName] {
			if binding.Digest == id.ImageDigest {
				delete(b.tagIndex[repositoryName], tag)
			}
		}

		b.images.Delete(key)

		return true
	}

	if id.ImageTag != "" {
		if _, ok := b.tagIndex[repositoryName][id.ImageTag]; !ok {
			return false
		}

		delete(b.tagIndex[repositoryName], id.ImageTag)

		return true
	}

	return false
}
