package ecrpublic

import (
	"fmt"
	"time"
)

const (
	layerUploadPartSize = 10 * 1024 * 1024
	minLayerPartSize    = 5 * 1024 * 1024
	// layerUploadTTL bounds how long an unfinished InitiateLayerUpload
	// session is retained before being pruned as abandoned. AWS does not
	// document an explicit expiry window for unfinished layer uploads; this
	// matches services/ecr's own layerUploadTTL default of 24h.
	layerUploadTTL = 24 * time.Hour
)

// BatchCheckLayerAvailability reports which of the given layer digests have
// already been uploaded (via a completed InitiateLayerUpload session) to repositoryName.
func (b *InMemoryBackend) BatchCheckLayerAvailability(
	registryID, repositoryName string, layerDigests []string,
) ([]LayerInfo, []LayerFailure, error) {
	b.mu.RLock("BatchCheckLayerAvailability")
	defer b.mu.RUnlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return nil, nil, err
	}

	if !b.repos.Has(repositoryName) {
		return nil, nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	uploaded := b.uploadedLayers[repositoryName]

	layers := make([]LayerInfo, 0, len(layerDigests))
	failures := make([]LayerFailure, 0, len(layerDigests))

	for _, digest := range layerDigests {
		if size, ok := uploaded[digest]; ok {
			layers = append(layers, LayerInfo{
				LayerDigest:       digest,
				LayerAvailability: "AVAILABLE",
				LayerSize:         size,
			})

			continue
		}

		failures = append(failures, LayerFailure{
			LayerDigest:   digest,
			FailureCode:   "MissingLayerDigest",
			FailureReason: "the layer digest does not exist in the repository",
		})
	}

	return layers, failures, nil
}

// InitiateLayerUpload starts a new layer upload session for repositoryName.
func (b *InMemoryBackend) InitiateLayerUpload(registryID, repositoryName string) (string, int64, error) {
	b.mu.Lock("InitiateLayerUpload")
	defer b.mu.Unlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return "", 0, err
	}

	if !b.repos.Has(repositoryName) {
		return "", 0, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	b.pruneExpiredLayerUploadsLocked(time.Now())

	b.layerUploadSeq++
	uploadID := fmt.Sprintf("upload-%d-%d", time.Now().UnixNano(), b.layerUploadSeq)
	b.layerUploads[uploadID] = &layerUploadState{RepositoryName: repositoryName, CreatedAt: time.Now()}

	return uploadID, layerUploadPartSize, nil
}

// UploadLayerPart appends a chunk of layer bytes to a live upload session.
// AWS requires each part's first byte to be consecutive to the bytes already
// received; a gap or overlap is rejected with InvalidLayerPartException.
func (b *InMemoryBackend) UploadLayerPart(
	registryID, repositoryName, uploadID string, firstByte, lastByte int64, blob []byte,
) (int64, error) {
	b.mu.Lock("UploadLayerPart")
	defer b.mu.Unlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return 0, err
	}

	if !b.repos.Has(repositoryName) {
		return 0, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	upload, ok := b.layerUploads[uploadID]
	if !ok || upload.RepositoryName != repositoryName {
		return 0, fmt.Errorf("%w: upload %s not found for %s", ErrUploadNotFound, uploadID, repositoryName)
	}

	if firstByte != upload.Size {
		return 0, fmt.Errorf(
			"%w: partFirstByte %d is not consecutive to the %d bytes already received",
			ErrInvalidLayerPart, firstByte, upload.Size,
		)
	}

	upload.Data = append(upload.Data, blob...)
	upload.Size = int64(len(upload.Data))
	upload.PartSizes = append(upload.PartSizes, int64(len(blob)))

	received := lastByte
	if received < 0 && len(blob) > 0 {
		received = upload.Size - 1
	}

	return received, nil
}

// CompleteLayerUpload finalizes an upload session, computing (and, if the
// caller supplied one, verifying) the layer's SHA256 digest.
func (b *InMemoryBackend) CompleteLayerUpload(
	registryID, repositoryName, uploadID string, layerDigests []string,
) (string, error) {
	b.mu.Lock("CompleteLayerUpload")
	defer b.mu.Unlock()

	if err := b.resolveRegistryIDLocked(registryID); err != nil {
		return "", err
	}

	if !b.repos.Has(repositoryName) {
		return "", fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	upload, ok := b.layerUploads[uploadID]
	if !ok || upload.RepositoryName != repositoryName {
		return "", fmt.Errorf("%w: upload %s not found for %s", ErrUploadNotFound, uploadID, repositoryName)
	}

	if len(upload.Data) == 0 {
		return "", fmt.Errorf("%w: upload %s received no layer parts", ErrEmptyUpload, uploadID)
	}

	if err := validatePartSizes(upload.PartSizes); err != nil {
		return "", err
	}

	digest, err := verifiedUploadDigest(upload.Data, layerDigests)
	if err != nil {
		return "", err
	}

	if _, exists := b.uploadedLayers[repositoryName][digest]; exists {
		delete(b.layerUploads, uploadID)

		return "", fmt.Errorf("%w: %s", ErrLayerAlreadyExists, digest)
	}

	if b.uploadedLayers[repositoryName] == nil {
		b.uploadedLayers[repositoryName] = make(map[string]int64)
	}

	b.uploadedLayers[repositoryName][digest] = upload.Size
	delete(b.layerUploads, uploadID)

	return digest, nil
}

// pruneExpiredLayerUploadsLocked removes InitiateLayerUpload sessions older
// than layerUploadTTL, preventing an unbounded leak from pushes that are
// initiated but never completed. Caller must hold b.mu.
func (b *InMemoryBackend) pruneExpiredLayerUploadsLocked(now time.Time) {
	for id, upload := range b.layerUploads {
		if now.Sub(upload.CreatedAt) > layerUploadTTL {
			delete(b.layerUploads, id)
		}
	}
}

// validatePartSizes enforces the 5MiB minimum-part-size rule against every
// part but the last (which cannot be known until CompleteLayerUpload).
func validatePartSizes(sizes []int64) error {
	for _, size := range sizes[:max(0, len(sizes)-1)] {
		if size < minLayerPartSize {
			return fmt.Errorf(
				"%w: layer parts must be at least %d bytes, except for the last part",
				ErrLayerPartTooSmall, minLayerPartSize,
			)
		}
	}

	return nil
}

func verifiedUploadDigest(data []byte, layerDigests []string) (string, error) {
	computed := sha256Digest(data)

	if len(layerDigests) == 0 || layerDigests[0] == "" {
		return computed, nil
	}

	provided := layerDigests[0]
	if isFullSHA256Digest(provided) && provided != computed {
		return "", fmt.Errorf("%w: digest mismatch: got %s, want %s", ErrInvalidLayer, provided, computed)
	}

	return provided, nil
}
