package ecr

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// BatchCheckLayerAvailability checks the availability of image layers in a repository.
func (b *InMemoryBackend) BatchCheckLayerAvailability(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
	layerDigests []string,
) ([]LayerAvailability, []LayerFailure, error) {
	b.mu.RLock("BatchCheckLayerAvailability")
	defer b.mu.RUnlock()

	if !b.repos.Has(repositoryName) {
		return nil, nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	layers := make([]LayerAvailability, 0, len(layerDigests))
	failures := make([]LayerFailure, 0, len(layerDigests))

	repoLayers := b.uploadedLayers[repositoryName]
	for _, digest := range layerDigests {
		if size, ok := repoLayers[digest]; ok {
			layers = append(layers, LayerAvailability{
				LayerDigest:       digest,
				LayerAvailability: "AVAILABLE",
				LayerSize:         size,
			})
		} else {
			failures = append(failures, LayerFailure{
				LayerDigest:   digest,
				FailureCode:   "MissingLayerDigest",
				FailureReason: "the layer digest does not exist in the repository",
			})
		}
	}

	return layers, failures, nil
}

// CompleteLayerUpload finalises the upload of an image layer.
// If an upload session exists, it computes the SHA256 of accumulated bytes and verifies the digest.
// If no session exists (direct digest path), the provided digest is trusted as-is.
func (b *InMemoryBackend) CompleteLayerUpload(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName, uploadID string,
	layerDigests []string,
) (*CompleteLayerUploadResult, error) {
	b.mu.Lock("CompleteLayerUpload")
	defer b.mu.Unlock()

	if !b.repos.Has(repositoryName) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	digest, size, err := b.resolveCompletedLayerLocked(repositoryName, uploadID, layerDigests)
	if err != nil {
		return nil, err
	}

	// AWS rejects re-completing a layer digest that has already been registered
	// as available in the repository with LayerAlreadyExistsException. Only
	// applies when a digest was actually resolved; an empty digest is not a
	// real layer identity and must not collide with itself across sessions.
	if digest != "" {
		if _, exists := b.uploadedLayers[repositoryName][digest]; exists {
			return nil, fmt.Errorf("%w: %s", ErrLayerAlreadyExists, digest)
		}
	}

	if b.uploadedLayers[repositoryName] == nil {
		b.uploadedLayers[repositoryName] = make(map[string]int64)
	}

	b.uploadedLayers[repositoryName][digest] = size

	return &CompleteLayerUploadResult{
		LayerDigest:    digest,
		RepositoryName: repositoryName,
		RegistryID:     b.accountID,
		UploadID:       uploadID,
	}, nil
}

// resolveCompletedLayerLocked determines the final layer digest and size for a
// CompleteLayerUpload call and retires the upload session (if one exists).
// Caller must hold the write lock.
func (b *InMemoryBackend) resolveCompletedLayerLocked(
	repositoryName, uploadID string,
	layerDigests []string,
) (string, int64, error) {
	var digest string
	var size int64

	upload, ok := b.layerUploads[uploadID]

	switch {
	case ok && upload.RepositoryName == repositoryName && len(upload.Data) > 0:
		verified, err := verifiedUploadDigestLocked(upload, layerDigests)
		if err != nil {
			return "", 0, err
		}

		digest = verified
		size = upload.Size
		b.retireLayerUploadLocked(repositoryName, uploadID)

	case ok && upload.RepositoryName == repositoryName:
		if len(layerDigests) > 0 {
			digest = layerDigests[0]
		}

		size = upload.Size
		b.retireLayerUploadLocked(repositoryName, uploadID)

	case len(layerDigests) > 0:
		// Direct digest path: no prior InitiateLayerUpload.
		digest = layerDigests[0]
	}

	return digest, size, nil
}

// verifiedUploadDigestLocked computes the SHA256 of the accumulated upload
// bytes and, when the caller provided a full SHA256 digest, verifies it
// matches before returning the digest to record.
func verifiedUploadDigestLocked(upload *layerUploadState, layerDigests []string) (string, error) {
	computed := "sha256:" + hex.EncodeToString(sha256Sum(upload.Data))

	provided := ""
	if len(layerDigests) > 0 {
		provided = layerDigests[0]
	}

	if provided == "" {
		return computed, nil
	}

	// Only enforce digest verification for full 64-char SHA256 digests.
	if isFullSHA256Digest(provided) && provided != computed {
		return "", fmt.Errorf("%w: digest mismatch: got %s, want %s",
			ErrLayerDigestMismatch, provided, computed)
	}

	return provided, nil
}

// retireLayerUploadLocked removes an upload session and its per-repository
// index entry once it has been finalised by CompleteLayerUpload.
// Caller must hold the write lock.
func (b *InMemoryBackend) retireLayerUploadLocked(repositoryName, uploadID string) {
	delete(b.layerUploads, uploadID)
	if idx, ok := b.repoUploadIndex[repositoryName]; ok {
		delete(idx, uploadID)
	}
}

// sha256Sum returns the SHA256 hash of data.
func sha256Sum(data []byte) []byte {
	h := sha256.New()
	h.Write(data)

	return h.Sum(nil)
}

// isFullSHA256Digest returns true when s is a properly-formed "sha256:<64 hex>" digest.
func isFullSHA256Digest(s string) bool {
	const prefix = "sha256:"
	if len(s) != len(prefix)+64 {
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

// GetDownloadURLForLayer resolves a local download URL for an uploaded layer.
func (b *InMemoryBackend) GetDownloadURLForLayer(
	ctx context.Context,
	repositoryName, layerDigest string,
) (string, error) {
	b.mu.RLock("GetDownloadURLForLayer")
	defer b.mu.RUnlock()

	if !b.repos.Has(repositoryName) {
		return "", fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	if _, ok := b.uploadedLayers[repositoryName][layerDigest]; !ok {
		return "", fmt.Errorf("%w: %s", ErrLayerInaccessible, layerDigest)
	}

	endpoint := b.endpoint
	if endpoint == "" {
		endpoint = fmt.Sprintf("%s.dkr.ecr.%s.amazonaws.com", b.accountID, b.regionFor(ctx))
	}

	return fmt.Sprintf("http://%s/v2/%s/blobs/%s", endpoint, repositoryName, layerDigest), nil
}

// InitiateLayerUpload starts a layer upload session.
func (b *InMemoryBackend) InitiateLayerUpload(
	ctx context.Context, //nolint:revive // existing issue.
	repositoryName string,
) (*LayerUploadInitiation, error) {
	b.mu.Lock("InitiateLayerUpload")
	defer b.mu.Unlock()

	if !b.repos.Has(repositoryName) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	now := time.Now()

	// Prune abandoned uploads using the FIFO queue: scan from the front and stop
	// at the first entry that is still within TTL (or has been refreshed by a
	// recent UploadLayerPart). Completed uploads may remain in the queue; skip
	// them when they are no longer present in layerUploads.
	for len(b.layerUploadQueue) > 0 {
		entry := b.layerUploadQueue[0]
		upload, exists := b.layerUploads[entry.id]
		if !exists {
			b.layerUploadQueue = b.layerUploadQueue[1:]

			continue
		}
		if now.Sub(upload.CreatedAt) <= layerUploadTTL {
			break
		}
		delete(b.layerUploads, entry.id)
		if idx, ok := b.repoUploadIndex[upload.RepositoryName]; ok {
			delete(idx, entry.id)
		}
		b.layerUploadQueue = b.layerUploadQueue[1:]
	}

	uploadID := fmt.Sprintf("upload-%d", now.UnixNano())
	b.layerUploads[uploadID] = &layerUploadState{RepositoryName: repositoryName, CreatedAt: now}
	if b.repoUploadIndex[repositoryName] == nil {
		b.repoUploadIndex[repositoryName] = make(map[string]struct{})
	}
	b.repoUploadIndex[repositoryName][uploadID] = struct{}{}
	b.layerUploadQueue = append(b.layerUploadQueue, layerUploadQueueEntry{id: uploadID})

	return &LayerUploadInitiation{PartSize: layerUploadPartSize, UploadID: uploadID}, nil
}

// UploadLayerPart records uploaded bytes for an existing upload session.
// AWS requires each part's first byte to be consecutive to the last byte
// received by the previous part (i.e. equal to the number of bytes already
// buffered for this session); a gap or overlap is rejected with
// InvalidLayerPartException.
func (b *InMemoryBackend) UploadLayerPart(ctx context.Context, //nolint:revive // existing issue.
	repositoryName, uploadID string,
	firstByte, lastByte int64,
	blob []byte,
) (*LayerUploadPartResult, error) {
	b.mu.Lock("UploadLayerPart")
	defer b.mu.Unlock()

	if !b.repos.Has(repositoryName) {
		return nil, fmt.Errorf("%w: %s", ErrRepositoryNotFound, repositoryName)
	}

	upload, ok := b.layerUploads[uploadID]
	if !ok || upload.RepositoryName != repositoryName {
		return nil, fmt.Errorf("%w: upload not found", ErrRepositoryNotFound)
	}

	if firstByte >= 0 && firstByte != upload.Size {
		return nil, fmt.Errorf(
			"%w: partFirstByte %d is not consecutive to the %d bytes already received",
			ErrInvalidLayerPart, firstByte, upload.Size,
		)
	}

	upload.Data = append(upload.Data, blob...)
	upload.Size = int64(len(upload.Data))
	// Refresh the activity timestamp so an in-progress multi-part upload is not
	// pruned as abandoned while parts are still arriving.
	upload.CreatedAt = time.Now()

	if lastByte < 0 && len(blob) > 0 {
		lastByte = upload.Size - 1
	}

	return &LayerUploadPartResult{
		LastByteReceived: lastByte,
		RepositoryName:   repositoryName,
		RegistryID:       b.accountID,
		UploadID:         uploadID,
	}, nil
}
