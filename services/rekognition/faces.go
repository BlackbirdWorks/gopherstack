package rekognition

import (
	"slices"

	"github.com/google/uuid"
)

const (
	faceModelVersion = "7.0"

	maxFacesPerPage = 4096

	// SearchFacesByImage synthetic similarity tuning.
	defaultSearchMaxFaces = 5
	minSearchSimilarity   = 75.0 // similarity range floor
	searchSimilaritySpan  = 24   // similarity range width: yields [75.0, 99.0]
	seedStride            = 7    // per-face seed multiplier for score variation

	// Deterministic similarity tuning for face/user matching. Scores are derived
	// from stored face/user state (IDs + ExternalImageId), never canned constants.
	// A perfect identity match (same ExternalImageId) yields 100.0; otherwise the
	// score is spread across [minSearchSimilarity, exactMatchSimilarity).
	exactMatchSimilarity = 100.0
	// Indexed-face confidence is derived per-face from its identity so distinct
	// faces carry distinct (but stable) detection confidence values.
	minFaceConfidence  = 90.0
	faceConfidenceSpan = 1000 // confidence range width in milli-percent: [90.000, 99.999]
	// milliScale expresses scores to 3 decimal places (milli-percent precision)
	// so deterministic hashing spreads values smoothly across the range.
	milliScale = 1000.0
)

// IndexFaces indexes faces into a collection (simulated — no real image processing).
func (b *InMemoryBackend) IndexFaces(collectionID, externalImageID string) ([]*Face, error) {
	b.mu.Lock("IndexFaces")
	defer b.mu.Unlock()

	if !b.collections.Has(collectionID) {
		return nil, ErrCollectionNotFound
	}

	face := &storedFace{
		FaceID:          uuid.NewString(),
		ImageID:         uuid.NewString(),
		ExternalImageID: externalImageID,
		CollectionID:    collectionID,
	}
	// Detection confidence is derived deterministically from the face's own
	// identity so distinct faces carry distinct (but stable) values.
	face.Confidence = faceConfidence(face)
	b.faces.Put(face)

	return []*Face{face.toFace()}, nil
}

// DeleteFaces removes faces from a collection.
func (b *InMemoryBackend) DeleteFaces(collectionID string, faceIDs []string) ([]string, error) {
	b.mu.Lock("DeleteFaces")
	defer b.mu.Unlock()

	if !b.collections.Has(collectionID) {
		return nil, ErrCollectionNotFound
	}

	toDelete := make(map[string]bool, len(faceIDs))
	for _, id := range faceIDs {
		toDelete[id] = true
	}

	var deleted []string

	// Index result slices mutate under Delete, so clone before the delete loop.
	for _, f := range slices.Clone(b.facesByCollection.Get(collectionID)) {
		if toDelete[f.FaceID] {
			b.faces.Delete(f.FaceID)

			deleted = append(deleted, f.FaceID)
		}
	}

	return deleted, nil
}

// ListFaces returns a paginated list of faces in a collection.
func (b *InMemoryBackend) ListFaces(collectionID string, maxResults int32, nextToken string) ([]*Face, string, error) {
	b.mu.RLock("ListFaces")
	defer b.mu.RUnlock()

	if !b.collections.Has(collectionID) {
		return nil, "", ErrCollectionNotFound
	}

	faces := b.facesByCollection.Get(collectionID)

	start := 0
	if nextToken != "" {
		for i, f := range faces {
			if f.FaceID == nextToken {
				start = i

				break
			}
		}
	}

	limit := int32(maxFacesPerPage)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	end := min(start+int(limit), len(faces))

	result := make([]*Face, 0, end-start)
	for _, f := range faces[start:end] {
		result = append(result, f.toFace())
	}

	var outToken string
	if end < len(faces) {
		outToken = faces[end].FaceID
	}

	return result, outToken, nil
}

// SearchFaces searches for faces that match a given face ID.
func (b *InMemoryBackend) SearchFaces(collectionID, faceID string, maxFaces int32) ([]*FaceMatch, error) {
	b.mu.RLock("SearchFaces")
	defer b.mu.RUnlock()

	if !b.collections.Has(collectionID) {
		return nil, ErrCollectionNotFound
	}

	var query *storedFace

	for _, f := range b.facesByCollection.Get(collectionID) {
		if f.FaceID == faceID {
			query = f

			break
		}
	}

	if query == nil {
		return nil, ErrFaceNotFound
	}

	limit := int(maxFaces)
	if limit <= 0 {
		limit = defaultSearchMaxFaces
	}

	var matches []*FaceMatch

	for _, f := range b.facesByCollection.Get(collectionID) {
		if f.FaceID == faceID {
			continue
		}

		// Similarity is derived from the stored query/candidate identities,
		// not a canned constant: same ExternalImageId scores 100.0.
		matches = append(matches, &FaceMatch{
			Similarity: faceSimilarity(query, f),
			Face:       f.toFace(),
		})

		if len(matches) >= limit {
			break
		}
	}

	return matches, nil
}

// SearchFacesByImage searches for faces matching an image (simulated).
// imageKey is a stable string derived from the image reference (S3 path or byte length)
// and is used to vary similarity scores per image rather than returning a fixed value.
func (b *InMemoryBackend) SearchFacesByImage(
	collectionID string,
	maxFaces int32,
	imageKey string,
) ([]*FaceMatch, error) {
	b.mu.RLock("SearchFacesByImage")
	defer b.mu.RUnlock()

	if !b.collections.Has(collectionID) {
		return nil, ErrCollectionNotFound
	}

	limit := int(maxFaces)
	if limit <= 0 {
		limit = defaultSearchMaxFaces
	}

	// Derive a per-image seed from the imageKey so similarity varies by image.
	seed := imageKeySeed(imageKey)
	var matches []*FaceMatch

	for i, f := range b.facesByCollection.Get(collectionID) {
		// Vary similarity in [75.0, 99.0] using image seed and face index.
		similarity := minSearchSimilarity + float64((seed+uint32(i)*seedStride)%searchSimilaritySpan)
		matches = append(matches, &FaceMatch{
			Similarity: similarity,
			Face:       f.toFace(),
		})

		if len(matches) >= limit {
			break
		}
	}

	return matches, nil
}

// imageKeySeed converts an image key string to a uint32 (FNV-1a) for deterministic variation.
func imageKeySeed(key string) uint32 {
	var h uint32 = 2166136261
	for i := range len(key) {
		h ^= uint32(key[i])
		h *= 16777619
	}

	return h
}

// faceConfidence derives a stable detection confidence in
// [minFaceConfidence, 99.999] from a stored face's identity. Distinct faces
// get distinct (but reproducible) confidence values rather than a flat constant.
func faceConfidence(f *storedFace) float64 {
	seed := imageKeySeed(f.FaceID + "|" + f.ExternalImageID)

	return minFaceConfidence + float64(seed%faceConfidenceSpan)/milliScale
}

// faceSimilarity derives a deterministic similarity score for a candidate face
// relative to a query face. Faces sharing a non-empty ExternalImageId are
// treated as the same subject and score exactly exactMatchSimilarity; otherwise
// the score is a stable value in [minSearchSimilarity, exactMatchSimilarity)
// derived from both face identities.
func faceSimilarity(query, candidate *storedFace) float64 {
	if query.ExternalImageID != "" && query.ExternalImageID == candidate.ExternalImageID {
		return exactMatchSimilarity
	}

	seed := imageKeySeed(query.FaceID + "|" + candidate.FaceID)
	span := uint32((exactMatchSimilarity - minSearchSimilarity) * milliScale)

	return minSearchSimilarity + float64(seed%span)/milliScale
}
