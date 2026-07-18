package rekognition

import (
	"slices"
	"strings"
)

// =============================================================================
// Users
// =============================================================================

// CreateUser creates a user in a collection.
func (b *InMemoryBackend) CreateUser(collectionID, userID string) error {
	b.mu.Lock("CreateUser")
	defer b.mu.Unlock()

	if !b.collections.Has(collectionID) {
		return ErrCollectionNotFound
	}

	key := userKey(collectionID, userID)
	if b.users.Has(key) {
		return ErrUserAlreadyExists
	}

	b.users.Put(&storedUser{
		CollectionID: collectionID,
		UserID:       userID,
		UserStatus:   "ACTIVE",
		FaceIDs:      []string{},
	})

	return nil
}

// DeleteUser removes a user from a collection.
func (b *InMemoryBackend) DeleteUser(collectionID, userID string) error {
	b.mu.Lock("DeleteUser")
	defer b.mu.Unlock()

	if !b.collections.Has(collectionID) {
		return ErrCollectionNotFound
	}

	key := userKey(collectionID, userID)
	if !b.users.Has(key) {
		return ErrUserNotFound
	}

	b.users.Delete(key)

	return nil
}

// ListUsers returns a paginated list of users in a collection.
func (b *InMemoryBackend) ListUsers(
	collectionID string, maxResults int32, nextToken string,
) ([]*User, string, error) {
	b.mu.RLock("ListUsers")
	defer b.mu.RUnlock()

	if !b.collections.Has(collectionID) {
		return nil, "", ErrCollectionNotFound
	}

	// Index result slices are insertion-ordered, not sorted by UserID --
	// clone (per the Index.Get contract) and sort to match the original
	// nested-map's collections.SortedKeys(userID) pagination order.
	group := slices.Clone(b.usersByCollection.Get(collectionID))
	slices.SortFunc(group, func(a, c *storedUser) int { return strings.Compare(a.UserID, c.UserID) })

	start := 0
	if nextToken != "" {
		for i, u := range group {
			if u.UserID == nextToken {
				start = i

				break
			}
		}
	}

	const maxPerPage = 4096
	limit := int32(maxPerPage)
	if maxResults > 0 && maxResults < limit {
		limit = maxResults
	}

	end := min(start+int(limit), len(group))

	result := make([]*User, 0, end-start)
	for _, u := range group[start:end] {
		result = append(result, u.toUser())
	}

	var outToken string
	if end < len(group) {
		outToken = group[end].UserID
	}

	return result, outToken, nil
}

// AssociateFaces associates faces with a user.
func (b *InMemoryBackend) AssociateFaces(
	collectionID, userID string, faceIDs []string,
) ([]*AssociatedFace, []*UnsuccessfulFaceAssociation, error) {
	b.mu.Lock("AssociateFaces")
	defer b.mu.Unlock()

	if !b.collections.Has(collectionID) {
		return nil, nil, ErrCollectionNotFound
	}

	user, exists := b.users.Get(userKey(collectionID, userID))
	if !exists {
		return nil, nil, ErrUserNotFound
	}

	// Build a set of known face IDs in this collection.
	knownFaces := make(map[string]bool)
	for _, f := range b.facesByCollection.Get(collectionID) {
		knownFaces[f.FaceID] = true
	}

	var associated []*AssociatedFace
	var unsuccessful []*UnsuccessfulFaceAssociation

	for _, faceID := range faceIDs {
		if knownFaces[faceID] {
			user.FaceIDs = append(user.FaceIDs, faceID)
			associated = append(associated, &AssociatedFace{FaceID: faceID})
		} else {
			unsuccessful = append(unsuccessful, &UnsuccessfulFaceAssociation{
				FaceID:  faceID,
				Reasons: []string{"FACE_NOT_FOUND"},
			})
		}
	}

	return associated, unsuccessful, nil
}

// DisassociateFaces removes faces from a user.
func (b *InMemoryBackend) DisassociateFaces(
	collectionID, userID string, faceIDs []string,
) ([]*DisassociatedFace, []*UnsuccessfulFaceDisassociation, error) {
	b.mu.Lock("DisassociateFaces")
	defer b.mu.Unlock()

	if !b.collections.Has(collectionID) {
		return nil, nil, ErrCollectionNotFound
	}

	user, exists := b.users.Get(userKey(collectionID, userID))
	if !exists {
		return nil, nil, ErrUserNotFound
	}

	// Build a set of faces associated with this user.
	associated := make(map[string]bool, len(user.FaceIDs))
	for _, id := range user.FaceIDs {
		associated[id] = true
	}

	toRemove := make(map[string]bool, len(faceIDs))
	var disassociated []*DisassociatedFace
	var unsuccessful []*UnsuccessfulFaceDisassociation

	for _, faceID := range faceIDs {
		if associated[faceID] {
			toRemove[faceID] = true
			disassociated = append(disassociated, &DisassociatedFace{FaceID: faceID})
		} else {
			unsuccessful = append(unsuccessful, &UnsuccessfulFaceDisassociation{
				FaceID:  faceID,
				Reasons: []string{"FACE_NOT_FOUND"},
			})
		}
	}

	remaining := user.FaceIDs[:0]
	for _, id := range user.FaceIDs {
		if !toRemove[id] {
			remaining = append(remaining, id)
		}
	}
	user.FaceIDs = remaining

	return disassociated, unsuccessful, nil
}

// userSimilarity derives a deterministic similarity score for a candidate user
// relative to a query identity (a user ID or image key), in
// [minSearchSimilarity, exactMatchSimilarity).
func userSimilarity(queryKey string, candidate *storedUser) float64 {
	seed := imageKeySeed(queryKey + "|" + candidate.UserID)
	span := uint32((exactMatchSimilarity - minSearchSimilarity) * milliScale)

	return minSearchSimilarity + float64(seed%span)/milliScale
}

// SearchUsers returns up to maxUsers users with a simulated similarity score.
func (b *InMemoryBackend) SearchUsers(collectionID, userID string, maxUsers int32) ([]*UserMatch, error) {
	b.mu.RLock("SearchUsers")
	defer b.mu.RUnlock()

	if !b.collections.Has(collectionID) {
		return nil, ErrCollectionNotFound
	}

	group := b.usersByCollection.Get(collectionID)
	if len(group) == 0 {
		return []*UserMatch{}, nil
	}

	limit := int(maxUsers)
	if limit <= 0 {
		limit = 5
	}

	var matches []*UserMatch
	for _, u := range group {
		if u.UserID == userID {
			continue
		}

		matches = append(matches, &UserMatch{
			User:       u.toUser(),
			Similarity: userSimilarity(userID, u),
		})

		if len(matches) >= limit {
			break
		}
	}

	return matches, nil
}

// SearchUsersByImage returns up to maxUsers users with a deterministic similarity
// score derived from the image reference and each candidate user's identity.
func (b *InMemoryBackend) SearchUsersByImage(
	collectionID string,
	maxUsers int32,
	imageKey string,
) ([]*UserMatch, error) {
	b.mu.RLock("SearchUsersByImage")
	defer b.mu.RUnlock()

	if !b.collections.Has(collectionID) {
		return nil, ErrCollectionNotFound
	}

	group := b.usersByCollection.Get(collectionID)
	if len(group) == 0 {
		return []*UserMatch{}, nil
	}

	limit := int(maxUsers)
	if limit <= 0 {
		limit = 5
	}

	var matches []*UserMatch
	for _, u := range group {
		matches = append(matches, &UserMatch{
			User:       u.toUser(),
			Similarity: userSimilarity(imageKey, u),
		})

		if len(matches) >= limit {
			break
		}
	}

	return matches, nil
}
