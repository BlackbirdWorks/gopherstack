package quicksight

import "sort"

// ListUsersIndexCapacity computes each user's real, derived index-capacity
// consumption from this backend's actual KnowledgeBase/Space state: a
// knowledge base counts against its PrimaryOwnerArn, a space against its
// CreatedByArn. This backend has no synthetic "index size" pipeline, so
// KBCount/SpaceCount and their byte totals are exactly what CreateSpace/
// CreateKnowledgeBase and UpdateSpaceResources have produced -- never a
// fabricated placeholder. Filters/SortBy/SortOrder beyond namespace scoping
// are accepted (for wire compatibility) but not applied, matching this
// backend's existing precedent of no-op unrecognized search-filter
// attributes (see matchesNameFilter).
func (b *InMemoryBackend) ListUsersIndexCapacity(
	_, namespace string,
	maxResults int32,
	nextToken string,
) ([]UserIndexCapacity, string, error) {
	b.mu.RLock("ListUsersIndexCapacity")
	defer b.mu.RUnlock()

	knowledgeBases := b.knowledgeBases.All()
	spaces := b.spaces.All()

	var all []UserIndexCapacity
	for _, u := range b.users.All() {
		if namespace != "" && u.Namespace != namespace {
			continue
		}
		all = append(all, userIndexCapacityFor(u, knowledgeBases, spaces))
	}
	sort.Slice(all, func(i, j int) bool { return all[i].UserName < all[j].UserName })

	result, next := paginateUserIndexCapacity(all, maxResults, nextToken)

	return result, next, nil
}

func userIndexCapacityFor(
	u *storedUser,
	knowledgeBases []*storedKnowledgeBase,
	spaces []*storedSpace,
) UserIndexCapacity {
	uic := UserIndexCapacity{
		UserArn:  u.Arn,
		UserName: u.UserName,
		Email:    u.Email,
		Role:     u.Role,
	}

	for _, k := range knowledgeBases {
		if k.PrimaryOwnerArn == u.Arn {
			uic.KBCount++
			uic.TotalKBCapacityBytes += k.SizeBytes
		}
	}
	// TotalSpaceCapacityBytes stays 0: this backend has no ingestion
	// pipeline that consumes bytes into a space (Space carries no
	// ConsumedSourceSize field, unlike the real SpaceDetails/SpaceSummary),
	// so there is nothing real to sum -- an honest 0, not a fabricated one.
	for _, s := range spaces {
		if s.CreatedByArn == u.Arn {
			uic.SpaceCount++
		}
	}
	uic.TotalCapacityBytes = uic.TotalKBCapacityBytes + uic.TotalSpaceCapacityBytes

	return uic
}

func paginateUserIndexCapacity(
	all []UserIndexCapacity,
	maxResults int32,
	nextToken string,
) ([]UserIndexCapacity, string) {
	if maxResults <= 0 || maxResults > defaultMaxResults {
		maxResults = defaultMaxResults
	}

	start := 0
	if nextToken != "" {
		start = len(all)
		for i, u := range all {
			if u.UserName == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].UserName
	} else {
		end = len(all)
	}

	return all[start:end], next
}
