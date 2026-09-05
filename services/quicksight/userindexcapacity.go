package quicksight

import (
	"sort"
	"strings"
)

// ListUsersIndexCapacity computes each user's real, derived index-capacity
// consumption from this backend's actual KnowledgeBase/Space state: a
// knowledge base counts against its PrimaryOwnerArn, a space against its
// CreatedByArn. This backend has no synthetic "index size" pipeline, so
// KBCount/SpaceCount and their byte totals are exactly what CreateSpace/
// CreateKnowledgeBase and UpdateSpaceResources have produced -- never a
// fabricated placeholder.
//
// namespace is optional (empty scans every namespace, per this op's own
// handler). storedUser's key is accountID/namespace/UserName, so UserName is
// only unique within one namespace -- across namespaces (or the namespace=""
// scan) two different users can share a UserName. The default sort and
// cursor therefore use UserArn, not UserName: UserArn embeds the namespace
// and so is globally unique, where UserName alone would (a) let
// store.Table.All()'s unordered iteration reorder tied users across calls
// and (b) make paginateUserIndexCapacity's equality-matched nextToken
// resolve to the same (first) tied user forever. query.SortByCapacity
// switches the sort key to TotalCapacityBytes (the only member
// UserIndexCapacitySortBy declares) with UserArn as its tiebreaker for the
// same total-ordering reason.
func (b *InMemoryBackend) ListUsersIndexCapacity(
	_, namespace string,
	query UserIndexCapacityQuery,
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
		uic := userIndexCapacityFor(u, knowledgeBases, spaces)
		if !matchesUserIndexCapacityQuery(uic, query) {
			continue
		}
		all = append(all, uic)
	}

	sort.Slice(all, userIndexCapacityLess(all, query))

	result, next := paginateUserIndexCapacity(all, maxResults, nextToken)

	return result, next, nil
}

// userIndexCapacityLess returns the sort.Slice less-func for all: by
// TotalCapacityBytes (query.SortByCapacity) or by UserName (the default),
// with UserArn as the tiebreaker either way -- see ListUsersIndexCapacity's
// doc comment for why UserArn, not UserName, is what makes the order total.
func userIndexCapacityLess(all []UserIndexCapacity, query UserIndexCapacityQuery) func(i, j int) bool {
	if !query.SortByCapacity {
		return func(i, j int) bool {
			if all[i].UserName != all[j].UserName {
				return all[i].UserName < all[j].UserName
			}

			return all[i].UserArn < all[j].UserArn
		}
	}

	return func(i, j int) bool {
		if all[i].TotalCapacityBytes == all[j].TotalCapacityBytes {
			return all[i].UserArn < all[j].UserArn
		}
		if query.SortDescending {
			return all[i].TotalCapacityBytes > all[j].TotalCapacityBytes
		}

		return all[i].TotalCapacityBytes < all[j].TotalCapacityBytes
	}
}

// matchesUserIndexCapacityQuery applies query's capacity-bytes range
// (CapacityBytesRangeFilter: "MinBytes/MaxBytes... inclusive") and
// username-or-email prefix filter (UserNameOrEmailFilter: "starts-with
// match" against username OR email).
func matchesUserIndexCapacityQuery(uic UserIndexCapacity, query UserIndexCapacityQuery) bool {
	if query.MinCapacityBytes != nil && uic.TotalCapacityBytes < *query.MinCapacityBytes {
		return false
	}
	if query.MaxCapacityBytes != nil && uic.TotalCapacityBytes > *query.MaxCapacityBytes {
		return false
	}
	if query.Prefix != nil {
		p := *query.Prefix
		if !strings.HasPrefix(uic.UserName, p) && !strings.HasPrefix(uic.Email, p) {
			return false
		}
	}

	return true
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
			if u.UserArn == nextToken {
				start = i

				break
			}
		}
	}

	end := start + int(maxResults)
	var next string
	if end < len(all) {
		next = all[end].UserArn
	} else {
		end = len(all)
	}

	return all[start:end], next
}
