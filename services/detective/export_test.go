package detective

import "time"

// SeedGraph inserts a synthetic graph ARN directly into the backend, bypassing the
// one-graph-per-account constraint of CreateGraph. Used only in tests that need to
// exercise multi-graph pagination.
func SeedGraph(b *InMemoryBackend, arnStr string) {
	b.mu.Lock("SeedGraph")
	defer b.mu.Unlock()
	b.graphs.Put(&storedGraph{Arn: arnStr, CreatedTime: time.Now().UTC()})
}

// GraphCount returns the number of stored behavior graphs.
func GraphCount(b *InMemoryBackend) int {
	b.mu.RLock("GraphCount")
	defer b.mu.RUnlock()

	return b.graphs.Len()
}

// MemberCount returns the number of members in a graph.
func MemberCount(b *InMemoryBackend, graphARN string) int {
	b.mu.RLock("MemberCount")
	defer b.mu.RUnlock()

	return len(b.membersByGraph.Get(graphARN))
}

// SeedMember inserts a synthetic member record directly into the backend,
// bypassing CreateMembers (which forbids the backend's own account from
// inviting itself). Used only in tests that need ListInvitations to return
// multiple entries for pagination coverage, since ListInvitations returns
// graphs where b.accountID itself holds a membership -- something the normal
// CreateMembers/AcceptInvitation flow can never produce for the account that
// owns the backend instance under test.
func SeedMember(b *InMemoryBackend, graphARN, accountID, status string) {
	b.mu.Lock("SeedMember")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	b.members.Put(&storedMember{
		InvitedTime:     now,
		UpdatedTime:     now,
		AccountID:       accountID,
		AdministratorID: "999999999999",
		EmailAddress:    "seed@example.com",
		GraphARN:        graphARN,
		Status:          status,
	})
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
