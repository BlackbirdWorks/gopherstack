package managedblockchain

// NetworkCount returns the number of networks stored in b (for test use only).
func NetworkCount(b *InMemoryBackend) int {
	b.mu.RLock("export_test")
	defer b.mu.RUnlock()

	return len(b.networks)
}

// MemberCount returns the total number of members across all networks (for test use only).
func MemberCount(b *InMemoryBackend) int {
	b.mu.RLock("export_test")
	defer b.mu.RUnlock()

	total := 0

	for _, members := range b.members {
		total += len(members)
	}

	return total
}

// NodeCount returns the total number of nodes across all networks and members (for test use only).
func NodeCount(b *InMemoryBackend) int {
	b.mu.RLock("export_test")
	defer b.mu.RUnlock()

	total := 0

	for _, memberMap := range b.nodes {
		for _, nodeMap := range memberMap {
			total += len(nodeMap)
		}
	}

	return total
}

// AccessorCount returns the number of accessors stored in b (for test use only).
func AccessorCount(b *InMemoryBackend) int {
	b.mu.RLock("export_test")
	defer b.mu.RUnlock()

	return len(b.accessors)
}

// ProposalCount returns the total number of proposals across all networks (for test use only).
func ProposalCount(b *InMemoryBackend) int {
	b.mu.RLock("export_test")
	defer b.mu.RUnlock()

	total := 0

	for _, proposals := range b.proposals {
		total += len(proposals)
	}

	return total
}

// InvitationCount returns the number of invitations stored in b (for test use only).
func InvitationCount(b *InMemoryBackend) int {
	b.mu.RLock("export_test")
	defer b.mu.RUnlock()

	return len(b.invitations)
}

// HandlerOpsLen returns the number of supported operations for h (for test use only).
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// ARNIndexSize returns the number of entries in the ARN index (for test use only).
func ARNIndexSize(b *InMemoryBackend) int {
	b.mu.RLock("export_test")
	defer b.mu.RUnlock()

	return len(b.arnToResource)
}
