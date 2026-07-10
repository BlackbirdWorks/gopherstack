package shield

// ProtectionCount returns the number of stored protections.
func ProtectionCount(b *InMemoryBackend) int {
	b.mu.RLock("ProtectionCount")
	defer b.mu.RUnlock()

	return b.protections.Len()
}

// ProtectionGroupCount returns the number of stored protection groups.
func ProtectionGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("ProtectionGroupCount")
	defer b.mu.RUnlock()

	return b.protectionGroups.Len()
}

// AttackCount returns the number of stored attacks.
func AttackCount(b *InMemoryBackend) int {
	b.mu.RLock("AttackCount")
	defer b.mu.RUnlock()

	return b.attacks.Len()
}

// EmergencyContactCount returns the number of stored emergency contacts.
func EmergencyContactCount(b *InMemoryBackend) int {
	b.mu.RLock("EmergencyContactCount")
	defer b.mu.RUnlock()

	return len(b.emergencyContacts)
}

// GetProactiveEngagementStatus returns the current proactive engagement status.
func GetProactiveEngagementStatus(b *InMemoryBackend) string {
	b.mu.RLock("GetProactiveEngagementStatus")
	defer b.mu.RUnlock()

	return b.proactiveEngagementStatus
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
