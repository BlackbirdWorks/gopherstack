package transfer

// ServerCount returns the number of servers stored in the backend.
// This is exported for use in tests only.
func ServerCount(b *InMemoryBackend) int {
	b.mu.RLock("ServerCount")
	defer b.mu.RUnlock()

	return b.servers.Len()
}

// UserCount returns the total number of users stored across all servers.
// This is exported for use in tests only.
func UserCount(b *InMemoryBackend) int {
	b.mu.RLock("UserCount")
	defer b.mu.RUnlock()

	return b.users.Len()
}

// AccessCount returns the total number of access entries across all servers.
// This is exported for use in tests only.
func AccessCount(b *InMemoryBackend) int {
	b.mu.RLock("AccessCount")
	defer b.mu.RUnlock()

	return b.accesses.Len()
}

// AgreementCount returns the total number of agreements across all servers.
// This is exported for use in tests only.
func AgreementCount(b *InMemoryBackend) int {
	b.mu.RLock("AgreementCount")
	defer b.mu.RUnlock()

	return b.agreements.Len()
}

// ConnectorCount returns the number of connectors stored in the backend.
// This is exported for use in tests only.
func ConnectorCount(b *InMemoryBackend) int {
	b.mu.RLock("ConnectorCount")
	defer b.mu.RUnlock()

	return b.connectors.Len()
}

// ProfileCount returns the number of profiles stored in the backend.
// This is exported for use in tests only.
func ProfileCount(b *InMemoryBackend) int {
	b.mu.RLock("ProfileCount")
	defer b.mu.RUnlock()

	return b.profiles.Len()
}

// WebAppCount returns the number of web apps stored in the backend.
// This is exported for use in tests only.
func WebAppCount(b *InMemoryBackend) int {
	b.mu.RLock("WebAppCount")
	defer b.mu.RUnlock()

	return b.webApps.Len()
}

// WorkflowCount returns the number of workflows stored in the backend.
// This is exported for use in tests only.
func WorkflowCount(b *InMemoryBackend) int {
	b.mu.RLock("WorkflowCount")
	defer b.mu.RUnlock()

	return b.workflows.Len()
}

// CertificateCount returns the number of certificates stored in the backend.
// This is exported for use in tests only.
func CertificateCount(b *InMemoryBackend) int {
	b.mu.RLock("CertificateCount")
	defer b.mu.RUnlock()

	return b.certificates.Len()
}

// HostKeyCount returns the total number of host keys stored across all servers.
// This is exported for use in tests only.
func HostKeyCount(b *InMemoryBackend) int {
	b.mu.RLock("HostKeyCount")
	defer b.mu.RUnlock()

	return b.hostKeys.Len()
}

// SSHPublicKeyCount returns the total number of SSH public keys stored across all users and servers.
// This is exported for use in tests only.
func SSHPublicKeyCount(b *InMemoryBackend) int {
	b.mu.RLock("SSHPublicKeyCount")
	defer b.mu.RUnlock()

	return b.sshPublicKeys.Len()
}

// HandlerOpsLen returns the number of operations pre-built in the handler dispatch map.
// This is exported for use in tests only.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}

// SSHKeyBodyIndexCount returns the total number of entries in the sshKeyBodies index.
// This is exported for use in tests only.
func SSHKeyBodyIndexCount(b *InMemoryBackend) int {
	b.mu.RLock("SSHKeyBodyIndexCount")
	defer b.mu.RUnlock()

	n := 0
	for _, userMap := range b.sshKeyBodies {
		for _, bodyMap := range userMap {
			n += len(bodyMap)
		}
	}

	return n
}
