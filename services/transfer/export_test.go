package transfer

// ServerCount returns the number of servers stored in the backend.
// This is exported for use in tests only.
func ServerCount(b *InMemoryBackend) int {
	b.mu.RLock("ServerCount")
	defer b.mu.RUnlock()

	return len(b.servers)
}

// UserCount returns the total number of users stored across all servers.
// This is exported for use in tests only.
func UserCount(b *InMemoryBackend) int {
	b.mu.RLock("UserCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.users {
		n += len(m)
	}

	return n
}

// AccessCount returns the total number of access entries across all servers.
// This is exported for use in tests only.
func AccessCount(b *InMemoryBackend) int {
	b.mu.RLock("AccessCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.accesses {
		n += len(m)
	}

	return n
}

// AgreementCount returns the total number of agreements across all servers.
// This is exported for use in tests only.
func AgreementCount(b *InMemoryBackend) int {
	b.mu.RLock("AgreementCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.agreements {
		n += len(m)
	}

	return n
}

// ConnectorCount returns the number of connectors stored in the backend.
// This is exported for use in tests only.
func ConnectorCount(b *InMemoryBackend) int {
	b.mu.RLock("ConnectorCount")
	defer b.mu.RUnlock()

	return len(b.connectors)
}

// ProfileCount returns the number of profiles stored in the backend.
// This is exported for use in tests only.
func ProfileCount(b *InMemoryBackend) int {
	b.mu.RLock("ProfileCount")
	defer b.mu.RUnlock()

	return len(b.profiles)
}

// WebAppCount returns the number of web apps stored in the backend.
// This is exported for use in tests only.
func WebAppCount(b *InMemoryBackend) int {
	b.mu.RLock("WebAppCount")
	defer b.mu.RUnlock()

	return len(b.webApps)
}

// WorkflowCount returns the number of workflows stored in the backend.
// This is exported for use in tests only.
func WorkflowCount(b *InMemoryBackend) int {
	b.mu.RLock("WorkflowCount")
	defer b.mu.RUnlock()

	return len(b.workflows)
}

// CertificateCount returns the number of certificates stored in the backend.
// This is exported for use in tests only.
func CertificateCount(b *InMemoryBackend) int {
	b.mu.RLock("CertificateCount")
	defer b.mu.RUnlock()

	return len(b.certificates)
}

// HostKeyCount returns the total number of host keys stored across all servers.
// This is exported for use in tests only.
func HostKeyCount(b *InMemoryBackend) int {
	b.mu.RLock("HostKeyCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.hostKeys {
		n += len(m)
	}

	return n
}

// SSHPublicKeyCount returns the total number of SSH public keys stored across all users and servers.
// This is exported for use in tests only.
func SSHPublicKeyCount(b *InMemoryBackend) int {
	b.mu.RLock("SSHPublicKeyCount")
	defer b.mu.RUnlock()

	n := 0
	for _, userMap := range b.sshPublicKeys {
		for _, keyMap := range userMap {
			n += len(keyMap)
		}
	}

	return n
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
