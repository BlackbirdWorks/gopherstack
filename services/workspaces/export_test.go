package workspaces

// WorkspaceCount returns the number of stored workspaces.
func WorkspaceCount(b *InMemoryBackend) int {
	b.mu.RLock("WorkspaceCount")
	defer b.mu.RUnlock()

	return b.workspaces.Len()
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// WorkspaceState returns the state of a workspace by ID.
func WorkspaceState(b *InMemoryBackend, id string) string {
	b.mu.RLock("WorkspaceState")
	defer b.mu.RUnlock()

	w, ok := b.workspaces.Get(id)
	if !ok {
		return ""
	}

	return w.State
}

// WorkspaceProps returns a copy of the properties stored for a workspace.
func WorkspaceProps(b *InMemoryBackend, id string) *WorkspaceProperties {
	b.mu.RLock("WorkspaceProps")
	defer b.mu.RUnlock()

	w, ok := b.workspaces.Get(id)
	if !ok || w.Properties == nil {
		return nil
	}

	p := *w.Properties

	return &p
}
