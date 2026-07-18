package organizations

// ListRoots returns the organization roots.
func (b *InMemoryBackend) ListRoots() ([]*Root, error) {
	b.mu.RLock("ListRoots")
	defer b.mu.RUnlock()

	if b.org == nil {
		return nil, ErrOrgNotFound
	}

	return []*Root{copyRoot(b.root)}, nil
}

// copyRoot returns a value copy of Root, including its PolicyTypes slice.
func copyRoot(r *Root) *Root {
	cp := *r

	if r.PolicyTypes != nil {
		cp.PolicyTypes = make([]PolicyTypeSummary, len(r.PolicyTypes))
		copy(cp.PolicyTypes, r.PolicyTypes)
	}

	return &cp
}
