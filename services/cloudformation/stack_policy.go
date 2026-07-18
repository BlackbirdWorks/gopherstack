package cloudformation

// SetStackPolicy sets the stack policy for the given stack.
func (b *InMemoryBackend) SetStackPolicy(nameOrID, policy string) error {
	b.mu.Lock("SetStackPolicy")
	defer b.mu.Unlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return ErrStackNotFound
	}

	b.stackPolicies[stack.StackID] = policy

	return nil
}

// GetStackPolicy returns the stack policy for the given stack.
// Returns an empty string if no policy has been set.
func (b *InMemoryBackend) GetStackPolicy(nameOrID string) (string, error) {
	b.mu.RLock("GetStackPolicy")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return "", ErrStackNotFound
	}

	return b.stackPolicies[stack.StackID], nil
}
