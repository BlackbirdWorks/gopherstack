package ssoadmin

// PutInlinePolicyToPermissionSet sets the inline policy on a permission set.
func (b *InMemoryBackend) PutInlinePolicyToPermissionSet(instanceArn, permissionSetArn, inlinePolicy string) error {
	b.mu.Lock("PutInlinePolicyToPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	ps.InlinePolicy = inlinePolicy

	return nil
}

// GetInlinePolicyForPermissionSet returns the inline policy for a permission set.
func (b *InMemoryBackend) GetInlinePolicyForPermissionSet(instanceArn, permissionSetArn string) (string, error) {
	b.mu.RLock("GetInlinePolicyForPermissionSet")
	defer b.mu.RUnlock()

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return "", ErrPermissionSetNotFound
	}

	return ps.InlinePolicy, nil
}

// DeleteInlinePolicyFromPermissionSet removes the inline policy from a permission set.
func (b *InMemoryBackend) DeleteInlinePolicyFromPermissionSet(instanceArn, permissionSetArn string) error {
	b.mu.Lock("DeleteInlinePolicyFromPermissionSet")
	defer b.mu.Unlock()

	ps, ok := b.permissionSets.Get(permissionSetArn)
	if !ok || ps.InstanceArn != instanceArn {
		return ErrPermissionSetNotFound
	}
	ps.InlinePolicy = ""

	return nil
}
