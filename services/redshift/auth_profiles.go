package redshift

import "fmt"

// CreateAuthenticationProfile creates a new authentication profile.
func (b *InMemoryBackend) CreateAuthenticationProfile(name, content string) (*AuthenticationProfile, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: AuthenticationProfileName is required", ErrInvalidParameter)
	}

	b.mu.Lock("CreateAuthenticationProfile")
	defer b.mu.Unlock()

	if _, exists := b.authProfiles.Get(name); exists {
		return nil, fmt.Errorf("%w: profile %s already exists", ErrAuthProfileAlreadyExists, name)
	}

	ap := &AuthenticationProfile{
		AuthenticationProfileName:    name,
		AuthenticationProfileContent: content,
	}
	b.authProfiles.Put(ap)

	cp := *ap

	return &cp, nil
}

// DeleteAuthenticationProfile deletes the named authentication profile.
func (b *InMemoryBackend) DeleteAuthenticationProfile(name string) error {
	if name == "" {
		return fmt.Errorf("%w: AuthenticationProfileName is required", ErrInvalidParameter)
	}

	b.mu.Lock("DeleteAuthenticationProfile")
	defer b.mu.Unlock()

	if _, exists := b.authProfiles.Get(name); !exists {
		return fmt.Errorf("%w: profile %s not found", ErrAuthProfileNotFound, name)
	}

	b.authProfiles.Delete(name)

	return nil
}

// DescribeAuthenticationProfiles returns authentication profiles, optionally filtered by name.
func (b *InMemoryBackend) DescribeAuthenticationProfiles(name string) ([]AuthenticationProfile, error) {
	b.mu.RLock("DescribeAuthenticationProfiles")
	defer b.mu.RUnlock()

	if name != "" {
		ap, exists := b.authProfiles.Get(name)
		if !exists {
			return nil, fmt.Errorf("%w: profile %s not found", ErrAuthProfileNotFound, name)
		}

		cp := *ap

		return []AuthenticationProfile{cp}, nil
	}

	result := make([]AuthenticationProfile, 0, b.authProfiles.Len())

	for _, ap := range b.authProfiles.All() {
		cp := *ap
		result = append(result, cp)
	}

	return result, nil
}

// ModifyAuthenticationProfile updates the content of an authentication profile.
func (b *InMemoryBackend) ModifyAuthenticationProfile(name, content string) (*AuthenticationProfile, error) {
	if name == "" {
		return nil, fmt.Errorf("%w: AuthenticationProfileName is required", ErrInvalidParameter)
	}

	b.mu.Lock("ModifyAuthenticationProfile")
	defer b.mu.Unlock()

	ap, exists := b.authProfiles.Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: profile %s not found", ErrAuthProfileNotFound, name)
	}

	ap.AuthenticationProfileContent = content
	cp := *ap

	return &cp, nil
}
