package iam

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// CreateInstanceProfile creates a new IAM instance profile.
func (b *InMemoryBackend) CreateInstanceProfile(name, path string) (*InstanceProfile, error) {
	b.mu.Lock("CreateInstanceProfile")
	defer b.mu.Unlock()

	if _, exists := b.instanceProfiles.Get(name); exists {
		return nil, fmt.Errorf(
			"%w: instance profile %q already exists",
			ErrInstanceProfileAlreadyExists,
			name,
		)
	}

	p := normPath(path)
	ip := InstanceProfile{
		InstanceProfileName: name,
		InstanceProfileID:   newID("AIPA"),
		Arn:                 arn.Build("iam", "", b.accountID, "instance-profile"+p+name),
		Path:                p,
		Roles:               []string{},
		CreateDate:          time.Now().UTC(),
	}
	b.instanceProfiles.Put(&ip)
	b.sortedIPNames = insertSorted(b.sortedIPNames, name)

	return &ip, nil
}

// DeleteInstanceProfile deletes an IAM instance profile by name.
//
// Matching real AWS: "The instance profile must not have an associated
// role" — rejected with DeleteConflictException otherwise; the caller must
// call RemoveRoleFromInstanceProfile first. See
// https://docs.aws.amazon.com/IAM/latest/APIReference/API_DeleteInstanceProfile.html.
func (b *InMemoryBackend) DeleteInstanceProfile(name string) error {
	b.mu.Lock("DeleteInstanceProfile")
	defer b.mu.Unlock()

	ip, exists := b.instanceProfiles.Get(name)
	if !exists {
		return fmt.Errorf("%w: instance profile %q not found", ErrInstanceProfileNotFound, name)
	}

	if len(ip.Roles) > 0 {
		return fmt.Errorf(
			"%w: instance profile %q has an associated role",
			ErrDeleteConflict, name,
		)
	}

	b.instanceProfiles.Delete(name)
	b.sortedIPNames = deleteSorted(b.sortedIPNames, name)

	return nil
}

// ListInstanceProfiles returns a paginated list of IAM instance profiles sorted by name.
func (b *InMemoryBackend) ListInstanceProfiles(
	marker string,
	maxItems int,
) (page.Page[InstanceProfile], error) {
	b.mu.RLock("ListInstanceProfiles")
	defer b.mu.RUnlock()

	return pageFromSortedNames(
		b.sortedIPNames,
		b.instanceProfiles.Get,
		marker,
		maxItems,
		iamDefaultMaxItems,
	), nil
}

// AddRoleToInstanceProfile adds a role to an IAM instance profile.
func (b *InMemoryBackend) AddRoleToInstanceProfile(instanceProfileName, roleName string) error {
	b.mu.Lock("AddRoleToInstanceProfile")
	defer b.mu.Unlock()

	ip, exists := b.instanceProfiles.Get(instanceProfileName)
	if !exists {
		return fmt.Errorf(
			"%w: instance profile %q not found",
			ErrInstanceProfileNotFound,
			instanceProfileName,
		)
	}

	if _, roleExists := b.roles.Get(roleName); !roleExists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	if slices.Contains(ip.Roles, roleName) {
		return nil // already attached (idempotent)
	}

	// AWS instance profiles can contain exactly one role.
	if len(ip.Roles) >= 1 {
		return fmt.Errorf(
			"%w: instance profile %q already has a role; an instance profile can contain only one role",
			ErrLimitExceeded,
			instanceProfileName,
		)
	}

	ip.Roles = append(ip.Roles, roleName)
	b.instanceProfiles.Put(ip)

	return nil
}

// RemoveRoleFromInstanceProfile removes a role from an IAM instance profile.
func (b *InMemoryBackend) RemoveRoleFromInstanceProfile(
	instanceProfileName, roleName string,
) error {
	b.mu.Lock("RemoveRoleFromInstanceProfile")
	defer b.mu.Unlock()

	ip, exists := b.instanceProfiles.Get(instanceProfileName)
	if !exists {
		return fmt.Errorf(
			"%w: instance profile %q not found",
			ErrInstanceProfileNotFound,
			instanceProfileName,
		)
	}

	for i, r := range ip.Roles {
		if r == roleName {
			ip.Roles = append(ip.Roles[:i], ip.Roles[i+1:]...)
			b.instanceProfiles.Put(ip)

			return nil
		}
	}

	return nil
}

// ListAllInstanceProfiles returns all instance profiles (for dashboard).
func (b *InMemoryBackend) ListAllInstanceProfiles() []InstanceProfile {
	b.mu.RLock("ListAllInstanceProfiles")
	defer b.mu.RUnlock()

	profiles := make([]InstanceProfile, 0, b.instanceProfiles.Len())
	for _, ip := range b.instanceProfiles.All() {
		profiles = append(profiles, *ip)
	}

	sort.Slice(profiles, func(i, j int) bool {
		return profiles[i].InstanceProfileName < profiles[j].InstanceProfileName
	})

	return profiles
}

// purgeInstanceProfilesLocked removes instance profiles created before cutoff.
// Caller must hold b.mu.
func (b *InMemoryBackend) purgeInstanceProfilesLocked(cutoff time.Time) {
	for _, ip := range b.instanceProfiles.All() {
		if ip.CreateDate.Before(cutoff) {
			b.instanceProfiles.Delete(ip.InstanceProfileName)
		}
	}
}

// GetInstanceProfile retrieves a single IAM instance profile by name.
func (b *InMemoryBackend) GetInstanceProfile(name string) (*InstanceProfile, error) {
	b.mu.RLock("GetInstanceProfile")
	defer b.mu.RUnlock()

	ip, exists := b.instanceProfiles.Get(name)
	if !exists {
		return nil, fmt.Errorf("%w: instance profile %q not found", ErrInstanceProfileNotFound, name)
	}

	return ip, nil
}

// ListInstanceProfilesForRole returns all instance profiles that contain the specified role.
func (b *InMemoryBackend) ListInstanceProfilesForRole(roleName string) ([]InstanceProfile, error) {
	b.mu.RLock("ListInstanceProfilesForRole")
	defer b.mu.RUnlock()

	if _, exists := b.roles.Get(roleName); !exists {
		return nil, fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	result := make([]InstanceProfile, 0, b.instanceProfiles.Len())
	for _, ip := range b.instanceProfiles.All() {
		if slices.Contains(ip.Roles, roleName) {
			result = append(result, *ip)
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].InstanceProfileName < result[j].InstanceProfileName
	})

	return result, nil
}
