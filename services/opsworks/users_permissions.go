package opsworks

import (
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// CreateUserProfile creates an OpsWorks IAM user profile.
func (b *InMemoryBackend) CreateUserProfile(
	iamUserArn, sshUsername, sshPublicKey string,
	allowSelfManagement bool,
) (*UserProfile, error) {
	if iamUserArn == "" {
		return nil, ErrValidation
	}

	b.mu.Lock("CreateUserProfile")
	defer b.mu.Unlock()

	name := iamUserArn
	if idx := strings.LastIndex(iamUserArn, "/"); idx >= 0 {
		name = iamUserArn[idx+1:]
	}

	u := &storedUserProfile{
		IamUserArn:          iamUserArn,
		Name:                name,
		SSHUsername:         sshUsername,
		SSHPublicKey:        sshPublicKey,
		AllowSelfManagement: allowSelfManagement,
	}
	b.userProfiles.Put(u)

	return u.toUserProfile(), nil
}

// DeleteUserProfile removes a user profile.
func (b *InMemoryBackend) DeleteUserProfile(iamUserArn string) error {
	b.mu.Lock("DeleteUserProfile")
	defer b.mu.Unlock()

	if !b.userProfiles.Delete(iamUserArn) {
		return ErrUserProfileNotFound
	}

	return nil
}

// DescribeUserProfiles returns user profiles optionally filtered by ARN.
func (b *InMemoryBackend) DescribeUserProfiles(iamUserArns []string) ([]*UserProfile, error) {
	b.mu.RLock("DescribeUserProfiles")
	defer b.mu.RUnlock()

	if len(iamUserArns) > 0 {
		result := make([]*UserProfile, 0, len(iamUserArns))
		for _, a := range iamUserArns {
			u, ok := b.userProfiles.Get(a)
			if !ok {
				return nil, ErrUserProfileNotFound
			}
			result = append(result, u.toUserProfile())
		}

		return result, nil
	}

	all := b.userProfiles.All()
	result := make([]*UserProfile, 0, len(all))
	for _, u := range all {
		result = append(result, u.toUserProfile())
	}

	return result, nil
}

// UpdateUserProfile updates a user profile's SSH settings.
func (b *InMemoryBackend) UpdateUserProfile(iamUserArn, sshUsername, sshPublicKey string) error {
	b.mu.Lock("UpdateUserProfile")
	defer b.mu.Unlock()

	u, ok := b.userProfiles.Get(iamUserArn)
	if !ok {
		return ErrUserProfileNotFound
	}

	if sshUsername != "" {
		u.SSHUsername = sshUsername
	}
	if sshPublicKey != "" {
		u.SSHPublicKey = sshPublicKey
	}

	return nil
}

// DescribeMyUserProfile returns a placeholder profile for the current user.
func (b *InMemoryBackend) DescribeMyUserProfile() (*UserProfile, error) {
	b.mu.RLock("DescribeMyUserProfile")
	defer b.mu.RUnlock()

	// Return a synthetic "my" profile backed by the account.
	return &UserProfile{
		IamUserArn:          fmt.Sprintf("arn:aws:iam::%s:user/opsworks-user", b.accountID),
		Name:                "opsworks-user",
		SSHUsername:         "opsworks",
		AllowSelfManagement: false,
	}, nil
}

// UpdateMyUserProfile updates the SSH public key for the current user.
func (b *InMemoryBackend) UpdateMyUserProfile(_ string) error {
	return nil
}

// SetPermission sets stack permissions for an IAM user.
func (b *InMemoryBackend) SetPermission(stackID, iamUserArn, level string, allowSSH, allowSudo bool) error {
	b.mu.Lock("SetPermission")
	defer b.mu.Unlock()

	if !b.stacks.Has(stackID) {
		return ErrStackNotFound
	}

	b.permissions.Put(&storedPermission{
		StackID:    stackID,
		IamUserArn: iamUserArn,
		Level:      level,
		AllowSSH:   allowSSH,
		AllowSudo:  allowSudo,
	})

	return nil
}

// DescribePermissions returns permissions optionally filtered by stack and user.
func (b *InMemoryBackend) DescribePermissions(stackID, iamUserArn string) ([]*Permission, error) {
	b.mu.RLock("DescribePermissions")
	defer b.mu.RUnlock()

	if stackID != "" && iamUserArn != "" {
		key := permissionKey(stackID, iamUserArn)
		p, ok := b.permissions.Get(key)
		if !ok {
			return []*Permission{}, nil
		}

		return []*Permission{p.toPermission()}, nil
	}

	result := make([]*Permission, 0)
	for _, p := range b.permissions.All() {
		if stackID != "" && p.StackID != stackID {
			continue
		}
		if iamUserArn != "" && p.IamUserArn != iamUserArn {
			continue
		}
		result = append(result, p.toPermission())
	}

	return result, nil
}

// GrantAccess returns temporary SSH credentials for an instance.
func (b *InMemoryBackend) GrantAccess(instanceID string, validForInMinutes int32) (*TemporaryCredential, error) {
	b.mu.RLock("GrantAccess")
	defer b.mu.RUnlock()

	if !b.instances.Has(instanceID) {
		return nil, ErrInstanceNotFound
	}

	mins := validForInMinutes
	if mins <= 0 {
		mins = 60
	}

	return &TemporaryCredential{
		InstanceID:        instanceID,
		Username:          "opsworks",
		Password:          uuid.NewString(),
		ValidForInMinutes: mins,
	}, nil
}
