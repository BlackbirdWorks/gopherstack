package iam

import (
	"fmt"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/google/uuid"
)

// ResetServiceSpecificCredentialFull resets a service-specific credential (regenerates password).
func (b *InMemoryBackend) ResetServiceSpecificCredentialFull(
	userName, credentialID string,
) (*ServiceSpecificCredential, error) {
	b.mu.Lock("ResetServiceSpecificCredential")
	defer b.mu.Unlock()

	if _, exists := b.users.Get(userName); !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	cred, exists := b.serviceSpecificCreds.Get(credentialID)
	if !exists || cred.UserName != userName {
		return nil, fmt.Errorf("%w: service-specific credential %q not found", ErrPolicyNotFound, credentialID)
	}

	// Generate a new password.
	cred.ServicePassword = newID("") + newID("")
	b.serviceSpecificCreds.Put(cred)

	return cred, nil
}

// ListServiceSpecificCredentials returns a page of a user's service-specific credentials,
// filtered to serviceName when non-empty.
func (b *InMemoryBackend) ListServiceSpecificCredentials(
	userName, serviceName, marker string, maxItems int,
) (page.Page[ServiceSpecificCredential], error) {
	b.mu.RLock("ListServiceSpecificCredentials")
	defer b.mu.RUnlock()

	if _, exists := b.users.Get(userName); !exists {
		return page.Page[ServiceSpecificCredential]{}, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	result := make([]ServiceSpecificCredential, 0, b.serviceSpecificCreds.Len())
	for _, cred := range b.serviceSpecificCreds.All() {
		if cred.UserName != userName {
			continue
		}

		if serviceName != "" && cred.ServiceName != serviceName {
			continue
		}

		result = append(result, *cred)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].ServiceSpecificCredentialID < result[j].ServiceSpecificCredentialID
	})

	return page.New(result, marker, maxItems, iamDefaultMaxItems), nil
}

// DeleteServiceSpecificCredential deletes a service-specific credential.
func (b *InMemoryBackend) DeleteServiceSpecificCredential(userName, credentialID string) error {
	b.mu.Lock("DeleteServiceSpecificCredential")
	defer b.mu.Unlock()

	cred, exists := b.serviceSpecificCreds.Get(credentialID)
	if !exists || cred.UserName != userName {
		return fmt.Errorf("%w: credential %q not found for user %q", ErrAccessKeyNotFound, credentialID, userName)
	}

	b.serviceSpecificCreds.Delete(credentialID)

	return nil
}

// UpdateServiceSpecificCredential updates the status of a service-specific credential.
func (b *InMemoryBackend) UpdateServiceSpecificCredential(
	userName, credentialID, status string,
) error {
	b.mu.Lock("UpdateServiceSpecificCredential")
	defer b.mu.Unlock()

	if _, exists := b.users.Get(userName); !exists {
		return fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	cred, exists := b.serviceSpecificCreds.Get(credentialID)
	if !exists {
		return fmt.Errorf(
			"%w: service-specific credential %q not found",
			ErrPolicyNotFound,
			credentialID,
		)
	}

	if cred.UserName != userName {
		return fmt.Errorf(
			"%w: credential %q does not belong to user %q",
			ErrPolicyNotFound,
			credentialID,
			userName,
		)
	}

	cred.Status = status
	b.serviceSpecificCreds.Put(cred)

	return nil
}

// CreateServiceSpecificCredential creates service-specific credentials for an IAM user.
func (b *InMemoryBackend) CreateServiceSpecificCredential(
	userName, serviceName string,
) (*ServiceSpecificCredential, error) {
	if serviceName == "" {
		return nil, fmt.Errorf("%w: ServiceName must not be empty", ErrInvalidAction)
	}

	b.mu.Lock("CreateServiceSpecificCredential")
	defer b.mu.Unlock()

	if _, exists := b.users.Get(userName); !exists {
		return nil, fmt.Errorf("%w: user %q not found", ErrUserNotFound, userName)
	}

	credID := newID("ACCAI")
	svcUserName := userName + "+" + serviceName
	svcPassword := uuid.New().String() + uuid.New().String()

	cred := ServiceSpecificCredential{
		ServiceSpecificCredentialID: credID,
		UserName:                    userName,
		ServiceName:                 serviceName,
		ServiceUserName:             svcUserName,
		ServicePassword:             svcPassword,
		Status:                      "Active",
		CreateDate:                  time.Now().UTC(),
	}

	b.serviceSpecificCreds.Put(&cred)

	return &cred, nil
}
