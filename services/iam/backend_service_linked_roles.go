package iam

import (
	"fmt"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/google/uuid"
)

// GetServiceLinkedRoleDeletionStatus returns the status of a service-linked role deletion task.
// Gopherstack synchronously deletes service-linked roles, so status is always SUCCEEDED.
func (b *InMemoryBackend) GetServiceLinkedRoleDeletionStatus(deletionTaskID string) (string, error) {
	if deletionTaskID == "" {
		return "", fmt.Errorf("%w: DeletionTaskId must not be empty", ErrInvalidAction)
	}

	return "SUCCEEDED", nil
}

// DeleteServiceLinkedRole deletes a service-linked role, forcibly removing all attached managed
// and inline policies first. AWS deletes service-linked roles asynchronously; the mock is
// synchronous — callers receive SUCCEEDED immediately from GetServiceLinkedRoleDeletionStatus.
func (b *InMemoryBackend) DeleteServiceLinkedRole(roleName string) error {
	if roleName == "" {
		return fmt.Errorf("%w: RoleName must not be empty", ErrInvalidAction)
	}

	b.mu.Lock("DeleteServiceLinkedRole")
	defer b.mu.Unlock()

	if _, exists := b.roles.Get(roleName); !exists {
		return fmt.Errorf("%w: role %q not found", ErrRoleNotFound, roleName)
	}

	// Force-clear attached managed policies.
	delete(b.rolePolicies, roleName)
	// Force-clear inline policies.
	delete(b.roleInlinePolicies, roleName)

	role, _ := b.roles.Get(roleName)
	b.roles.Delete(roleName)
	delete(b.roleByARN, role.Arn)
	b.sortedRoleNames = deleteSorted(b.sortedRoleNames, roleName)

	return nil
}

// CreateServiceLinkedRole creates an IAM role that is linked to a specific AWS service.
// The role name is derived from the service name and optional custom suffix.
func (b *InMemoryBackend) CreateServiceLinkedRole(
	awsServiceName, description, customSuffix string,
) (*Role, error) {
	if awsServiceName == "" {
		return nil, fmt.Errorf("%w: AWSServiceName must not be empty", ErrInvalidAction)
	}

	// Build a role name from the service name.
	// e.g., elasticloadbalancing.amazonaws.com → AWSServiceRoleForElasticLoadBalancing
	svcShort := strings.TrimSuffix(awsServiceName, ".amazonaws.com")
	roleName := "AWSServiceRoleFor" + strings.ToUpper(svcShort[:1]) + svcShort[1:]
	if customSuffix != "" {
		roleName = roleName + "_" + customSuffix
	}

	const svcLinkedPath = "/aws-service-role/"

	b.mu.Lock("CreateServiceLinkedRole")
	defer b.mu.Unlock()

	if _, exists := b.roles.Get(roleName); exists {
		return nil, fmt.Errorf("%w: role %q already exists", ErrRoleAlreadyExists, roleName)
	}

	assumeRolePolicy := fmt.Sprintf(
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":%q},"Action":"sts:AssumeRole"}]}`,
		awsServiceName,
	)

	p := normPath(svcLinkedPath + awsServiceName + "/")
	role := Role{
		RoleName:                 roleName,
		RoleID:                   "AROA" + strings.ToUpper(uuid.New().String()[:16]),
		Arn:                      arn.Build("iam", "", b.accountID, "role"+p+roleName),
		Path:                     p,
		AssumeRolePolicyDocument: assumeRolePolicy,
		CreateDate:               time.Now().UTC(),
	}

	if description != "" {
		// Store description as a tag; IAM roles do not have a description field here.
		_ = description
	}

	b.roles.Put(&role)
	b.roleByARN[role.Arn] = roleName
	b.sortedRoleNames = insertSorted(b.sortedRoleNames, roleName)

	return &role, nil
}
