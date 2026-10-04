package iam

import (
	"context"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

// ServiceRoleTrust checks a service principal may assume a role.
type ServiceRoleTrust interface {
	CheckServiceRoleTrust(servicePrincipal, roleArn string) error
}

// RoleBackend is the IAM storage a RoleAuthorizer reads policies from.
type RoleBackend interface {
	EnforcementBackend
	GetRoleByArn(roleArn string) (*Role, error)
}

// RoleAuthorizer evaluates a role's identity policies for service-initiated calls.
type RoleAuthorizer struct {
	backend   RoleBackend
	trust     ServiceRoleTrust
	providers []ResourcePolicyProvider
}

var _ roleauth.Authorizer = (*RoleAuthorizer)(nil)

// NewRoleAuthorizer builds a RoleAuthorizer; trust may be nil to skip trust checks.
func NewRoleAuthorizer(
	backend RoleBackend, trust ServiceRoleTrust, providers []ResourcePolicyProvider,
) *RoleAuthorizer {
	return &RoleAuthorizer{backend: backend, trust: trust, providers: providers}
}

// AuthorizeRole implements roleauth.Authorizer with the same evaluator the enforcement middleware uses.
func (a *RoleAuthorizer) AuthorizeRole(servicePrincipal, roleArn, action, resource string) error {
	if a.trust != nil {
		if err := a.trust.CheckServiceRoleTrust(servicePrincipal, roleArn); err != nil {
			return fmt.Errorf("%w: %w", roleauth.ErrNotAssumable, err)
		}
	}

	role, err := a.backend.GetRoleByArn(roleArn)
	if err != nil {
		return fmt.Errorf("%w: %w", roleauth.ErrNotAssumable, err)
	}

	docs, err := a.backend.GetPoliciesForRole(role.RoleName)
	if err != nil {
		return fmt.Errorf("%w: %w", roleauth.ErrNotAssumable, err)
	}

	if resource == "" {
		resource = "*"
	}

	condCtx := ConditionContext{
		PrincipalARN:     roleArn,
		PrincipalAccount: arnAccount(roleArn),
		RequestedRegion:  arnRegion(resource),
	}

	result := EvaluatePolicies(docs, action, resource, condCtx)

	var boundary []string
	if doc := boundaryDocForRole(a.backend, role.RoleName); doc != "" {
		boundary = []string{doc}
	}

	result, boundaryDenied, _ := applyPermissionsBoundary(boundary, action, resource, condCtx, result)
	if boundaryDenied || result == EvalExplicitDeny {
		return deniedError(roleArn, action, resource)
	}

	if resDocs := collectResourcePolicies(context.Background(), a.providers, resource); len(resDocs) > 0 {
		switch EvaluatePolicies(resDocs, action, resource, condCtx) {
		case EvalExplicitDeny:
			return deniedError(roleArn, action, resource)
		case EvalAllow:
			return nil
		case EvalImplicitDeny:
		}
	}

	if result != EvalAllow {
		return deniedError(roleArn, action, resource)
	}

	return nil
}

func deniedError(roleArn, action, resource string) error {
	return fmt.Errorf("%w: %s is not authorized to perform %s on resource %s",
		roleauth.ErrAccessDenied, roleArn, action, resource)
}

func arnAccount(arn string) string {
	parts := strings.SplitN(arn, ":", arnMinSegments+1)
	if len(parts) < arnMinSegments {
		return ""
	}

	return parts[arnAccountSegmentIndex]
}

func arnRegion(arn string) string {
	parts := strings.SplitN(arn, ":", arnMinSegments+1)
	if len(parts) < arnMinSegments {
		return ""
	}

	return parts[3]
}
