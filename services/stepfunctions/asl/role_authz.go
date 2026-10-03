package asl

import (
	"errors"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

// SetRoleAuthorizer makes direct (non-SDK) service calls run under the execution role's policies.
func (e *Executor) SetRoleAuthorizer(a roleauth.Authorizer) { e.auth = a }

// authorizeRole checks the state machine role may perform action on resource.
func (e *Executor) authorizeRole(errPrefix, action, resource string) error {
	if e.auth == nil {
		return nil
	}

	err := e.auth.AuthorizeRole(roleauth.PrincipalStates, e.execMeta.RoleArn, action, resource)

	switch {
	case err == nil:
		return nil
	case errors.Is(err, roleauth.ErrNotAssumable):
		return &FailError{ErrCode: "States.Permissions", Cause: err.Error()}
	default:
		return &FailError{ErrCode: errPrefix + ".AccessDeniedException", Cause: err.Error()}
	}
}

// resourceARN expands a bare name into an ARN of the given service and resource type.
func (e *Executor) resourceARN(service, kind, name string) string {
	if strings.HasPrefix(name, "arn:") {
		return name
	}

	region, account := e.regionAndAccount()

	return "arn:aws:" + service + ":" + region + ":" + account + ":" + kind + name
}
