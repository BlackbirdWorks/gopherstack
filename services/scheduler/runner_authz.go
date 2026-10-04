package scheduler

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

// SetRoleAuthorizer makes target invocations run under the schedule's RoleArn policies.
func (r *Runner) SetRoleAuthorizer(a roleauth.Authorizer) { r.auth = a }

// authorizeTarget checks the schedule's execution role may invoke its target.
func (r *Runner) authorizeTarget(s *Schedule) error {
	if r.auth == nil {
		return nil
	}

	action, ok := roleauth.TargetAction(s.Target.ARN)
	if !ok {
		return nil
	}

	resource := s.Target.ARN
	if action == "ecs:RunTask" && s.Target.EcsParameters != nil && s.Target.EcsParameters.TaskDefinitionArn != "" {
		resource = s.Target.EcsParameters.TaskDefinitionArn
	}

	return r.auth.AuthorizeRole(roleauth.PrincipalScheduler, s.Target.RoleARN, action, resource)
}

// isPermanentTargetError reports an authorization failure, which Scheduler does not retry.
func isPermanentTargetError(err error) bool {
	return errors.Is(err, roleauth.ErrAccessDenied) || errors.Is(err, roleauth.ErrNotAssumable)
}
