package eventbridge

import (
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

const (
	dlqReasonNoPermissions = "NO_PERMISSIONS"
	dlqReasonAssumeRole    = "FAILED_TO_ASSUME_ROLE"
)

// SetRoleAuthorizer makes role-based rule targets run under the target RoleArn's policies.
func (b *InMemoryBackend) SetRoleAuthorizer(a roleauth.Authorizer) {
	b.mu.Lock("SetRoleAuthorizer")
	defer b.mu.Unlock()

	b.roleAuth = a
	if b.deliveryTargets != nil {
		b.deliveryTargets.RoleAuth = a
	}
}

// usesTargetRole reports whether EventBridge delivers to the target as the target's RoleArn;
// Lambda, SQS, SNS and CloudWatch Logs targets are authorized by resource policy instead.
func usesTargetRole(arn string) bool {
	switch {
	case isKinesisStreamARN(arn), isKinesisFirehoseARN(arn), isECSARN(arn), isStateMachineARN(arn),
		isAPIDestinationARN(arn), isEventBusARN(arn):
		return true
	default:
		return false
	}
}

// authorizeTarget returns the DLQ error code when the target's role may not be used, or "".
func authorizeTarget(target *Target, dt DeliveryTargets) string {
	if dt.RoleAuth == nil || !usesTargetRole(target.Arn) {
		return ""
	}

	action, ok := roleauth.TargetAction(target.Arn)
	if !ok {
		return ""
	}

	resource := target.Arn
	if action == "ecs:RunTask" && target.EcsParameters != nil && target.EcsParameters.TaskDefinitionArn != "" {
		resource = target.EcsParameters.TaskDefinitionArn
	}

	err := dt.RoleAuth.AuthorizeRole(roleauth.PrincipalEvents, target.RoleArn, action, resource)

	switch {
	case err == nil:
		return ""
	case errors.Is(err, roleauth.ErrNotAssumable):
		return dlqReasonAssumeRole
	default:
		return dlqReasonNoPermissions
	}
}
