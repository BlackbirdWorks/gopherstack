package eventbridge

import (
	"errors"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

const (
	dlqReasonNoPermissions = "NO_PERMISSIONS"
	dlqReasonAssumeRole    = "FAILED_TO_ASSUME_ROLE"
	dlqReasonFromTarget    = "ERROR_FROM_TARGET"
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
	if dt.RoleAuth == nil {
		return ""
	}

	if !usesTargetRole(target.Arn) {
		return authorizeResourcePolicyTarget(target, dt)
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

// authorizeResourcePolicyTarget checks the destination's resource policy allows events.amazonaws.com
// from the delivering rule.
func authorizeResourcePolicyTarget(target *Target, dt DeliveryTargets) string {
	action, ok := roleauth.ResourcePolicyAction(target.Arn)
	if !ok {
		return ""
	}

	for _, resource := range policyResources(target.Arn) {
		if roleauth.AuthorizeResource(dt.RoleAuth, roleauth.PrincipalEvents, action, resource, dt.ruleARN) == nil {
			return ""
		}
	}

	return dlqReasonNoPermissions
}

// policyResources lists the resource spellings a policy may name; log group policies use both
// "log-group:name" and the documented "log-group:name:*".
func policyResources(arn string) []string {
	if !isCloudWatchLogsARN(arn) {
		return []string{arn}
	}

	bare := strings.TrimSuffix(arn, ":*")

	return []string{bare + ":*", bare}
}
