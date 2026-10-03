// Package roleauth authorizes AWS-service-initiated calls against the customer role the service acts as.
package roleauth

import (
	"errors"
	"strings"
)

// Service principals documented as the trusted principal of each service's execution role.
const (
	PrincipalStates    = "states.amazonaws.com"
	PrincipalEvents    = "events.amazonaws.com"
	PrincipalScheduler = "scheduler.amazonaws.com"
	PrincipalPipes     = "pipes.amazonaws.com"
)

const arnFields = 6

var (
	// ErrNotAssumable means the service principal may not assume the role.
	ErrNotAssumable = errors.New("role cannot be assumed by the service")
	// ErrAccessDenied means the role's policies do not allow the action.
	ErrAccessDenied = errors.New("AccessDeniedException")
)

// Authorizer answers whether servicePrincipal, acting as roleArn, may perform action on resource.
type Authorizer interface {
	AuthorizeRole(servicePrincipal, roleArn, action, resource string) error
}

// Authorize is a nil-safe call: a nil Authorizer allows everything (enforcement off).
func Authorize(a Authorizer, servicePrincipal, roleArn, action, resource string) error {
	if a == nil {
		return nil
	}

	return a.AuthorizeRole(servicePrincipal, roleArn, action, resource)
}

// TargetAction maps a rule/schedule/pipe target ARN to the IAM action its execution role needs.
func TargetAction(targetARN string) (string, bool) {
	parts := strings.SplitN(targetARN, ":", arnFields)
	if len(parts) < arnFields {
		return "", false
	}

	res := parts[5]

	switch parts[2] {
	case "lambda":
		return "lambda:InvokeFunction", true
	case "sqs":
		return "sqs:SendMessage", true
	case "sns":
		return "sns:Publish", true
	case "states":
		return "states:StartExecution", true
	case "kinesis":
		return "kinesis:PutRecord", true
	case "firehose":
		return "firehose:PutRecord", true
	case "ecs":
		return "ecs:RunTask", true
	case "sagemaker":
		return "sagemaker:StartPipelineExecution", true
	case "events":
		if strings.HasPrefix(res, "api-destination/") {
			return "events:InvokeApiDestination", true
		}

		return "events:PutEvents", true
	default:
		return "", false
	}
}
