package cloudwatchlogs

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

// SetRoleAuthorizer makes subscription delivery run under the filter's RoleArn (Kinesis, Firehose)
// or the destination function's resource policy (Lambda).
func (b *InMemoryBackend) SetRoleAuthorizer(a roleauth.Authorizer) {
	b.mu.Lock("SetRoleAuthorizer")
	defer b.mu.Unlock()

	b.roleAuth = a
}

func (b *InMemoryBackend) subscriptionAuthorizer() roleauth.Authorizer {
	b.mu.RLock("subscriptionAuthorizer")
	defer b.mu.RUnlock()

	return b.roleAuth
}

// authorizeSubscription reports whether logs.amazonaws.com may deliver to destinationArn.
func (b *InMemoryBackend) authorizeSubscription(region, groupName, roleArn, destinationArn string) error {
	auth := b.subscriptionAuthorizer()
	if auth == nil {
		return nil
	}

	if strings.Contains(destinationArn, ":lambda:") {
		source := arn.Build("logs", region, b.accountID, "log-group:"+groupName+":*")

		return roleauth.AuthorizeResource(auth, roleauth.PrincipalLogs, "lambda:InvokeFunction", destinationArn, source)
	}

	action, ok := roleauth.TargetAction(destinationArn)
	if !ok {
		return nil
	}

	return roleauth.Authorize(auth, roleauth.PrincipalLogs, roleArn, action, destinationArn)
}

const (
	msgLambdaDenied = "Could not execute the lambda function. " +
		"Make sure you have given CloudWatch Logs permission to execute your function"
	msgFirehoseDenied = "Could not deliver test message to specified Firehose stream. " +
		"Check if the given Firehose stream is in ACTIVE state"
	msgKinesisDenied = "Could not deliver test message to specified Kinesis stream. " +
		"Check if the given kinesis stream is in ACTIVE state"
)

// subscriptionDeniedError is the InvalidParameterException PutSubscriptionFilter returns when its
// test message cannot be delivered.
func subscriptionDeniedError(destinationArn string) error {
	switch {
	case strings.Contains(destinationArn, ":lambda:"):
		return fmt.Errorf("%w: %s", ErrValidation, msgLambdaDenied)
	case strings.Contains(destinationArn, ":firehose:"):
		return fmt.Errorf("%w: %s", ErrValidation, msgFirehoseDenied)
	default:
		return fmt.Errorf("%w: %s", ErrValidation, msgKinesisDenied)
	}
}

func (b *InMemoryBackend) groupExists(region, groupName string) bool {
	b.mu.RLock("groupExists")
	defer b.mu.RUnlock()

	return b.groupHas(region, groupName)
}
