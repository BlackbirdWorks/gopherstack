package firehose

import (
	"context"
	"errors"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

const (
	errCodeS3AccessDenied     = "S3.AccessDenied"
	errCodeS3AssumeRoleDenied = "S3.AssumeRoleAccessDenied"
	errCodeESAccessDenied     = "ES.AccessDenied"
)

// SetRoleAuthorizer makes deliveries run under the destination RoleARN's policies.
func (b *InMemoryBackend) SetRoleAuthorizer(a roleauth.Authorizer) {
	b.mu.Lock("SetRoleAuthorizer")
	defer b.mu.Unlock()

	b.roleAuth = a
}

func (b *InMemoryBackend) roleAuthorizer() roleauth.Authorizer {
	b.mu.RLock("roleAuthorizer")
	defer b.mu.RUnlock()

	return b.roleAuth
}

// authorizeDelivery returns assumeCode when roleARN's trust denies Firehose, denyCode when
// its policies deny action on resource, else "".
func (b *InMemoryBackend) authorizeDelivery(roleARN, action, resource, assumeCode, denyCode string) string {
	auth := b.roleAuthorizer()
	if auth == nil || resource == "" {
		return ""
	}

	err := auth.AuthorizeRole(roleauth.PrincipalFirehose, roleARN, action, resource)

	switch {
	case err == nil:
		return ""
	case errors.Is(err, roleauth.ErrNotAssumable):
		return assumeCode
	default:
		return denyCode
	}
}

// authorizeS3Write returns the documented Firehose error code when roleARN may not write to
// bucketARN, or "".
func (b *InMemoryBackend) authorizeS3Write(roleARN, bucketARN string) string {
	if bucketARN == "" {
		return ""
	}

	return b.authorizeDelivery(
		roleARN,
		"s3:PutObject",
		bucketARN+"/*",
		errCodeS3AssumeRoleDenied,
		errCodeS3AccessDenied,
	)
}

func (b *InMemoryBackend) authorizeLambdaInvoke(roleARN, functionARN string) string {
	if roleARN == "" {
		return ""
	}

	return b.authorizeDelivery(
		roleARN,
		"lambda:InvokeFunction",
		functionARN,
		codeLambdaAssumeDenied,
		codeLambdaInvokeDenied,
	)
}

func (b *InMemoryBackend) authorizeESWrite(roleARN, domainARN string) string {
	if domainARN == "" || roleARN == "" {
		return ""
	}

	return b.authorizeDelivery(roleARN, "es:ESHttpPost", domainARN+"/*", errCodeESAccessDenied, errCodeESAccessDenied)
}

// failDenied records a role-denied delivery in the destination's CloudWatch log and FailedRecords.
func (b *InMemoryBackend) failDenied(
	ctx context.Context, snap *flushSnapshot, cwLog *CloudWatchLoggingOptions, code string, n int,
) {
	cause := fmt.Errorf("%w: delivery role denied", roleauth.ErrAccessDenied)
	b.logDeliveryIssue(ctx, cwLog, snap.streamName, code, cause)
	b.recordFailedRecords(snap.region, snap.streamName, n)
}
