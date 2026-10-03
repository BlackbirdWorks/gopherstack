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
)

// SetRoleAuthorizer makes S3 delivery run under the destination RoleARN's policies.
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

// authorizeS3Write returns the documented Firehose error code when roleARN may not write to
// bucketARN, or "".
func (b *InMemoryBackend) authorizeS3Write(roleARN, bucketARN string) string {
	auth := b.roleAuthorizer()
	if auth == nil || bucketARN == "" {
		return ""
	}

	err := auth.AuthorizeRole(roleauth.PrincipalFirehose, roleARN, "s3:PutObject", bucketARN+"/*")

	switch {
	case err == nil:
		return ""
	case errors.Is(err, roleauth.ErrNotAssumable):
		return errCodeS3AssumeRoleDenied
	default:
		return errCodeS3AccessDenied
	}
}

// failDenied records a role-denied delivery in the destination's CloudWatch log and FailedRecords.
func (b *InMemoryBackend) failDenied(
	ctx context.Context, snap *flushSnapshot, cwLog *CloudWatchLoggingOptions, code string, n int,
) {
	cause := fmt.Errorf("%w: delivery role denied", roleauth.ErrAccessDenied)
	b.logDeliveryIssue(ctx, cwLog, snap.streamName, code, cause)
	b.recordFailedRecords(snap.region, snap.streamName, n)
}
