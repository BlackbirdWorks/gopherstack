package lambdaweb

import (
	"context"
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

// Sentinel errors a CodeStore returns.
var (
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrBucketNotVersioned = errors.New("bucket versioning is not enabled")
	ErrObjectNotFound     = errors.New("object not found")
)

// CodeStore reads deployment packages from versioned S3 buckets.
type CodeStore interface {
	// ResolveObject returns the object version a revision pins: versionID when given, else the current version.
	ResolveObject(ctx context.Context, bucket, key, versionID string) (string, error)
	// GetObject returns the package bytes of one object version.
	GetObject(ctx context.Context, bucket, key, versionID string) ([]byte, error)
}

// SetCodeStore wires the S3 backend revisions pull their code from.
func (b *InMemoryBackend) SetCodeStore(cs CodeStore) {
	b.mu.Lock("SetCodeStore")
	defer b.mu.Unlock()

	b.codes = cs
}

func (b *InMemoryBackend) codeStore() CodeStore {
	b.mu.RLock("codeStore")
	defer b.mu.RUnlock()

	return b.codes
}

// pinCode checks the package exists in a versioned bucket and pins the revision to its object version.
func (b *InMemoryBackend) pinCode(ctx context.Context, region, fn string, s3 *S3Object) error {
	cs := b.codeStore()
	if cs == nil {
		return nil
	}

	if err := roleauth.AuthorizeResource(
		b.authorizer(), roleauth.PrincipalLambda, "s3:GetObject", arn.BuildS3(s3.Bucket+"/"+s3.Key),
		b.pendingRevisionARN(region, fn),
	); err != nil {
		return accessDeniedError("The bucket policy of %s does not allow %s to read s3://%s/%s.",
			s3.Bucket, ServicePrincipal, s3.Bucket, s3.Key)
	}

	version, err := cs.ResolveObject(ctx, s3.Bucket, s3.Key, s3.VersionID)

	switch {
	case err == nil:
		s3.VersionID = version

		return nil
	case errors.Is(err, ErrBucketNotFound):
		return validationError("The S3 bucket %s does not exist.", s3.Bucket)
	case errors.Is(err, ErrBucketNotVersioned):
		return validationError("The S3 bucket %s must have versioning enabled.", s3.Bucket)
	case errors.Is(err, ErrObjectNotFound):
		return validationError("The S3 object s3://%s/%s (version %q) was not found.", s3.Bucket, s3.Key, s3.VersionID)
	default:
		return validationError("The S3 object s3://%s/%s could not be read.", s3.Bucket, s3.Key)
	}
}

// VerifyRevisions re-checks that every Active revision's pinned object can still be read by Lambda; a revision
// whose object version is gone or whose bucket policy no longer admits Lambda becomes Failed, permanently.
func (b *InMemoryBackend) VerifyRevisions(ctx context.Context) {
	cs := b.codeStore()
	if cs == nil {
		return
	}

	b.mu.Lock("VerifyRevisions")
	b.advanceLocked()

	var active []*Revision

	for _, r := range b.revisions.All() {
		if r.State == stateActive {
			active = append(active, r.clone())
		}
	}
	b.mu.Unlock()

	for _, rev := range active {
		if reason := b.verifyObject(ctx, cs, rev); reason != "" {
			b.failRevision(rev.Region, rev.FunctionName, rev.ID, reason)
			b.run.stopRevision(rev.Region, rev.FunctionName, rev.ID)
		}
	}
}

func (b *InMemoryBackend) verifyObject(ctx context.Context, cs CodeStore, rev *Revision) string {
	s3 := rev.S3

	if _, err := cs.ResolveObject(ctx, s3.Bucket, s3.Key, s3.VersionID); err != nil {
		return "The S3 object s3://" + s3.Bucket + "/" + s3.Key + " (version " + s3.VersionID + ") can no longer be read."
	}

	if err := roleauth.AuthorizeResource(
		b.authorizer(), roleauth.PrincipalLambda, "s3:GetObject", arn.BuildS3(s3.Bucket+"/"+s3.Key), rev.ARN,
	); err != nil {
		return "The bucket policy of " + s3.Bucket + " no longer allows " + ServicePrincipal + " to read the object."
	}

	return ""
}

// RunVerifier calls VerifyRevisions every interval until ctx ends.
func (b *InMemoryBackend) RunVerifier(ctx context.Context, interval time.Duration) {
	t := time.NewTicker(interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.VerifyRevisions(ctx)
		}
	}
}
