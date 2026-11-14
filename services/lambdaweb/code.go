package lambdaweb

import (
	"context"
	"errors"
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
func (b *InMemoryBackend) pinCode(ctx context.Context, s3 *S3Object) error {
	cs := b.codeStore()
	if cs == nil {
		return nil
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
