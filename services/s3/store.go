package s3

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/store"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type (
	md5ContextKey struct{}
	sseContextKey struct{}
)

var (
	md5Key = md5ContextKey{} //nolint:gochecknoglobals // internal context key
	sseKey = sseContextKey{} //nolint:gochecknoglobals // internal context key
)

// objectVersionIDBytes is the number of random bytes used to generate a version ID.
// 16 bytes produces a 32-character lowercase hex string.
const objectVersionIDBytes = 16

// newObjectVersionID generates a random S3-compatible version ID using 16 random
// bytes encoded as a 32-character lowercase hex string. This matches the format
// expected by AWS clients better than the previous Unix-nanosecond approach.
func newObjectVersionID() string {
	b := make([]byte, objectVersionIDBytes)
	if _, err := rand.Read(b); err != nil {
		// Fallback to time-based ID if crypto/rand fails (should never happen).
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	return hex.EncodeToString(b)
}

const defaultRegionName = config.DefaultRegion

// objectChecksums holds the optional checksum values supplied with a PutObject request.
type objectChecksums struct {
	crc32, crc32c, sha1, sha256, crc64nvme *string
}

// populateComputed fills the appropriate checksum field when the client requested
// server-side computation (supplied algorithm but no value).
func (c *objectChecksums) populateComputed(computed, algo string) {
	if computed == "" {
		return
	}

	switch algo {
	case ChecksumCRC32:
		if c.crc32 == nil {
			c.crc32 = aws.String(computed)
		}
	case ChecksumCRC32C:
		if c.crc32c == nil {
			c.crc32c = aws.String(computed)
		}
	case ChecksumSHA1:
		if c.sha1 == nil {
			c.sha1 = aws.String(computed)
		}
	case ChecksumSHA256:
		if c.sha256 == nil {
			c.sha256 = aws.String(computed)
		}
	case ChecksumCRC64NVME:
		if c.crc64nvme == nil {
			c.crc64nvme = aws.String(computed)
		}
	}
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// getRegionFromS3Context extracts the region from S3 request context.
// Returns the default region if region is not found in context.
func getRegionFromS3Context(ctx context.Context, defaultRegion string) string {
	if region, ok := ctx.Value(regionContextKey{}).(string); ok && region != "" {
		return region
	}

	return defaultRegion
}

type InMemoryBackend struct {
	// registry lets Reset/Snapshot/Restore collapse the buckets/uploads
	// lifecycle to one call each (registry.ResetAll/SnapshotAll/RestoreAll)
	// instead of hand-rolled per-map wiring. See pkgs/store's package doc and
	// the services/sqs pilot (commit 0f09d77c) for the pattern this follows.
	registry *store.Registry
	// buckets is keyed by bucket name (globally unique — see StoredBucket.Region's
	// doc comment). This replaces the old region->name->*StoredBucket nesting plus
	// the separate bucketIndex name->region map: Table.Get(name) alone now answers
	// both "does it exist" and "give me the bucket", and StoredBucket.Region
	// carries what bucketIndex used to.
	buckets *store.Table[StoredBucket]
	// uploads is keyed by UploadID (a random 32-hex-char string — see
	// newObjectVersionID — so it is unique across all buckets). uploadsByBucket
	// is a secondary index replacing the old bucket->uploadID->*StoredMultipartUpload
	// nesting for the "all uploads in bucket X" access pattern (ListMultipartUploads,
	// janitor cleanup, DeleteBucket cleanup). A caller-supplied uploadID is only
	// valid for the bucket it was issued against — see getUpload, which enforces
	// that the same way the old b.uploads[bucketName][uploadID] nesting did.
	uploads         *store.Table[StoredMultipartUpload]
	uploadsByBucket *store.Index[StoredMultipartUpload]
	// tags is intentionally left as a plain map (not a store.Table): its key is
	// a composite "bucket/key/versionID" string that is not a pure function of
	// the stored value (a bare []types.Tag has no identity field of its own) —
	// the same reason services/ec2's store_setup.go leaves e.g.
	// vpcPeeringOptions/instanceIMDSOptions unconverted.
	tags       map[string][]types.Tag
	mu         *lockmetrics.RWMutex
	compressor Compressor
	// serviceCtx is the long-lived context for background work (replication).
	// Initialised in NewInMemoryBackend so it is always non-nil; overridden by
	// SetServiceContext when the handler wires in the real service context.
	serviceCtx    context.Context
	serviceCancel context.CancelFunc
	defaultRegion string
	// serviceCtxMu guards serviceCtx and serviceCancel.
	serviceCtxMu sync.RWMutex
	// replicationWg tracks all in-flight replication goroutines.
	// DrainReplicationGoroutines blocks until they all finish.
	replicationWg       sync.WaitGroup
	compressionMinBytes int
	// skipMultipartSizeCheck disables the 5 MiB minimum part size check during
	// CompleteMultipartUpload. This is intended for use in unit tests only.
	skipMultipartSizeCheck bool
}

// WithSkipMultipartSizeCheck disables the 5 MiB minimum non-last-part size
// enforcement in CompleteMultipartUpload. Use only in tests.
func (b *InMemoryBackend) WithSkipMultipartSizeCheck() *InMemoryBackend {
	b.skipMultipartSizeCheck = true

	return b
}

// DrainReplicationGoroutines blocks until all in-flight replication goroutines
// complete. Use in tests to establish a happens-before boundary between
// operations that spawn replication goroutines and subsequent state assertions.
func (b *InMemoryBackend) DrainReplicationGoroutines() {
	b.replicationWg.Wait()
}

// SetServiceContext wires the long-lived service context used to parent background
// work (replication). Called from the handler's StartWorker. Cancels the previous
// default background context before switching to the service-provided one.
func (b *InMemoryBackend) SetServiceContext(ctx context.Context) {
	newCtx, newCancel := context.WithCancel(ctx)

	b.serviceCtxMu.Lock()
	defer b.serviceCtxMu.Unlock()

	if b.serviceCancel != nil {
		b.serviceCancel()
	}

	b.serviceCtx = newCtx
	b.serviceCancel = newCancel
}

// replicationContext builds the context for a replication goroutine: parented to
// the service context (so shutdown cancels it) and carrying the request's logger,
// but never the request's cancellation or its SSE key. serviceCtx is always
// non-nil (initialised in NewInMemoryBackend).
func (b *InMemoryBackend) replicationContext(reqCtx context.Context) context.Context {
	return logger.Save(b.currentServiceContextLocked(), logger.Load(reqCtx))
}

// currentServiceContextLocked returns the backend's current long-lived service
// context under b.serviceCtxMu. Extracted from replicationContext so the locked
// read is a plain method body rather than a function literal.
func (b *InMemoryBackend) currentServiceContextLocked() context.Context {
	b.serviceCtxMu.RLock()
	defer b.serviceCtxMu.RUnlock()

	return b.serviceCtx
}

// bucketTableKey is the [store.Table] key function for b.buckets: bucket
// names are globally unique (CreateBucket enforces this), so the name alone
// is the bucket's identity regardless of region.
func bucketTableKey(bucket *StoredBucket) string { return bucket.Name }

// uploadTableKey is the [store.Table] key function for b.uploads: a
// multipart upload's own UploadID is already its unique identity.
func uploadTableKey(u *StoredMultipartUpload) string { return u.UploadID }

// uploadBucketIndexKey is the [store.Index] key function grouping uploads by
// their owning bucket, replacing the old bucket->uploadID->*StoredMultipartUpload
// map nesting for bucket-scoped scans (ListMultipartUploads, janitor cleanup).
func uploadBucketIndexKey(u *StoredMultipartUpload) string { return u.Bucket }

func NewInMemoryBackend(compressor Compressor) *InMemoryBackend {
	ctx, cancel := context.WithCancel(context.Background())

	registry := store.NewRegistry()
	uploads := store.Register(registry, "uploads", store.New(uploadTableKey))

	return &InMemoryBackend{
		registry:        registry,
		buckets:         store.Register(registry, "buckets", store.New(bucketTableKey)),
		uploads:         uploads,
		uploadsByBucket: uploads.AddIndex("bucket", uploadBucketIndexKey),
		compressor:      compressor,
		defaultRegion:   defaultRegionName,
		mu:              lockmetrics.New("s3"),
		serviceCtx:      ctx,
		serviceCancel:   cancel,
	}
}

// Shutdown cancels the backend's service context and waits for all in-flight
// replication goroutines to complete. Safe to call more than once.
func (b *InMemoryBackend) Shutdown() {
	func() {
		b.serviceCtxMu.Lock()
		defer b.serviceCtxMu.Unlock()

		if b.serviceCancel != nil {
			b.serviceCancel()
		}
	}()

	b.replicationWg.Wait()
}

// WithCompressionMinBytes sets the minimum object size (in bytes) below which
// gzip compression is skipped. A value of 0 compresses all objects regardless
// of size (the original behaviour). Negative values are clamped to 0 to
// prevent misconfiguration (e.g., via env/flags) from silently changing semantics.
func (b *InMemoryBackend) WithCompressionMinBytes(n int) *InMemoryBackend {
	if n < 0 {
		n = 0
	}

	b.compressionMinBytes = n

	return b
}

// getBucket returns the bucket for a given name, returning ErrNoSuchBucket when the
// bucket does not exist or is pending async deletion. The caller must hold at least b.mu.RLock.
// b.buckets is keyed by name, so a single Table.Get resolves it in O(1).
func (b *InMemoryBackend) getBucket(name string) (*StoredBucket, error) {
	bucket, ok := b.buckets.Get(name)
	if !ok || bucket.DeletePending {
		return nil, ErrNoSuchBucket
	}

	return bucket, nil
}

// BucketRegion returns the region a bucket is stored in, or "" if the bucket
// does not exist or is pending async deletion. Safe for concurrent use.
func (b *InMemoryBackend) BucketRegion(name string) string {
	b.mu.RLock("BucketRegion")
	defer b.mu.RUnlock()

	bucket, err := b.getBucket(name)
	if err != nil {
		return ""
	}

	return bucket.Region
}

// SetDefaultRegion sets the default region for this backend.
func (b *InMemoryBackend) SetDefaultRegion(region string) {
	if region == "" {
		region = defaultRegionName
	}
	b.defaultRegion = region
}

// Multipart

// purgeUploadsForBucketLocked removes every multipart upload belonging to
// bucketName from b.uploads. Caller must hold b.mu. The uploadsByBucket group
// slice is owned by the index (see [store.Index.Get]'s doc), so upload IDs
// are copied out before any Delete call mutates that group.
func (b *InMemoryBackend) purgeUploadsForBucketLocked(bucketName string) {
	grouped := b.uploadsByBucket.Get(bucketName)
	uploadIDs := make([]string, len(grouped))
	for i, u := range grouped {
		uploadIDs[i] = u.UploadID
	}

	for _, id := range uploadIDs {
		b.uploads.Delete(id)
	}
}

// purgeBucketLocked removes a single bucket and its associated data from the
// backend. Caller must hold b.mu.
func (b *InMemoryBackend) purgeBucketLocked(bucketName string, bucket *StoredBucket) {
	// Close per-object mutexes to avoid Prometheus metric leaks.
	for _, obj := range bucket.Objects {
		obj.mu.Close()
	}

	bucket.mu.Close()
	b.buckets.Delete(bucketName)

	for tagKey := range b.tags {
		if strings.HasPrefix(tagKey, bucketName+"/") {
			delete(b.tags, tagKey)
		}
	}

	b.purgeUploadsForBucketLocked(bucketName)
}

// Purge removes all buckets created before the given cutoff time.
func (b *InMemoryBackend) Purge(ctx context.Context, cutoff time.Time) {
	if ctx.Err() != nil {
		return
	}

	b.mu.Lock("Purge")
	defer b.mu.Unlock()

	for _, bucket := range b.buckets.All() {
		if ctx.Err() != nil {
			return
		}

		if bucket.CreationDate.Before(cutoff) {
			b.purgeBucketLocked(bucket.Name, bucket)
		}
	}
}

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	// Close all bucket and object mutexes to prevent Prometheus metric leaks.
	for _, bucket := range b.buckets.All() {
		for _, obj := range bucket.Objects {
			obj.mu.Close()
		}
		bucket.mu.Close()
	}

	b.registry.ResetAll()
	b.tags = make(map[string][]types.Tag)
}
