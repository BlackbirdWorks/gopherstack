package connect

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const attachedBucketPrefix = "connect-attached-files-"

// AttachmentStore is the S3 surface behind attached files. Resolvers that implement it make
// attached-file content live in S3 behind real pre-signed URLs.
type AttachmentStore interface {
	EnsureBucket(ctx context.Context, bucket string) error
	ObjectSize(ctx context.Context, bucket, key string) (int64, bool, error)
	PutObject(ctx context.Context, bucket, key string, data []byte) error
	DeleteObject(ctx context.Context, bucket, key string) error
	Presign(
		ctx context.Context,
		method, baseURL, bucket, key string,
		ttl time.Duration,
		at time.Time,
		bound map[string]string,
	) string
}

// attachStore returns the S3 attachment store, or nil when files are kept in the emulator.
func (b *Backend) attachStore() AttachmentStore {
	b.mu.RLock("attachStore")
	defer b.mu.RUnlock()

	s, _ := b.resolvers.(AttachmentStore)

	return s
}

func (b *Backend) attachBucket(ctx context.Context) string {
	account, region := b.scope(ctx)

	return attachedBucketPrefix + account + "-" + region
}

func attachedKey(inst, fileID string) string { return "attached-files/" + inst + "/" + fileID }

// storeURL pre-signs a request on the attachment object of r.
func (b *Backend) storeURL(
	ctx context.Context,
	s AttachmentStore,
	method string,
	r *attachedFileRec,
	ttl time.Duration,
) *URLMetadata {
	now := b.now()
	bound := map[string]string(nil)
	headers := map[string]string{}

	if method == http.MethodPut {
		bound = map[string]string{"Content-Length": strconv.FormatInt(r.Size, 10)}
		headers = bound
	}

	return &URLMetadata{
		URL:              s.Presign(ctx, method, b.baseURL(ctx), r.Bucket, r.Key, ttl, now, bound),
		URLExpiry:        isoTime(now.Add(ttl)),
		HeadersToInclude: headers,
	}
}

func (b *Backend) dropStoredObject(ctx context.Context, s AttachmentStore, bucket, key string) {
	if s == nil || bucket == "" {
		return
	}

	if err := s.DeleteObject(ctx, bucket, key); err != nil {
		logger.Load(ctx).WarnContext(ctx, "connect: attached file object delete failed", "error", err)
	}
}
