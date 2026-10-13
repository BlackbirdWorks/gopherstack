package s3

import (
	"context"
	"net/http"
	"time"
)

type objectCommonDetails struct {
	ETag                      *string
	ChecksumCRC64NVME         *string
	ChecksumMD5               *string
	ChecksumType              string
	ChecksumSHA512            *string
	ContentType               *string
	ContentLength             *int64
	LastModified              *time.Time
	VersionID                 *string
	TagCount                  *int32
	ExpiresString             *string
	ChecksumCRC32C            *string
	Metadata                  map[string]string
	ChecksumCRC32             *string
	ChecksumSHA256            *string
	ChecksumSHA1              *string
	ObjectLockRetainUntilDate *time.Time
	Restore                   *string
	SSECAlgorithm             string
	SSECKeyMD5                string
	SSEKMSKeyID               string
	ObjectLockMode            string
	ObjectLockLegalHoldStatus string
	SSEAlgorithm              string
	StorageClass              string
}

func (h *S3Handler) handleObjectOperation(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
) {
	switch r.Method {
	case http.MethodPut:
		h.routeObjectPut(ctx, w, r, bucket, key)
	case http.MethodGet:
		h.routeObjectGet(ctx, w, r, bucket, key)
	case http.MethodDelete:
		h.routeObjectDelete(ctx, w, r, bucket, key)
	case http.MethodPost:
		h.routeObjectPost(ctx, w, r, bucket, key)
	case http.MethodHead:
		h.headObject(ctx, w, r, bucket, key)
	case http.MethodOptions:
		h.handleCORSPreflight(ctx, w, r, bucket)
	default:
		WriteError(ctx, w, r, ErrMethodNotAllowed)
	}
}

func (h *S3Handler) routeObjectPut(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
) {
	q := r.URL.Query()

	switch {
	case q.Has("tagging"):
		h.putObjectTagging(ctx, w, r, bucket, key)
	case q.Has("annotation"):
		h.putObjectAnnotation(ctx, w, r, bucket, key)
	case q.Has("acl"):
		h.putObjectACL(ctx, w, r, bucket, key)
	case q.Has("partNumber") && q.Has("uploadId"):
		h.uploadPart(ctx, w, r, bucket, key)
	case q.Has("retention"):
		h.putObjectRetention(ctx, w, r, bucket, key)
	case q.Has("legal-hold"):
		h.putObjectLegalHold(ctx, w, r, bucket, key)
	case q.Has("renameObject"):
		h.handleRenameObject(ctx, w, r)
	case q.Has("encryption") && key != "":
		h.handleUpdateObjectEncryption(ctx, w, r)
	case r.Header.Get("X-Amz-Copy-Source") != "":
		h.copyObject(ctx, w, r, bucket, key)
	default:
		h.putObject(ctx, w, r, bucket, key)
	}
}

func (h *S3Handler) routeObjectGet(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
) {
	q := r.URL.Query()

	switch {
	case q.Has("tagging"):
		h.getObjectTagging(ctx, w, r, bucket, key)
	// GetObjectAnnotation and ListObjectAnnotations share the identical
	// GET /{Key+}?annotation route (both s3@v1.106.5 serializers.go:
	// httpbinding.SplitURI("/{Key+}?annotation[&x-id=...]") -- x-id is a
	// disambiguation query param the SDK sends but never binds a value from,
	// so it carries no routing signal here). The only real distinguisher is
	// whether "annotationName" -- a query param GetObjectAnnotation's own
	// HttpBindings function binds and ListObjectAnnotations' does not -- is
	// present.
	case q.Has("annotation") && q.Has("annotationName"):
		h.getObjectAnnotation(ctx, w, r, bucket, key)
	case q.Has("annotation"):
		h.listObjectAnnotations(ctx, w, r, bucket, key)
	case q.Has("acl"):
		h.getObjectACL(ctx, w, r, bucket, key)
	case q.Has("uploadId"):
		h.listParts(ctx, w, r, bucket, key)
	case q.Has("retention"):
		h.getObjectRetention(ctx, w, r, bucket, key)
	case q.Has("legal-hold"):
		h.getObjectLegalHold(ctx, w, r, bucket, key)
	case q.Has("attributes"):
		h.handleGetObjectAttributes(ctx, w, r)
	case q.Has("torrent"):
		h.handleGetObjectTorrent(ctx, w, r)
	default:
		h.getObject(ctx, w, r, bucket, key)
	}
}

func (h *S3Handler) routeObjectDelete(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
) {
	q := r.URL.Query()

	switch {
	case q.Has("tagging"):
		h.deleteObjectTagging(ctx, w, r, bucket, key)
	case q.Has("annotation"):
		h.deleteObjectAnnotation(ctx, w, r, bucket, key)
	case q.Has("uploadId"):
		h.abortMultipartUpload(ctx, w, r, bucket, key)
	default:
		h.deleteObject(ctx, w, r, bucket, key)
	}
}

func (h *S3Handler) routeObjectPost(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucket, key string,
) {
	q := r.URL.Query()

	switch {
	case q.Has("uploads"):
		h.createMultipartUpload(ctx, w, r, bucket, key)
	case q.Has("uploadId"):
		h.completeMultipartUpload(ctx, w, r, bucket, key)
	case q.Has("restore"):
		h.handleRestoreObject(ctx, w, r)
	case q.Has("select"):
		h.selectObjectContent(ctx, w, r, bucket, key)
	default:
		WriteError(ctx, w, r, ErrMethodNotAllowed)
	}
}
