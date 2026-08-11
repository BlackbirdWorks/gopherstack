package s3

import (
	"context"
	"net/http"
	"time"
)

type objectCommonDetails struct {
	Metadata          map[string]string
	ETag              *string
	ContentType       *string
	ContentLength     *int64
	LastModified      *time.Time
	VersionID         *string
	StorageClass      string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	ChecksumCRC64NVME *string
	TagCount          *int32
	SSEAlgorithm      string
	SSEKMSKeyID       string
	SSECAlgorithm     string
	SSECKeyMD5        string
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
	switch {
	case r.URL.Query().Has("tagging"):
		h.putObjectTagging(ctx, w, r, bucket, key)
	case r.URL.Query().Has("acl"):
		h.putObjectACL(ctx, w, r, bucket, key)
	case r.URL.Query().Has("partNumber") && r.URL.Query().Has("uploadId"):
		h.uploadPart(ctx, w, r, bucket, key)
	case r.URL.Query().Has("retention"):
		h.putObjectRetention(ctx, w, r, bucket, key)
	case r.URL.Query().Has("legal-hold"):
		h.putObjectLegalHold(ctx, w, r, bucket, key)
	case r.URL.Query().Has("rename"):
		h.handleRenameObject(ctx, w, r)
	case r.URL.Query().Has("encryption") && key != "":
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
	switch {
	case r.URL.Query().Has("tagging"):
		h.getObjectTagging(ctx, w, r, bucket, key)
	case r.URL.Query().Has("acl"):
		h.getObjectACL(ctx, w, r, bucket, key)
	case r.URL.Query().Has("uploadId"):
		h.listParts(ctx, w, r, bucket, key)
	case r.URL.Query().Has("retention"):
		h.getObjectRetention(ctx, w, r, bucket, key)
	case r.URL.Query().Has("legal-hold"):
		h.getObjectLegalHold(ctx, w, r, bucket, key)
	case r.URL.Query().Has("attributes"):
		h.handleGetObjectAttributes(ctx, w, r)
	case r.URL.Query().Has("torrent"):
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
	switch {
	case r.URL.Query().Has("tagging"):
		h.deleteObjectTagging(ctx, w, r, bucket, key)
	case r.URL.Query().Has("uploadId"):
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
	switch {
	case r.URL.Query().Has("uploads"):
		h.createMultipartUpload(ctx, w, r, bucket, key)
	case r.URL.Query().Has("uploadId"):
		h.completeMultipartUpload(ctx, w, r, bucket, key)
	case r.URL.Query().Has("restore"):
		h.handleRestoreObject(ctx, w, r)
	case r.URL.Query().Has("select"):
		h.selectObjectContent(ctx, w, r, bucket, key)
	default:
		WriteError(ctx, w, r, ErrMethodNotAllowed)
	}
}
