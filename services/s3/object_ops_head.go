package s3

import (
	"context"
	"errors"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (h *S3Handler) headObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "HeadObject")

	if err := validateExpectedBucketOwner(r); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if err := h.authorizeObjectAccess(ctx, r, bucketName, key, actionGetObject); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	newCtx, ok := h.attachSSEInfoToCtx(ctx, w, r)
	if !ok {
		return
	}
	ctx = newCtx

	versionID := r.URL.Query().Get("versionId")
	var vid *string

	if versionID != "" {
		vid = aws.String(versionID)
	}

	out, err := h.Backend.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	})
	var nsb *types.NoSuchBucket
	var nsk *types.NoSuchKey
	if errors.Is(err, ErrLatestDeleteMarker) {
		// HEAD of a key whose latest version is a delete marker: 404 + header.
		w.Header().Set("X-Amz-Delete-Marker", "true")
		w.WriteHeader(http.StatusNotFound)

		return
	}

	if errors.As(err, &nsb) || errors.As(err, &nsk) ||
		errors.Is(err, ErrNoSuchBucket) || errors.Is(err, ErrNoSuchKey) {
		w.WriteHeader(http.StatusNotFound)

		return
	}

	if errors.Is(err, ErrDeleteMarker) {
		w.Header().Set("X-Amz-Delete-Marker", "true")
		w.Header().Set("Allow", "DELETE")
		w.WriteHeader(http.StatusMethodNotAllowed)

		return
	}

	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)

		return
	}

	if validateErr := validateSSECOnRead(
		r, aws.ToString(out.SSECustomerAlgorithm), aws.ToString(out.SSECustomerKeyMD5),
	); validateErr != nil {
		WriteError(ctx, w, r, validateErr)

		return
	}

	if status, condOK := checkConditionalHeaders(r, aws.ToString(out.ETag), aws.ToTime(out.LastModified)); !condOK {
		w.WriteHeader(status)

		return
	}

	h.writeHeadObjectResponse(ctx, w, r, bucketName, key, out)
}

// writeHeadObjectResponse builds the HEAD response headers (common, SSE,
// content-encoding/disposition, expiration) and dispatches an access-log
// record. Extracted so headObject stays under the funlen cap.
func (h *S3Handler) writeHeadObjectResponse(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
	out *s3.HeadObjectOutput,
) {
	details := objectCommonDetails{
		Metadata:          out.Metadata,
		ETag:              out.ETag,
		ContentType:       out.ContentType,
		ContentLength:     out.ContentLength,
		LastModified:      out.LastModified,
		VersionID:         out.VersionId,
		StorageClass:      string(out.StorageClass),
		ChecksumCRC32:     out.ChecksumCRC32,
		ChecksumCRC32C:    out.ChecksumCRC32C,
		ChecksumSHA1:      out.ChecksumSHA1,
		ChecksumSHA256:    out.ChecksumSHA256,
		ChecksumCRC64NVME: out.ChecksumCRC64NVME,
		SSEAlgorithm:      string(out.ServerSideEncryption),
		SSEKMSKeyID:       aws.ToString(out.SSEKMSKeyId),
		SSECAlgorithm:     aws.ToString(out.SSECustomerAlgorithm),
		SSECKeyMD5:        aws.ToString(out.SSECustomerKeyMD5),
	}

	h.setCommonHeaders(w, details)
	setSSEHeaders(w, details)
	h.setExpirationHeader(ctx, w, bucketName, key, out.LastModified)

	if ce := aws.ToString(out.ContentEncoding); ce != "" {
		w.Header().Set("Content-Encoding", ce)
	}
	if cd := aws.ToString(out.ContentDisposition); cd != "" {
		w.Header().Set("Content-Disposition", cd)
	}

	applyResponseOverrideHeaders(w, r)

	h.dispatchAccessLog(ctx, r, bucketName, "REST.HEAD.OBJECT", key, http.StatusOK, 0)

	w.WriteHeader(http.StatusOK)
}
