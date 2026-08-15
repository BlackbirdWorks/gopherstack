package s3

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (h *S3Handler) listObjectsV2(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "ListObjectsV2")

	if err := h.authorizeObjectAccess(ctx, r, bucketName, "", actionListBucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	q := r.URL.Query()
	input := h.prepareListObjectsV2Input(bucketName, q)

	outV2, err := h.Backend.ListObjectsV2(ctx, input)
	if err != nil {
		h.handleListObjectsV2Error(ctx, w, r, err)

		return
	}

	if aws.ToBool(outV2.IsTruncated) {
		q.Set("is-truncated", "true")
		q.Set("next-continuation-token", aws.ToString(outV2.NextContinuationToken))
	}

	h.renderListObjectsV2Response(ctx, w, r, bucketName, q, outV2.Contents, outV2.CommonPrefixes)
}

func (h *S3Handler) prepareListObjectsV2Input(
	bucketName string,
	q url.Values,
) *s3.ListObjectsV2Input {
	maxKeys := int32(defaultMaxKeys)
	if mk := q.Get("max-keys"); mk != "" {
		if n, err := strconv.Atoi(mk); err == nil && n >= 0 && n <= 1000 {
			maxKeys = int32(n) //nolint:gosec // Validated range
		}
	}

	return &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucketName),
		Prefix:            aws.String(q.Get("prefix")),
		Delimiter:         aws.String(q.Get("delimiter")),
		ContinuationToken: aws.String(q.Get("continuation-token")),
		StartAfter:        aws.String(q.Get("start-after")),
		MaxKeys:           aws.Int32(maxKeys),
		EncodingType:      types.EncodingType(q.Get("encoding-type")),
		FetchOwner:        aws.Bool(q.Get("fetch-owner") == sqlValTrue),
	}
}

func (h *S3Handler) handleListObjectsV2Error(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	err error,
) {
	if errors.Is(err, ErrNoSuchBucket) {
		WriteError(ctx, w, r, err)

		return
	}
	WriteError(ctx, w, r, err)
}

func (h *S3Handler) renderListObjectsV2Response(
	ctx context.Context,
	w http.ResponseWriter,
	_ *http.Request,
	bucketName string,
	q url.Values,
	objects []types.Object,
	commonPrefixes []types.CommonPrefix,
) {
	isTruncated := q.Get("is-truncated") == "true"
	nextCont := q.Get("next-continuation-token")
	encodingType := q.Get("encoding-type")

	resp := ListBucketV2Result{
		Name:                  bucketName,
		Prefix:                encodeListKey(encodingType, q.Get("prefix")),
		Delimiter:             encodeListKey(encodingType, q.Get("delimiter")),
		ContinuationToken:     q.Get("continuation-token"),
		StartAfter:            encodeListKey(encodingType, q.Get("start-after")),
		MaxKeys:               defaultMaxKeys,
		EncodingType:          encodingType,
		IsTruncated:           isTruncated,
		NextContinuationToken: nextCont,
	}
	if mk := q.Get("max-keys"); mk != "" {
		if n, err := strconv.Atoi(mk); err == nil && n >= 0 && n <= 1000 {
			resp.MaxKeys = n
		}
	}

	seenPrefixes := make(map[string]struct{})
	// ListObjectsV2 only includes Owner on each Contents item when the request's
	// FetchOwner is true (s3@v1.106.5 api_op_ListObjectsV2.go's FetchOwner doc:
	// "the owner field is not returned" by default) -- unlike ListObjects V1,
	// which has no such request member and always includes it.
	resp.Contents, resp.CommonPrefixes = h.mapObjectsToXML(
		objects,
		q.Get("prefix"),
		q.Get("delimiter"),
		seenPrefixes,
		encodingType,
		q.Get("fetch-owner") == sqlValTrue,
	)
	// Add common prefixes from backend (if any)
	for _, cp := range commonPrefixes {
		resp.CommonPrefixes = append(
			resp.CommonPrefixes,
			CommonPrefixXML{Prefix: encodeListKey(encodingType, aws.ToString(cp.Prefix))},
		)
	}
	resp.KeyCount = len(resp.Contents) + len(resp.CommonPrefixes)

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
}
