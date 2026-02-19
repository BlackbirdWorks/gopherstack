package s3

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"

	"Gopherstack/pkgs/httputils"
	"Gopherstack/pkgs/logger"

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
	log := logger.Load(ctx)
	q := r.URL.Query()
	input := h.prepareListObjectsV2Input(bucketName, q)

	outV2, err := h.Backend.ListObjectsV2(ctx, input)
	if err != nil {
		h.handleListObjectsV2Error(log, w, r, err)

		return
	}

	h.renderListObjectsV2Response(ctx, w, r, bucketName, q, outV2.Contents)
}

func (h *S3Handler) prepareListObjectsV2Input(
	bucketName string,
	q url.Values,
) *s3.ListObjectsV2Input {
	maxKeys := int32(defaultMaxKeys)
	if mk := q.Get("max-keys"); mk != "" {
		if n, err := strconv.Atoi(mk); err == nil && n >= 0 {
			maxKeys = int32(n) //nolint:gosec // Validated non-negative
		}
	}

	return &s3.ListObjectsV2Input{
		Bucket:            aws.String(bucketName),
		Prefix:            aws.String(q.Get("prefix")),
		ContinuationToken: aws.String(q.Get("continuation-token")),
		StartAfter:        aws.String(q.Get("start-after")),
		MaxKeys:           aws.Int32(maxKeys),
	}
}

func (h *S3Handler) handleListObjectsV2Error(
	log *slog.Logger,
	w http.ResponseWriter,
	r *http.Request,
	err error,
) {
	if errors.Is(err, ErrNoSuchBucket) {
		httputils.WriteError(log, w, r, err, http.StatusNotFound)

		return
	}
	httputils.WriteError(log, w, r, err, http.StatusInternalServerError)
}

func (h *S3Handler) renderListObjectsV2Response(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
	q url.Values,
	objects []types.Object,
) {
	log := logger.Load(ctx)
	maxKeys := defaultMaxKeys
	if mk := q.Get("max-keys"); mk != "" {
		if n, err := strconv.Atoi(mk); err == nil && n > 0 {
			maxKeys = n
		}
	}

	startCursor := q.Get("start-after")
	if ct := q.Get("continuation-token"); ct != "" {
		startCursor = ct
	}

	if startCursor != "" {
		for i, obj := range objects {
			if *obj.Key > startCursor {
				objects = objects[i:]

				break
			}
		}
	}

	isTruncated := false
	var nextToken string
	if len(objects) > maxKeys {
		isTruncated = true
		objects = objects[:maxKeys]
		nextToken = *objects[maxKeys-1].Key
	}

	resp := ListBucketV2Result{
		Name:                  bucketName,
		Prefix:                q.Get("prefix"),
		Delimiter:             q.Get("delimiter"),
		ContinuationToken:     q.Get("continuation-token"),
		StartAfter:            q.Get("start-after"),
		MaxKeys:               maxKeys,
		EncodingType:          q.Get("encoding-type"),
		IsTruncated:           isTruncated,
		NextContinuationToken: nextToken,
	}

	seenPrefixes := make(map[string]struct{})
	resp.Contents, resp.CommonPrefixes = h.mapObjectsToXML(
		r,
		bucketName,
		objects,
		q.Get("prefix"),
		q.Get("delimiter"),
		seenPrefixes,
	)
	resp.KeyCount = len(resp.Contents) + len(resp.CommonPrefixes)

	httputils.WriteXML(log, w, http.StatusOK, resp)
}
