package s3

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *S3Handler) listObjects(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "ListObjects")

	if err := h.authorizeObjectAccess(ctx, r, bucketName, "", actionListBucket); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	prefix := r.URL.Query().Get("prefix")
	delimiter := r.URL.Query().Get("delimiter")
	marker := r.URL.Query().Get("marker")
	encodingType := r.URL.Query().Get("encoding-type")

	logger.Load(ctx).DebugContext(
		ctx,
		"S3 listObjects input",
		"bucket", bucketName, "prefix", prefix, "delimiter", delimiter, "marker", marker,
	)

	// n is provably in [0, defaultMaxKeys] before the int32 conversion: it
	// starts at the constant default and is only reassigned to a parsed value
	// that is non-negative and strictly less than defaultMaxKeys. AWS clamps
	// MaxKeys to [0, 1000] rather than rejecting an over-limit value, so a
	// value at or above the limit is treated as the limit.
	n := defaultMaxKeys
	if mk := r.URL.Query().Get("max-keys"); mk != "" {
		if v, err := strconv.Atoi(mk); err == nil && v >= 0 && v < defaultMaxKeys {
			n = v
		}
	}
	maxKeys := int32(n)

	// Pass marker and delimiter to backend so it can seek and group correctly.
	out, err := h.Backend.ListObjects(ctx, &s3.ListObjectsInput{
		Bucket:    aws.String(bucketName),
		Prefix:    aws.String(prefix),
		MaxKeys:   aws.Int32(maxKeys),
		Delimiter: aws.String(delimiter),
		Marker:    aws.String(marker),
	})
	if errors.Is(err, ErrNoSuchBucket) {
		WriteError(ctx, w, r, err)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// Use the backend's pagination state. AWS only returns NextMarker when a
	// delimiter is specified; without one, clients use the last key as marker.
	isTruncated := aws.ToBool(out.IsTruncated)
	nextMarker := ""
	if isTruncated && delimiter != "" {
		nextMarker = aws.ToString(out.NextMarker)
	}

	logger.Load(ctx).DebugContext(
		ctx,
		"S3 listObjects output",
		"bucket", bucketName, "objectCount", len(out.Contents), "isTruncated", isTruncated,
	)

	resp := ListBucketResult{
		Name:         bucketName,
		Prefix:       encodeListKey(encodingType, prefix),
		Delimiter:    encodeListKey(encodingType, delimiter),
		Marker:       encodeListKey(encodingType, marker),
		NextMarker:   encodeListKey(encodingType, nextMarker),
		EncodingType: encodingType,
		MaxKeys:      int(maxKeys),
		IsTruncated:  isTruncated,
	}

	seenPrefixes := make(map[string]struct{})
	// mapObjectsToXML formats regular objects. When delimiter is set, the backend
	// already grouped CP-objects into out.CommonPrefixes and removed them from
	// out.Contents, so mapObjectsToXML will not produce duplicate CPs.
	resp.Contents, resp.CommonPrefixes = h.mapObjectsToXML(
		out.Contents,
		prefix,
		delimiter,
		seenPrefixes,
		encodingType,
	)
	// Merge backend-level common prefixes (populated when delimiter is set).
	for _, cp := range out.CommonPrefixes {
		p := aws.ToString(cp.Prefix)
		if _, seen := seenPrefixes[p]; !seen {
			seenPrefixes[p] = struct{}{}
			resp.CommonPrefixes = append(
				resp.CommonPrefixes,
				CommonPrefixXML{Prefix: encodeListKey(encodingType, p)},
			)
		}
	}

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
}

func (h *S3Handler) mapObjectsToXML(
	objects []types.Object,
	prefix, delimiter string,
	seenPrefixes map[string]struct{},
	encodingType string,
) ([]ObjectXML, []CommonPrefixXML) {
	var contents []ObjectXML
	var commonPrefixes []CommonPrefixXML

	for _, obj := range objects {
		key := *obj.Key
		if cp, isCommon := commonPrefixFor(key, prefix, delimiter); isCommon {
			if _, seen := seenPrefixes[cp]; !seen {
				seenPrefixes[cp] = struct{}{}
				commonPrefixes = append(
					commonPrefixes,
					CommonPrefixXML{Prefix: encodeListKey(encodingType, cp)},
				)
			}

			continue
		}

		checksumAlgo := ""
		if len(obj.ChecksumAlgorithm) > 0 {
			checksumAlgo = string(obj.ChecksumAlgorithm[0])
		}

		sc := string(obj.StorageClass)
		if sc == "" {
			sc = storageStandard
		}
		contents = append(contents, ObjectXML{
			Key:               encodeListKey(encodingType, key),
			LastModified:      obj.LastModified.Format(time.RFC3339),
			Size:              *obj.Size,
			ETag:              aws.ToString(obj.ETag),
			StorageClass:      sc,
			ChecksumAlgorithm: checksumAlgo,
		})
	}

	return contents, commonPrefixes
}

func commonPrefixFor(key, prefix, delimiter string) (string, bool) {
	if delimiter == "" {
		return "", false
	}

	rest := strings.TrimPrefix(key, prefix)
	idx := strings.Index(rest, delimiter)

	if idx < 0 {
		return "", false
	}

	return prefix + rest[:idx+len(delimiter)], true
}

func (h *S3Handler) listObjectVersions(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName string,
) {
	h.setOperation(ctx, "ListObjectVersions")
	q := r.URL.Query()
	prefix := q.Get("prefix")
	keyMarker := q.Get("key-marker")
	versionIDMarker := q.Get("version-id-marker")
	delimiter := q.Get("delimiter")
	encodingType := q.Get("encoding-type")

	// n is provably in [0, defaultMaxKeys] before the int32 conversion: it
	// starts at the constant default and is only reassigned to a parsed value
	// that is positive and no greater than defaultMaxKeys.
	n := defaultMaxKeys
	if mk := q.Get("max-keys"); mk != "" {
		if v, err := strconv.Atoi(mk); err == nil && v >= 0 && v <= defaultMaxKeys {
			n = v
		}
	}
	maxKeys := int32(n)

	input := &s3.ListObjectVersionsInput{
		Bucket:          aws.String(bucketName),
		Prefix:          aws.String(prefix),
		KeyMarker:       aws.String(keyMarker),
		VersionIdMarker: aws.String(versionIDMarker),
		Delimiter:       aws.String(delimiter),
		MaxKeys:         aws.Int32(maxKeys),
	}

	out, err := h.Backend.ListObjectVersions(ctx, input)
	if errors.Is(err, ErrNoSuchBucket) {
		WriteError(ctx, w, r, err)

		return
	}

	if err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	resp := ListVersionsResult{
		Name:                bucketName,
		Prefix:              encodeListKey(encodingType, prefix),
		KeyMarker:           encodeListKey(encodingType, keyMarker),
		VersionIDMarker:     versionIDMarker,
		NextKeyMarker:       encodeListKey(encodingType, aws.ToString(out.NextKeyMarker)),
		NextVersionIDMarker: aws.ToString(out.NextVersionIdMarker),
		MaxKeys:             int(maxKeys),
		IsTruncated:         aws.ToBool(out.IsTruncated),
		Delimiter:           encodeListKey(encodingType, delimiter),
		EncodingType:        encodingType,
	}

	mapListVersionsOutput(&resp, out, encodingType)

	httputils.WriteXML(ctx, w, http.StatusOK, resp)
}

// mapListVersionsOutput maps the backend ListObjectVersions output (SDK types)
// into the XML response, applying the requested encoding type to keys/prefixes.
func mapListVersionsOutput(
	resp *ListVersionsResult,
	out *s3.ListObjectVersionsOutput,
	encodingType string,
) {
	for _, v := range out.Versions {
		size := int64(0)
		if v.Size != nil {
			size = *v.Size
		}
		etag := ""
		if v.ETag != nil {
			etag = *v.ETag
		}
		resp.Versions = append(resp.Versions, ObjectVersionXML{
			Key:          encodeListKey(encodingType, *v.Key),
			VersionID:    *v.VersionId,
			IsLatest:     *v.IsLatest,
			LastModified: v.LastModified.Format(time.RFC3339),
			ETag:         etag,
			Size:         size,
			Owner: &Owner{
				ID:          gopherstackName,
				DisplayName: gopherstackName,
			},
			StorageClass: storageStandard,
		})
	}

	for _, d := range out.DeleteMarkers {
		resp.DeleteMarkers = append(resp.DeleteMarkers, DeleteMarkerXML{
			Key:          encodeListKey(encodingType, *d.Key),
			VersionID:    *d.VersionId,
			IsLatest:     *d.IsLatest,
			LastModified: d.LastModified.Format(time.RFC3339),
			Owner: &Owner{
				ID:          gopherstackName,
				DisplayName: gopherstackName,
			},
		})
	}

	for _, cp := range out.CommonPrefixes {
		resp.CommonPrefixes = append(
			resp.CommonPrefixes,
			CommonPrefixXML{Prefix: encodeListKey(encodingType, aws.ToString(cp.Prefix))},
		)
	}
}
