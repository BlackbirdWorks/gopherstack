package s3

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func (h *S3Handler) getObject(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
) {
	h.setOperation(ctx, "GetObject")

	// If the bucket has an Object Lambda configuration, delegate to the lambda path.
	if lambdaARN := h.objectLambdaARN(bucketName); lambdaARN != "" {
		h.handleObjectLambdaGetObject(ctx, w, r, bucketName, key, lambdaARN)

		return
	}

	if err := validateExpectedBucketOwner(r); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	if err := h.authorizeObjectAccess(ctx, r, bucketName, key, actionGetObject); err != nil {
		WriteError(ctx, w, r, err)

		return
	}

	// Propagate SSE-C key on the request through to the backend so envelope-
	// encrypted versions can be decrypted. Errors here are surfaced before we
	// touch any state.
	newCtx, ok := h.attachSSEInfoToCtx(ctx, w, r)
	if !ok {
		return
	}
	ctx = newCtx

	versionID := r.URL.Query().Get("versionId")
	logger.Load(ctx).DebugContext(
		ctx,
		"S3 getObject input",
		"bucket",
		bucketName,
		"key",
		key,
		"versionId",
		versionID,
	)

	var vid *string
	if versionID != "" {
		vid = aws.String(versionID)
	}

	ver, err := h.Backend.GetObject(ctx, &s3.GetObjectInput{
		Bucket:    aws.String(bucketName),
		Key:       aws.String(key),
		VersionId: vid,
	})
	if err != nil {
		h.writeGetObjectError(ctx, w, r, err)

		return
	}
	defer ver.Body.Close()

	if validateErr := validateSSECOnRead(
		r, aws.ToString(ver.SSECustomerAlgorithm), aws.ToString(ver.SSECustomerKeyMD5),
	); validateErr != nil {
		WriteError(ctx, w, r, validateErr)

		return
	}

	if status, condOK := checkConditionalHeaders(r, aws.ToString(ver.ETag), aws.ToTime(ver.LastModified)); !condOK {
		w.WriteHeader(status)

		return
	}

	h.setGetObjectResponseHeaders(ctx, w, r, bucketName, key, ver, buildGetObjectDetails(ver))

	if served := h.serveObjectBody(ctx, w, r, ver); served {
		return
	}

	logger.Load(ctx).DebugContext(
		ctx,
		"S3 getObject output",
		"bucket", bucketName, "key", key, "etag", aws.ToString(ver.ETag),
		"contentLength", aws.ToInt64(ver.ContentLength),
	)

	w.WriteHeader(http.StatusOK)

	written, copyErr := io.Copy(w, ver.Body)
	if copyErr != nil {
		logger.Load(ctx).ErrorContext(ctx, "failed to write object data", "error", copyErr)
	}

	h.dispatchAccessLog(ctx, r, bucketName, "REST.GET.OBJECT", key, http.StatusOK, written)
}

// writeGetObjectError renders the correct error response for a failed GetObject.
// A delete-marker error carries the x-amz-delete-marker header (and Allow: DELETE
// for a version-targeted request); all other errors defer to WriteError.
func (h *S3Handler) writeGetObjectError(
	ctx context.Context, w http.ResponseWriter, r *http.Request, err error,
) {
	if errors.Is(err, ErrDeleteMarker) || errors.Is(err, ErrLatestDeleteMarker) {
		w.Header().Set("X-Amz-Delete-Marker", "true")
		if errors.Is(err, ErrDeleteMarker) {
			w.Header().Set("Allow", "DELETE")
		}
	}

	WriteError(ctx, w, r, err)
}

// setGetObjectResponseHeaders writes all response headers for GetObject.
func (h *S3Handler) setGetObjectResponseHeaders(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	bucketName, key string,
	ver *s3.GetObjectOutput,
	details objectCommonDetails,
) {
	h.setCommonHeaders(w, details)
	setSSEHeaders(w, details)
	h.setExpirationHeader(ctx, w, bucketName, key, ver.LastModified)

	if ce := aws.ToString(ver.ContentEncoding); ce != "" {
		w.Header().Set("Content-Encoding", ce)
	}

	if cd := aws.ToString(ver.ContentDisposition); cd != "" {
		w.Header().Set("Content-Disposition", cd)
	}

	if r.Header.Get("X-Amz-Checksum-Mode") == "ENABLED" {
		h.handleChecksumMode(w, ver, details)
	}

	applyResponseOverrideHeaders(w, r)
}

// responseOverrideParams maps the AWS GetObject/HeadObject response-override
// query parameters to the HTTP response header each one replaces. These let a
// caller (commonly via a presigned URL) override the object's stored headers,
// e.g. response-content-disposition to force a download filename.
var responseOverrideParams = map[string]string{ //nolint:gochecknoglobals // fixed lookup table
	"response-content-type":        "Content-Type",
	"response-content-language":    "Content-Language",
	"response-expires":             "Expires",
	"response-cache-control":       "Cache-Control",
	"response-content-disposition": "Content-Disposition",
	"response-content-encoding":    "Content-Encoding",
}

// applyResponseOverrideHeaders applies any response-* query-parameter overrides
// to the outgoing headers, matching real S3 GetObject/HeadObject behaviour. An
// override always wins over the object's stored header value.
func applyResponseOverrideHeaders(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	for param, header := range responseOverrideParams {
		if v := q.Get(param); v != "" {
			w.Header().Set(header, v)
		}
	}
}

// serveObjectBody handles range requests and writes the object body.
// Returns true if the response was fully handled (range served or error written).
// ifRangeMatches reports whether a Range request should be served given the
// If-Range header. With no If-Range the range always applies. An ETag-form
// If-Range matches when it equals the current ETag; an HTTP-date form matches
// when the object was not modified after that date. A non-match means the caller
// should return the full object.
func ifRangeMatches(r *http.Request, etag string, lastModified time.Time) bool {
	ifRange := r.Header.Get("If-Range")
	if ifRange == "" {
		return true
	}

	if strings.HasPrefix(ifRange, "\"") || strings.HasPrefix(ifRange, "W/") {
		return strings.Trim(ifRange, "\"") == strings.Trim(etag, "\"")
	}

	if t, err := http.ParseTime(ifRange); err == nil {
		return !lastModified.After(t)
	}

	return false
}

func (h *S3Handler) serveObjectBody(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	ver *s3.GetObjectOutput,
) bool {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		return false
	}

	// Honor If-Range: when it no longer matches the current ETag/Last-Modified,
	// AWS ignores the Range and returns the full 200 representation.
	if !ifRangeMatches(r, aws.ToString(ver.ETag), aws.ToTime(ver.LastModified)) {
		return false
	}

	data, readErr := io.ReadAll(ver.Body)
	if readErr != nil {
		WriteError(ctx, w, r, readErr)

		return true
	}

	if h.serveRange(ctx, w, r, data, rangeHeader) {
		return true
	}

	ver.Body = io.NopCloser(bytes.NewReader(data))

	return false
}

// buildGetObjectDetails packs the response-header fields from a backend
// GetObject result. Lives outside getObject so the parent function fits
// under the funlen cap.
func buildGetObjectDetails(ver *s3.GetObjectOutput) objectCommonDetails {
	return objectCommonDetails{
		Metadata:          ver.Metadata,
		ETag:              ver.ETag,
		ContentType:       ver.ContentType,
		ContentLength:     ver.ContentLength,
		LastModified:      ver.LastModified,
		VersionID:         ver.VersionId,
		StorageClass:      string(ver.StorageClass),
		ChecksumCRC32:     ver.ChecksumCRC32,
		ChecksumCRC32C:    ver.ChecksumCRC32C,
		ChecksumSHA1:      ver.ChecksumSHA1,
		ChecksumSHA256:    ver.ChecksumSHA256,
		ChecksumCRC64NVME: ver.ChecksumCRC64NVME,
		SSEAlgorithm:      string(ver.ServerSideEncryption),
		SSEKMSKeyID:       aws.ToString(ver.SSEKMSKeyId),
		SSECAlgorithm:     aws.ToString(ver.SSECustomerAlgorithm),
		SSECKeyMD5:        aws.ToString(ver.SSECustomerKeyMD5),
	}
}

// attachSSEInfoToCtx extracts SSE-* headers from the request, validates the
// SSE-C key/MD5 pair if present, and returns a context carrying the parsed
// sseInfo so backends can apply envelope encryption/decryption. The bool
// return is false when validation failed (the handler has already written
// the error to w).
func (h *S3Handler) attachSSEInfoToCtx(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
) (context.Context, bool) {
	sse, err := extractSSEInfo(r)
	if err != nil {
		WriteError(ctx, w, r, err)

		return ctx, false
	}

	return context.WithValue(ctx, sseKey, sse), true
}

// rangeResult classifies the outcome of parsing a Range header, so the caller
// can reproduce S3's three distinct behaviors:
//   - rangeOK: a satisfiable range -> 206 Partial Content.
//   - rangeIgnore: a malformed/unparseable range (e.g. unknown unit, or
//     last-byte-pos < first-byte-pos) -> S3 ignores it and returns the full
//     object with 200 OK.
//   - rangeUnsatisfiable: a syntactically valid range whose first-byte-pos is
//     at or beyond the object size -> 416 with an InvalidRange XML body.
type rangeResult int

const (
	rangeIgnore rangeResult = iota
	rangeOK
	rangeUnsatisfiable
)

func (h *S3Handler) serveRange(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	data []byte,
	rangeHeader string,
) bool {
	total := int64(len(data))
	start, end, result := parseRange(rangeHeader, total)

	switch result {
	case rangeIgnore:
		// Malformed range: fall through to a normal full-object response (200).
		return false
	case rangeUnsatisfiable:
		h.writeInvalidRange(ctx, w, r, rangeHeader, total)

		return true
	case rangeOK:
	}

	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, total))
	w.Header().Set("Content-Length", strconv.FormatInt(end-start+1, 10))
	w.WriteHeader(http.StatusPartialContent)

	// #nosec G705
	if _, err := w.Write(data[start : end+1]); err != nil {
		logger.Load(ctx).ErrorContext(ctx, "failed to write range data", "error", err)
	}

	return true
}

// writeInvalidRange emits S3's 416 response for an unsatisfiable Range request:
// a Content-Range header advertising the actual size and an InvalidRange XML
// body carrying the rejected range and object size.
func (h *S3Handler) writeInvalidRange(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	rangeHeader string,
	size int64,
) {
	// Drop the full-object Content-Length set earlier by setCommonHeaders so the
	// XML error body's length is advertised correctly instead.
	w.Header().Del("Content-Length")
	w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", size))
	httputils.WriteS3ErrorResponse(ctx, w, r, InvalidRangeError{
		Code:             "InvalidRange",
		Message:          "The requested range is not satisfiable",
		RangeRequested:   rangeHeader,
		ActualObjectSize: size,
		Resource:         r.URL.Path,
	}, http.StatusRequestedRangeNotSatisfiable)
}

// parseRange parses a "bytes=X-Y" Range header and returns clamped [start, end]
// indices together with a rangeResult classifying how S3 should respond.
func parseRange(header string, size int64) (int64, int64, rangeResult) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, rangeIgnore
	}

	const rangeSpecMaxParts = 2
	// S3 honors only the first range when several are supplied.
	spec := strings.TrimSpace(strings.SplitN(header[len("bytes="):], ",", rangeSpecMaxParts)[0])
	startStr, endStr, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, rangeIgnore
	}

	// bytes=-0 is a syntactically valid suffix range that selects zero bytes;
	// RFC 9110 §14.1.2 requires 416.
	if startStr == "" && endStr == "0" {
		return 0, 0, rangeUnsatisfiable
	}

	start, end, ok := computeRangeBounds(startStr, endStr, size)
	if !ok {
		return 0, 0, rangeIgnore
	}

	// A first-byte-pos at or beyond the object size is unsatisfiable: S3 returns
	// 416 InvalidRange. A suffix range ("-N") always resolves to a satisfiable
	// window (clamped to the object), so it is never unsatisfiable here.
	if start >= size {
		return 0, 0, rangeUnsatisfiable
	}

	// last-byte-pos < first-byte-pos is malformed; S3 ignores it (full object).
	if start > end {
		return 0, 0, rangeIgnore
	}

	if end >= size {
		end = size - 1
	}

	return start, end, rangeOK
}

// computeRangeBounds resolves the raw first/last byte positions from the two
// halves of a "X-Y" range spec. The bool is false when the spec is malformed.
func computeRangeBounds(startStr, endStr string, size int64) (int64, int64, bool) {
	switch {
	case startStr == "":
		n, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || n < 0 {
			return 0, 0, false
		}
		// bytes=-0 is unsatisfiable per RFC 9110 §14.1.2 (no bytes selected).
		if n == 0 {
			return 0, 0, false
		}

		return max(size-n, 0), size - 1, true
	case endStr == "":
		start, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil || start < 0 {
			return 0, 0, false
		}

		return start, size - 1, true
	default:
		start, err := strconv.ParseInt(startStr, 10, 64)
		if err != nil || start < 0 {
			return 0, 0, false
		}

		end, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || end < 0 {
			return 0, 0, false
		}

		return start, end, true
	}
}

// checkConditionalHeaders evaluates HTTP conditional request headers per AWS/HTTP spec.
// Returns (304, false) or (412, false) if a condition fails, or (0, true) if all pass.
func checkConditionalHeaders(r *http.Request, etag string, lastModified time.Time) (int, bool) {
	// First evaluate 412-returning conditions (If-Match, If-Unmodified-Since).
	if status, ok := evaluatePreconditions(r.Header, standardConditionals, etag, lastModified); !ok {
		return status, false
	}

	// Then evaluate 304-returning conditions (If-None-Match, If-Modified-Since).
	stripQ := func(s string) string { return strings.Trim(s, "\"") }
	normalizedETag := stripQ(etag)

	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		// "*" matches any existing representation; this is only reached when the
		// object exists, so it always yields 304.
		if ifNoneMatch == "*" || stripQ(ifNoneMatch) == normalizedETag {
			return http.StatusNotModified, false
		}
	}

	if ifModSince := r.Header.Get("If-Modified-Since"); ifModSince != "" {
		if t, err := http.ParseTime(ifModSince); err == nil && !lastModified.After(t) {
			return http.StatusNotModified, false
		}
	}

	return 0, true
}

// handleGetObjectTorrent handles GET /{bucket}/{key}?torrent.
// As of 2022 AWS S3 itself returns NotImplemented for new buckets and the
// operation is being phased out. We mirror that behaviour rather than emit
// an empty/invalid torrent payload that would be parsed as malformed.
func (h *S3Handler) handleGetObjectTorrent(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) {
	h.setOperation(ctx, "GetObjectTorrent")
	WriteError(ctx, w, r, ErrNotImplemented)
}
