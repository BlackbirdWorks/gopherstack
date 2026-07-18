package s3

import (
	"net/http"
	"strings"
	"time"
)

// copyConditionalParams holds the header name prefixes for conditional evaluation.
type copyConditionalParams struct {
	ifMatch           string
	ifUnmodifiedSince string
	ifNoneMatch       string
	ifModifiedSince   string
}

// standardConditionals are the standard HTTP/S3 conditional header names.
var standardConditionals = copyConditionalParams{ //nolint:gochecknoglobals // fixed constant set
	ifMatch:           "If-Match",
	ifUnmodifiedSince: "If-Unmodified-Since",
	ifNoneMatch:       "If-None-Match",
	ifModifiedSince:   "If-Modified-Since",
}

// copySourceConditionals are the x-amz-copy-source-if-* conditional header names.
var copySourceConditionals = copyConditionalParams{ //nolint:gochecknoglobals // fixed constant set
	ifMatch:           "X-Amz-Copy-Source-If-Match",
	ifUnmodifiedSince: "X-Amz-Copy-Source-If-Unmodified-Since",
	ifNoneMatch:       "X-Amz-Copy-Source-If-None-Match",
	ifModifiedSince:   "X-Amz-Copy-Source-If-Modified-Since",
}

// evaluatePreconditions checks only the 412-returning conditions (if-match and
// if-unmodified-since) from a set of conditional headers. Returns (412, false) on failure.
func evaluatePreconditions(
	h http.Header,
	params copyConditionalParams,
	etag string,
	lastModified time.Time,
) (int, bool) {
	stripQ := func(s string) string { return strings.Trim(s, "\"") }
	normalizedETag := stripQ(etag)

	// if-match: fail 412 when ETag does NOT match. "*" matches any existing
	// representation, so it always passes here (callers only evaluate this for
	// objects that exist).
	if v := h.Get(params.ifMatch); v != "" && v != "*" {
		if stripQ(v) != normalizedETag {
			return http.StatusPreconditionFailed, false
		}
	}

	// if-unmodified-since: fail 412 when modified after the given date.
	if v := h.Get(params.ifUnmodifiedSince); v != "" {
		if t, err := http.ParseTime(v); err == nil && lastModified.After(t) {
			return http.StatusPreconditionFailed, false
		}
	}

	return 0, true
}

// evaluateCopySourceConditionals checks all four x-amz-copy-source-if-* headers.
// All failures return 412 (copy conditionals don't have a 304 variant).
func evaluateCopySourceConditionals(
	h http.Header,
	params copyConditionalParams,
	etag string,
	lastModified time.Time,
) (int, bool) {
	if status, ok := evaluatePreconditions(h, params, etag, lastModified); !ok {
		return status, false
	}

	stripQ := func(s string) string { return strings.Trim(s, "\"") }
	normalizedETag := stripQ(etag)

	// if-none-match: fail 412 when ETag matches (copy variant → 412 not 304).
	// "*" matches any existing representation.
	if v := h.Get(params.ifNoneMatch); v != "" {
		if v == "*" || stripQ(v) == normalizedETag {
			return http.StatusPreconditionFailed, false
		}
	}

	// if-modified-since: fail 412 when NOT modified after the given date (copy → 412).
	if v := h.Get(params.ifModifiedSince); v != "" {
		if t, err := http.ParseTime(v); err == nil && !lastModified.After(t) {
			return http.StatusPreconditionFailed, false
		}
	}

	return 0, true
}

// checkCopySourceConditionals evaluates the four x-amz-copy-source-if-* headers
// against the source object's ETag and LastModified. Returns false and a 412
// status when a precondition fails.
func checkCopySourceConditionals(
	r *http.Request,
	srcETag string,
	srcLastModified time.Time,
) (int, bool) {
	return evaluateCopySourceConditionals(
		r.Header,
		copySourceConditionals,
		srcETag,
		srcLastModified,
	)
}
