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

// evaluateDestinationConditionals evaluates RenameObject's four DestinationIf*
// conditionals against the rename target, which unlike a copy source may not
// exist yet. The header names are the plain standardConditionals set (If-Match,
// If-None-Match, If-Modified-Since, If-Unmodified-Since): RenameObjectInput's
// DestinationIfMatch/DestinationIfNoneMatch/DestinationIfModifiedSince/
// DestinationIfUnmodifiedSince all serialize to those exact headers (pinned SDK
// serializers.go:9674-9691), not a Destination-prefixed variant. All failures
// return 412 -- RenameObject has no 304 variant.
func evaluateDestinationConditionals(
	h http.Header,
	etag string,
	lastModified time.Time,
	exists bool,
) (int, bool) {
	if destinationIfMatchFails(h, etag, exists) ||
		destinationIfUnmodifiedSinceFails(h, lastModified, exists) ||
		destinationIfNoneMatchFails(h, etag, exists) ||
		destinationIfModifiedSinceFails(h, lastModified, exists) {
		return http.StatusPreconditionFailed, false
	}

	return 0, true
}

// destinationIfMatchFails evaluates DestinationIfMatch. Per RFC 7232 §3.1,
// "*" is false when there is no current representation, so a missing
// destination fails this regardless of the header's value.
func destinationIfMatchFails(h http.Header, etag string, exists bool) bool {
	v := h.Get(standardConditionals.ifMatch)
	if v == "" {
		return false
	}

	stripQ := func(s string) string { return strings.Trim(s, "\"") }

	return !exists || (v != "*" && stripQ(v) != stripQ(etag))
}

// destinationIfUnmodifiedSinceFails evaluates DestinationIfUnmodifiedSince.
// Nothing can be "unmodified" without a destination, so a missing
// destination fails closed rather than passing vacuously.
func destinationIfUnmodifiedSinceFails(h http.Header, lastModified time.Time, exists bool) bool {
	v := h.Get(standardConditionals.ifUnmodifiedSince)
	if v == "" {
		return false
	}

	t, err := http.ParseTime(v)

	return err == nil && (!exists || lastModified.After(t))
}

// destinationIfNoneMatchFails evaluates DestinationIfNoneMatch. "*" is the
// documented create-only case and passes when the destination is absent; a
// specific ETag only fails when it matches.
func destinationIfNoneMatchFails(h http.Header, etag string, exists bool) bool {
	v := h.Get(standardConditionals.ifNoneMatch)
	if v == "" {
		return false
	}

	if v == "*" {
		return exists
	}

	return exists && strings.Trim(v, "\"") == strings.Trim(etag, "\"")
}

// destinationIfModifiedSinceFails evaluates DestinationIfModifiedSince. AWS
// documents this as "renames the object if the destination exists and if it
// has been modified since" -- an explicit existence requirement, so a
// missing destination fails.
func destinationIfModifiedSinceFails(h http.Header, lastModified time.Time, exists bool) bool {
	v := h.Get(standardConditionals.ifModifiedSince)
	if v == "" {
		return false
	}

	t, err := http.ParseTime(v)

	return err == nil && (!exists || !lastModified.After(t))
}
