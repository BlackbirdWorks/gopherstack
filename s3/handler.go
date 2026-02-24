package s3

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// regionContextKey is used to store the AWS region in request context.
type regionContextKey struct{}

// AWS SigV4 credential format has at least 3 parts: AKID/date/region.
const minSigV4CredentialParts = 3

// extractRegionFromRequest extracts the AWS region from an S3 request.
// Tries to extract from Authorization header's credential scope, Host header, or falls back to default.
func extractRegionFromRequest(r *http.Request, defaultRegion string) string {
	// Try to extract from Authorization header (AWS SigV4)
	authHeader := r.Header.Get("Authorization")
	if authHeader != "" && strings.Contains(authHeader, "Credential=") {
		// Extract from "Credential=AKID/date/region/s3/aws4_request"
		parts := strings.Split(authHeader, "Credential=")
		if len(parts) > 1 {
			credParts := strings.Split(parts[1], "/")
			if len(credParts) >= minSigV4CredentialParts {
				return credParts[2]
			}
		}
	}

	// Check for X-Amz-Region header
	if region := r.Header.Get("X-Amz-Region"); region != "" {
		return region
	}

	return defaultRegion
}

const (
	pathSplitParts   = 2
	tagKeyValueParts = 2
	defaultMaxKeys   = 1000

	checksumCRC32   = "CRC32"
	checksumCRC32C  = "CRC32C"
	checksumSHA1    = "SHA1"
	checksumSHA256  = "SHA256"
	storageStandard = "STANDARD"

	maxDeleteObjects = 1000
)

// S3Handler implements the S3-compatible service for object storage operations.
//
//nolint:revive // Stuttering preferred here for clarity per Plan.md
type S3Handler struct {
	Logger        *slog.Logger
	DefaultRegion string
	janitor       *Janitor
	Backend       StorageBackend
	// Endpoint is the base host (e.g. "localhost:9000") of this server.
	// When set, virtual-hosted-style URLs (bucket.host/key) are supported
	// in addition to path-style URLs (/bucket/key).
	Endpoint string
}

// NewHandler creates a new S3 Handler with the given backend.
func NewHandler(backend StorageBackend, logger *slog.Logger) *S3Handler {
	return &S3Handler{
		Backend:       backend,
		Logger:        logger,
		DefaultRegion: "us-east-1",
	}
}

// WithJanitor attaches a background janitor to the handler.
func (h *S3Handler) WithJanitor(settings Settings) *S3Handler {
	h.DefaultRegion = settings.DefaultRegion
	if h.DefaultRegion == "" {
		h.DefaultRegion = "us-east-1"
	}
	if memBackend, ok := h.Backend.(*InMemoryBackend); ok {
		memBackend.SetDefaultRegion(h.DefaultRegion)
		h.janitor = NewJanitor(memBackend, h.Logger, settings)
	}

	return h
}

// StartWorker starts the background janitor if it is configured.
func (h *S3Handler) StartWorker(ctx context.Context) error {
	if h.janitor != nil {
		go h.janitor.Run(ctx)
	}

	return nil
}

type s3Metrics struct {
	operation string
}

type s3ContextKey struct{}

//nolint:gochecknoglobals // Context key must be package-level for consistent use.
var s3Key s3ContextKey

func (h *S3Handler) setOperation(ctx context.Context, op string) {
	if m, ok := ctx.Value(s3Key).(*s3Metrics); ok {
		m.operation = op
	}
}

// GetSupportedOperations returns a list of supported S3 operations.
func (h *S3Handler) GetSupportedOperations() []string {
	return []string{
		"CreateBucket",
		"DeleteBucket",
		"ListBuckets",
		"HeadBucket",
		"GetBucketVersioning",
		"PutBucketVersioning",
		"PutObject",
		"GetObject",
		"HeadObject",
		"DeleteObject",
		"DeleteObjects",
		"CopyObject",
		"ListObjects",
		"ListObjectsV2",
		"ListObjectVersions",
		"PutObjectTagging",
		"GetObjectTagging",
		"DeleteObjectTagging",
		"CreateMultipartUpload",
		"UploadPart",
		"CompleteMultipartUpload",
		"AbortMultipartUpload",
		"ListMultipartUploads",
		"ListParts",
	}
}

// Handler returns the Echo handler function for S3 requests.
func (h *S3Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		metrics := &s3Metrics{operation: "Unknown"}
		ctx = context.WithValue(ctx, s3Key, metrics)

		// Extract region from request and add to context
		region := extractRegionFromRequest(c.Request(), h.DefaultRegion)
		ctx = context.WithValue(ctx, regionContextKey{}, region)

		requestWithCtx := c.Request().WithContext(ctx)
		*c.Request() = *requestWithCtx

		sw := httputil.NewResponseWriter(c.Response())

		log := logger.Load(ctx)

		bucketName, key, ok := h.resolveBucketAndKey(ctx, sw, requestWithCtx)

		if !ok {
			return nil
		}

		log.DebugContext(ctx, "S3 request", "method", requestWithCtx.Method, "bucket", bucketName, "key", key)

		if bucketName == "" {
			if requestWithCtx.Method != http.MethodGet {
				WriteError(log, sw, requestWithCtx, ErrMethodNotAllowed)

				return nil
			}

			h.listBuckets(ctx, sw, requestWithCtx)

			return nil
		}

		if key == "" {
			h.handleBucketOperation(ctx, sw, requestWithCtx, bucketName)

			return nil
		}

		h.handleObjectOperation(ctx, sw, requestWithCtx, bucketName, key)

		return nil
	}
}

// Name returns the service identifier.
func (h *S3Handler) Name() string {
	return "S3"
}

// RouteMatcher returns a matcher that accepts all S3 requests (catch-all).
// With priority-based routing, S3 is matched last due to low priority.
// Excludes API endpoints and dashboard routes which take precedence.
func (h *S3Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path
		// Exclude API and dashboard endpoints - let them be handled by other routes
		// Matches /api/, /metrics/, /dashboard/ but NOT /api, /metrics, /dashboard
		// which could be valid bucket names.
		if strings.HasPrefix(path, "/api/") || strings.HasPrefix(path, "/metrics/") ||
			strings.HasPrefix(path, "/dashboard/") || path == "/favicon.ico" || path == "/robots.txt" {
			return false
		}
		// Accept all other requests - priority ensures we're evaluated last
		return true
	}
}

// MatchPriority returns the priority for the S3 matcher.
// Catch-all matchers have the lowest priority (0), ensuring other services match first.
func (h *S3Handler) MatchPriority() int {
	return 0
}

// ExtractOperation returns the current S3 operation from context.
func (h *S3Handler) ExtractOperation(c *echo.Context) string {
	ctx := c.Request().Context()
	if m, ok := ctx.Value(s3Key).(*s3Metrics); ok {
		return m.operation
	}

	return "Unknown"
}

// ExtractResource returns the bucket name for this request.
func (h *S3Handler) ExtractResource(c *echo.Context) string {
	ctx := c.Request().Context()
	bucketName, _, _ := h.resolveBucketAndKey(ctx, c.Response(), c.Request())

	return bucketName
}

// resolveBucketAndKey extracts the bucket name and object key from the request.
// It supports both path-style (/bucket/key) and virtual-hosted-style (bucket.host/key).
// Returns (bucket, key, true) on success, or ("", "", false) when an error response
// has already been written.
func (h *S3Handler) resolveBucketAndKey(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
) (string, string, bool) {
	log := logger.Load(ctx)
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", pathSplitParts)

	// Try virtual-hosted-style first: bucket name as subdomain in Host header.
	if vhBucket := h.extractVirtualHostedBucketName(r); vhBucket != "" {
		bucket := vhBucket
		key := path
		if key != "" && !IsValidObjectKey(key) {
			WriteError(log, w, r, ErrInvalidArgument)

			return "", "", false
		}

		return bucket, key, true
	}

	// Fall back to path-style (/bucket/key).
	bucket, key := "", ""
	if path != "" && path != "/" {
		bucket = parts[0]
		if !IsValidBucketName(bucket) {
			WriteError(log, w, r, ErrInvalidBucketName)

			return "", "", false
		}

		if len(parts) > 1 {
			key = parts[1]
			if key != "" && !IsValidObjectKey(key) {
				WriteError(log, w, r, ErrInvalidArgument)

				return "", "", false
			}
		}
	}

	return bucket, key, true
}

// extractVirtualHostedBucketName returns the bucket name from the Host header
// when using virtual-hosted-style URLs (e.g. my-bucket.localhost:8080).
// Returns "" when Endpoint is not configured or the Host does not match.
func (h *S3Handler) extractVirtualHostedBucketName(r *http.Request) string {
	if h.Endpoint == "" {
		return ""
	}

	reqHost := r.Host
	if reqHost == "" {
		return ""
	}

	// Normalise both sides: strip ports before comparing.
	baseHost := h.Endpoint
	if stripped, _, err := net.SplitHostPort(baseHost); err == nil {
		baseHost = stripped
	}

	reqHostNoPort := reqHost
	if stripped, _, err := net.SplitHostPort(reqHost); err == nil {
		reqHostNoPort = stripped
	}

	// Request Host must end with ".<baseHost>".
	suffix := "." + baseHost
	if !strings.HasSuffix(reqHostNoPort, suffix) {
		return ""
	}

	// candidate is everything before ".<baseHost>" (e.g. "bucket.s3.us-east-1")
	candidate := reqHostNoPort[:len(reqHostNoPort)-len(suffix)]

	// Some SDKs use <bucket>.s3.<region> or <bucket>.s3
	// We want to extract just <bucket>.
	parts := strings.Split(candidate, ".")
	if len(parts) > 1 {
		// Check for s3. or s3-<region>. parts
		for i, p := range parts {
			if p == "s3" || strings.HasPrefix(p, "s3-") {
				// The bucket is everything before the first 's3' part.
				bucket := strings.Join(parts[:i], ".")
				if IsValidBucketName(bucket) {
					return bucket
				}

				break
			}
		}
	}

	if IsValidBucketName(candidate) {
		return candidate
	}

	return ""
}

// Methods moved to bucket_ops.go and object_ops.go

// listObjectsV2 is in handler_list_v2.go

// parseRange parses a "bytes=X-Y" Range header and returns clamped [start, end] indices.

// Multipart Upload Handlers
