package s3

import (
	"context"
	"encoding/xml"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"

	s3SDK "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
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
	DefaultRegion string
	janitor       *Janitor
	notifier      NotificationDispatcher
	Backend       StorageBackend
	// Endpoint is the base host (e.g. "localhost:9000") of this server.
	// When set, virtual-hosted-style URLs (bucket.host/key) are supported
	// in addition to path-style URLs (/bucket/key).
	Endpoint string
}

// NewHandler creates a new S3 Handler with the given backend.
func NewHandler(backend StorageBackend) *S3Handler {
	return &S3Handler{
		Backend:       backend,
		DefaultRegion: config.DefaultRegion,
	}
}

// WithJanitor attaches a background janitor to the handler.
func (h *S3Handler) WithJanitor(settings Settings) *S3Handler {
	h.DefaultRegion = settings.DefaultRegion
	if h.DefaultRegion == "" {
		h.DefaultRegion = config.DefaultRegion
	}
	if memBackend, ok := h.Backend.(*InMemoryBackend); ok {
		memBackend.SetDefaultRegion(h.DefaultRegion)
		h.janitor = NewJanitor(memBackend, settings)
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

// SetNotificationDispatcher attaches a NotificationDispatcher that delivers
// S3 event notifications to SQS/SNS/Lambda targets on PutObject and DeleteObject.
func (h *S3Handler) SetNotificationDispatcher(d NotificationDispatcher) {
	h.notifier = d
}

type s3Metrics struct {
	operation string
}

type s3ContextKey struct{}

var s3Key = s3ContextKey{} //nolint:gochecknoglobals // unexported context key used internally

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
		"PresignedGetObject",
		"PresignedPutObject",
		"GetBucketAcl",
		"PutBucketAcl",
		"PutBucketPolicy",
		"GetBucketPolicy",
		"DeleteBucketPolicy",
		"PutBucketCors",
		"GetBucketCors",
		"DeleteBucketCors",
		"PutBucketLifecycleConfiguration",
		"GetBucketLifecycleConfiguration",
		"DeleteBucketLifecycleConfiguration",
		"PutBucketNotificationConfiguration",
		"GetBucketNotificationConfiguration",
		"PutBucketWebsite",
		"GetBucketWebsite",
		"DeleteBucketWebsite",
		"PutBucketEncryption",
		"GetBucketEncryption",
		"DeleteBucketEncryption",
		"PutObjectLockConfiguration",
		"GetObjectLockConfiguration",
		"PutObjectRetention",
		"GetObjectRetention",
		"PutObjectLegalHold",
		"GetObjectLegalHold",
		"PutPublicAccessBlock",
		"GetPublicAccessBlock",
		"DeletePublicAccessBlock",
		"PutBucketOwnershipControls",
		"GetBucketOwnershipControls",
		"DeleteBucketOwnershipControls",
		"PutBucketLogging",
		"GetBucketLogging",
		"PutBucketReplication",
		"GetBucketReplication",
		"DeleteBucketReplication",
		"SelectObjectContent",
	}
}

// Regions returns all regions with buckets in the backend.
// Returns an empty slice when not using the in-memory backend.
func (h *S3Handler) Regions() []string {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		return b.Regions()
	}

	return []string{}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *S3Handler) ChaosServiceName() string { return "s3" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *S3Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this S3 instance handles.
func (h *S3Handler) ChaosRegions() []string {
	regions := h.Regions()
	if len(regions) == 0 {
		return []string{h.DefaultRegion}
	}

	return regions
}

// BucketsByRegion returns buckets in the given region (all if empty).
// Returns an empty slice when not using the in-memory backend.
func (h *S3Handler) BucketsByRegion(region string) []types.Bucket {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		return b.BucketsByRegion(region)
	}

	return []types.Bucket{}
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

		sw := httputils.NewResponseWriter(c.Response())

		log := logger.Load(ctx)

		// Validate presigned URL expiry before processing.
		if isPresignedRequest(requestWithCtx) && !h.validatePresignedRequest(ctx, sw, requestWithCtx) {
			return nil
		}

		bucketName, key, ok := h.resolveBucketAndKey(ctx, sw, requestWithCtx)

		if !ok {
			return nil
		}

		log.DebugContext(ctx, "S3 request", "method", requestWithCtx.Method, "bucket", bucketName, "key", key)

		if bucketName == "" {
			if requestWithCtx.Method != http.MethodGet {
				WriteError(ctx, sw, requestWithCtx, ErrMethodNotAllowed)

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
		// Exclude API, dashboard, and internal Gopherstack endpoints so they
		// fall through to Echo's registered routes.
		// Matches /api/, /metrics/, /dashboard/ but NOT /api, /metrics, /dashboard
		// which could be valid bucket names.
		if strings.HasPrefix(path, "/api/") || strings.HasPrefix(path, "/metrics/") ||
			strings.HasPrefix(path, "/dashboard/") || strings.HasPrefix(path, "/_gopherstack/") ||
			path == "/favicon.ico" || path == "/robots.txt" {
			return false
		}
		// Accept all other requests - priority ensures we're evaluated last
		return true
	}
}

// MatchPriority returns the priority for the S3 matcher.
// Catch-all matchers have the lowest priority (0), ensuring other services match first.
func (h *S3Handler) MatchPriority() int {
	return service.PriorityCatchAll
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
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", pathSplitParts)

	// Try virtual-hosted-style first: bucket name as subdomain in Host header.
	if vhBucket := h.extractVirtualHostedBucketName(r); vhBucket != "" {
		bucket := vhBucket
		key := path
		if key != "" && !IsValidObjectKey(key) {
			WriteError(ctx, w, r, ErrInvalidArgument)

			return "", "", false
		}

		return bucket, key, true
	}

	// Fall back to path-style (/bucket/key).
	bucket, key := "", ""
	if path != "" && path != "/" {
		bucket = parts[0]
		if !IsValidBucketName(bucket) {
			WriteError(ctx, w, r, ErrInvalidBucketName)

			return "", "", false
		}

		if len(parts) > 1 {
			key = parts[1]
			if key != "" && !IsValidObjectKey(key) {
				WriteError(ctx, w, r, ErrInvalidArgument)

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
		if bucket := findBucketInParts(parts); bucket != "" {
			return bucket
		}
	}

	if IsValidBucketName(candidate) {
		return candidate
	}

	return ""
}

// findBucketInParts scans parts for an "s3" or "s3-*" segment and returns the
// bucket name formed by joining everything before that segment. Returns an
// empty string if no s3 segment is found or the resulting name is invalid.
func findBucketInParts(parts []string) string {
	for i, p := range parts {
		if p == "s3" || strings.HasPrefix(p, "s3-") {
			bucket := strings.Join(parts[:i], ".")
			if IsValidBucketName(bucket) {
				return bucket
			}

			return ""
		}
	}

	return ""
}

// Methods moved to bucket_ops.go and object_ops.go

// listObjectsV2 is in handler_list_v2.go

// parseRange parses a "bytes=X-Y" Range header and returns clamped [start, end] indices.

// Multipart Upload Handlers

// Reset clears all in-memory state from the backend. It is used by the
// POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (h *S3Handler) Reset() {
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		b.Reset()
	}
}

// ServeWebsite serves a static file from an S3 bucket configured for website hosting.
// It is invoked by the GET /_gopherstack/website/{bucket}/{key+} route registered in cli.go.
// The bucket must have a website configuration stored via PutBucketWebsite.
func (h *S3Handler) ServeWebsite(c *echo.Context) error {
	ctx := c.Request().Context()
	bucket := c.Param("bucket")
	key := strings.TrimPrefix(c.Param("key"), "/")

	websiteXML, err := h.Backend.GetBucketWebsite(ctx, bucket)
	if err != nil {
		if errors.Is(err, ErrNoSuchBucket) {
			return c.JSON(http.StatusNotFound, map[string]string{
				"Code":    "NoSuchBucket",
				"Message": "The specified bucket does not exist",
			})
		}

		return c.JSON(http.StatusNotFound, map[string]string{
			"Code":    "NoSuchWebsiteConfiguration",
			"Message": "The specified bucket does not have a website configuration",
		})
	}

	var cfg WebsiteConfiguration
	if xmlErr := xml.Unmarshal([]byte(websiteXML), &cfg); xmlErr != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{
			"Code":    "InternalError",
			"Message": "Failed to parse website configuration",
		})
	}

	if cfg.RedirectAllRequestsTo != nil {
		return c.Redirect(http.StatusMovedPermanently, websiteRedirectAllURL(cfg.RedirectAllRequestsTo, key))
	}

	if loc, code, ok := applyWebsiteRoutingRules(cfg.RoutingRules, key, c.Request().Host); ok {
		return c.Redirect(code, loc)
	}

	effectiveKey := websiteEffectiveKey(key, cfg.IndexDocument)

	out, getErr := h.Backend.GetObject(ctx, &s3SDK.GetObjectInput{
		Bucket: &bucket,
		Key:    &effectiveKey,
	})
	if getErr == nil {
		return serveWebsiteObject(c, out, http.StatusOK)
	}

	if cfg.ErrorDocument != nil && cfg.ErrorDocument.Key != "" {
		errOut, errDocErr := h.Backend.GetObject(ctx, &s3SDK.GetObjectInput{
			Bucket: &bucket,
			Key:    &cfg.ErrorDocument.Key,
		})
		if errDocErr == nil {
			return serveWebsiteObject(c, errOut, http.StatusNotFound)
		}
	}

	return c.JSON(http.StatusNotFound, map[string]string{
		"Code":    "NoSuchKey",
		"Message": "The specified key does not exist",
	})
}

// websiteRedirectAllURL builds a redirect URL when RedirectAllRequestsTo is set.
func websiteRedirectAllURL(redir *WebsiteRedirectAll, key string) string {
	protocol := redir.Protocol
	if protocol == "" {
		protocol = "http"
	}

	return protocol + "://" + redir.HostName + "/" + key
}

// applyWebsiteRoutingRules evaluates routing rules against the given key and host.
// Returns the redirect location, HTTP status code, and true if a rule matched.
func applyWebsiteRoutingRules(rules []WebsiteRoutingRule, key, reqHost string) (string, int, bool) {
	for _, rule := range rules {
		cond := rule.Condition
		if cond != nil && cond.KeyPrefixEquals != "" && !strings.HasPrefix(key, cond.KeyPrefixEquals) {
			continue
		}

		redir := rule.Redirect
		if !websiteRuleHasRedirect(redir) {
			continue
		}

		code := websiteRedirectCode(redir.HTTPRedirectCode)
		targetKey := websiteTargetKey(redir, cond, key)
		protocol := redir.Protocol
		if protocol == "" {
			protocol = "http"
		}

		host := redir.HostName
		if host == "" {
			host = reqHost
		}

		return protocol + "://" + host + "/" + targetKey, code, true
	}

	return "", 0, false
}

// websiteRuleHasRedirect reports whether a routing rule redirect spec is non-empty.
func websiteRuleHasRedirect(r WebsiteRoutingRuleRedirect) bool {
	return r.HostName != "" || r.Protocol != "" || r.ReplaceKeyWith != "" || r.ReplaceKeyPrefixWith != ""
}

// websiteRedirectCode converts an HTTP redirect code string to an int status code.
func websiteRedirectCode(code string) int {
	if code == "301" {
		return http.StatusMovedPermanently
	}

	return http.StatusFound
}

// websiteTargetKey computes the effective target key for a routing rule redirect.
func websiteTargetKey(redir WebsiteRoutingRuleRedirect, cond *WebsiteRoutingRuleCondition, key string) string {
	if redir.ReplaceKeyWith != "" {
		return redir.ReplaceKeyWith
	}

	if redir.ReplaceKeyPrefixWith != "" && cond != nil && cond.KeyPrefixEquals != "" {
		return redir.ReplaceKeyPrefixWith + strings.TrimPrefix(key, cond.KeyPrefixEquals)
	}

	return key
}

// websiteEffectiveKey resolves the effective object key for a website request,
// appending the index document suffix when the key refers to a directory.
func websiteEffectiveKey(key string, indexDoc *WebsiteIndexDocument) string {
	if indexDoc == nil || indexDoc.Suffix == "" {
		return key
	}

	if key == "" || strings.HasSuffix(key, "/") {
		base := strings.TrimSuffix(key, "/")
		if base != "" {
			return base + "/" + indexDoc.Suffix
		}

		return indexDoc.Suffix
	}

	return key
}

// serveWebsiteObject writes an S3 GetObjectOutput to the Echo response.
func serveWebsiteObject(c *echo.Context, out *s3SDK.GetObjectOutput, statusCode int) error {
	defer out.Body.Close()

	contentType := "application/octet-stream"
	if out.ContentType != nil {
		contentType = *out.ContentType
	}

	c.Response().Header().Set("Content-Type", contentType)
	c.Response().WriteHeader(statusCode)
	_, _ = io.Copy(c.Response(), out.Body)

	return nil
}
