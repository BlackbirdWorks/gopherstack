package service

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// minCredentialParts is the minimum number of slash-delimited fields in an AWS
// SigV4 Credential value: AKID / date / region / service / aws4_request.
const minCredentialParts = 5

// CloudTrailEventInput carries the fields the central service registry
// extracts from a single mutating API call so the CloudTrail backend can
// record a real management event. It is defined here (rather than reusing the
// cloudtrail package's own Event type) because services/cloudtrail already
// imports pkgs/service for its Provider/Registerable wiring, so the reverse
// import would create an import cycle.
type CloudTrailEventInput struct {
	// EventName is the AWS API operation name, e.g. "CreateBucket", "RunInstances".
	EventName string
	// EventSource is the AWS service's CloudTrail event source, e.g. "s3.amazonaws.com".
	EventSource string
	// AwsRegion is the region the request targeted.
	AwsRegion string
	// Username identifies the caller. Empty when no signed identity was present.
	Username string
	// AccessKeyID is the SigV4 access key used to sign the request, if any.
	AccessKeyID string
	// ResourceName is the primary resource the operation acted on, if known.
	ResourceName string
}

// CloudTrailRecorder is implemented by the CloudTrail service backend to
// accept management events captured centrally by the service registry.
// The registry auto-discovers the live backend via SetCloudTrailRecorder;
// no service package other than cloudtrail need know about this interface.
type CloudTrailRecorder interface {
	// RecordManagementEvent records a single mutating API call as a CloudTrail
	// management event so it is later returned by LookupEvents.
	RecordManagementEvent(ev CloudTrailEventInput)
}

// readOnlyOperationPrefixes lists AWS API operation name prefixes that are
// conventionally read-only. CloudTrail trails only log management events by
// default (data/read events require an explicit read event selector), so
// operations matching one of these prefixes are excluded from capture.
var readOnlyOperationPrefixes = []string{ //nolint:gochecknoglobals // static lookup table
	"Get", "List", "Describe", "Head", "Lookup", "Check", "Query", "Search",
	"Test", "Validate", "Preview", "Estimate", "Simulate",
}

// IsReadOnlyOperation reports whether op looks like a read-only API call based
// on its AWS-conventional naming prefix.
func IsReadOnlyOperation(op string) bool {
	for _, p := range readOnlyOperationPrefixes {
		if strings.HasPrefix(op, p) {
			return true
		}
	}

	return false
}

// IsUnknownOperation reports whether the observer could not determine a real
// operation name for the request (services return "Unknown"/"unknown" in that case).
func IsUnknownOperation(op string) bool {
	return op == "" || strings.EqualFold(op, "unknown")
}

// ExtractAccessKeyID extracts the AWS access key ID from the SigV4
// Authorization header's Credential value. Returns "" when absent or malformed.
func ExtractAccessKeyID(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" || !strings.Contains(auth, "Credential=") {
		return ""
	}

	_, after, found := strings.Cut(auth, "Credential=")
	if !found {
		return ""
	}

	credOnly, _, _ := strings.Cut(after, ",")
	parts := strings.Split(credOnly, "/")

	if len(parts) < minCredentialParts {
		return ""
	}

	return parts[0]
}

// EventSourceFor derives the CloudTrail EventSource for a request, e.g.
// "s3.amazonaws.com", from the SigV4 signing name. Falls back to the
// service's registered name when no Authorization header is present.
func EventSourceFor(r *http.Request, svc Registerable) string {
	if signingName := httputils.ExtractServiceFromRequest(r); signingName != "" {
		return signingName + ".amazonaws.com"
	}

	return strings.ToLower(svc.Name()) + ".amazonaws.com"
}

// WrapCloudTrailCapture wraps an already fully-assembled service handler chain
// so that, once it returns, a CloudTrail management event is recorded for
// mutating operations. It is applied as the outermost layer around a single
// service's handler in Registry.Register, so it observes request/response
// state exactly as the existing telemetry wrapper does — reusing each
// service's own ExtractOperation/ExtractResource (required by the
// Registerable/ResourceObserver contract every service already implements)
// gives accurate operation names across every wire protocol (JSON, query,
// REST) without any per-service capture code.
func WrapCloudTrailCapture(rec CloudTrailRecorder, svc Registerable, next echo.HandlerFunc) echo.HandlerFunc {
	return func(c *echo.Context) error {
		err := next(c)

		op := svc.ExtractOperation(c)
		if IsUnknownOperation(op) || IsReadOnlyOperation(op) {
			return err
		}

		req := c.Request()
		accessKeyID := ExtractAccessKeyID(req)
		username := accessKeyID

		rec.RecordManagementEvent(CloudTrailEventInput{
			EventName:    op,
			EventSource:  EventSourceFor(req, svc),
			AwsRegion:    httputils.ExtractRegionFromRequest(req, config.DefaultRegion),
			Username:     username,
			AccessKeyID:  accessKeyID,
			ResourceName: svc.ExtractResource(c),
		})

		return err
	}
}
