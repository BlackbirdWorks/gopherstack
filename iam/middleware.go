package iam

import (
	"context"
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// accessDeniedResponse is the XML error returned when IAM enforcement denies a request.
type accessDeniedResponse struct {
	XMLName   xml.Name       `xml:"ErrorResponse"`
	Error     iamDeniedError `xml:"Error"`
	Xmlns     string         `xml:"xmlns,attr"`
	RequestID string         `xml:"RequestId"`
}

type iamDeniedError struct {
	Code    string `xml:"Code"`
	Message string `xml:"Message"`
	Type    string `xml:"Type"`
}

// internalPathPrefixes contains URL path prefixes that always bypass IAM enforcement.
//
//nolint:gochecknoglobals // read-only package-level lookup table
var internalPathPrefixes = []string{
	"/dashboard",
	"/_gopherstack",
}

// EnforcementBackend is the minimal interface the IAM enforcement middleware
// requires from the IAM storage backend.
type EnforcementBackend interface {
	GetUserByAccessKeyID(accessKeyID string) (*User, error)
	GetPoliciesForUser(userName string) ([]string, error)
}

// EnforcementConfig carries optional configuration for the enforcement middleware.
type EnforcementConfig struct {
	// AccountID is the mock AWS account ID used in resource ARN construction.
	AccountID string
	// Region is the default region used in resource ARN construction.
	Region string
	// ResourceProviders is a list of backends that can return resource-based
	// policies (e.g. S3 bucket policies, SQS queue policies).
	ResourceProviders []ResourcePolicyProvider
	// ActionExtractors is an optional list of per-service extractors consulted
	// when the global ExtractIAMAction function cannot determine the IAM action
	// (e.g. for REST-based services that bypass the standard mappers).
	ActionExtractors []ActionExtractor
}

// EnforcementMiddleware returns an Echo middleware that enforces IAM policies on
// every incoming request. It extracts the caller's access key from the
// SigV4 Authorization header, resolves the associated IAM user, collects all
// attached policies, and evaluates them against the requested IAM action.
//
// If the access key is not found in the IAM backend (e.g. a test/dummy key),
// the request is allowed through without enforcement so existing tooling is
// not disrupted.
//
// Requests to dashboard and internal health-check paths are always allowed.
func EnforcementMiddleware(backend EnforcementBackend, cfg ...EnforcementConfig) echo.MiddlewareFunc {
	var ecfg EnforcementConfig
	if len(cfg) > 0 {
		ecfg = cfg[0]
	}

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if isInternalPath(c.Request().URL.Path) {
				return next(c)
			}

			return enforceIAMPolicy(c, next, backend, ecfg)
		}
	}
}

// isInternalPath returns true if the path should bypass IAM enforcement.
func isInternalPath(path string) bool {
	for _, prefix := range internalPathPrefixes {
		if strings.HasPrefix(path, prefix) {
			return true
		}
	}

	return false
}

// enforceIAMPolicy evaluates IAM policies for the request and either allows or denies it.
func enforceIAMPolicy(c *echo.Context, next echo.HandlerFunc, backend EnforcementBackend, cfg EnforcementConfig) error {
	r := c.Request()
	ctx := r.Context()
	log := logger.Load(ctx)

	accessKeyID := ExtractAccessKeyID(r)
	if accessKeyID == "" {
		return next(c)
	}

	user, err := backend.GetUserByAccessKeyID(accessKeyID)
	if err != nil {
		// Unknown key (test/dummy) — pass through without enforcement.
		return next(c)
	}

	policyDocs, err := backend.GetPoliciesForUser(user.UserName)
	if err != nil {
		log.WarnContext(ctx, "IAM enforcement: failed to load policies",
			"user", user.UserName, "error", err)

		return next(c)
	}

	action := ExtractIAMAction(r)
	if action == "" {
		action = extractActionFromProviders(r, cfg.ActionExtractors)
	}

	if action == "" {
		// Cannot determine action — allow to avoid false denials.
		return next(c)
	}

	resourceARN := extractResourceARN(r, cfg.AccountID, cfg.Region)

	// Collect resource-based policies for the accessed resource.
	resourceDocs := collectResourcePolicies(ctx, cfg.ResourceProviders, resourceARN)

	// Build condition context from the request and resolved user.
	condCtx := buildConditionContext(r, user)

	// Determine what resource string to match against policy Resource fields.
	matchResource := resourceARN
	if matchResource == "" {
		matchResource = "*"
	}

	// Identity-based policies.
	idResult := EvaluatePolicies(policyDocs, action, matchResource, condCtx)

	// Explicit Deny from identity policy always wins.
	if idResult == EvalExplicitDeny {
		log.InfoContext(ctx, "IAM enforcement: access denied (identity policy)",
			"user", user.UserName, "action", action, "resource", matchResource)

		return writeAccessDenied(c, action)
	}

	// Resource-based policies: allow if any grants access, deny on explicit deny.
	if len(resourceDocs) > 0 {
		resResult := EvaluatePolicies(resourceDocs, action, matchResource, condCtx)

		if resResult == EvalExplicitDeny {
			log.InfoContext(ctx, "IAM enforcement: access denied (resource policy)",
				"user", user.UserName, "action", action, "resource", matchResource)

			return writeAccessDenied(c, action)
		}

		// Resource policy Allow is sufficient even without identity Allow.
		if resResult == EvalAllow {
			return next(c)
		}
	}

	// No Allow from either identity or resource policy.
	if idResult != EvalAllow {
		log.InfoContext(ctx, "IAM enforcement: access denied (implicit deny)",
			"user", user.UserName, "action", action, "resource", matchResource)

		return writeAccessDenied(c, action)
	}

	return next(c)
}

// extractActionFromProviders calls each action extractor until one returns a non-empty action.
func extractActionFromProviders(r *http.Request, extractors []ActionExtractor) string {
	for _, ae := range extractors {
		if action := ae.IAMAction(r); action != "" {
			return action
		}
	}

	return ""
}

// collectResourcePolicies queries all registered resource policy providers for
// a policy attached to resourceARN and returns the non-empty policy documents.
func collectResourcePolicies(ctx context.Context, providers []ResourcePolicyProvider, resourceARN string) []string {
	if resourceARN == "" || len(providers) == 0 {
		return nil
	}

	docs := make([]string, 0, len(providers))

	for _, p := range providers {
		doc, err := p.GetResourcePolicy(ctx, resourceARN)
		if err == nil && doc != "" {
			docs = append(docs, doc)
		}
	}

	return docs
}

// buildConditionContext constructs the per-request condition evaluation context.
func buildConditionContext(r *http.Request, user *User) ConditionContext {
	return ConditionContext{
		SourceIP: extractClientIP(r),
		Username: user.UserName,
		UserID:   user.UserID,
	}
}

// extractClientIP returns the IP address of the client without the port.
func extractClientIP(r *http.Request) string {
	// Prefer X-Forwarded-For when behind a proxy.
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		parts := strings.Split(xff, ",")
		if len(parts) > 0 {
			return strings.TrimSpace(parts[0])
		}
	}

	host, _, err := splitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}

	return r.RemoteAddr
}

// splitHostPort extracts the host portion from an "host:port" address string.
func splitHostPort(addr string) (string, string, error) {
	// Handle [::1]:port IPv6 form.
	if len(addr) > 0 && addr[0] == '[' {
		end := strings.LastIndex(addr, "]")
		if end < 0 {
			return "", "", errNoPort
		}

		host := addr[1:end]
		port := ""

		if end+1 < len(addr) && addr[end+1] == ':' {
			port = addr[end+2:]
		}

		return host, port, nil
	}

	// IPv4 / hostname.
	lastColon := strings.LastIndex(addr, ":")
	if lastColon < 0 {
		return addr, "", nil
	}

	return addr[:lastColon], addr[lastColon+1:], nil
}

// errNoPort is returned when an IPv6 address is malformed.
var errNoPort = sentinelError("address has no port")

// sentinelError is a simple string error type.
type sentinelError string

func (e sentinelError) Error() string { return string(e) }

// ExtractAccessKeyID extracts the AWS access key ID from the SigV4 Authorization header.
// The expected format is:
//
//	AWS4-HMAC-SHA256 Credential=AKID/date/region/service/aws4_request, ...
func ExtractAccessKeyID(r *http.Request) string {
	auth := r.Header.Get("Authorization")
	if auth == "" || !strings.Contains(auth, "Credential=") {
		return ""
	}

	_, after, found := strings.Cut(auth, "Credential=")
	if !found {
		return ""
	}

	akid, _, _ := strings.Cut(after, "/")

	return akid
}

// writeAccessDenied writes an HTTP 403 XML error response.
func writeAccessDenied(c *echo.Context, action string) error {
	resp := accessDeniedResponse{
		Xmlns: iamXMLNS,
		Error: iamDeniedError{
			Code:    "AccessDenied",
			Message: "User is not authorized to perform: " + action,
			Type:    "Sender",
		},
		RequestID: c.Response().Header().Get("X-Amz-Request-Id"),
	}

	body, err := xml.Marshal(resp)
	if err != nil {
		return c.String(http.StatusForbidden, "AccessDenied")
	}

	c.Response().Header().Set("Content-Type", "text/xml; charset=utf-8")

	return c.XMLBlob(http.StatusForbidden, body)
}
