package iam

import (
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
func EnforcementMiddleware(backend EnforcementBackend) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if isInternalPath(c.Request().URL.Path) {
				return next(c)
			}

			return enforceIAMPolicy(c, next, backend)
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
func enforceIAMPolicy(c *echo.Context, next echo.HandlerFunc, backend EnforcementBackend) error {
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
		// Cannot determine action — allow to avoid false denials.
		return next(c)
	}

	result := EvaluatePolicies(policyDocs, action, "*")
	if result != EvalAllow {
		log.InfoContext(ctx, "IAM enforcement: access denied",
			"user", user.UserName,
			"action", action,
			"result", result,
		)

		return writeAccessDenied(c, action)
	}

	return next(c)
}

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
