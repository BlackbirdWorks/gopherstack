package iam

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
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
	GetPoliciesForRole(roleName string) ([]string, error)
}

type EnforcementConfig struct {
	// Global is the shared AWS configuration state.
	Global *config.GlobalConfig `json:"global,omitempty"`
	// ResourceProviders is a list of backends that can return resource-based
	// policies (e.g. S3 bucket policies, SQS queue policies).
	ResourceProviders []ResourcePolicyProvider `json:"resourceProviders,omitempty"`
	// ActionExtractors is an optional list of per-service extractors consulted
	// when the global ExtractIAMAction function cannot determine the IAM action
	// (e.g. for REST-based services that bypass the standard mappers).
	ActionExtractors []ActionExtractor `json:"actionExtractors,omitempty"`
}

// EnforcementMiddleware returns an Echo middleware that enforces IAM policies on
// every incoming request. It extracts the caller's access key from the
// SigV4 Authorization header, resolves the associated IAM user or assumed role,
// collects all attached policies, and evaluates them against the requested IAM action.
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

func extractRoleNameFromArn(arn string) string {
	parts := strings.Split(arn, ":")
	if len(parts) == 0 {
		return ""
	}

	res := parts[len(parts)-1]
	res = strings.TrimPrefix(res, "assumed-role/")
	res = strings.TrimPrefix(res, "role/")
	roleName, _, _ := strings.Cut(res, "/")

	return roleName
}

func resolveAssumedRoleIdentityPolicies(
	ctx context.Context,
	r *http.Request,
	principal *awsmeta.Principal,
	backend EnforcementBackend,
) ([]string, ConditionContext, string, bool) {
	if principal == nil || principal.Kind != awsmeta.PrincipalKindAssumedRole {
		return nil, ConditionContext{}, "", false
	}

	roleName := extractRoleNameFromArn(principal.Arn)
	if roleName == "" {
		return nil, ConditionContext{}, "", false
	}

	docs, err := backend.GetPoliciesForRole(roleName)
	if err != nil {
		return nil, ConditionContext{}, "", false
	}

	region := awsmeta.Region(ctx)
	if region == "" {
		region = httputils.ExtractRegionFromRequest(r, "us-east-1")
	}

	condCtx := ConditionContext{
		PrincipalARN:     principal.Arn,
		PrincipalAccount: principal.AccountID,
		RequestedRegion:  region,
		Username:         principal.SessionName,
		UserID:           principal.UserID,
		SourceIP:         extractClientIP(r),
	}

	return docs, condCtx, roleName, true
}

func resolveCallerIdentityPolicies(
	ctx context.Context,
	r *http.Request,
	backend EnforcementBackend,
) ([]string, ConditionContext, string, bool) {
	principal := awsmeta.GetPrincipal(ctx)
	if docs, condCtx, roleName, ok := resolveAssumedRoleIdentityPolicies(ctx, r, principal, backend); ok {
		return docs, condCtx, roleName, true
	}

	accessKeyID := ExtractAccessKeyID(r)
	if accessKeyID == "" {
		return nil, ConditionContext{}, "", false
	}

	user, err := backend.GetUserByAccessKeyID(accessKeyID)
	if err != nil {
		return nil, ConditionContext{}, "", false
	}

	docs, err := backend.GetPoliciesForUser(user.UserName)
	if err != nil {
		return nil, ConditionContext{}, "", false
	}

	return docs, buildConditionContext(r, user), user.UserName, true
}

// enforceIAMPolicy evaluates IAM policies for the request and either allows or denies it.
func enforceIAMPolicy(c *echo.Context, next echo.HandlerFunc, backend EnforcementBackend, cfg EnforcementConfig) error {
	r := c.Request()
	ctx := r.Context()
	log := logger.Load(ctx)

	policyDocs, condCtx, callerName, enforced := resolveCallerIdentityPolicies(ctx, r, backend)
	if !enforced {
		// Unknown key (test/dummy) — pass through without enforcement.
		return next(c)
	}

	action := ExtractTargetOrFormIAMAction(r)
	if action == "" {
		action = extractActionFromProviders(r, cfg.ActionExtractors)
	}
	if action == "" {
		action = extractS3IAMAction(r)
	}

	if action == "" {
		// Cannot determine action — allow to avoid false denials.
		return next(c)
	}

	accountID := ""
	region := ""

	if cfg.Global != nil {
		accountID = cfg.Global.GetAccountID()
		region = cfg.Global.GetRegion()
	}

	resourceARN := extractResourceARN(r, accountID, region)

	// Collect resource-based policies for the accessed resource.
	resourceDocs := collectResourcePolicies(ctx, cfg.ResourceProviders, resourceARN)

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
			"caller", callerName, "action", action, "resource", matchResource)

		return writeAccessDenied(c, action, matchResource)
	}

	// Resource-based policies: allow if any grants access, deny on explicit deny.
	if len(resourceDocs) > 0 {
		resResult := EvaluatePolicies(resourceDocs, action, matchResource, condCtx)

		if resResult == EvalExplicitDeny {
			log.InfoContext(ctx, "IAM enforcement: access denied (resource policy)",
				"caller", callerName, "action", action, "resource", matchResource)

			return writeAccessDenied(c, action, matchResource)
		}

		// Resource policy Allow is sufficient even without identity Allow.
		if resResult == EvalAllow {
			return next(c)
		}
	}

	// No Allow from either identity or resource policy.
	if idResult != EvalAllow {
		log.InfoContext(ctx, "IAM enforcement: access denied (implicit deny)",
			"caller", callerName, "action", action, "resource", matchResource)

		return writeAccessDenied(c, action, matchResource)
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

const (
	arnMinSegments         = 5
	arnAccountSegmentIndex = 4
)

// buildConditionContext constructs the per-request condition evaluation context.
func buildConditionContext(r *http.Request, user *User) ConditionContext {
	accountID := ""
	if parts := strings.Split(user.Arn, ":"); len(parts) >= arnMinSegments {
		accountID = parts[arnAccountSegmentIndex]
	}

	region := awsmeta.Region(r.Context())
	if region == "" {
		region = httputils.ExtractRegionFromRequest(r, "us-east-1")
	}

	return ConditionContext{
		PrincipalARN:     user.Arn,
		PrincipalAccount: accountID,
		RequestedRegion:  region,
		SourceIP:         extractClientIP(r),
		Username:         user.UserName,
		UserID:           user.UserID,
		PrincipalTags:    user.Tags,
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

// ExtractAccessKeyID extracts the AWS access key ID from the SigV4 Authorization header or query params.
func ExtractAccessKeyID(r *http.Request) string {
	return httputils.ExtractAccessKeyFromRequest(r)
}

type jsonRPCErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"Message"`
}

type s3AccessDeniedError struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
	HostID    string   `xml:"HostId,omitempty"`
}

func isJSONRPCRequest(r *http.Request) bool {
	if r.Header.Get("X-Amz-Target") != "" {
		return true
	}

	ct := r.Header.Get("Content-Type")

	return strings.Contains(ct, "application/x-amz-json-1.0") || strings.Contains(ct, "application/x-amz-json-1.1")
}

func isS3Request(r *http.Request) bool {
	if r.Header.Get("X-Amz-Target") != "" {
		return false
	}

	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, "application/x-www-form-urlencoded") {
		return false
	}

	for _, prefix := range nonS3RESTPathPrefixes {
		if strings.HasPrefix(r.URL.Path, prefix) {
			return false
		}
	}

	return true
}

func writeJSONRPCAccessDenied(c *echo.Context, action string) error {
	resp := jsonRPCErrorResponse{
		Type:    "com.amazon.coral.service#AccessDeniedException",
		Message: "User is not authorized to perform: " + action,
	}

	body, err := json.Marshal(resp)
	if err != nil {
		return c.String(http.StatusBadRequest, `{"__type":"AccessDeniedException"}`)
	}

	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")

	return c.Blob(http.StatusBadRequest, "application/x-amz-json-1.0", body)
}

func writeS3AccessDenied(c *echo.Context, resource string) error {
	reqID := c.Response().Header().Get("X-Amz-Request-Id")
	if reqID == "" {
		reqID = "gopherstack-request"
	}

	resp := s3AccessDeniedError{
		Code:      "AccessDenied",
		Message:   "Access Denied",
		Resource:  resource,
		RequestID: reqID,
		HostID:    "gopherstack",
	}

	body, err := xml.Marshal(resp)
	if err != nil {
		return c.String(http.StatusForbidden, "AccessDenied")
	}

	c.Response().Header().Set("Content-Type", "application/xml")

	return c.XMLBlob(http.StatusForbidden, append([]byte(xml.Header), body...))
}

func writeQueryXMLAccessDenied(c *echo.Context, action string) error {
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

// writeAccessDenied writes a protocol-appropriate access denied error response.
func writeAccessDenied(c *echo.Context, action, resource string) error {
	r := c.Request()
	if isJSONRPCRequest(r) {
		return writeJSONRPCAccessDenied(c, action)
	}

	if isS3Request(r) {
		return writeS3AccessDenied(c, resource)
	}

	return writeQueryXMLAccessDenied(c, action)
}
