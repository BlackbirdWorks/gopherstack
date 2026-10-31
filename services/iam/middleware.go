package iam

import (
	"cmp"
	"context"
	"encoding/json"
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
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
	// Recorder, when non-nil, receives one event per evaluated request.
	Recorder *iampolicystream.Recorder `json:"-"`
	// ResourceProviders is a list of backends that can return resource-based
	// policies (e.g. S3 bucket policies, SQS queue policies).
	ResourceProviders []ResourcePolicyProvider `json:"resourceProviders,omitempty"`
	// ActionExtractors is an optional list of per-service extractors consulted
	// when the global ExtractIAMAction function cannot determine the IAM action
	// (e.g. for REST-based services that bypass the standard mappers).
	ActionExtractors []ActionExtractor `json:"actionExtractors,omitempty"`
	// SCPs, when non-nil, gates otherwise-allowed requests on the caller account's service control policies.
	SCPs SCPProvider `json:"-"`
	// Audit evaluates and records but never denies.
	Audit bool `json:"audit,omitempty"`
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

// userArnResourcePrefix is the ARN path segment identifying an IAM user resource.
const userArnResourcePrefix = ":user/"

// extractUserNameFromArn returns the IAM user name embedded in an
// "arn:...:user/NAME" ARN, or "" if arn does not reference a user resource.
func extractUserNameFromArn(arn string) string {
	_, after, ok := strings.CutLast(arn, userArnResourcePrefix)
	if !ok {
		return ""
	}

	return after
}

// permissionsBoundaryLookup is an optional capability an EnforcementBackend may
// implement to expose permission boundaries to the enforcement middleware. It is
// deliberately kept separate from EnforcementBackend (rather than adding methods
// to that required interface) because dozens of per-service test mocks implement
// EnforcementBackend directly; a backend that doesn't implement this capability
// is simply enforced without boundary support.
type permissionsBoundaryLookup interface {
	PermissionsBoundaryDocForUser(userName string) string
	PermissionsBoundaryDocForRole(roleName string) string
}

// principalPolicyLookup is an optional EnforcementBackend capability that
// resolves identity policies by principal ARN in the ARN's own account.
type principalPolicyLookup interface {
	GetPoliciesForPrincipalArn(arn string) ([]string, error)
	PermissionsBoundaryDocForPrincipalArn(arn string) string
	AccountID() string
}

// failClosedAccount reports whether ctx names a non-default account, in which
// case an unresolvable principal must be denied rather than passed through.
func failClosedAccount(ctx context.Context, backend EnforcementBackend) bool {
	pl, ok := backend.(principalPolicyLookup)
	if !ok {
		return false
	}

	m, present := awsmeta.Key.Get(ctx)

	return present && m != nil && m.Account != "" && m.Account != pl.AccountID()
}

// policiesForPrincipalArn resolves arn's policies via the account-aware
// capability. found is false when the backend lacks it. A lookup error for a
// non-default-account principal yields an enforced result with no policies.
func policiesForPrincipalArn(
	backend EnforcementBackend, arn string,
) ([]string, string, bool, bool) {
	pl, ok := backend.(principalPolicyLookup)
	if !ok {
		return nil, "", false, false
	}

	docs, err := pl.GetPoliciesForPrincipalArn(arn)
	if err != nil {
		acct, hasAcct := arnAccountID(arn)

		return nil, "", hasAcct && acct != pl.AccountID(), true
	}

	return docs, pl.PermissionsBoundaryDocForPrincipalArn(arn), true, true
}

func boundaryDocForUser(backend EnforcementBackend, userName string) string {
	if pb, ok := backend.(permissionsBoundaryLookup); ok {
		return pb.PermissionsBoundaryDocForUser(userName)
	}

	return ""
}

func boundaryDocForRole(backend EnforcementBackend, roleName string) string {
	if pb, ok := backend.(permissionsBoundaryLookup); ok {
		return pb.PermissionsBoundaryDocForRole(roleName)
	}

	return ""
}

func buildPrincipalConditionContext(
	ctx context.Context,
	r *http.Request,
	principal *awsmeta.Principal,
) ConditionContext {
	region := awsmeta.Region(ctx)
	if region == "" {
		region = httputils.ExtractRegionFromRequest(r, "us-east-1")
	}

	return ConditionContext{
		PrincipalARN:     principal.Arn,
		PrincipalAccount: principal.AccountID,
		RequestedRegion:  region,
		Username:         principal.SessionName,
		UserID:           principal.UserID,
		SourceIP:         extractClientIP(r),
		SecureTransport:  secureTransportValue(r),
	}
}

func resolveAssumedRoleIdentityPolicies(
	ctx context.Context,
	r *http.Request,
	principal *awsmeta.Principal,
	backend EnforcementBackend,
) ([]string, string, ConditionContext, string, bool) {
	if principal == nil || principal.Kind != awsmeta.PrincipalKindAssumedRole {
		return nil, "", ConditionContext{}, "", false
	}

	roleName := extractRoleNameFromArn(principal.Arn)
	if roleName == "" {
		return nil, "", ConditionContext{}, "", false
	}

	if docs, boundary, enforced, found := policiesForPrincipalArn(backend, principal.Arn); found {
		return docs, boundary, buildPrincipalConditionContext(ctx, r, principal), roleName, enforced
	}

	docs, err := backend.GetPoliciesForRole(roleName)
	if err != nil {
		return nil, "", ConditionContext{}, "", false
	}

	return docs, boundaryDocForRole(backend, roleName),
		buildPrincipalConditionContext(ctx, r, principal), roleName, true
}

// resolveSTSUserIdentityPolicies handles a caller whose access key ID was not
// found in IAM's own user table but whom principalMiddleware already resolved
// to a Kind=User principal via STS's ResolvePrincipal -- i.e. an ASIA session
// minted by GetSessionToken, GetFederationToken, or GetDelegatedAccessToken,
// which keep the caller's own (non-role) identity rather than assuming a role
// (see sts.SessionInfo.IsAssumedRole). Without this, such a session falls
// through resolveCallerIdentityPolicies entirely and enforceIAMPolicy treats
// it as an unrecognized/dummy key, allowing every request through unchecked
// (gopherstack-s982).
//
// The account root user has no identity policy to evaluate, and AWS grants it
// access absent an explicit resource-policy/SCP deny that this middleware does
// not model; root is therefore left for the caller's existing unenforced
// fallback by returning ok=false here rather than manufacturing a policy set.
//
// A federated-user ARN (GetFederationToken) or any other STS-user ARN this
// emulator cannot map to a real IAM user has no identity-policy record at all.
// Rather than skip enforcement, this returns an enforced result with zero
// policies so EvaluatePolicies below yields an implicit deny. This mirrors
// GetFederationToken's documented rule that a call which passes no policy
// leaves the resulting temporary credentials with no effective permissions.
func resolveSTSUserIdentityPolicies(
	ctx context.Context,
	r *http.Request,
	principal *awsmeta.Principal,
	backend EnforcementBackend,
) ([]string, string, ConditionContext, string, bool) {
	if principal == nil || principal.Kind != awsmeta.PrincipalKindUser || principal.UserName != "" {
		return nil, "", ConditionContext{}, "", false
	}

	if strings.HasSuffix(principal.Arn, ":root") {
		return nil, "", ConditionContext{}, "", false
	}

	condCtx := buildPrincipalConditionContext(ctx, r, principal)

	if userName := extractUserNameFromArn(principal.Arn); userName != "" {
		if docs, boundary, enforced, found := policiesForPrincipalArn(backend, principal.Arn); found && enforced {
			return docs, boundary, condCtx, userName, true
		}

		if docs, err := backend.GetPoliciesForUser(userName); err == nil {
			return docs, boundaryDocForUser(backend, userName), condCtx, userName, true
		}
	}

	return nil, "", condCtx, principal.Arn, true
}

func resolveCallerIdentityPolicies(
	ctx context.Context,
	r *http.Request,
	backend EnforcementBackend,
) ([]string, string, ConditionContext, string, bool) {
	principal := awsmeta.GetPrincipal(ctx)
	if docs, boundaryDoc, condCtx, roleName, ok := resolveAssumedRoleIdentityPolicies(ctx, r, principal, backend); ok {
		return docs, boundaryDoc, condCtx, roleName, true
	}

	accessKeyID := ExtractAccessKeyID(r)
	if accessKeyID == "" {
		return nil, "", ConditionContext{}, "", false
	}

	user, userErr := backend.GetUserByAccessKeyID(accessKeyID)
	if userErr == nil {
		if docs, boundary, enforced, found := policiesForPrincipalArn(backend, user.Arn); found {
			if !enforced {
				return nil, "", ConditionContext{}, "", false
			}

			return docs, boundary, buildConditionContext(r, user), user.UserName, true
		}

		docs, err := backend.GetPoliciesForUser(user.UserName)
		if err != nil {
			return nil, "", ConditionContext{}, "", false
		}

		return docs, boundaryDocForUser(backend, user.UserName), buildConditionContext(r, user), user.UserName, true
	}

	// accessKeyID isn't a registered IAM user key. It may still be a valid
	// STS-issued non-assumed-role session that principalMiddleware resolved.
	if docs, boundaryDoc, condCtx, name, ok := resolveSTSUserIdentityPolicies(ctx, r, principal, backend); ok {
		return docs, boundaryDoc, condCtx, name, true
	}

	return nil, "", ConditionContext{}, "", false
}

// applyPermissionsBoundary evaluates the caller's permission boundary (if any)
// against the request and returns the possibly-downgraded identity-policy
// result, whether the boundary explicitly denies the request outright, and
// whether the boundary itself evaluated to Allow.
//
// AWS's IAM User Guide, "Permissions boundaries for IAM entities", states
// that an entity's permissions boundary lets it act only within what both its
// identity-based policies and its permissions boundary allow. An explicit
// deny in the boundary denies outright, like any other policy type. An
// implicit deny downgrades an identity-policy Allow to implicit deny, but per
// the same page a resource-based policy that separately grants an IAM user
// ARN access is not limited by an implicit deny in an identity-based policy
// or permissions boundary, so this does not touch the resource-policy check
// that follows in enforceIAMPolicy.
//
// boundaryDocs holds one or more policy documents evaluated together as the
// boundary (a real IAM principal has at most one attached, but
// SimulateCustomPolicy's PermissionsBoundaryPolicyInputList accepts several).
func applyPermissionsBoundary(
	boundaryDocs []string, action, resource string,
	condCtx ConditionContext,
	idResult EvaluationResult,
) (EvaluationResult, bool, bool) {
	res, denied, allowed, _ := applyBoundaryTraced(boundaryDocs, action, resource, condCtx, idResult)

	return res, denied, allowed
}

// applyBoundaryTraced is applyPermissionsBoundary plus the boundary's deciding statement.
func applyBoundaryTraced(
	boundaryDocs []string, action, resource string,
	condCtx ConditionContext,
	idResult EvaluationResult,
) (EvaluationResult, bool, bool, boundaryTrace) {
	if len(boundaryDocs) == 0 {
		return idResult, false, false, boundaryTrace{}
	}

	boundaryResult, bm := ExplainPolicies(boundaryDocs, action, resource, condCtx)
	trace := boundaryTrace{present: true, match: bm}

	switch boundaryResult {
	case EvalExplicitDeny:
		return idResult, true, false, trace
	case EvalImplicitDeny:
		if idResult == EvalAllow {
			idResult = EvalImplicitDeny
		}
	case EvalAllow:
	}

	return idResult, false, boundaryResult == EvalAllow, trace
}

// enforceIAMPolicy evaluates IAM policies for the request and either allows or denies it.
func enforceIAMPolicy(c *echo.Context, next echo.HandlerFunc, backend EnforcementBackend, cfg EnforcementConfig) error {
	r := c.Request()
	ctx := r.Context()
	log := logger.Load(ctx)

	scope, _ := requestScopeFromProviders(r, cfg.ActionExtractors)
	if scope.Anonymous {
		return enforceAnonymousScope(c, next, cfg, scope)
	}

	policyDocs, boundaryDoc, condCtx, callerName, enforced := resolveCallerIdentityPolicies(ctx, r, backend)
	if !enforced {
		if failClosedAccount(ctx, backend) {
			log.InfoContext(ctx, "IAM enforcement: access denied (unresolved principal)",
				"reason", reasonUnresolvedCaller)
			cfg.record(c, requestDecision{reason: reasonUnresolvedCaller}, ExtractTargetOrFormIAMAction(r), "*",
				awsmeta.CallerArn(ctx), "")

			if cfg.Audit {
				return next(c)
			}

			return writeAccessDenied(c, ExtractTargetOrFormIAMAction(r), "*")
		}

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
	resourceARN, action, condCtx = scope.override(resourceARN, action, condCtx)

	resourceDocs := canonicalDocs(collectResourcePolicies(ctx, cfg.ResourceProviders, resourceARN))

	matchResource := arn.Canonical(resourceARN)
	if matchResource == "" {
		matchResource = "*"
	}

	var boundaryDocs []string
	if boundaryDoc != "" {
		boundaryDocs = []string{boundaryDoc}
	}

	dec := decideScoped(policyDocs, boundaryDocs, resourceDocs, action, matchResource, condCtx, scope)

	var scpMsg string

	if dec.allow {
		if v, denied := cfg.scpDecision(action, matchResource, condCtx); denied {
			dec, scpMsg = v.toDecision(), scpDeniedMessage(condCtx.PrincipalARN, action, v)
		}
	}

	if cfg.Recorder != nil {
		dec = dec.attribute(backend, attribution{
			principalArn: condCtx.PrincipalARN, resourceArn: resourceARN, identity: policyDocs, resource: resourceDocs,
		})
	}

	cfg.record(c, dec, action, matchResource, condCtx.PrincipalARN, callerName)

	if dec.allow {
		return next(c)
	}

	if cfg.Audit {
		log.InfoContext(ctx, "IAM audit: request would be denied",
			"caller", callerName, "action", action, "resource", matchResource, "reason", dec.reason)

		return next(c)
	}

	log.InfoContext(ctx, "IAM enforcement: access denied ("+dec.reason+")",
		"caller", callerName, "action", action, "resource", matchResource, "reason", dec.reason,
		"matchedSid", dec.match.Sid)

	if scpMsg != "" && scope.Deny == nil {
		return writeAccessDeniedMsg(c, action, matchResource, scpMsg)
	}

	return scope.deny(c, action, matchResource)
}

// decideRequest applies identity, boundary and resource policies to one request.
func decideRequest(
	policyDocs, boundaryDocs, resourceDocs []string,
	action, matchResource string, condCtx ConditionContext,
) requestDecision {
	idResult, match := ExplainPolicies(policyDocs, action, matchResource, condCtx)
	identityAllowed := idResult == EvalAllow

	idResult, boundaryDenied, _, bt := applyBoundaryTraced(boundaryDocs, action, matchResource, condCtx, idResult)

	explainResource := func() (EvaluationResult, EvalMatch) {
		if len(resourceDocs) == 0 {
			return EvalImplicitDeny, EvalMatch{}
		}

		return ExplainPolicies(resourceDocs, action, matchResource, condCtx)
	}

	dec := decideAfterBoundary(idResult, identityAllowed, boundaryDenied, bt, match, explainResource)
	dec.boundary = bt

	return dec
}

func decideAfterBoundary(
	idResult EvaluationResult, identityAllowed, boundaryDenied bool, bt boundaryTrace, match EvalMatch,
	resource func() (EvaluationResult, EvalMatch),
) requestDecision {
	boundaryDeny := requestDecision{
		reason: reasonPermissionBoundary, source: sourceBoundary, match: bt.match, explicit: boundaryDenied,
	}
	boundaryDeny.match.ConditionKeys = match.ConditionKeys

	if boundaryDenied {
		return boundaryDeny
	}

	if idResult == EvalExplicitDeny {
		return requestDecision{reason: reasonIdentityPolicy, source: sourceIdentity, match: match, explicit: true}
	}

	if resResult, resMatch := resource(); resMatch.Matched {
		return requestDecision{
			allow: resResult == EvalAllow, explicit: resResult == EvalExplicitDeny,
			reason: reasonResourcePolicy, source: sourceResource, match: resMatch,
		}
	}

	if idResult == EvalAllow {
		return requestDecision{allow: true, reason: reasonIdentityPolicy, source: sourceIdentity, match: match}
	}

	if identityAllowed {
		boundaryDeny.match = EvalMatch{ConditionKeys: match.ConditionKeys}

		return boundaryDeny
	}

	return requestDecision{reason: reasonImplicitDeny, match: match}
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
		SecureTransport:  secureTransportValue(r),
	}
}

// secureTransportValue reports whether the request arrived over TLS, as the
// aws:SecureTransport condition key expects ("true"/"false"). Checks r.TLS
// directly (gopherstack terminating TLS itself) and X-Forwarded-Proto (behind
// a reverse proxy), the same two signals services/sqs's CreateQueue already
// uses to pick a scheme for QueueURL.
func secureTransportValue(r *http.Request) string {
	if r.TLS != nil {
		return credTrue
	}

	if strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
		return credTrue
	}

	return "false"
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
	before, after, ok := strings.CutLast(addr, ":")
	if !ok {
		return addr, "", nil
	}

	return before, after, nil
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

func writeJSONRPCAccessDenied(c *echo.Context, action, msg string) error {
	resp := jsonRPCErrorResponse{
		Type:    "com.amazon.coral.service#AccessDeniedException",
		Message: cmp.Or(msg, "User is not authorized to perform: "+action),
	}

	body, err := json.Marshal(resp)
	if err != nil {
		return c.String(http.StatusBadRequest, `{"__type":"AccessDeniedException"}`)
	}

	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")

	return c.Blob(http.StatusBadRequest, "application/x-amz-json-1.0", body)
}

// restJSONActionPrefixes are IAM service prefixes whose SDKs use the REST-JSON protocol.
//
//nolint:gochecknoglobals // read-only package-level lookup table
var restJSONActionPrefixes = []string{"eks:", "bedrock:", "lambda:", "apigateway:"}

func isRESTJSONAction(action string) bool {
	for _, p := range restJSONActionPrefixes {
		if strings.HasPrefix(action, p) {
			return true
		}
	}

	return false
}

// writeRESTJSONAccessDenied answers with the code in X-Amzn-Errortype, which REST-JSON SDKs read first.
func writeRESTJSONAccessDenied(c *echo.Context, action, msg string) error {
	c.Response().Header().Set("X-Amzn-Errortype", "AccessDeniedException")

	return c.JSON(http.StatusForbidden, map[string]string{
		"message": cmp.Or(msg, "User is not authorized to perform: "+action),
	})
}

func writeS3AccessDenied(c *echo.Context, resource, msg string) error {
	reqID := c.Response().Header().Get("X-Amz-Request-Id")
	if reqID == "" {
		reqID = "gopherstack-request"
	}

	resp := s3AccessDeniedError{
		Code:      codeAccessDenied,
		Message:   cmp.Or(msg, "Access Denied"),
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

func writeQueryXMLAccessDenied(c *echo.Context, action, msg string) error {
	resp := accessDeniedResponse{
		Xmlns: iamXMLNS,
		Error: iamDeniedError{
			Code:    "AccessDenied",
			Message: cmp.Or(msg, "User is not authorized to perform: "+action),
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
	return writeAccessDeniedMsg(c, action, resource, "")
}

// writeAccessDeniedMsg is writeAccessDenied with msg replacing the default text when non-empty.
func writeAccessDeniedMsg(c *echo.Context, action, resource, msg string) error {
	r := c.Request()
	if isJSONRPCRequest(r) {
		return writeJSONRPCAccessDenied(c, action, msg)
	}

	if isRESTJSONAction(action) {
		return writeRESTJSONAccessDenied(c, action, msg)
	}

	if isS3Request(r) {
		return writeS3AccessDenied(c, resource, msg)
	}

	return writeQueryXMLAccessDenied(c, action, msg)
}

// canonicalDocs rewrites non-commercial ARN partitions to the form the evaluator matches against.
func canonicalDocs(docs []string) []string {
	out := make([]string, len(docs))
	for i, d := range docs {
		out[i] = arn.Canonical(d)
	}

	return out
}
