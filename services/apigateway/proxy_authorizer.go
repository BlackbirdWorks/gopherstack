package apigateway

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// authorizerCacheEntry is a cached Lambda authorizer policy; a nil policy denies.
type authorizerCacheEntry struct {
	expiresAt time.Time
	policy    *PolicyDocument
	key       string
}

// authorizerCache caches Lambda authorizer policies keyed by authorizer, stage and identity-source values.
type authorizerCache struct {
	entries    map[string]*list.Element
	order      *list.List
	maxEntries int
	mu         sync.Mutex
}

func newAuthorizerCache() *authorizerCache {
	return newAuthorizerCacheWithMaxEntries(defaultAuthorizerCacheMaxEntries)
}

func newAuthorizerCacheWithMaxEntries(maxEntries int) *authorizerCache {
	if maxEntries <= 0 {
		maxEntries = 1
	}

	return &authorizerCache{
		entries:    make(map[string]*list.Element),
		order:      list.New(),
		maxEntries: maxEntries,
	}
}

// get returns the cached policy and whether an unexpired entry was found.
func (c *authorizerCache) get(key string) (*PolicyDocument, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[key]
	if !ok {
		return nil, false
	}

	e, ok := elem.Value.(authorizerCacheEntry)
	if !ok || !time.Now().Before(e.expiresAt) {
		c.removeElement(elem)

		return nil, false
	}

	c.order.MoveToFront(elem)

	return e.policy, true
}

// set stores the policy under key for ttl; a non-positive ttl disables caching.
func (c *authorizerCache) set(key string, policy *PolicyDocument, ttl time.Duration) {
	if ttl <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	entry := authorizerCacheEntry{key: key, policy: policy, expiresAt: now.Add(ttl)}

	if elem, ok := c.entries[key]; ok {
		elem.Value = entry
		c.order.MoveToFront(elem)

		return
	}

	if len(c.entries) >= c.maxEntries {
		c.evictExpired(now)
	}

	c.entries[key] = c.order.PushFront(entry)

	for len(c.entries) > c.maxEntries {
		c.removeElement(c.order.Back())
	}
}

// evictExpired drops every entry whose TTL has elapsed at now.
func (c *authorizerCache) evictExpired(now time.Time) {
	for elem := c.order.Back(); elem != nil; {
		prev := elem.Prev()

		if e, ok := elem.Value.(authorizerCacheEntry); !ok || !now.Before(e.expiresAt) {
			c.removeElement(elem)
		}

		elem = prev
	}
}

// flush removes all entries from the cache (used by FlushStageAuthorizersCache).
func (c *authorizerCache) flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = make(map[string]*list.Element)
	c.order.Init()
}

func (c *authorizerCache) removeElement(elem *list.Element) {
	if elem == nil {
		return
	}

	if entry, ok := elem.Value.(authorizerCacheEntry); ok {
		delete(c.entries, entry.key)
	}

	c.order.Remove(elem)
}

// AuthorizerEvent is the event payload sent to a Lambda authorizer function.
type AuthorizerEvent struct {
	Headers               map[string]string  `json:"headers,omitempty"`
	QueryStringParameters map[string]string  `json:"queryStringParameters,omitempty"`
	StageVariables        map[string]string  `json:"stageVariables,omitempty"`
	RequestContext        LambdaProxyContext `json:"requestContext"`
	Type                  string             `json:"type"`
	AuthorizationToken    string             `json:"authorizationToken,omitempty"`
	MethodArn             string             `json:"methodArn"`
	Resource              string             `json:"resource,omitempty"`
	Path                  string             `json:"path,omitempty"`
	HTTPMethod            string             `json:"httpMethod,omitempty"`
}

// authorizerTTL returns the authorizer result TTL; zero disables caching.
func authorizerTTL(auth *Authorizer) time.Duration {
	if auth.AuthorizerResultTTLInSeconds <= 0 {
		return 0
	}

	return time.Duration(auth.AuthorizerResultTTLInSeconds) * time.Second
}

// writeAuthorizerError writes API Gateway's JSON error body for an authorizer rejection.
func writeAuthorizerError(w http.ResponseWriter, status int, message string) {
	w.Header().Set(headerContentType, "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{jsonMessageKey: message})
}

// runAuthorizer enforces the method's authorizer and returns true if the request
// was denied (response already written).
func (h *Handler) runAuthorizer(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	apiID, stageName string,
	cfg *DeploymentConfig,
	authorizerID string,
) bool {
	auth, ok := cfg.Authorizers[authorizerID]
	if !ok {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: authorizer not found", "authorizerId", authorizerID)
		http.Error(w, "Authorizer configuration error", http.StatusInternalServerError)

		return true
	}

	if auth.Type == AuthTypeCognitoUserPool {
		return h.runCognitoAuthorizer(ctx, w, r, auth)
	}

	ttl := authorizerTTL(auth)

	identity, valid := h.lambdaAuthorizerIdentity(r, auth, apiID, stageName, ttl)
	if !valid {
		writeAuthorizerError(w, http.StatusUnauthorized, msgUnauthorized)

		return true
	}

	cacheKey := authorizerID + "\n" + stageName + "\n" + strings.Join(identity, "\n")
	if len(identity) == 0 {
		ttl = 0
	}

	resourcePath := authorizerResourcePath(r, apiID, stageName)
	methodArn := authorizerMethodArn(ctx, r, apiID, stageName, resourcePath)

	if ttl > 0 {
		if policy, found := h.authCache.get(cacheKey); found {
			return writeAuthorizerDecision(w, policy, methodArn)
		}
	}

	if h.lambda == nil {
		http.Error(w, "Lambda integration not configured", http.StatusServiceUnavailable)

		return true
	}

	event := h.buildAuthorizerEvent(ctx, r, auth, apiID, stageName)
	event.MethodArn = methodArn

	payload, _ := json.Marshal(event)

	funcName := ExtractLambdaFunctionName(auth.AuthorizerURI)
	respBytes, _, invokeErr := h.lambda.InvokeFunction(ctx, funcName, "RequestResponse", payload)
	if invokeErr != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: authorizer invocation failed",
			"authorizerId", authorizerID, "error", invokeErr)
		writeAuthorizerError(w, http.StatusUnauthorized, msgUnauthorized)

		return true
	}

	var authResp AuthorizerResponse
	if parseErr := json.Unmarshal(respBytes, &authResp); parseErr != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: failed to parse authorizer response", "error", parseErr)
		writeAuthorizerError(w, http.StatusUnauthorized, msgUnauthorized)

		return true
	}

	h.authCache.set(cacheKey, authResp.PolicyDocument, ttl)

	return writeAuthorizerDecision(w, authResp.PolicyDocument, methodArn)
}

// writeAuthorizerDecision evaluates policy against methodArn and writes a 403 on denial.
func writeAuthorizerDecision(w http.ResponseWriter, policy *PolicyDocument, methodArn string) bool {
	allowed, explicit := evaluateAuthorizerPolicy(policy, methodArn)
	if allowed {
		return false
	}

	msg := msgNotAuthorized
	if explicit {
		msg = msgExplicitDeny
	}

	writeAuthorizerError(w, http.StatusForbidden, msg)

	return true
}

// lambdaAuthorizerIdentity returns the cache-key identity values; false means 401
// without invoking (bad TOKEN, or REQUEST with caching on and a missing source).
func (h *Handler) lambdaAuthorizerIdentity(
	r *http.Request, auth *Authorizer, apiID, stageName string, ttl time.Duration,
) ([]string, bool) {
	if auth.Type == "TOKEN" {
		token := extractTokenFromIdentitySource(r, auth.IdentitySource)
		if token == "" {
			return nil, false
		}

		if auth.IdentityValidationExpression != "" {
			if re := h.cachedRegexp(auth.IdentityValidationExpression); re != nil && !re.MatchString(token) {
				return nil, false
			}
		}

		return []string{token}, true
	}

	if ttl <= 0 {
		return nil, true
	}

	var stageVars map[string]string

	sources := splitIdentitySources(auth.IdentitySource)
	values := make([]string, 0, len(sources))

	for _, src := range sources {
		if strings.HasPrefix(src, "stageVariables.") && stageVars == nil {
			stageVars = h.stageVars(apiID, stageName)
		}

		v := resolveRESTIdentitySource(r, src, apiID, stageName, stageVars)
		if v == "" {
			return nil, false
		}

		values = append(values, v)
	}

	return values, true
}

// splitIdentitySources splits a comma-separated IdentitySource into trimmed expressions.
func splitIdentitySources(identitySource string) []string {
	var out []string

	for part := range strings.SplitSeq(identitySource, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}

	return out
}

// resolveRESTIdentitySource resolves one REQUEST authorizer identity source
// (method.request.header.X, method.request.querystring.X, context.X, stageVariables.X).
func resolveRESTIdentitySource(
	r *http.Request, src, apiID, stageName string, stageVars map[string]string,
) string {
	if name, ok := strings.CutPrefix(src, "method.request.header."); ok {
		return r.Header.Get(name)
	}

	if name, ok := strings.CutPrefix(src, "method.request.querystring."); ok {
		return r.URL.Query().Get(name)
	}

	if name, ok := strings.CutPrefix(src, "stageVariables."); ok {
		return stageVars[name]
	}

	name, ok := strings.CutPrefix(src, "context.")
	if !ok {
		return ""
	}

	switch name {
	case "httpMethod":
		return r.Method
	case "path":
		return authorizerResourcePath(r, apiID, stageName)
	case "stage":
		return stageName
	case "apiId":
		return apiID
	case "identity.sourceIp":
		return realClientIP(r)
	default:
		return ""
	}
}

// runCognitoAuthorizer verifies a JWT token for COGNITO_USER_POOLS authorizer type.
func (h *Handler) runCognitoAuthorizer(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	auth *Authorizer,
) bool {
	tokenSource := auth.IdentitySource
	if tokenSource == "" {
		tokenSource = defaultIdentitySource
	}

	var tokenStr string

	if headerName, found := strings.CutPrefix(tokenSource, "method.request.header."); found {
		tokenStr = r.Header.Get(headerName)
	} else {
		tokenStr = r.Header.Get("Authorization")
	}

	if tokenStr == "" {
		writeAuthorizerError(w, http.StatusUnauthorized, msgUnauthorized)

		return true
	}

	keyfunc := func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, errUnexpectedSigningMethod
		}

		iss, _ := t.Claims.(jwt.MapClaims)["iss"].(string)
		kid, _ := t.Header["kid"].(string)

		if h.jwksProvider == nil {
			return nil, errNoJWKSProvider
		}

		return h.jwksProvider.GetJWTPublicKey(iss, kid)
	}

	token, parseErr := jwt.Parse(tokenStr, keyfunc, jwt.WithExpirationRequired())
	if parseErr != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: cognito authorizer invalid token", "error", parseErr)
		writeAuthorizerError(w, http.StatusUnauthorized, msgUnauthorized)

		return true
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		writeAuthorizerError(w, http.StatusUnauthorized, msgUnauthorized)

		return true
	}

	*r = *r.WithContext(context.WithValue(r.Context(), ctxKeyClaims, claims))

	return false
}

// authorizerResourcePath strips internal proxy prefixes so the path matches the API definition.
func authorizerResourcePath(r *http.Request, apiID, stageName string) string {
	resourcePath := r.URL.Path
	prefixes := []string{
		fmt.Sprintf("/restapis/%s/%s/_user_request_", apiID, stageName),
		fmt.Sprintf("/restapis/%s/%s", apiID, stageName),
		fmt.Sprintf("/proxy/%s/%s", apiID, stageName),
		"/" + stageName,
	}

	for _, prefix := range prefixes {
		if after, ok := strings.CutPrefix(resourcePath, prefix); ok {
			resourcePath = after
			if resourcePath == "" {
				resourcePath = "/"
			} else if !strings.HasPrefix(resourcePath, "/") {
				resourcePath = "/" + resourcePath
			}

			break
		}
	}

	return resourcePath
}

// authorizerMethodArn builds the execute-api method ARN the authorizer policy is evaluated against.
func authorizerMethodArn(ctx context.Context, r *http.Request, apiID, stageName, resourcePath string) string {
	region := awsmeta.Region(ctx)
	if region == "" {
		region = config.DefaultRegion
	}

	return arn.Build("execute-api", region, awsmeta.Account(ctx),
		fmt.Sprintf("%s/%s/%s%s", apiID, stageName, r.Method, resourcePath))
}

// buildAuthorizerEvent constructs the event payload for the Lambda authorizer.
func (h *Handler) buildAuthorizerEvent(
	ctx context.Context, r *http.Request, auth *Authorizer, apiID, stageName string,
) AuthorizerEvent {
	headers := make(map[string]string)
	for k, vs := range r.Header {
		if len(vs) > 0 {
			headers[strings.ToLower(k)] = vs[0]
		}
	}

	qsp := make(map[string]string)
	for k, vs := range r.URL.Query() {
		if len(vs) > 0 {
			qsp[k] = vs[0]
		}
	}

	resourcePath := authorizerResourcePath(r, apiID, stageName)
	methodArn := authorizerMethodArn(ctx, r, apiID, stageName, resourcePath)

	event := AuthorizerEvent{
		Type:                  auth.Type,
		MethodArn:             methodArn,
		Path:                  resourcePath,
		HTTPMethod:            r.Method,
		Resource:              resourcePath,
		Headers:               headers,
		QueryStringParameters: qsp,
		RequestContext: LambdaProxyContext{
			Stage: stageName,
			APIId: apiID,
		},
	}

	// For TOKEN type: extract token from identity source header.
	if auth.Type == "TOKEN" {
		token := extractTokenFromIdentitySource(r, auth.IdentitySource)
		event.AuthorizationToken = token
		// TOKEN authorizers only need type, token, and methodArn.
		event.Headers = nil
		event.QueryStringParameters = nil
	}

	return event
}

// extractTokenFromIdentitySource extracts the token value from the request
// based on the authorizer's identitySource (e.g. "method.request.header.Authorization").
func extractTokenFromIdentitySource(r *http.Request, identitySource string) string {
	const headerPrefix = "method.request.header."
	if strings.HasPrefix(identitySource, headerPrefix) {
		headerName := identitySource[len(headerPrefix):]

		return r.Header.Get(headerName)
	}

	// Default: try Authorization header.

	return r.Header.Get("Authorization")
}

// evaluateAuthorizerPolicy evaluates a Lambda authorizer policy for methodArn.
// An explicit Deny wins; otherwise a matching Allow is needed (implicit deny).
func evaluateAuthorizerPolicy(policy *PolicyDocument, methodArn string) (bool, bool) {
	if policy == nil {
		return false, false
	}

	allowed := false

	for _, stmt := range policy.Statement {
		if !policyActionCoversInvoke(stmt.Action) || !policyResourceMatches(stmt.Resource, methodArn) {
			continue
		}

		switch strings.ToUpper(stmt.Effect) {
		case "DENY":
			return false, true
		case "ALLOW":
			allowed = true
		}
	}

	return allowed, false
}

// policyStrings normalizes an IAM string-or-list field.
func policyStrings(v any) []string {
	switch t := v.(type) {
	case string:
		return []string{t}
	case []any:
		out := make([]string, 0, len(t))

		for _, e := range t {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}

		return out
	default:
		return nil
	}
}

func policyActionCoversInvoke(action any) bool {
	actions := policyStrings(action)
	if len(actions) == 0 {
		return true
	}

	for _, a := range actions {
		if policyGlob(a, "execute-api:Invoke") {
			return true
		}
	}

	return false
}

func policyResourceMatches(resource any, methodArn string) bool {
	for _, pat := range policyStrings(resource) {
		if policyGlob(pat, methodArn) {
			return true
		}
	}

	return false
}

// policyGlob matches an IAM pattern with '*' and '?' wildcards.
func policyGlob(pattern, s string) bool {
	var p, i, star, mark int

	star = -1

	for i < len(s) {
		switch {
		case p < len(pattern) && (pattern[p] == '?' || pattern[p] == s[i]):
			p++
			i++
		case p < len(pattern) && pattern[p] == '*':
			star, mark = p, i
			p++
		case star >= 0:
			mark++
			p, i = star+1, mark
		default:
			return false
		}
	}

	for p < len(pattern) && pattern[p] == '*' {
		p++
	}

	return p == len(pattern)
}
