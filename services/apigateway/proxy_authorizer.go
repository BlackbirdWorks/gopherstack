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

// authorizerCacheEntry holds a cached authorizer result.
type authorizerCacheEntry struct {
	expiresAt time.Time
	key       string
	allowed   bool
}

// authorizerCache caches Lambda authorizer results keyed by authorizerID + cacheKey.
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

// get returns (allowed, found) for the given cache key.
func (c *authorizerCache) get(key string) (bool, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	elem, ok := c.entries[key]
	if !ok {
		return false, false
	}

	e, ok := elem.Value.(authorizerCacheEntry)
	if !ok {
		c.removeElement(elem)

		return false, false
	}

	if time.Now().After(e.expiresAt) {
		c.removeElement(elem)

		return false, false
	}

	c.order.MoveToFront(elem)

	return e.allowed, true
}

// set stores the result for the given cache key with a TTL.
func (c *authorizerCache) set(key string, allowed bool, ttl time.Duration) {
	if ttl <= 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, ok := c.entries[key]; ok {
		entry, valueOk := elem.Value.(authorizerCacheEntry)
		if !valueOk {
			c.removeElement(elem)
		} else {
			entry.allowed = allowed
			entry.expiresAt = time.Now().Add(ttl)
			elem.Value = entry
			c.order.MoveToFront(elem)

			return
		}
	}

	elem := c.order.PushFront(authorizerCacheEntry{
		key:       key,
		allowed:   allowed,
		expiresAt: time.Now().Add(ttl),
	})
	c.entries[key] = elem

	for len(c.entries) > c.maxEntries {
		c.removeElement(c.order.Back())
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

	entry, ok := elem.Value.(authorizerCacheEntry)
	if !ok {
		c.order.Remove(elem)

		return
	}

	delete(c.entries, entry.key)
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

// runAuthorizer invokes the Lambda authorizer and returns true if the request
// should be denied (i.e., the response was written with a 4xx status).
func (h *Handler) runAuthorizer(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	apiID, stageName, authorizerID string,
) bool {
	auth, err := h.Backend.GetAuthorizer(apiID, authorizerID)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: authorizer not found", "authorizerId", authorizerID)
		http.Error(w, "Authorizer configuration error", http.StatusInternalServerError)

		return true
	}

	// Determine TTL for cache (authorizer-level setting, default 300 s).
	ttl := defaultAuthorizerTTL
	if auth.AuthorizerResultTTLInSeconds > 0 {
		ttl = time.Duration(auth.AuthorizerResultTTLInSeconds) * time.Second
	} else if auth.AuthorizerResultTTLInSeconds < 0 {
		ttl = 0 // caching disabled
	}

	// Build cache key: for TOKEN type use the token, for REQUEST type use the full path.
	cacheKey := h.authorizerCacheKey(r, auth, authorizerID)
	if ttl > 0 {
		if allowed, found := h.authCache.get(cacheKey); found {
			if !allowed {
				http.Error(w, "Forbidden", http.StatusForbidden)

				return true
			}

			return false
		}
	}

	// Cognito authorizers verify the JWT locally — no Lambda needed.
	if auth.Type == AuthTypeCognitoUserPool {
		return h.runCognitoAuthorizer(ctx, w, r, auth, cacheKey, ttl)
	}

	// All other authorizer types invoke a Lambda function.
	if h.lambda == nil {
		http.Error(w, "Lambda integration not configured", http.StatusServiceUnavailable)

		return true
	}

	// Build the authorizer event based on type.
	event := h.buildAuthorizerEvent(ctx, r, auth, apiID, stageName)

	payload, _ := json.Marshal(event)

	funcName := ExtractLambdaFunctionName(auth.AuthorizerURI)
	respBytes, _, invokeErr := h.lambda.InvokeFunction(ctx, funcName, "RequestResponse", payload)
	if invokeErr != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: authorizer invocation failed",
			"authorizerId", authorizerID, "error", invokeErr)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)

		return true
	}

	// Parse the authorizer response (IAM policy document).
	var authResp AuthorizerResponse
	if parseErr := json.Unmarshal(respBytes, &authResp); parseErr != nil {
		logger.Load(ctx).WarnContext(ctx, "APIGateway proxy: failed to parse authorizer response", "error", parseErr)
		http.Error(w, "Unauthorized", http.StatusUnauthorized)

		return true
	}

	// Evaluate the policy document to determine allow/deny.
	allowed := isAuthorizerAllowed(&authResp)
	h.authCache.set(cacheKey, allowed, ttl)

	if !allowed {
		http.Error(w, "Forbidden", http.StatusForbidden)

		return true
	}

	return false
}

// runCognitoAuthorizer verifies a JWT token for COGNITO_USER_POOLS authorizer type.
func (h *Handler) runCognitoAuthorizer(
	ctx context.Context,
	w http.ResponseWriter,
	r *http.Request,
	auth *Authorizer,
	cacheKey string,
	ttl time.Duration,
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
		http.Error(w, "Unauthorized", http.StatusUnauthorized)

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
		http.Error(w, "Unauthorized", http.StatusUnauthorized)

		return true
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok || !token.Valid {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)

		return true
	}

	*r = *r.WithContext(context.WithValue(r.Context(), ctxKeyClaims, claims))

	if ttl > 0 {
		h.authCache.set(cacheKey, true, ttl)
	}

	return false
}

// authorizerCacheKey builds the cache key for an authorizer invocation.
// TOKEN / COGNITO_USER_POOLS: authorizerID + ":" + extracted token (per-token granularity)
// REQUEST: authorizerID + ":" + method + " " + path (per-request granularity).
func (h *Handler) authorizerCacheKey(r *http.Request, auth *Authorizer, authorizerID string) string {
	if auth.Type == "TOKEN" || auth.Type == AuthTypeCognitoUserPool {
		token := extractTokenFromIdentitySource(r, auth.IdentitySource)
		token = h.applyIdentityValidation(auth.IdentityValidationExpression, token)

		return authorizerID + ":" + token
	}

	return authorizerID + ":" + r.Method + " " + r.URL.Path
}

// applyIdentityValidation normalizes a token using the authorizer's identityValidationExpression.
// Returns the first capture group if present, the full match otherwise, or the original token unchanged.
func (h *Handler) applyIdentityValidation(expr, token string) string {
	if expr == "" {
		return token
	}

	re := h.cachedRegexp(expr)
	if re == nil {
		return token
	}

	m := re.FindStringSubmatch(token)
	if len(m) > 1 {
		return m[1]
	}

	if len(m) == 1 {
		return m[0]
	}

	return token
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

	// Strip internal proxy prefixes so the resource path matches the API definition path.
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

	region := awsmeta.Region(ctx)
	if region == "" {
		region = config.DefaultRegion
	}

	methodArn := arn.Build("execute-api", region, awsmeta.Account(ctx),
		fmt.Sprintf("%s/%s/%s%s", apiID, stageName, r.Method, resourcePath))

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

// isAuthorizerAllowed evaluates the IAM policy document returned by a Lambda authorizer.
// Returns true if at least one Allow statement exists and no explicit Deny overrides it.
func isAuthorizerAllowed(authResp *AuthorizerResponse) bool {
	if authResp.PolicyDocument == nil {
		return false
	}

	allow := false

	for _, stmt := range authResp.PolicyDocument.Statement {
		effect := strings.ToUpper(stmt.Effect)
		if effect == "DENY" {
			return false
		}
		if effect == "ALLOW" {
			allow = true
		}
	}

	return allow
}
