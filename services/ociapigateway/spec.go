package ociapigateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Backend, authentication and policy discriminators.
const (
	backendHTTP    = "HTTP_BACKEND"
	backendFn      = "ORACLE_FUNCTIONS_BACKEND"
	backendStock   = "STOCK_RESPONSE_BACKEND"
	backendDynamic = "DYNAMIC_ROUTING_BACKEND"

	authJWT       = "JWT_AUTHENTICATION"
	failureModify = "MODIFY_RESPONSE"
	authToken     = "TOKEN_AUTHENTICATION"
	authCustom    = "CUSTOM_AUTHENTICATION"

	keysStatic = "STATIC_KEYS"
	keysRemote = "REMOTE_JWKS"

	methodAny       = "ANY"
	maxStatusCode   = 599
	minStatusCode   = 100
	maxRoutes       = 1000
	maxSpecDepth    = 2
	fnOCIDPrefix    = "ocid1.fnfunc."
	rateKeyClient   = "CLIENT_IP"
	rateKeyTotal    = "TOTAL"
	selectorSingle  = "SINGLE"
	anyOf           = "ANY_OF"
	wildcardKey     = "WILDCARD"
	authzAnonymous  = "ANONYMOUS"
	jsonNull        = "null"
	keyType         = "type"
	msgUnauthorized = "Unauthorized"
)

type headerSpec struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type selectionKey struct {
	IsDefault  *bool    `json:"isDefault"`
	Type       string   `json:"type"`
	Name       string   `json:"name"`
	Expression string   `json:"expression"`
	Values     []string `json:"values"`
}

type dynamicBackend struct {
	Backend *backendDoc  `json:"backend"`
	Key     selectionKey `json:"key"`
}

type backendDoc struct {
	SelectionSource *struct {
		Type     string `json:"type"`
		Selector string `json:"selector"`
	} `json:"selectionSource"`
	URL                 *string          `json:"url"`
	ConnectTimeout      *float64         `json:"connectTimeoutInSeconds"`
	ReadTimeout         *float64         `json:"readTimeoutInSeconds"`
	SendTimeout         *float64         `json:"sendTimeoutInSeconds"`
	IsSslVerifyDisabled *bool            `json:"isSslVerifyDisabled"`
	Status              *int             `json:"status"`
	Body                *string          `json:"body"`
	Type                string           `json:"type"`
	FunctionID          string           `json:"functionId"`
	Headers             []headerSpec     `json:"headers"`
	RoutingBackends     []dynamicBackend `json:"routingBackends"`
}

type corsDoc struct {
	MaxAge           *int     `json:"maxAgeInSeconds"`
	AllowCredentials *bool    `json:"isAllowCredentialsEnabled"`
	AllowedOrigins   []string `json:"allowedOrigins"`
	AllowedMethods   []string `json:"allowedMethods"`
	AllowedHeaders   []string `json:"allowedHeaders"`
	ExposedHeaders   []string `json:"exposedHeaders"`
}

type rateDoc struct {
	Rate *int   `json:"rateInRequestsPerSecond"`
	Key  string `json:"rateKey"`
}

type claimDoc struct {
	IsRequired *bool    `json:"isRequired"`
	Key        string   `json:"key"`
	Values     []string `json:"values"`
}

type staticKeyDoc struct {
	Kid    string `json:"kid"`
	Format string `json:"format"`
	Key    string `json:"key"`
	N      string `json:"n"`
	E      string `json:"e"`
	Kty    string `json:"kty"`
}

type additionalDoc struct {
	Issuers      []string   `json:"issuers"`
	Audiences    []string   `json:"audiences"`
	VerifyClaims []claimDoc `json:"verifyClaims"`
}

type validationDoc struct {
	Additional          *additionalDoc    `json:"additionalValidationPolicy"`
	ClientDetails       *clientDetailsDoc `json:"clientDetails"`
	SourceURI           *sourceURIDoc     `json:"sourceUriDetails"`
	URI                 string            `json:"uri"`
	Type                string            `json:"type"`
	MaxCacheHours       *int              `json:"maxCacheDurationInHours"`
	IsSslVerifyDisabled *bool             `json:"isSslVerifyDisabled"`
	Keys                []staticKeyDoc    `json:"keys"`
}

type publicKeysDoc struct {
	URI           string         `json:"uri"`
	Type          string         `json:"type"`
	MaxCacheHours *int           `json:"maxCacheDurationInHours"`
	Keys          []staticKeyDoc `json:"keys"`
}

type failureDoc struct {
	ResponseHeaders *transformDoc `json:"responseHeaderTransformations"`
	Type            string        `json:"type"`
	ResponseCode    string        `json:"responseCode"`
	ResponseMessage string        `json:"responseMessage"`
}

type authDoc struct {
	ValidationFailure  *failureDoc       `json:"validationFailurePolicy"`
	PublicKeys         *publicKeysDoc    `json:"publicKeys"`
	TokenHeader        *string           `json:"tokenHeader"`
	TokenQueryParam    *string           `json:"tokenQueryParam"`
	TokenAuthScheme    *string           `json:"tokenAuthScheme"`
	ValidationPolicy   *validationDoc    `json:"validationPolicy"`
	IsAnonymousAllowed *bool             `json:"isAnonymousAccessAllowed"`
	MaxClockSkew       *float64          `json:"maxClockSkewInSeconds"`
	Parameters         map[string]string `json:"parameters"`
	Type               string            `json:"type"`
	FunctionID         string            `json:"functionId"`
	CacheKey           []string          `json:"cacheKey"`
	VerifyClaims       []claimDoc        `json:"verifyClaims"`
	Audiences          []string          `json:"audiences"`
	Issuers            []string          `json:"issuers"`
}

type requestPolicies struct {
	Authentication        *authDoc        `json:"authentication"`
	RateLimiting          *rateDoc        `json:"rateLimiting"`
	Cors                  *corsDoc        `json:"cors"`
	MutualTLS             *mtlsDoc        `json:"mutualTls"`
	UsagePlans            *usagePlansDoc  `json:"usagePlans"`
	DynamicAuthentication *dynamicAuthDoc `json:"dynamicAuthentication"`
}

type setItem struct {
	Name     string   `json:"name"`
	IfExists string   `json:"ifExists"`
	Values   []string `json:"values"`
}

type renameItem struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type nameItem struct {
	Name string `json:"name"`
}

type transformDoc struct {
	SetHeaders *struct {
		Items []setItem `json:"items"`
	} `json:"setHeaders"`
	RenameHeaders *struct {
		Items []renameItem `json:"items"`
	} `json:"renameHeaders"`
	FilterHeaders *struct {
		Type  string     `json:"type"`
		Items []nameItem `json:"items"`
	} `json:"filterHeaders"`
	SetQuery *struct {
		Items []setItem `json:"items"`
	} `json:"setQueryParameters"`
	RenameQuery *struct {
		Items []renameItem `json:"items"`
	} `json:"renameQueryParameters"`
	FilterQuery *struct {
		Type  string     `json:"type"`
		Items []nameItem `json:"items"`
	} `json:"filterQueryParameters"`
}

type authzDoc struct {
	Type         string   `json:"type"`
	AllowedScope []string `json:"allowedScope"`
}

type routeRequestPolicies struct {
	Authorization       *authzDoc           `json:"authorization"`
	Cors                *corsDoc            `json:"cors"`
	HeaderTransform     *transformDoc       `json:"headerTransformations"`
	QueryTransform      *transformDoc       `json:"queryParameterTransformations"`
	QueryValidations    *paramValidationDoc `json:"queryParameterValidations"`
	HeaderValidations   *paramValidationDoc `json:"headerValidations"`
	BodyValidation      *bodyValidationDoc  `json:"bodyValidation"`
	ResponseCacheLookup *cacheLookupDoc     `json:"responseCacheLookup"`
}

type routeResponsePolicies struct {
	HeaderTransform    *transformDoc  `json:"headerTransformations"`
	ResponseCacheStore *cacheStoreDoc `json:"responseCacheStore"`
}

type routeDoc struct {
	Backend          *backendDoc            `json:"backend"`
	RequestPolicies  *routeRequestPolicies  `json:"requestPolicies"`
	ResponsePolicies *routeResponsePolicies `json:"responsePolicies"`
	Path             string                 `json:"path"`
	Methods          []string               `json:"methods"`
}

type specDoc struct {
	RequestPolicies *requestPolicies `json:"requestPolicies"`
	LoggingPolicies json.RawMessage  `json:"loggingPolicies"`
	Routes          []routeDoc       `json:"routes"`
}

var (
	pathCharsRe  = regexp.MustCompile(`^[A-Za-z0-9$\-_.+!*'(),%;:@&=]+$`)
	paramRe      = regexp.MustCompile(`^\{([A-Za-z0-9_]+)(\*?)\}$`)
	prefixCharRe = regexp.MustCompile(`^[A-Za-z0-9$\-_.+!*'(),%;:@&=]+$`)
)

func isPresent(raw json.RawMessage) bool {
	return len(raw) > 0 && !bytes.Equal(raw, []byte(jsonNull))
}

// validatePathPrefix applies the deployment pathPrefix rules documented by Oracle.
func validatePathPrefix(p string) *APIError {
	if !strings.HasPrefix(p, "/") {
		return errParam("pathPrefix must start with a forward slash")
	}

	if p == "/" {
		return nil
	}

	if strings.HasSuffix(p, "/") {
		return errParam("pathPrefix must not end with a forward slash")
	}

	for seg := range strings.SplitSeq(p[1:], "/") {
		if seg == "" {
			return errParam("pathPrefix must not contain adjacent forward slashes")
		}

		if !prefixCharRe.MatchString(seg) {
			return errParam(
				"pathPrefix contains characters that are not allowed (parameters and wildcards are not permitted)",
			)
		}
	}

	return nil
}

// compileSpec decodes and validates a deployment specification.
func compileSpec(raw json.RawMessage) (*compiledSpec, *APIError) {
	if !isPresent(raw) {
		return nil, errMissing("specification is required")
	}

	var doc specDoc
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, errParam("specification is not valid: " + err.Error())
	}

	if len(doc.Routes) > maxRoutes {
		return nil, errLimit(fmt.Sprintf("a specification may have at most %d routes", maxRoutes))
	}

	if err := validateRequestPolicies(doc.RequestPolicies); err != nil {
		return nil, err
	}

	cs := &compiledSpec{doc: &doc}

	for i := range doc.Routes {
		cr, err := compileRoute(&doc.Routes[i], i)
		if err != nil {
			return nil, err
		}

		cs.routes = append(cs.routes, cr)
	}

	if err := checkAnonymousRoutes(&doc); err != nil {
		return nil, err
	}

	if err := checkDuplicateRoutes(cs.routes); err != nil {
		return nil, err
	}

	sortRoutes(cs.routes)

	return cs, nil
}

func validateRequestPolicies(p *requestPolicies) *APIError {
	if p == nil {
		return nil
	}

	if err := validateGlobalPolicies(p); err != nil {
		return err
	}

	if p.RateLimiting != nil {
		if p.RateLimiting.Rate == nil || *p.RateLimiting.Rate < 1 {
			return errParam("rateLimiting.rateInRequestsPerSecond must be a positive integer")
		}

		if p.RateLimiting.Key != rateKeyClient && p.RateLimiting.Key != rateKeyTotal {
			return errParam("rateLimiting.rateKey must be CLIENT_IP or TOTAL")
		}
	}

	if err := validateCors(p.Cors, "requestPolicies.cors"); err != nil {
		return err
	}

	return validateAuth(p.Authentication)
}

func validateCors(c *corsDoc, where string) *APIError {
	if c == nil {
		return nil
	}

	if len(c.AllowedOrigins) == 0 {
		return errParam(where + ".allowedOrigins must not be empty")
	}

	if c.MaxAge != nil && *c.MaxAge < 0 {
		return errParam(where + ".maxAgeInSeconds must not be negative")
	}

	return nil
}

func validateAuth(a *authDoc) *APIError {
	if a == nil {
		return nil
	}

	if a.TokenHeader != nil && *a.TokenHeader != "" && (a.TokenAuthScheme == nil || *a.TokenAuthScheme == "") &&
		a.Type != authCustom {
		return errParam("authentication.tokenAuthScheme is required when tokenHeader is set")
	}

	if err := validateAuthTuning(a); err != nil {
		return err
	}

	switch a.Type {
	case authJWT:
		return validateJWT(a)
	case authToken:
		return validateTokenPolicy(a)
	case authCustom:
		if !strings.HasPrefix(a.FunctionID, fnOCIDPrefix) {
			return errParam("authentication.functionId must be a function OCID")
		}

		return nil
	default:
		return errParam("authentication.type must be JWT_AUTHENTICATION, TOKEN_AUTHENTICATION or CUSTOM_AUTHENTICATION")
	}
}

// maxClockSkewSeconds is the documented maximum of maxClockSkewInSeconds (minimum and default 0).
const maxClockSkewSeconds = 120

func validateAuthTuning(a *authDoc) *APIError {
	if a.MaxClockSkew != nil && (*a.MaxClockSkew < 0 || *a.MaxClockSkew > maxClockSkewSeconds) {
		return errParam("authentication.maxClockSkewInSeconds must be between 0 and 120")
	}

	return validateFailurePolicy(a.ValidationFailure)
}

func validateFailurePolicy(f *failureDoc) *APIError {
	if f == nil {
		return nil
	}

	if f.Type != failureModify {
		return errUnsupported("validationFailurePolicy type " + f.Type)
	}

	if f.ResponseCode != "" && !strings.Contains(f.ResponseCode, "${") {
		if _, ok := failureStatus(f.ResponseCode); !ok {
			return errParam("validationFailurePolicy.responseCode must be an HTTP status code")
		}
	}

	return nil
}

func failureStatus(code string) (int, bool) {
	n, err := strconv.Atoi(strings.TrimSpace(code))

	return n, err == nil && n >= 100 && n <= 599
}

func validateJWT(a *authDoc) *APIError {
	if len(a.Issuers) == 0 || len(a.Audiences) == 0 {
		return errParam("JWT authentication requires issuers and audiences")
	}

	if a.PublicKeys == nil {
		return errMissing("JWT authentication requires publicKeys")
	}

	return validateKeySource(a.PublicKeys.Type, a.PublicKeys.URI, a.PublicKeys.Keys)
}

func validateTokenPolicy(a *authDoc) *APIError {
	if a.ValidationPolicy == nil {
		return errMissing("TOKEN_AUTHENTICATION requires validationPolicy")
	}

	if a.ValidationPolicy.Type == keysRemoteDiscovery {
		return validateRemoteDiscovery(a.ValidationPolicy)
	}

	return validateKeySource(a.ValidationPolicy.Type, a.ValidationPolicy.URI, a.ValidationPolicy.Keys)
}

func validateKeySource(kind, uri string, keys []staticKeyDoc) *APIError {
	switch kind {
	case keysStatic:
		if len(keys) == 0 {
			return errParam("static keys require at least one key")
		}

		for i := range keys {
			if _, err := parseStaticKey(&keys[i]); err != nil {
				return errParam("static key " + keys[i].Kid + ": " + err.Error())
			}
		}

		return nil
	case keysRemote:
		u, err := url.Parse(uri)
		if err != nil || !httpURL(u) {
			return errParam("remote JWKS uri must be an http or https URL")
		}

		return nil
	default:
		return errUnsupported("key source type " + kind)
	}
}

func validateMethods(methods []string) *APIError {
	for _, m := range methods {
		if !slices.Contains([]string{methodAny, "HEAD", "GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}, m) {
			return errParam("unsupported route method " + m)
		}
	}

	return nil
}

func validateBackend(b *backendDoc, depth int) *APIError {
	if b == nil {
		return errMissing("route backend is required")
	}

	switch b.Type {
	case backendHTTP:
		return validateHTTPBackend(b)
	case backendFn:
		if !strings.HasPrefix(b.FunctionID, fnOCIDPrefix) {
			return errParam("ORACLE_FUNCTIONS_BACKEND functionId must be a function OCID")
		}

		return nil
	case backendStock:
		if b.Status == nil || *b.Status < minStatusCode || *b.Status > maxStatusCode {
			return errParam("STOCK_RESPONSE_BACKEND status must be between 100 and 599")
		}

		return nil
	case backendDynamic:
		return validateDynamic(b, depth)
	default:
		return errUnsupported("backend type " + b.Type)
	}
}

var varRe = regexp.MustCompile(`\$\{[^}]*\}`)

// substituteStub replaces ${...} variables so a templated URL can be parsed at validation time.
func substituteStub(s string) string { return varRe.ReplaceAllString(s, "x") }

func validateHTTPBackend(b *backendDoc) *APIError {
	if b.URL == nil {
		return errMissing("HTTP_BACKEND requires url")
	}

	u, err := url.Parse(substituteStub(*b.URL))
	if err != nil || !httpURL(u) {
		return errParam("HTTP_BACKEND url must be an http or https URL")
	}

	return nil
}

func validateDynamic(b *backendDoc, depth int) *APIError {
	if depth >= maxSpecDepth {
		return errParam("dynamic routing backends cannot be nested")
	}

	if b.SelectionSource == nil || b.SelectionSource.Type != selectorSingle || b.SelectionSource.Selector == "" {
		return errParam("DYNAMIC_ROUTING_BACKEND requires a SINGLE selectionSource with a selector")
	}

	if _, _, ok := parseVariable(b.SelectionSource.Selector); !ok {
		return errParam("selectionSource.selector must be request.path[..], request.query[..] or request.headers[..]")
	}

	if len(b.RoutingBackends) == 0 {
		return errParam("DYNAMIC_ROUTING_BACKEND requires routingBackends")
	}

	for i := range b.RoutingBackends {
		rb := &b.RoutingBackends[i]
		if rb.Key.Name == "" {
			return errParam("routing backend key name is required")
		}

		if rb.Key.Type != anyOf && rb.Key.Type != wildcardKey {
			return errParam("routing backend key type must be ANY_OF or WILDCARD")
		}

		if rb.Key.Type == wildcardKey &&
			strings.Count(rb.Key.Expression, "*")+strings.Count(rb.Key.Expression, "+") > 1 {
			return errParam("a wildcard key expression can only contain one wildcard")
		}

		if err := validateBackend(rb.Backend, depth+1); err != nil {
			return err
		}
	}

	return nil
}

func validateRouteRequestPolicies(p *routeRequestPolicies) *APIError {
	if p == nil {
		return nil
	}

	if err := validateRouteValidations(p); err != nil {
		return err
	}

	if a := p.Authorization; a != nil {
		switch a.Type {
		case authzAnonymous, "AUTHENTICATION_ONLY":
		case anyOf:
			if len(a.AllowedScope) == 0 {
				return errParam("ANY_OF authorization requires allowedScope")
			}
		default:
			return errParam("authorization.type must be ANONYMOUS, ANY_OF or AUTHENTICATION_ONLY")
		}
	}

	return validateCors(p.Cors, "route requestPolicies.cors")
}

func checkAnonymousRoutes(doc *specDoc) *APIError {
	anonymousOK := anonymousAllowed(doc.RequestPolicies)

	for i := range doc.Routes {
		p := doc.Routes[i].RequestPolicies
		if p != nil && p.Authorization != nil && p.Authorization.Type == authzAnonymous && !anonymousOK {
			return errParam("ANONYMOUS route authorization requires authentication.isAnonymousAccessAllowed to be true")
		}
	}

	return nil
}

func anonymousAllowed(rp *requestPolicies) bool {
	if rp == nil {
		return false
	}

	if rp.Authentication != nil && isTrue(rp.Authentication.IsAnonymousAllowed) {
		return true
	}

	if d := rp.DynamicAuthentication; d != nil {
		for i := range d.Servers {
			if s := d.Servers[i].Detail; s != nil && isTrue(s.IsAnonymousAllowed) {
				return true
			}
		}
	}

	return false
}

func httpURL(u *url.URL) bool {
	return (u.Scheme == "http" || u.Scheme == "https") && u.Host != ""
}
