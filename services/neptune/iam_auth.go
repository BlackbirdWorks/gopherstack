package neptune

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/bolt"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/neptune/gremlin"
	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	iamSigningService = "neptune-db"
	iamMaxSkew        = 5 * time.Minute
	iamArnParts       = 6
	iamAmzDateLayout  = "20060102T150405Z"
	boltTokenPath     = "/opencypher"

	actionRead   = "neptune-db:ReadDataViaQuery"
	actionWrite  = "neptune-db:WriteDataViaQuery"
	actionDelete = "neptune-db:DeleteDataViaQuery"
	actionStatus = "neptune-db:GetQueryStatus"
	actionCancel = "neptune-db:CancelQuery"

	codeAccessDenied = "AccessDeniedException"
	langGremlin      = "Gremlin"
	langOpenCypher   = "OpenCypher"
	condQueryLang    = "neptune-db:querylanguage"
)

var (
	errIAMNoCredentials = errors.New("missing authentication token")
	errIAMBadService    = errors.New("credential should be scoped to the neptune-db service")
	errIAMBadRegion     = errors.New("credential should be scoped to the cluster's region")
	errIAMSkew          = errors.New("signature expired: the request time is outside the allowed window")
	errIAMUnknownKey    = errors.New("the security token included in the request is invalid")
	errIAMBadSignature  = errors.New("the request signature does not match the signature provided")
	errIAMBadBoltToken  = errors.New("malformed Bolt IAM authentication token")
)

// IAMAuthIdentityProvider is the slice of the IAM backend that data-plane request verification needs.
type IAMAuthIdentityProvider interface {
	ListAllAccessKeys() []iam.AccessKey
	GetUserByAccessKeyID(accessKeyID string) (*iam.User, error)
	GetPoliciesForUser(userName string) ([]string, error)
	GetPoliciesForRole(roleName string) ([]string, error)
}

// IAMAuthSessionProvider resolves STS-issued temporary credentials.
type IAMAuthSessionProvider interface {
	LookupSession(accessKeyID, sessionToken string) *sts.SessionInfo
}

// IAMAuthConfig wires Neptune IAM database authentication to the credential stores.
type IAMAuthConfig struct {
	Identities IAMAuthIdentityProvider
	Sessions   IAMAuthSessionProvider
	// Enforce evaluates neptune-db: actions against the caller's policies; off, any validly signed caller is admitted.
	Enforce bool
}

// IAMAuthResolver returns the current IAM wiring; it is called per request so IAM may be created later.
type IAMAuthResolver func() IAMAuthConfig

// SetIAMAuth registers the resolver consulted for data-plane requests to clusters with IAM authentication enabled.
func (b *InMemoryBackend) SetIAMAuth(r IAMAuthResolver) {
	b.mu.Lock("SetIAMAuth")
	defer b.mu.Unlock()

	b.iamAuth = r
}

// clusterAuth is a cluster's data-plane auth state, re-read on every request.
type clusterAuth struct {
	cfg         IAMAuthConfig
	resourceARN string
	region      string
	enabled     bool
}

// clusterAuthFn returns the live auth state of the cluster behind lb.
func (b *InMemoryBackend) clusterAuthFn(lb *liveCluster) func() clusterAuth {
	return func() clusterAuth {
		b.mu.RLock("neptuneClusterAuth")
		defer b.mu.RUnlock()

		c, ok := b.clusterGet(lb.region, lb.id)
		if !ok || !c.EnableIAMDatabaseAuthentication {
			return clusterAuth{}
		}

		cfg := IAMAuthConfig{}
		if b.iamAuth != nil {
			cfg = b.iamAuth()
		}

		return clusterAuth{
			enabled: true, cfg: cfg, region: lb.region,
			resourceARN: arn.Build("neptune-db", lb.region, b.accountID, c.DBClusterResourceID+"/*"),
		}
	}
}

type iamPrincipal struct {
	arn      string
	account  string
	boundary string
	secret   string
	policies []string
}

// iamCaller is an authenticated data-plane caller.
type iamCaller struct {
	pr       *iamPrincipal
	resource string
	region   string
	sourceIP string
	enforce  bool
}

type callerKey struct{}

func callerFrom(ctx context.Context) *iamCaller {
	c, _ := ctx.Value(callerKey{}).(*iamCaller)

	return c
}

func (c *iamCaller) allows(action, language string) bool {
	if !c.enforce {
		return true
	}

	cc := iam.ConditionContext{
		PrincipalARN: c.pr.arn, PrincipalAccount: c.pr.account, RequestedRegion: c.region,
		SourceIP: c.sourceIP, SecureTransport: formTrue,
		Extra: map[string]string{condQueryLang: language},
	}

	if iam.EvaluatePolicies(c.pr.policies, action, c.resource, cc) != iam.EvalAllow {
		return false
	}

	return c.pr.boundary == "" || iam.EvaluatePolicies([]string{c.pr.boundary}, action, c.resource, cc) == iam.EvalAllow
}

// denied returns the first action the caller lacks, or "".
func (c *iamCaller) denied(language string, actions ...string) string {
	for _, a := range actions {
		if !c.allows(a, language) {
			return a
		}
	}

	return ""
}

func queryActions(write, del bool) []string {
	out := []string{actionRead}

	if write {
		out = append(out, actionWrite)
	}

	if del {
		out = append(out, actionDelete)
	}

	return out
}

func accessDeniedDetail(action string) string {
	return "Access denied: the caller is not authorized to perform " + action
}

func (c IAMAuthConfig) principalFor(accessKeyID, sessionToken string) (*iamPrincipal, error) {
	if c.Sessions != nil && sessionToken != "" {
		if s := c.Sessions.LookupSession(accessKeyID, sessionToken); s != nil {
			return c.sessionPrincipal(s), nil
		}
	}

	if c.Identities == nil {
		return nil, errIAMUnknownKey
	}

	for _, ak := range c.Identities.ListAllAccessKeys() {
		if ak.AccessKeyID != accessKeyID || (ak.Status != "" && ak.Status != "Active") {
			continue
		}

		user, err := c.Identities.GetUserByAccessKeyID(ak.AccessKeyID)
		if err != nil {
			return nil, errIAMUnknownKey
		}

		docs, _ := c.Identities.GetPoliciesForUser(user.UserName)
		p := &iamPrincipal{arn: user.Arn, secret: ak.SecretAccessKey, policies: docs}

		if pb, ok := c.Identities.(interface{ PermissionsBoundaryDocForUser(string) string }); ok {
			p.boundary = pb.PermissionsBoundaryDocForUser(user.UserName)
		}

		return p, nil
	}

	return nil, errIAMUnknownKey
}

func (c IAMAuthConfig) sessionPrincipal(s *sts.SessionInfo) *iamPrincipal {
	p := &iamPrincipal{arn: s.AssumedRoleArn, account: s.AccountID, secret: s.SecretAccessKey}
	if c.Identities == nil {
		return p
	}

	resource := ""
	if parts := strings.SplitN(s.AssumedRoleArn, ":", iamArnParts); len(parts) == iamArnParts {
		resource = parts[iamArnParts-1]
	}

	switch {
	case s.IsAssumedRole && strings.HasPrefix(resource, "assumed-role/"):
		role, _, _ := strings.Cut(strings.TrimPrefix(resource, "assumed-role/"), "/")
		p.policies, _ = c.Identities.GetPoliciesForRole(role)
	case strings.HasPrefix(resource, "user/"):
		p.policies, _ = c.Identities.GetPoliciesForUser(strings.TrimPrefix(resource, "user/"))
	}

	return p
}

// authenticate verifies r's SigV4 signature (service neptune-db) and resolves the caller.
func (ca clusterAuth) authenticate(r *http.Request, now time.Time) (*iamCaller, error) {
	if httputils.HeaderValue(r.Header, "Authorization") == "" {
		return nil, errIAMNoCredentials
	}

	accessKey, region, service := httputils.SigV4RequestFields(r, "")
	if accessKey == "" {
		return nil, errIAMNoCredentials
	}

	if service != iamSigningService {
		return nil, errIAMBadService
	}

	if region != ca.region {
		return nil, errIAMBadRegion
	}

	signedAt, err := time.Parse(iamAmzDateLayout, httputils.HeaderValue(r.Header, "X-Amz-Date"))
	if err != nil || signedAt.Before(now.Add(-iamMaxSkew)) || signedAt.After(now.Add(iamMaxSkew)) {
		return nil, errIAMSkew
	}

	pr, err := ca.cfg.principalFor(accessKey, httputils.HeaderValue(r.Header, "X-Amz-Security-Token"))
	if err != nil {
		return nil, err
	}

	if verr := httputils.NewSigV4Validator(pr.secret).Verify(r); verr != nil {
		return nil, errIAMBadSignature
	}

	return &iamCaller{
		pr: pr, resource: ca.resourceARN, region: ca.region, sourceIP: remoteIP(r.RemoteAddr), enforce: ca.cfg.Enforce,
	}, nil
}

// authenticateBolt verifies the signed-request JSON a Neptune Bolt client sends as the basic-auth password.
func (ca clusterAuth) authenticateBolt(auth map[string]any, now time.Time) (*iamCaller, error) {
	scheme, _ := auth["scheme"].(string)
	raw, _ := auth["credentials"].(string)

	if scheme != "basic" || raw == "" {
		return nil, errIAMNoCredentials
	}

	var tok map[string]string
	if err := json.Unmarshal([]byte(raw), &tok); err != nil {
		return nil, errIAMBadBoltToken
	}

	if tok["Authorization"] == "" || tok["Host"] == "" || tok["X-Amz-Date"] == "" {
		return nil, errIAMBadBoltToken
	}

	method := tok["HttpMethod"]
	if method == "" {
		method = http.MethodGet
	}

	req, err := http.NewRequestWithContext(context.Background(), method, "http://"+tok["Host"]+boltTokenPath, nil)
	if err != nil {
		return nil, errIAMBadBoltToken
	}

	req.Host = tok["Host"]
	req.Header.Set("Authorization", tok["Authorization"])
	req.Header.Set("X-Amz-Date", tok["X-Amz-Date"])

	if st := tok["X-Amz-Security-Token"]; st != "" {
		req.Header.Set("X-Amz-Security-Token", st)
	}

	return ca.authenticate(req, now)
}

func remoteIP(addr string) string {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	return host
}

// gremlinAuthorizer vets Gremlin queries against the caller bound to the request context.
func gremlinAuthorizer(ctx context.Context, write, del bool) error {
	c := callerFrom(ctx)
	if c == nil {
		return nil
	}

	if a := c.denied(langGremlin, queryActions(write, del)...); a != "" {
		return &gremlin.Error{
			Code: gremlin.CodeUnauthorized, Status: gremlin.StatusForbidden, Detail: accessDeniedDetail(a),
		}
	}

	return nil
}

func cypherAccessActions(query string) []string {
	a := opencypher.QueryAccess(query)

	return queryActions(a.Write, a.Delete)
}

func boltDenied(detail string) *bolt.Error {
	return &bolt.Error{Code: bolt.CodeForbidden, Message: "AccessDeniedException: " + detail}
}

// authenticate admits r when its cluster has no IAM authentication or it carries a valid neptune-db signature; the
// returned request carries the caller. A false result means the 403 was already written.
func (f *cypherFront) authenticate(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	if f.auth == nil {
		return r, true
	}

	ca := f.auth()
	if !ca.enabled {
		return r, true
	}

	caller, err := ca.authenticate(r, time.Now())
	if err != nil {
		writeCypherError(w, http.StatusForbidden, codeAccessDenied, err.Error())

		return r, false
	}

	return r.WithContext(context.WithValue(r.Context(), callerKey{}, caller)), true
}

// authorizeActions requires every action for the authenticated caller; true when there is no caller to vet.
func authorizeActions(w http.ResponseWriter, r *http.Request, language string, actions ...string) bool {
	c := callerFrom(r.Context())
	if c == nil {
		return true
	}

	if a := c.denied(language, actions...); a != "" {
		writeCypherError(w, http.StatusForbidden, codeAccessDenied, accessDeniedDetail(a))

		return false
	}

	return true
}

// authorizeStatus gates the query status and cancel routes.
func authorizeStatus(w http.ResponseWriter, r *http.Request) bool {
	path := strings.ToLower(strings.TrimRight(r.URL.Path, "/"))
	if path != cypherStatusPath && !strings.HasPrefix(path, cypherStatusPath+"/") &&
		path != gremlinStatusPath && !strings.HasPrefix(path, gremlinStatusPath+"/") {
		return true
	}

	language := langOpenCypher
	if strings.HasPrefix(path, gremlinStatusPath) {
		language = langGremlin
	}

	if r.Method == http.MethodDelete {
		return authorizeActions(w, r, language, actionCancel)
	}

	return authorizeActions(w, r, language, actionStatus)
}

// authorizeProxied vets a request forwarded to the Gremlin Server container. A WebSocket handshake needs read access
// here and its frames are authorized individually by proxyGremlinWS;
// HTTP queries are classified from their gremlin field.
func authorizeProxied(w http.ResponseWriter, r *http.Request) bool {
	if callerFrom(r.Context()) == nil {
		return true
	}

	script := r.URL.Query().Get("gremlin")

	if r.Method == http.MethodPost && r.Body != nil {
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, cypherMaxBody))
		if err != nil {
			writeCypherError(w, http.StatusBadRequest, opencypher.CodeMalformedQuery, err.Error())

			return false
		}

		r.Body = io.NopCloser(bytes.NewReader(body))

		var req struct {
			Gremlin string `json:"gremlin"`
		}

		_ = json.Unmarshal(body, &req)
		script = cmp.Or(req.Gremlin, script)
	}

	a := gremlin.ScriptAccess(script)

	return authorizeActions(w, r, langGremlin, queryActions(a.Write, a.Delete)...)
}
