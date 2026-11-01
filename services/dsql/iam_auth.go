package dsql

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

// Token wire format: feature/dsql/auth@v1.2.8 auth_token_generator.go -- a SigV4 query presign of
// https://<endpoint>/?Action=DbConnect[Admin] for signing name "dsql" with the scheme stripped.
const (
	tokenSigningName = "dsql"
	tokenActionUser  = "DbConnect"
	tokenActionAdmin = "DbConnectAdmin"
	tokenMaxExpires  = 7 * 24 * 3600
	tokenMaxSkew     = 15 * time.Minute
	tokenDateLayout  = "20060102T150405Z"
	sigV4Algorithm   = "AWS4-HMAC-SHA256"
	tokenCredParts   = 5
	tokenArnParts    = 6
	adminRole        = "admin"
	hostLocalhost    = "localhost"
)

var (
	errTokenMalformed = errors.New("malformed IAM authentication token")
	errTokenExpired   = errors.New("IAM authentication token expired")
	errTokenSignature = errors.New("IAM authentication token signature mismatch")
	errTokenEndpoint  = errors.New("IAM authentication token is bound to a different endpoint")
	errTokenRegion    = errors.New("IAM authentication token region mismatch")
	errTokenUnknownID = errors.New("IAM authentication token access key is unknown")
	errTokenDenied    = errors.New("IAM policy does not allow the dsql connect action")
	errTokenRole      = errors.New("IAM authentication token action does not match the database role")
	errTokenNoGrant   = errors.New("no AWS IAM GRANT maps this principal to the database role")
)

// IAMAuthIdentityProvider is the slice of the IAM backend that token verification needs.
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

// IAMAuthConfig wires token verification to the credential stores.
type IAMAuthConfig struct {
	Identities IAMAuthIdentityProvider
	Sessions   IAMAuthSessionProvider
	// Enforce evaluates dsql:DbConnect[Admin] against the caller's policies; off, any known principal may connect.
	Enforce bool
}

// IAMAuthResolver returns the current IAM wiring; it is called per connection so IAM may be created later.
type IAMAuthResolver func() IAMAuthConfig

// SetIAMAuth registers the resolver consulted for every connection to a docker-backed cluster.
func (b *InMemoryBackend) SetIAMAuth(r IAMAuthResolver) {
	b.mu.Lock("SetIAMAuth")
	defer b.mu.Unlock()

	b.iamAuth = r
}

func (b *InMemoryBackend) iamAuthConfig() *IAMAuthConfig {
	b.mu.RLock("iamAuthConfig")
	r := b.iamAuth
	b.mu.RUnlock()

	if r == nil {
		return nil
	}

	cfg := r()

	return &cfg
}

type authToken struct {
	query        url.Values
	signedAt     time.Time
	host         string
	action       string
	accessKeyID  string
	region       string
	signature    string
	sessionToken string
	expires      time.Duration
}

func parseAuthToken(raw string) (*authToken, error) {
	raw = strings.TrimPrefix(strings.TrimPrefix(raw, "https://"), "http://")

	u, err := url.Parse("https://" + raw)
	if err != nil || u.Host == "" || (u.Path != "" && u.Path != "/") {
		return nil, errTokenMalformed
	}

	q := u.Query()
	action := q.Get("Action")

	if (action != tokenActionUser && action != tokenActionAdmin) || q.Get("X-Amz-Algorithm") != sigV4Algorithm ||
		q.Get("X-Amz-SignedHeaders") != "host" || q.Get("X-Amz-Signature") == "" {
		return nil, errTokenMalformed
	}

	signedAt, err := time.Parse(tokenDateLayout, q.Get("X-Amz-Date"))
	if err != nil {
		return nil, errTokenMalformed
	}

	expires, err := strconv.Atoi(q.Get("X-Amz-Expires"))
	if err != nil || expires < 1 || expires > tokenMaxExpires {
		return nil, errTokenMalformed
	}

	accessKey, region, ok := parseCredentialScope(q.Get("X-Amz-Credential"), signedAt)
	if !ok {
		return nil, errTokenMalformed
	}

	return &authToken{
		query: q, signedAt: signedAt, host: u.Host, action: action, accessKeyID: accessKey, region: region,
		signature: q.Get("X-Amz-Signature"), sessionToken: q.Get("X-Amz-Security-Token"),
		expires: time.Duration(expires) * time.Second,
	}, nil
}

func parseCredentialScope(cred string, signedAt time.Time) (string, string, bool) {
	parts := strings.Split(cred, "/")
	if len(parts) != tokenCredParts || parts[0] == "" || parts[1] != signedAt.Format("20060102") ||
		parts[3] != tokenSigningName || parts[4] != "aws4_request" {
		return "", "", false
	}

	return parts[0], parts[2], true
}

func (t *authToken) checkTime(now time.Time) error {
	if now.After(t.signedAt.Add(t.expires)) {
		return errTokenExpired
	}

	if t.signedAt.After(now.Add(tokenMaxSkew)) {
		return errTokenMalformed
	}

	return nil
}

// verifySignature re-signs the token request with the real SigV4 presigner and compares signatures.
func (t *authToken) verifySignature(secret string) error {
	q := url.Values{}

	for k, v := range t.query {
		switch k {
		case "X-Amz-Algorithm", "X-Amz-Credential", "X-Amz-Date", "X-Amz-SignedHeaders",
			"X-Amz-Signature", "X-Amz-Security-Token":
		default:
			q[k] = v
		}
	}

	creds := aws.Credentials{AccessKeyID: t.accessKeyID, SecretAccessKey: secret, SessionToken: t.sessionToken}

	empty := sha256.Sum256(nil)

	for _, payloadHash := range []string{hex.EncodeToString(empty[:]), "UNSIGNED-PAYLOAD"} {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			"https://"+t.host+"/?"+q.Encode(), nil)
		if err != nil {
			return errTokenMalformed
		}

		signed, _, err := v4.NewSigner().PresignHTTP(context.Background(), creds, req, payloadHash,
			tokenSigningName, t.region, t.signedAt)
		if err != nil {
			return fmt.Errorf("%w: %w", errTokenSignature, err)
		}

		su, err := url.Parse(signed)
		if err != nil {
			return errTokenSignature
		}

		if hmac.Equal([]byte(su.Query().Get("X-Amz-Signature")), []byte(t.signature)) {
			return nil
		}
	}

	return errTokenSignature
}

type authPrincipal struct {
	arn      string
	account  string
	boundary string
	secret   string
	policies []string
}

func (c IAMAuthConfig) principalFor(t *authToken) (*authPrincipal, error) {
	if c.Sessions != nil && t.sessionToken != "" {
		if s := c.Sessions.LookupSession(t.accessKeyID, t.sessionToken); s != nil {
			return c.sessionPrincipal(s)
		}
	}

	if c.Identities == nil {
		return nil, errTokenUnknownID
	}

	for _, ak := range c.Identities.ListAllAccessKeys() {
		if ak.AccessKeyID != t.accessKeyID || (ak.Status != "" && ak.Status != "Active") {
			continue
		}

		user, err := c.Identities.GetUserByAccessKeyID(ak.AccessKeyID)
		if err != nil {
			return nil, errTokenUnknownID
		}

		docs, _ := c.Identities.GetPoliciesForUser(user.UserName)
		p := &authPrincipal{arn: user.Arn, secret: ak.SecretAccessKey, policies: docs}

		if pb, ok := c.Identities.(interface{ PermissionsBoundaryDocForUser(string) string }); ok {
			p.boundary = pb.PermissionsBoundaryDocForUser(user.UserName)
		}

		return p, nil
	}

	return nil, errTokenUnknownID
}

func (c IAMAuthConfig) sessionPrincipal(s *sts.SessionInfo) (*authPrincipal, error) {
	p := &authPrincipal{arn: s.AssumedRoleArn, account: s.AccountID, secret: s.SecretAccessKey}
	if c.Identities == nil {
		return p, nil
	}

	resource := arnResource(s.AssumedRoleArn)

	switch {
	case s.IsAssumedRole && strings.HasPrefix(resource, "assumed-role/"):
		role, _, _ := strings.Cut(strings.TrimPrefix(resource, "assumed-role/"), "/")
		p.policies, _ = c.Identities.GetPoliciesForRole(role)
	case strings.HasPrefix(resource, "user/"):
		p.policies, _ = c.Identities.GetPoliciesForUser(strings.TrimPrefix(resource, "user/"))
	}

	return p, nil
}

func arnResource(a string) string {
	parts := strings.SplitN(a, ":", tokenArnParts)
	if len(parts) != tokenArnParts {
		return ""
	}

	return parts[tokenArnParts-1]
}

// grantARN is the IAM ARN an AWS IAM GRANT names for the principal: an assumed-role session maps to its role.
func (p *authPrincipal) grantARN() string {
	resource := arnResource(p.arn)
	if !strings.HasPrefix(resource, "assumed-role/") {
		return p.arn
	}

	role, _, _ := strings.Cut(strings.TrimPrefix(resource, "assumed-role/"), "/")
	parts := strings.SplitN(p.arn, ":", tokenArnParts)

	return "arn:" + parts[1] + ":iam::" + parts[4] + ":role/" + role
}

func (p *authPrincipal) allows(action, resourceARN, region, sourceIP string) bool {
	cc := iam.ConditionContext{
		PrincipalARN: p.arn, PrincipalAccount: p.account, RequestedRegion: region,
		SourceIP: sourceIP, SecureTransport: "true",
	}

	if iam.EvaluatePolicies(p.policies, action, resourceARN, cc) != iam.EvalAllow {
		return false
	}

	if p.boundary == "" {
		return true
	}

	return iam.EvaluatePolicies([]string{p.boundary}, action, resourceARN, cc) == iam.EvalAllow
}

// authTarget is the cluster a token is presented to.
type authTarget struct {
	endpoint string
	region   string
	arn      string
	port     int
}

func sameEndpoint(tokenHost string, t authTarget) bool {
	if tokenHost == t.endpoint || tokenHost == net.JoinHostPort(t.endpoint, strconv.Itoa(t.port)) {
		return true
	}

	th, tp, err := net.SplitHostPort(tokenHost)
	if err != nil || tp != strconv.Itoa(t.port) {
		return false
	}

	ip := net.ParseIP(th)

	return th == hostLocalhost || (ip != nil && ip.IsLoopback())
}

// session is an authenticated connection: the database role it may use.
type session struct {
	role string
}

// grantLookup reports whether principalARN is mapped to role by AWS IAM GRANT.
type grantLookup func(role, principalARN string) bool

// authenticateToken validates the token password presented for dbUser and returns the session it grants.
func authenticateToken(
	cfg IAMAuthConfig, target authTarget, dbUser, raw, sourceIP string, now time.Time, granted grantLookup,
) (*session, error) {
	t, err := parseAuthToken(raw)
	if err != nil {
		return nil, err
	}

	if !sameEndpoint(t.host, target) {
		return nil, errTokenEndpoint
	}

	if t.region != target.region {
		return nil, errTokenRegion
	}

	if err = t.checkTime(now); err != nil {
		return nil, err
	}

	if (t.action == tokenActionAdmin) != (dbUser == adminRole) {
		return nil, errTokenRole
	}

	p, err := cfg.principalFor(t)
	if err != nil {
		return nil, err
	}

	if err = t.verifySignature(p.secret); err != nil {
		return nil, err
	}

	if cfg.Enforce && !p.allows("dsql:"+t.action, target.arn, target.region, sourceIP) {
		return nil, errTokenDenied
	}

	if t.action == tokenActionUser && !granted(dbUser, p.grantARN()) {
		return nil, errTokenNoGrant
	}

	return &session{role: dbUser}, nil
}
