package rds

import (
	"context"
	"crypto/hmac"
	"errors"
	"fmt"
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

const (
	iamAuthSigningService = "rds-db"
	iamAuthAction         = "connect"
	iamAuthIAMAction      = "rds-db:connect"
	iamAuthMaxExpires     = 900
	iamAuthMaxSkew        = 15 * time.Minute
	iamAuthDateLayout     = "20060102T150405Z"
	iamAuthAlgorithm      = "AWS4-HMAC-SHA256"
	iamAuthEmptyHash      = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	iamAuthUnsignedHash   = "UNSIGNED-PAYLOAD"
	iamAuthCredParts      = 5
	iamAuthArnParts       = 6
)

var (
	errTokenMalformed = errors.New("malformed IAM authentication token")
	errTokenExpired   = errors.New("IAM authentication token expired")
	errTokenSignature = errors.New("IAM authentication token signature mismatch")
	errTokenEndpoint  = errors.New("IAM authentication token is bound to a different endpoint")
	errTokenRegion    = errors.New("IAM authentication token region mismatch")
	errTokenUnknownID = errors.New("IAM authentication token access key is unknown")
	errTokenDenied    = errors.New("IAM policy does not allow rds-db:connect")
	errTokenNoResID   = errors.New("database has no resource id")
	errTokenDisabled  = errors.New("IAM database authentication is not enabled")
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

// IAMAuthConfig wires IAM database authentication to the credential stores.
type IAMAuthConfig struct {
	Identities IAMAuthIdentityProvider
	Sessions   IAMAuthSessionProvider
	// Enforce evaluates rds-db:connect against the caller's policies; off, any known principal may connect.
	Enforce bool
}

// IAMAuthResolver returns the current IAM wiring; it is called per connection so IAM may be created later.
type IAMAuthResolver func() IAMAuthConfig

type authToken struct {
	query        url.Values
	signedAt     time.Time
	host         string
	dbUser       string
	accessKeyID  string
	region       string
	signature    string
	sessionToken string
	expires      time.Duration
}

func looksLikeAuthToken(password string) bool {
	return strings.Contains(password, "Action=connect") && strings.Contains(password, "X-Amz-Signature=")
}

func parseAuthToken(raw string) (*authToken, error) {
	raw = strings.TrimPrefix(strings.TrimPrefix(raw, "https://"), "http://")

	u, err := url.Parse("https://" + raw)
	if err != nil || u.Host == "" || (u.Path != "" && u.Path != "/") {
		return nil, errTokenMalformed
	}

	q := u.Query()
	if q.Get("Action") != iamAuthAction || q.Get("DBUser") == "" || q.Get("X-Amz-Algorithm") != iamAuthAlgorithm ||
		q.Get("X-Amz-SignedHeaders") != "host" || q.Get("X-Amz-Signature") == "" {
		return nil, errTokenMalformed
	}

	signedAt, err := time.Parse(iamAuthDateLayout, q.Get("X-Amz-Date"))
	if err != nil {
		return nil, errTokenMalformed
	}

	expires, err := strconv.Atoi(q.Get("X-Amz-Expires"))
	if err != nil || expires < 1 || expires > iamAuthMaxExpires {
		return nil, errTokenMalformed
	}

	accessKey, region, ok := parseCredentialScope(q.Get("X-Amz-Credential"), signedAt)
	if !ok {
		return nil, errTokenMalformed
	}

	return &authToken{
		query: q, signedAt: signedAt, host: u.Host, dbUser: q.Get("DBUser"), accessKeyID: accessKey,
		region: region, signature: q.Get("X-Amz-Signature"), sessionToken: q.Get("X-Amz-Security-Token"),
		expires: time.Duration(expires) * time.Second,
	}, nil
}

func parseCredentialScope(cred string, signedAt time.Time) (string, string, bool) {
	parts := strings.Split(cred, "/")
	if len(parts) != iamAuthCredParts || parts[0] == "" || parts[1] != signedAt.Format("20060102") ||
		parts[3] != iamAuthSigningService || parts[4] != "aws4_request" {
		return "", "", false
	}

	return parts[0], parts[2], true
}

func (t *authToken) checkTime(now time.Time) error {
	if now.After(t.signedAt.Add(t.expires)) {
		return errTokenExpired
	}

	if t.signedAt.After(now.Add(iamAuthMaxSkew)) {
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

	for _, payloadHash := range []string{iamAuthEmptyHash, iamAuthUnsignedHash} {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			"https://"+t.host+"/?"+q.Encode(), nil)
		if err != nil {
			return errTokenMalformed
		}

		signed, _, err := v4.NewSigner().PresignHTTP(context.Background(), creds, req, payloadHash,
			iamAuthSigningService, t.region, t.signedAt)
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

	resource := ""
	if parts := strings.SplitN(s.AssumedRoleArn, ":", iamAuthArnParts); len(parts) == iamAuthArnParts {
		resource = parts[iamAuthArnParts-1]
	}

	switch {
	case s.IsAssumedRole && strings.HasPrefix(resource, "assumed-role/"):
		role, _, _ := strings.Cut(strings.TrimPrefix(resource, "assumed-role/"), "/")
		p.policies, _ = c.Identities.GetPoliciesForRole(role)
	case strings.HasPrefix(resource, "user/"):
		p.policies, _ = c.Identities.GetPoliciesForUser(strings.TrimPrefix(resource, "user/"))
	}

	return p, nil
}

func (p *authPrincipal) allows(resourceARN, region, sourceIP string) bool {
	cc := iam.ConditionContext{
		PrincipalARN: p.arn, PrincipalAccount: p.account, RequestedRegion: region,
		SourceIP: sourceIP, SecureTransport: formTrue,
	}

	if iam.EvaluatePolicies(p.policies, iamAuthIAMAction, resourceARN, cc) != iam.EvalAllow {
		return false
	}

	if p.boundary == "" {
		return true
	}

	return iam.EvaluatePolicies([]string{p.boundary}, iamAuthIAMAction, resourceARN, cc) == iam.EvalAllow
}

// iamAuthTarget is the database a token is presented to.
type iamAuthTarget struct {
	endpoint   string
	region     string
	account    string
	resourceID string
	partition  string
	enabled    bool
}

func (g iamAuthTarget) resourceARN(dbUser string) string {
	return "arn:" + g.partition + ":rds-db:" + g.region + ":" + g.account + ":dbuser:" + g.resourceID + "/" + dbUser
}

// authenticateToken validates a generate-db-auth-token password for target and returns the DB user it grants.
func authenticateToken(cfg IAMAuthConfig, target iamAuthTarget, raw, sourceIP string, now time.Time) (string, error) {
	t, err := parseAuthToken(raw)
	if err != nil {
		return "", err
	}

	if !target.enabled {
		return "", errTokenDisabled
	}

	if !sameEndpoint(t.host, target.endpoint) {
		return "", errTokenEndpoint
	}

	if t.region != target.region {
		return "", errTokenRegion
	}

	if err = t.checkTime(now); err != nil {
		return "", err
	}

	p, err := cfg.principalFor(t)
	if err != nil {
		return "", err
	}

	if err = t.verifySignature(p.secret); err != nil {
		return "", err
	}

	if target.resourceID == "" {
		return "", errTokenNoResID
	}

	if cfg.Enforce && !p.allows(target.resourceARN(t.dbUser), target.region, sourceIP) {
		return "", errTokenDenied
	}

	return t.dbUser, nil
}

// SetIAMAuth registers the resolver consulted for every IAM-token connection to a docker-backed database.
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
