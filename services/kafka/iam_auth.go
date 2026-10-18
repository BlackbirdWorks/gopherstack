package kafka

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/services/iam"
	"github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	iamAuthService      = "kafka-cluster"
	iamAuthAction       = "kafka-cluster:Connect"
	iamAuthAlgorithm    = "AWS4-HMAC-SHA256"
	iamAuthDateLayout   = "20060102T150405Z"
	iamAuthMaxSkew      = 15 * time.Minute
	iamAuthDefaultTTL   = 900
	iamAuthCredParts    = 5
	iamAuthArnParts     = 6
	iamAuthEmptyHash    = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
	iamAuthUnsignedHash = "UNSIGNED-PAYLOAD"
)

var (
	errIAMMalformed = errors.New("malformed AWS_MSK_IAM authentication payload")
	errIAMExpired   = errors.New("AWS_MSK_IAM authentication payload expired")
	errIAMSignature = errors.New("AWS_MSK_IAM authentication signature mismatch")
	errIAMRegion    = errors.New("AWS_MSK_IAM authentication region mismatch")
	errIAMUnknownID = errors.New("AWS_MSK_IAM authentication access key is unknown")
	errIAMDenied    = errors.New("IAM policy does not allow kafka-cluster:Connect")
)

// IAMAuthIdentityProvider is the slice of the IAM backend that AWS_MSK_IAM verification needs.
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

// IAMAuthConfig wires AWS_MSK_IAM authentication to the emulator's credential stores.
type IAMAuthConfig struct {
	Identities IAMAuthIdentityProvider
	Sessions   IAMAuthSessionProvider
	// Enforce evaluates kafka-cluster:Connect against the caller's policies; off, any known principal connects.
	Enforce bool
}

// IAMAuthResolver returns the current IAM wiring; it is called per connection so IAM may be created later.
type IAMAuthResolver func() IAMAuthConfig

// SetIAMAuth registers the resolver consulted for every AWS_MSK_IAM connection to a docker-backed cluster.
func (b *InMemoryBackend) SetIAMAuth(r IAMAuthResolver) {
	b.mu.Lock("SetIAMAuth")
	defer b.mu.Unlock()

	b.iamAuth = r
}

type iamPayload struct {
	query        url.Values
	signedAt     time.Time
	host         string
	accessKeyID  string
	region       string
	signature    string
	sessionToken string
	expires      time.Duration
}

// parseIAMPayload decodes the JSON sent by the aws-msk-iam-auth clients (version 2020_10_22).
func parseIAMPayload(raw []byte) (*iamPayload, error) {
	var kv map[string]string
	if err := json.Unmarshal(raw, &kv); err != nil {
		return nil, errIAMMalformed
	}

	if kv["action"] != iamAuthAction || kv["x-amz-algorithm"] != iamAuthAlgorithm ||
		kv["x-amz-signedheaders"] != "host" || kv["x-amz-signature"] == "" || kv["host"] == "" {
		return nil, errIAMMalformed
	}

	signedAt, err := time.Parse(iamAuthDateLayout, kv["x-amz-date"])
	if err != nil {
		return nil, errIAMMalformed
	}

	ttl := iamAuthDefaultTTL

	if e := kv["x-amz-expires"]; e != "" {
		if ttl, err = strconv.Atoi(e); err != nil || ttl < 1 {
			return nil, errIAMMalformed
		}
	}

	accessKey, region, ok := parseIAMCredential(kv["x-amz-credential"], signedAt)
	if !ok {
		return nil, errIAMMalformed
	}

	q := url.Values{"Action": {kv["action"]}}

	for k, v := range kv {
		if strings.HasPrefix(k, "x-amz-") && k != "x-amz-signature" {
			q.Set(canonicalQueryKey(k), v)
		}
	}

	return &iamPayload{
		query: q, signedAt: signedAt, host: kv["host"], accessKeyID: accessKey, region: region,
		signature: kv["x-amz-signature"], sessionToken: kv["x-amz-security-token"],
		expires: time.Duration(ttl) * time.Second,
	}, nil
}

func canonicalQueryKey(k string) string {
	if k == "x-amz-signedheaders" {
		return "X-Amz-SignedHeaders"
	}

	parts := strings.Split(k, "-")
	for i, p := range parts {
		if p != "" {
			parts[i] = strings.ToUpper(p[:1]) + p[1:]
		}
	}

	return strings.Join(parts, "-")
}

func parseIAMCredential(cred string, signedAt time.Time) (string, string, bool) {
	parts := strings.Split(cred, "/")
	if len(parts) != iamAuthCredParts || parts[0] == "" || parts[1] != signedAt.Format("20060102") ||
		parts[3] != iamAuthService || parts[4] != "aws4_request" {
		return "", "", false
	}

	return parts[0], parts[2], true
}

func (p *iamPayload) checkTime(now time.Time) error {
	if now.After(p.signedAt.Add(p.expires)) {
		return errIAMExpired
	}

	if p.signedAt.After(now.Add(iamAuthMaxSkew)) {
		return errIAMMalformed
	}

	return nil
}

// verifySignature recomputes the SigV4 query signature the client produced (GET / with only the host header signed).
func (p *iamPayload) verifySignature(secret string) error {
	date := p.signedAt.Format("20060102")
	scope := strings.Join([]string{date, p.region, iamAuthService, "aws4_request"}, "/")
	qps := strings.ReplaceAll(p.query.Encode(), "+", "%20")

	for _, payloadHash := range []string{iamAuthEmptyHash, iamAuthUnsignedHash} {
		canonical := "GET\n/\n" + qps + "\nhost:" + p.host + "\n\nhost\n" + payloadHash
		sum := sha256.Sum256([]byte(canonical))
		toSign := iamAuthAlgorithm + "\n" + p.signedAt.Format(iamAuthDateLayout) + "\n" + scope + "\n" +
			hex.EncodeToString(sum[:])

		key := sigHMAC([]byte("AWS4"+secret), date)
		key = sigHMAC(key, p.region)
		key = sigHMAC(key, iamAuthService)
		key = sigHMAC(key, "aws4_request")

		if hmac.Equal([]byte(hex.EncodeToString(sigHMAC(key, toSign))), []byte(p.signature)) {
			return nil
		}
	}

	return errIAMSignature
}

func sigHMAC(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))

	return h.Sum(nil)
}

type iamPrincipal struct {
	arn      string
	account  string
	boundary string
	secret   string
	policies []string
}

func (c IAMAuthConfig) principalFor(p *iamPayload) (*iamPrincipal, error) {
	if c.Sessions != nil && p.sessionToken != "" {
		if s := c.Sessions.LookupSession(p.accessKeyID, p.sessionToken); s != nil {
			return c.sessionPrincipal(s), nil
		}
	}

	if c.Identities == nil {
		return nil, errIAMUnknownID
	}

	for _, ak := range c.Identities.ListAllAccessKeys() {
		if ak.AccessKeyID != p.accessKeyID || (ak.Status != "" && ak.Status != "Active") {
			continue
		}

		user, err := c.Identities.GetUserByAccessKeyID(ak.AccessKeyID)
		if err != nil {
			return nil, errIAMUnknownID
		}

		docs, _ := c.Identities.GetPoliciesForUser(user.UserName)
		pr := &iamPrincipal{arn: user.Arn, secret: ak.SecretAccessKey, policies: docs}

		if pb, ok := c.Identities.(interface{ PermissionsBoundaryDocForUser(string) string }); ok {
			pr.boundary = pb.PermissionsBoundaryDocForUser(user.UserName)
		}

		return pr, nil
	}

	return nil, errIAMUnknownID
}

func (c IAMAuthConfig) sessionPrincipal(s *sts.SessionInfo) *iamPrincipal {
	p := &iamPrincipal{arn: s.AssumedRoleArn, account: s.AccountID, secret: s.SecretAccessKey}
	if c.Identities == nil {
		return p
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

	return p
}

func (p *iamPrincipal) allows(clusterArn, region, sourceIP string) bool {
	cc := iam.ConditionContext{
		PrincipalARN: p.arn, PrincipalAccount: p.account, RequestedRegion: region,
		SourceIP: sourceIP, SecureTransport: "true",
	}

	if iam.EvaluatePolicies(p.policies, iamAuthAction, clusterArn, cc) != iam.EvalAllow {
		return false
	}

	if p.boundary == "" {
		return true
	}

	return iam.EvaluatePolicies([]string{p.boundary}, iamAuthAction, clusterArn, cc) == iam.EvalAllow
}

// authenticateIAM validates an AWS_MSK_IAM payload presented to clusterArn's IAM listener.
func (b *InMemoryBackend) authenticateIAM(clusterArn string, raw []byte, sourceIP string, now time.Time) error {
	b.mu.RLock("authenticateIAM")
	resolve := b.iamAuth
	b.mu.RUnlock()

	cfg := IAMAuthConfig{}
	if resolve != nil {
		cfg = resolve()
	}

	return authenticateIAMPayload(cfg, clusterArn, raw, sourceIP, now)
}

func authenticateIAMPayload(cfg IAMAuthConfig, clusterArn string, raw []byte, sourceIP string, now time.Time) error {
	p, err := parseIAMPayload(raw)
	if err != nil {
		return err
	}

	if p.region != regionFromARN(clusterArn, p.region) {
		return errIAMRegion
	}

	if err = p.checkTime(now); err != nil {
		return err
	}

	pr, err := cfg.principalFor(p)
	if err != nil {
		return err
	}

	if err = p.verifySignature(pr.secret); err != nil {
		return err
	}

	if cfg.Enforce && !pr.allows(clusterArn, p.region, sourceIP) {
		return errIAMDenied
	}

	return nil
}
