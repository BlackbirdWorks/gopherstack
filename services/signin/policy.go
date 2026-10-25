package signin

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"net/netip"
	"regexp"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	policyVersion        = "2012-10-17"
	idempotencyWindow    = 24 * time.Hour
	statementIDBytes     = 48
	maxClientTokenLen    = 64
	minExcludedPrincipal = 20
	maxExcludedPrincipal = 2048
	defaultListLimit     = 100
)

var (
	vpcPattern       = regexp.MustCompile(`^vpc-([0-9a-f]{8}|[0-9a-f]{17})$`)
	vpcePattern      = regexp.MustCompile(`^vpce-[a-z0-9]{8,20}$`)
	regionPattern    = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-\d+$`)
	tokenPattern     = regexp.MustCompile(`^[!-~]+$`)
	statementPattern = regexp.MustCompile(`^[A-Za-z0-9+/]{64}=?$`)
	principalPattern = regexp.MustCompile(
		`^arn:aws[a-z-]*:(iam::\d{12}:(role|user)/.+|iam::\d{12}:root|sts::\d{12}:federated-user/.{2,193})$`,
	)
)

// StatementInput is the PutResourcePermissionStatement request.
type StatementInput struct {
	ClientToken       string
	ConsoleSourceVpce string
	ExcludedPrincipal string
	RequestedRegion   string
	SigninSourceVpce  string
	SourceIP          string
	SourceVpc         string
	VpcSourceIP       string
}

// PolicyStatement is one statement of the resource-based policy document.
type PolicyStatement struct {
	Condition map[string]map[string][]string
	Principal map[string]string
	Effect    string
	Resource  string
	Action    []string
}

func validIP(s string) bool {
	if _, err := netip.ParsePrefix(s); err == nil {
		return true
	}

	_, err := netip.ParseAddr(s)

	return err == nil
}

func (in StatementInput) validate() error {
	if in.ClientToken != "" && (len(in.ClientToken) > maxClientTokenLen || !tokenPattern.MatchString(in.ClientToken)) {
		return validation("clientToken must be 1 to 64 printable characters")
	}

	checks := []struct {
		val string
		ok  func(string) bool
		msg string
	}{
		{in.SourceVpc, vpcPattern.MatchString, "sourceVpc is not a valid VPC ID"},
		{in.SigninSourceVpce, vpcePattern.MatchString, "signinSourceVpce is not a valid VPC endpoint ID"},
		{in.ConsoleSourceVpce, vpcePattern.MatchString, "consoleSourceVpce is not a valid VPC endpoint ID"},
		{in.RequestedRegion, regionPattern.MatchString, "requestedRegion is not a valid Region"},
		{in.SourceIP, validIP, "sourceIp is not a valid IP address or CIDR"},
		{in.VpcSourceIP, validIP, "vpcSourceIp is not a valid IP address or CIDR"},
	}
	for _, c := range checks {
		if c.val != "" && !c.ok(c.val) {
			return validation(c.msg)
		}
	}

	if p := in.ExcludedPrincipal; p != "" &&
		(len(p) < minExcludedPrincipal || len(p) > maxExcludedPrincipal || !principalPattern.MatchString(p)) {
		return validation("excludedPrincipal must be the ARN of an IAM role, user, federated user or root")
	}

	if in.SourceVpc+in.SigninSourceVpce+in.ConsoleSourceVpce != "" && in.RequestedRegion == "" {
		return validation("requestedRegion is required with sourceVpc or a VPC endpoint")
	}

	if in.fingerprint() == fingerprintOf(StatementInput{}) {
		return validation("at least one permission condition is required")
	}

	return nil
}

func fingerprintOf(in StatementInput) string {
	h := sha256.New()

	for _, v := range []string{
		in.ConsoleSourceVpce, in.ExcludedPrincipal, in.RequestedRegion, in.SigninSourceVpce,
		in.SourceIP, in.SourceVpc, in.VpcSourceIP,
	} {
		h.Write([]byte(v))
		h.Write([]byte{0})
	}

	return hex.EncodeToString(h.Sum(nil))
}

func (in StatementInput) fingerprint() string { return fingerprintOf(in) }

// condition renders the inputs as policy condition operators: the statement denies access
// whose context matches none of the permitted values.
func (in StatementInput) condition() map[string]map[string][]string {
	cond := map[string]map[string][]string{}
	add := func(op, key string, vals ...string) {
		for _, v := range vals {
			if v == "" {
				continue
			}

			if cond[op] == nil {
				cond[op] = map[string][]string{}
			}

			cond[op][key] = append(cond[op][key], v)
		}
	}

	add("StringNotEquals", "aws:SourceVpc", in.SourceVpc)
	add("StringNotEquals", "aws:SourceVpce", in.SigninSourceVpce, in.ConsoleSourceVpce)
	add("StringEquals", "aws:RequestedRegion", in.RequestedRegion)
	add("NotIpAddress", "aws:SourceIp", in.SourceIP)
	add("NotIpAddress", "aws:VpcSourceIp", in.VpcSourceIP)
	add("ArnNotLike", "aws:PrincipalArn", in.ExcludedPrincipal)

	return cond
}

func newStatementID() string {
	raw := make([]byte, statementIDBytes)
	_, _ = rand.Read(raw)

	return base64.StdEncoding.EncodeToString(raw)
}

func statementKey(account, sid string) string { return account + "|" + sid }

func (b *InMemoryBackend) accountStatementsLocked(account string) []*PermissionStatement {
	var out []*PermissionStatement

	for _, s := range b.statements.All() {
		if s.Account == account {
			out = append(out, s)
		}
	}

	sort.Slice(out, func(i, j int) bool {
		if !out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].CreatedAt.Before(out[j].CreatedAt)
		}

		return out[i].SID < out[j].SID
	})

	return out
}

// PutResourcePermissionStatement adds a statement; replays of the same request return the same statement.
func (b *InMemoryBackend) PutResourcePermissionStatement(ctx context.Context, in StatementInput) (string, error) {
	c := b.callerOf(ctx)
	if c.anon {
		return "", accessDenied(oauthInsufficientPerms, 403, "anonymous callers cannot manage permission statements")
	}

	if err := in.validate(); err != nil {
		return "", err
	}

	fp := in.fingerprint()

	b.mu.Lock("PutResourcePermissionStatement")
	defer b.mu.Unlock()

	now := b.now().UTC()

	for _, s := range b.accountStatementsLocked(c.account) {
		sameToken := in.ClientToken != "" && s.ClientToken == in.ClientToken
		sameParams := s.Fingerprint == fp
		inWindow := now.Before(s.CreatedAt.Add(idempotencyWindow))

		switch {
		case sameToken && sameParams:
			return s.SID, nil
		case sameToken:
			return "", conflict("clientToken was already used with different parameters")
		case sameParams && inWindow && in.ClientToken != "":
			return "", conflict("an equivalent statement exists under a different clientToken")
		}
	}

	st := &PermissionStatement{
		Account: c.account, SID: newStatementID(), ClientToken: in.ClientToken, Fingerprint: fp, CreatedAt: now,
		Condition: in.condition(), ConsoleSourceVpce: in.ConsoleSourceVpce, ExcludedPrincipal: in.ExcludedPrincipal,
		RequestedRegion: in.RequestedRegion, SigninSourceVpce: in.SigninSourceVpce, SourceIP: in.SourceIP,
		SourceVpc: in.SourceVpc, VpcSourceIP: in.VpcSourceIP,
	}
	st.Key = statementKey(st.Account, st.SID)
	b.statements.Put(st)

	return st.SID, nil
}

// DeleteResourcePermissionStatement removes a statement from the caller's policy.
func (b *InMemoryBackend) DeleteResourcePermissionStatement(ctx context.Context, sid string) error {
	c := b.callerOf(ctx)
	if c.anon {
		return accessDenied(oauthInsufficientPerms, 403, "anonymous callers cannot manage permission statements")
	}

	if !statementPattern.MatchString(sid) {
		return validation("statementId is malformed")
	}

	b.mu.Lock("DeleteResourcePermissionStatement")
	defer b.mu.Unlock()

	if !b.statements.Delete(statementKey(c.account, sid)) {
		return notFound("permission statement " + sid + " not found")
	}

	return nil
}

// ListResourcePermissionStatements pages through the caller's statements.
func (b *InMemoryBackend) ListResourcePermissionStatements(
	ctx context.Context, token string, maxResults int,
) (page.Page[PermissionStatement], error) {
	c := b.callerOf(ctx)
	if c.anon {
		return page.Page[PermissionStatement]{},
			accessDenied(oauthInsufficientPerms, 403, "anonymous callers cannot list permission statements")
	}

	if maxResults < 0 {
		return page.Page[PermissionStatement]{}, validation("maxResults must be positive")
	}

	b.mu.RLock("ListResourcePermissionStatements")
	defer b.mu.RUnlock()

	all := b.accountStatementsLocked(c.account)
	vals := make([]PermissionStatement, len(all))

	for i, s := range all {
		vals[i] = *s
	}

	return page.New(vals, token, maxResults, defaultListLimit), nil
}

// GetResourcePolicy returns the caller's consolidated policy document.
func (b *InMemoryBackend) GetResourcePolicy(ctx context.Context) ([]PolicyStatement, string, error) {
	c := b.callerOf(ctx)
	if c.anon {
		return nil, "", accessDenied(oauthInsufficientPerms, 403, "anonymous callers cannot read the policy")
	}

	b.mu.RLock("GetResourcePolicy")
	defer b.mu.RUnlock()

	sts := b.accountStatementsLocked(c.account)
	if len(sts) == 0 {
		return nil, "", notFound("no resource-based policy exists for account " + c.account)
	}

	out := make([]PolicyStatement, len(sts))
	for i, s := range sts {
		out[i] = PolicyStatement{
			Effect: "Deny", Principal: map[string]string{"AWS": "*"}, Action: []string{"*"}, Resource: "*",
			Condition: s.Condition,
		}
	}

	return out, policyVersion, nil
}
