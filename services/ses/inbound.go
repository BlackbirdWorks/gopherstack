package ses

import (
	"context"
	"net/netip"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	defaultInboundMaxSize = 40 << 20
	inboundLogCap         = 100
	inboundTimeFormat     = "2006-01-02T15:04:05.000Z"
)

// InboundObjectStore writes a received message into S3.
type InboundObjectStore interface {
	PutObject(ctx context.Context, bucket, key string, body []byte) error
}

// InboundLambdaInvoker invokes the function named by a Lambda receipt action.
type InboundLambdaInvoker interface {
	InvokeFunction(ctx context.Context, name, invocationType string, payload []byte) ([]byte, int, error)
}

// InboundConfig configures the opt-in inbound SMTP listener.
type InboundConfig struct {
	Addr    string
	MaxSize int64
}

// InboundExecution records one rule's run against a received message.
type InboundExecution struct {
	Rule        string
	Error       string
	Recipients  []string
	Actions     []string
	Notes       []string
	ScanEnabled bool
}

// InboundMessage records one message accepted by the inbound listener.
type InboundMessage struct {
	Received   time.Time
	MessageID  string
	Source     string
	ClientIP   string
	Recipients []string
	Executions []InboundExecution
	Size       int
	TLS        bool
}

// SetInboundObjectStore wires the S3 backend used by S3 receipt actions.
func (b *InMemoryBackend) SetInboundObjectStore(s InboundObjectStore) {
	b.mu.Lock("SetInboundObjectStore")
	defer b.mu.Unlock()

	b.inboundStore = s
}

// SetInboundLambdaInvoker wires the Lambda backend used by Lambda receipt actions.
func (b *InMemoryBackend) SetInboundLambdaInvoker(l InboundLambdaInvoker) {
	b.mu.Lock("SetInboundLambdaInvoker")
	defer b.mu.Unlock()

	b.inboundLambda = l
}

// ListInboundMessages returns the most recent messages accepted by the inbound listener, oldest first.
func (b *InMemoryBackend) ListInboundMessages() []InboundMessage {
	b.mu.RLock("ListInboundMessages")
	defer b.mu.RUnlock()

	return append([]InboundMessage(nil), b.inboundLog...)
}

func (b *InMemoryBackend) recordInbound(m InboundMessage) {
	b.mu.Lock("recordInbound")
	defer b.mu.Unlock()

	b.inboundLog = append(b.inboundLog, m)
	if len(b.inboundLog) > inboundLogCap {
		b.inboundLog = b.inboundLog[len(b.inboundLog)-inboundLogCap:]
	}
}

// inboundPlan is a lock-free copy of the state one delivery needs.
type inboundPlan struct {
	store      InboundObjectStore
	lambda     InboundLambdaInvoker
	sns        SNSPublisher
	rules      []ReceiptRule
	ruleSet    string
	accountID  string
	region     string
	identities []string
}

func (b *InMemoryBackend) inboundSnapshot() inboundPlan {
	b.mu.RLock("inboundSnapshot")
	defer b.mu.RUnlock()

	p := inboundPlan{
		store: b.inboundStore, lambda: b.inboundLambda, sns: b.snsPublisher,
		ruleSet: b.activeRuleSet, accountID: b.accountID, region: b.region,
	}

	if rs, ok := b.receiptRuleSets.Get(b.activeRuleSet); ok {
		p.rules = append([]ReceiptRule(nil), rs.Rules...)
	}

	for _, rec := range b.identities.All() {
		if rec.Verified {
			p.identities = append(p.identities, rec.Identity)
		}
	}

	return p
}

// inboundIPBlocked applies the receipt IP filters; the most specific matching CIDR wins and block wins a tie.
func (b *InMemoryBackend) inboundIPBlocked(ip netip.Addr) bool {
	b.mu.RLock("inboundIPBlocked")
	defer b.mu.RUnlock()

	best, blocked := -1, false

	for _, f := range b.receiptFilters.All() {
		pfx, err := netip.ParsePrefix(f.CIDR)
		if err != nil {
			addr, aerr := netip.ParseAddr(f.CIDR)
			if aerr != nil {
				continue
			}

			pfx = netip.PrefixFrom(addr, addr.BitLen())
		}

		if !pfx.Contains(ip.Unmap()) && !pfx.Contains(ip) {
			continue
		}

		isBlock := f.Policy == FilterPolicyBlock
		if pfx.Bits() > best || (pfx.Bits() == best && isBlock) {
			best, blocked = pfx.Bits(), isBlock
		}
	}

	return blocked
}

func domainOf(addr string) string {
	if _, after, ok := strings.CutLast(addr, "@"); ok {
		return strs.Fold(after)
	}

	return strs.Fold(addr)
}

// recipientMatches implements SES rule recipient patterns: full address, domain, or ".domain" for subdomains.
func recipientMatches(pattern, addr string) bool {
	switch {
	case strings.Contains(pattern, "@"):
		return strs.Equal(pattern, addr)
	case strings.HasPrefix(pattern, "."):
		d := domainOf(addr)

		return strings.HasSuffix(d, strs.Fold(pattern))
	default:
		return domainOf(addr) == strs.Fold(pattern)
	}
}

func ruleMatches(rule ReceiptRule, addr string, isTLS bool) bool {
	if !rule.Enabled || (rule.TLSPolicy == TLSPolicyRequire && !isTLS) {
		return false
	}

	if len(rule.Recipients) == 0 {
		return true
	}

	for _, p := range rule.Recipients {
		if recipientMatches(p, addr) {
			return true
		}
	}

	return false
}

// identityVerified reports whether addr, or its domain or a parent domain, is a verified identity.
func (p inboundPlan) identityVerified(addr string) bool {
	d := domainOf(addr)

	for _, id := range p.identities {
		if strs.Equal(id, addr) {
			return true
		}

		if strings.Contains(id, "@") {
			continue
		}

		f := strs.Fold(id)
		if d == f || strings.HasSuffix(d, "."+f) {
			return true
		}
	}

	return false
}

// inboundAcceptRcpt decides RCPT TO; ok is false with an SMTP reply text when the recipient is refused.
func (b *InMemoryBackend) inboundAcceptRcpt(addr string, isTLS bool) (string, bool) {
	p := b.inboundSnapshot()

	if !p.identityVerified(addr) {
		return "5.1.1 Mailbox does not exist: domain or address not verified", false
	}

	for _, r := range p.rules {
		if ruleMatches(r, addr, isTLS) {
			return "", true
		}
	}

	return "5.1.1 Mailbox does not exist: no matching receipt rule", false
}
