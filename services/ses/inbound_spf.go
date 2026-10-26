package ses

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	maxSPFLookups     = 10
	maxSPFVoidLookups = 2
	maxSPFNames       = 10
	ipv4Bits          = 32
	ipv6Bits          = 128
	macroNibbleBits   = 4
)

// ErrDNSNotFound is returned by an InboundDNSResolver for a name with no matching records.
var ErrDNSNotFound = errors.New("dns: no such record")

// InboundDNSResolver adds the address, MX and PTR lookups SPF a, mx, ptr and exists mechanisms need.
type InboundDNSResolver interface {
	InboundTXTResolver
	// LookupIP returns the A and AAAA addresses of name.
	LookupIP(ctx context.Context, name string) ([]netip.Addr, error)
	// LookupMX returns the mail exchanger host names of name.
	LookupMX(ctx context.Context, name string) ([]string, error)
	// LookupAddr returns the PTR names of ip.
	LookupAddr(ctx context.Context, ip netip.Addr) ([]string, error)
}

type spfResult int

const (
	spfNone spfResult = iota
	spfNeutral
	spfPass
	spfFail
	spfSoftFail
	spfTempError
	spfPermError
)

func (r spfResult) verdict() string {
	switch r {
	case spfPass:
		return verdictPass
	case spfFail, spfSoftFail:
		return verdictFail
	case spfTempError, spfPermError:
		return verdictProcessingFailed
	default:
		return verdictGray
	}
}

func notFound(err error) bool {
	var dnsErr *net.DNSError

	return errors.Is(err, ErrDNSNotFound) || (errors.As(err, &dnsErr) && dnsErr.IsNotFound)
}

type spfChecker struct {
	res     InboundTXTResolver
	dns     InboundDNSResolver
	sender  string
	ip      netip.Addr
	lookups int
	voids   int
}

// checkSPF evaluates the sender domain's v=spf1 record against the client IP (RFC 7208 check_host).
func checkSPF(ctx context.Context, res InboundTXTResolver, from, clientIP string) string {
	domain := domainOf(from)
	ip, err := netip.ParseAddr(clientIP)

	if domain == "" || domain == strs.Fold(from) || err != nil {
		return verdictGray
	}

	c := &spfChecker{res: res, sender: strs.Fold(from), ip: ip.Unmap()}
	c.dns, _ = res.(InboundDNSResolver)

	return c.check(ctx, domain).verdict()
}

func (c *spfChecker) check(ctx context.Context, domain string) spfResult {
	recs := txtRecords(ctx, c.res, domain, "v=spf1")

	switch {
	case len(recs) == 0:
		return spfNone
	case len(recs) > 1:
		return spfPermError
	}

	terms := strings.Fields(recs[0])[1:]
	redirect := ""

	for _, t := range terms {
		if name, val, ok := modifier(t); ok {
			if strs.Equal(name, "redirect") && redirect == "" {
				redirect = val
			}

			continue
		}

		if res, matched := c.term(ctx, t, domain); matched {
			return res
		}
	}

	if redirect == "" {
		return spfNeutral
	}

	target, ok := c.expand(redirect, domain)
	if !ok || !c.countLookup() {
		return spfPermError
	}

	if res := c.check(ctx, target); res != spfNone {
		return res
	}

	return spfPermError
}

// modifier splits name=value terms; a mechanism with a domain-spec never has "=" before ":" or "/".
func modifier(t string) (string, string, bool) {
	eq := strings.IndexByte(t, '=')
	if eq <= 0 || strings.ContainsAny(t[:eq], ":/") || !isAlpha(t[0]) {
		return "", "", false
	}

	return t[:eq], t[eq+1:], true
}

func isAlpha(b byte) bool { return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') }

func (c *spfChecker) countLookup() bool {
	c.lookups++

	return c.lookups <= maxSPFLookups
}

func (c *spfChecker) countVoid() bool {
	c.voids++

	return c.voids <= maxSPFVoidLookups
}

var spfQualifiers = map[byte]spfResult{'+': spfPass, '-': spfFail, '~': spfSoftFail, '?': spfNeutral}

// term evaluates one mechanism; matched reports whether it decided the result.
func (c *spfChecker) term(ctx context.Context, term, domain string) (spfResult, bool) {
	q := spfPass

	if r, ok := spfQualifiers[term[0]]; ok {
		q, term = r, term[1:]
	}

	name, arg, hasArg := strings.Cut(term, ":")
	cidr := ""

	if !hasArg {
		name, cidr, _ = strings.Cut(term, "/")
		if cidr != "" {
			cidr = "/" + cidr
		}
	}

	name = strs.Fold(name)

	switch name {
	case "all":
		return q, true
	case "ip4", "ip6":
		return q, spfIPMatch(arg, c.ip)
	case "include":
		return c.include(ctx, q, arg, domain)
	case "a", "mx", "ptr", "exists":
		return c.dnsMechanism(ctx, q, name, arg, cidr, domain)
	}

	return spfPermError, true
}

func (c *spfChecker) include(ctx context.Context, q spfResult, arg, domain string) (spfResult, bool) {
	target, ok := c.expand(arg, domain)
	if !ok || !c.countLookup() {
		return spfPermError, true
	}

	switch inner := c.check(ctx, target); inner {
	case spfPass:
		return q, true
	case spfTempError, spfPermError:
		return inner, true
	case spfNone:
		return spfPermError, true
	default:
		return spfNone, false
	}
}

// dnsMechanism evaluates a, mx, ptr and exists; a resolver without address lookups never matches them.
func (c *spfChecker) dnsMechanism(ctx context.Context, q spfResult, name, arg, cidr, domain string) (spfResult, bool) {
	if c.dns == nil {
		return spfNone, false
	}

	if !c.countLookup() {
		return spfPermError, true
	}

	spec := domain
	if arg != "" {
		spec = arg
	}

	if before, after, found := strings.Cut(spec, "/"); found && name != "exists" && name != "ptr" {
		spec, cidr = before, "/"+after
	}

	target, ok := c.expand(spec, domain)
	if !ok {
		return spfPermError, true
	}

	var (
		matched bool
		res     spfResult
	)

	switch name {
	case "a":
		matched, res = c.matchHost(ctx, target, cidr)
	case "mx":
		matched, res = c.matchMX(ctx, target, cidr)
	case "exists":
		matched, res = c.matchExists(ctx, target)
	default:
		matched, res = c.matchPTR(ctx, target)
	}

	switch {
	case res != spfNone:
		return res, true
	case matched:
		return q, true
	}

	return spfNone, false
}

func (c *spfChecker) lookupIPs(ctx context.Context, name string) ([]netip.Addr, spfResult) {
	addrs, err := c.dns.LookupIP(ctx, name)

	switch {
	case err != nil && !notFound(err):
		return nil, spfTempError
	case len(addrs) == 0:
		if !c.countVoid() {
			return nil, spfPermError
		}
	}

	return addrs, spfNone
}

// splitCIDR parses "/n", "//m" or "/n//m" into IPv4 and IPv6 prefix lengths.
func splitCIDR(cidr string) (int, int) {
	v4, v6 := ipv4Bits, ipv6Bits

	if cidr == "" {
		return v4, v6
	}

	first, second, dual := strings.Cut(strings.TrimPrefix(cidr, "/"), "//")

	if n, err := strconv.Atoi(first); err == nil && n >= 0 && n <= ipv4Bits {
		v4 = n
	}

	if dual {
		if n, err := strconv.Atoi(second); err == nil && n >= 0 && n <= ipv6Bits {
			v6 = n
		}
	}

	return v4, v6
}

func (c *spfChecker) addrMatches(addrs []netip.Addr, cidr string) bool {
	v4, v6 := splitCIDR(cidr)

	for _, a := range addrs {
		a = a.Unmap()
		if a.Is4() != c.ip.Is4() {
			continue
		}

		bits := v6
		if a.Is4() {
			bits = v4
		}

		if netip.PrefixFrom(a, bits).Masked().Contains(c.ip) {
			return true
		}
	}

	return false
}

func (c *spfChecker) matchHost(ctx context.Context, name, cidr string) (bool, spfResult) {
	addrs, res := c.lookupIPs(ctx, name)
	if res != spfNone {
		return false, res
	}

	return c.addrMatches(addrs, cidr), spfNone
}

func (c *spfChecker) matchMX(ctx context.Context, name, cidr string) (bool, spfResult) {
	hosts, err := c.dns.LookupMX(ctx, name)

	switch {
	case err != nil && !notFound(err):
		return false, spfTempError
	case len(hosts) == 0:
		if !c.countVoid() {
			return false, spfPermError
		}

		return false, spfNone
	case len(hosts) > maxSPFNames:
		return false, spfPermError
	}

	for _, h := range hosts {
		if matched, res := c.matchHost(ctx, strings.TrimSuffix(h, "."), cidr); res != spfNone || matched {
			return matched, res
		}
	}

	return false, spfNone
}

func (c *spfChecker) matchExists(ctx context.Context, name string) (bool, spfResult) {
	addrs, res := c.lookupIPs(ctx, name)
	if res != spfNone {
		return false, res
	}

	for _, a := range addrs {
		if a.Unmap().Is4() {
			return true, spfNone
		}
	}

	return false, spfNone
}

// matchPTR validates the client's reverse names with a forward lookup, then compares them to the target domain.
func (c *spfChecker) matchPTR(ctx context.Context, target string) (bool, spfResult) {
	names, err := c.dns.LookupAddr(ctx, c.ip)
	if err != nil && !notFound(err) {
		return false, spfNone
	}

	target = strs.Fold(strings.TrimSuffix(target, "."))

	for i, n := range names {
		if i >= maxSPFNames {
			break
		}

		n = strs.Fold(strings.TrimSuffix(n, "."))
		if n != target && !strings.HasSuffix(n, "."+target) {
			continue
		}

		if addrs, ferr := c.dns.LookupIP(ctx, n); ferr == nil && c.addrMatches(addrs, "") {
			return true, spfNone
		}
	}

	return false, spfNone
}

func spfIPMatch(cidr string, ip netip.Addr) bool {
	if pfx, err := netip.ParsePrefix(cidr); err == nil {
		return pfx.Contains(ip)
	}

	a, err := netip.ParseAddr(cidr)

	return err == nil && a.Unmap() == ip
}

// expand applies RFC 7208 section 7 macro expansion; ok is false for a malformed macro.
func (c *spfChecker) expand(spec, domain string) (string, bool) {
	var sb strings.Builder

	for i := 0; i < len(spec); i++ {
		if spec[i] != '%' {
			sb.WriteByte(spec[i])

			continue
		}

		if i+1 >= len(spec) {
			return "", false
		}

		i++

		switch spec[i] {
		case '%':
			sb.WriteByte('%')
		case '_':
			sb.WriteByte(' ')
		case '-':
			sb.WriteString("%20")
		case '{':
			end := strings.IndexByte(spec[i:], '}')
			if end < 0 {
				return "", false
			}

			val, ok := c.macro(spec[i+1:i+end], domain)
			if !ok {
				return "", false
			}

			sb.WriteString(val)

			i += end
		default:
			return "", false
		}
	}

	return sb.String(), true
}

// macro expands one {letter transformers delimiters} body.
func (c *spfChecker) macro(body, domain string) (string, bool) {
	if body == "" {
		return "", false
	}

	letter, rest := strings.ToLower(body[:1]), body[1:]
	local, senderDomain, _ := strings.Cut(c.sender, "@")

	if !strings.Contains(c.sender, "@") {
		local, senderDomain = "postmaster", c.sender
	}

	var val string

	switch letter {
	case "s":
		val = local + "@" + senderDomain
	case "l":
		val = local
	case "o", "h":
		val = senderDomain
	case "d":
		val = domain
	case "i":
		val = macroIP(c.ip)
	case "p", "r":
		val = "unknown"
	case "v":
		val = "in-addr"
		if c.ip.Is6() {
			val = "ip6"
		}
	case "c":
		val = c.ip.String()
	case "t":
		val = strconv.FormatInt(time.Now().Unix(), 10)
	default:
		return "", false
	}

	return transform(val, rest), true
}

func macroIP(ip netip.Addr) string {
	if ip.Is4() {
		return ip.String()
	}

	b := ip.As16()
	parts := make([]string, 0, len(b)*2)

	for _, x := range b {
		parts = append(parts, strconv.FormatUint(uint64(x>>macroNibbleBits), 16), strconv.FormatUint(uint64(x&0xf), 16))
	}

	return strings.Join(parts, ".")
}

// transform applies the digit, r and delimiter parts of a macro.
func transform(val, spec string) string {
	digits := ""

	for digits != spec && spec != "" && spec[0] >= '0' && spec[0] <= '9' {
		digits, spec = digits+spec[:1], spec[1:]
	}

	reverse := false
	if spec != "" && (spec[0] == 'r' || spec[0] == 'R') {
		reverse, spec = true, spec[1:]
	}

	delims := spec
	if delims == "" {
		delims = "."
	}

	parts := strings.FieldsFunc(val, func(r rune) bool { return strings.ContainsRune(delims, r) })
	if len(parts) == 0 {
		parts = []string{val}
	}

	if reverse {
		for i, j := 0, len(parts)-1; i < j; i, j = i+1, j-1 {
			parts[i], parts[j] = parts[j], parts[i]
		}
	}

	if n, err := strconv.Atoi(digits); err == nil && n > 0 && n < len(parts) {
		parts = parts[len(parts)-n:]
	}

	return strings.Join(parts, ".")
}
