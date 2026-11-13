package ocidns

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/miekg/dns"
)

const (
	maxTTL          = 2147483647
	maxDomainLen    = 253
	maxLabelLen     = 63
	rtypeSOA        = "SOA"
	rtypeNS         = "NS"
	rtypeCNAME      = "CNAME"
	rtypeALIAS      = "ALIAS"
	soaTTL          = 300
	nsTTL           = 86400
	soaRefresh      = 3600
	soaRetry        = 600
	soaExpire       = 604800
	soaMinimum      = 1800
	hashBytes       = 16
	opRequire       = "REQUIRE"
	opProhibit      = "PROHIBIT"
	opAdd           = "ADD"
	opRemove        = "REMOVE"
	nameserverCount = 4
)

// supportedRtypes are the record types a user may manage (SOA and the apex NS set are managed by the service).
var supportedRtypes = map[string]bool{ //nolint:gochecknoglobals // immutable lookup table
	"A": true, "AAAA": true, "ALIAS": true, "CAA": true, "CDNSKEY": true, "CDS": true, "CERT": true, "CNAME": true,
	"CSYNC": true, "DHCID": true, "DNAME": true, "DNSKEY": true, "DS": true, "HTTPS": true, "IPSECKEY": true,
	"KX": true, "LOC": true, "MX": true, "NAPTR": true, "NS": true, "PTR": true, "SMIMEA": true, "SPF": true,
	"SRV": true, "SSHFP": true, "SVCB": true, "TLSA": true, "TXT": true, "URI": true,
}

// RecordIn is a record or a PATCH operation in a request body.
type RecordIn struct {
	Domain       *string `json:"domain"`
	Rdata        *string `json:"rdata"`
	Rtype        *string `json:"rtype"`
	TTL          *int    `json:"ttl"`
	RecordHash   *string `json:"recordHash"`
	RrsetVersion *string `json:"rrsetVersion"`
	IsProtected  *bool   `json:"isProtected"`
	Operation    string  `json:"operation"`
}

// ItemsIn is the body of the record PATCH and PUT operations.
type ItemsIn struct {
	Items []RecordIn `json:"items"`
}

type recordWire struct {
	Domain       string `json:"domain"`
	RecordHash   string `json:"recordHash"`
	Rdata        string `json:"rdata"`
	RrsetVersion string `json:"rrsetVersion"`
	Rtype        string `json:"rtype"`
	TTL          int    `json:"ttl"`
	IsProtected  bool   `json:"isProtected"`
}

// RecordCollectionWire is the RecordCollection and RrSet body.
type RecordCollectionWire struct {
	Items []recordWire `json:"items"`
}

func rrKey(domain, rtype string) string { return domain + "|" + rtype }

func recordHash(domain, rtype string, ttl int, rdata string) string {
	sum := sha256.Sum256([]byte(domain + "|" + rtype + "|" + strconv.Itoa(ttl) + "|" + rdata))

	return hex.EncodeToString(sum[:hashBytes])
}

func str(p *string) string {
	if p == nil {
		return ""
	}

	return *p
}

func validLabel(l string, wildcardOK bool) bool {
	if l == "*" {
		return wildcardOK
	}

	if l == "" || len(l) > maxLabelLen {
		return false
	}

	for i := range len(l) {
		c := l[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			continue
		}

		return false
	}

	return true
}

// normName lowercases a DNS name and drops the trailing dot; it rejects names with empty or oversized labels.
func normName(s string, wildcardOK bool) (string, bool) {
	s = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(s)), ".")
	if s == "" || len(s) > maxDomainLen {
		return "", false
	}

	for i, l := range strings.Split(s, ".") {
		if !validLabel(l, wildcardOK && i == 0) {
			return "", false
		}
	}

	return s, true
}

func inZone(zone, domain string) bool { return domain == zone || strings.HasSuffix(domain, "."+zone) }

func (b *InMemoryBackend) nameservers(z *Zone) []string {
	if z.Scope != scopeGlobal {
		return nil
	}

	out := make([]string, 0, nameserverCount)
	for i := 1; i <= nameserverCount; i++ {
		out = append(out, "ns"+strconv.Itoa(i)+".p68.dns.oraclecloud.net")
	}

	return out
}

func soaRdata(z *Zone, ns []string) string {
	primary := "ns1.p68.dns.oraclecloud.net"
	if len(ns) > 0 {
		primary = ns[0]
	}

	return fmt.Sprintf("%s. hostmaster.%s. %d %d %d %d %d", primary, z.Name, z.Version, soaRefresh, soaRetry,
		soaExpire, soaMinimum)
}

// managed lists the protected apex records the service owns: SOA and, for GLOBAL zones, the NS set.
func (b *InMemoryBackend) managed(z *Zone) []recordWire {
	ns := b.nameservers(z)
	out := make([]recordWire, 0, 1+len(ns))
	out = append(out, recordWire{
		Domain: z.Name, Rtype: rtypeSOA, TTL: soaTTL, Rdata: soaRdata(z, ns), IsProtected: true,
		RrsetVersion: strconv.FormatInt(z.Version, 10),
	})

	for _, n := range ns {
		out = append(out, recordWire{
			Domain: z.Name, Rtype: rtypeNS, TTL: nsTTL, Rdata: n + ".", IsProtected: true, RrsetVersion: "1",
		})
	}

	for i := range out {
		out[i].RecordHash = recordHash(out[i].Domain, out[i].Rtype, out[i].TTL, out[i].Rdata)
	}

	return out
}

// allRecords returns the managed records followed by every user record.
func (b *InMemoryBackend) allRecords(z *Zone) []recordWire {
	out := b.managed(z)

	for _, rs := range z.Records {
		for _, rd := range rs.Rdata {
			out = append(out, recordWire{
				Domain: rs.Domain, Rtype: rs.Rtype, TTL: rs.TTL, Rdata: rd,
				RecordHash:   recordHash(rs.Domain, rs.Rtype, rs.TTL, rd),
				RrsetVersion: strconv.FormatInt(rs.Version, 10),
			})
		}
	}

	return out
}

// RecordFilter selects records of a zone.
type RecordFilter struct {
	Domain         string
	DomainContains string
	Rtype          string
	ZoneVersion    string
}

func (b *InMemoryBackend) filterRecords(z *Zone, f RecordFilter) ([]recordWire, *APIError) {
	if f.ZoneVersion != "" && f.ZoneVersion != strconv.FormatInt(z.Version, 10) {
		return nil, errNotFound("zone version " + f.ZoneVersion)
	}

	domain := ""

	if f.Domain != "" {
		d, ok := normName(f.Domain, true)
		if !ok {
			return nil, errParam("domain is not a valid domain name")
		}

		domain = d
	}

	rtype := strings.ToUpper(f.Rtype)

	var out []recordWire

	for _, r := range b.allRecords(z) {
		if (domain != "" && r.Domain != domain) || (rtype != "" && r.Rtype != rtype) ||
			(f.DomainContains != "" && !containsFold(r.Domain, f.DomainContains)) {
			continue
		}

		out = append(out, r)
	}

	return out, nil
}

var recordSorters = sorter[recordWire]{ //nolint:gochecknoglobals // immutable comparator table
	"domain": func(a, c recordWire) int {
		return cmp.Or(cmp.Compare(a.Domain, c.Domain), cmp.Compare(a.Rtype, c.Rtype), cmp.Compare(a.Rdata, c.Rdata))
	},
	"rtype": func(a, c recordWire) int {
		return cmp.Or(cmp.Compare(a.Rtype, c.Rtype), cmp.Compare(a.Domain, c.Domain), cmp.Compare(a.Rdata, c.Rdata))
	},
	"ttl": func(a, c recordWire) int {
		return cmp.Or(cmp.Compare(a.TTL, c.TTL), cmp.Compare(a.Domain, c.Domain), cmp.Compare(a.Rtype, c.Rtype),
			cmp.Compare(a.Rdata, c.Rdata))
	},
}

// RecordList is a page of records with the matching total.
type RecordList = listResult[recordWire]

func (b *InMemoryBackend) listRecordsLocked(
	z *Zone,
	f RecordFilter,
	o ListOpts,
	mustExist bool,
) (RecordList, *APIError) {
	recs, err := b.filterRecords(z, f)
	if err != nil {
		return RecordList{}, err
	}

	if mustExist && len(recs) == 0 {
		return RecordList{}, errNotFound(f.Domain)
	}

	pg, err := sortPage(recs, o, "domain", false, recordSorters)
	if err != nil {
		return RecordList{}, err
	}

	return finish(pg, len(recs), func(r recordWire) recordWire { return r }), nil
}

// normRdata validates rdata for rtype at owner and returns its normalized presentation form.
func normRdata(owner, rtype, rdata string, ttl int) (string, *APIError) {
	rdata = strings.TrimSpace(rdata)
	if rdata == "" || strings.ContainsAny(rdata, "\r\n") {
		return "", errParam("rdata must be a single line of record data")
	}

	parseType := rtype
	if rtype == rtypeALIAS {
		parseType = rtypeCNAME
	}

	rr, err := dns.NewRR(fmt.Sprintf("%s. %d IN %s %s", owner, ttl, parseType, rdata))
	if err != nil || rr == nil || rr.Header().Rrtype != dns.StringToType[parseType] {
		return "", errParam("rdata is not valid for record type " + rtype)
	}

	lowerTargets(rr)

	return strings.TrimPrefix(rr.String(), rr.Header().String()), nil
}

// lowerTargets lowercases the domain names inside rdata: DNS names are case-insensitive.
func lowerTargets(rr dns.RR) {
	switch v := rr.(type) {
	case *dns.CNAME:
		v.Target = strings.ToLower(v.Target)
	case *dns.NS:
		v.Ns = strings.ToLower(v.Ns)
	case *dns.PTR:
		v.Ptr = strings.ToLower(v.Ptr)
	case *dns.DNAME:
		v.Target = strings.ToLower(v.Target)
	case *dns.MX:
		v.Mx = strings.ToLower(v.Mx)
	case *dns.SRV:
		v.Target = strings.ToLower(v.Target)
	}
}

type recSpec struct {
	domain string
	rtype  string
	rdata  string
	ttl    int
	hasTTL bool
}

// spec validates the identifying fields of a record against the zone; scope fills empty domain and rtype.
func (z *Zone) spec(in RecordIn, scopeDomain, scopeRtype string, needData bool) (recSpec, *APIError) {
	var (
		s   recSpec
		err *APIError
	)

	if s.domain, err = z.specDomain(str(in.Domain), scopeDomain); err != nil {
		return s, err
	}

	if s.rtype, err = specRtype(str(in.Rtype), scopeRtype); err != nil {
		return s, err
	}

	if in.TTL != nil {
		if *in.TTL < 0 || *in.TTL > maxTTL {
			return s, errParam("ttl must be between 0 and 2147483647")
		}

		s.ttl, s.hasTTL = *in.TTL, true
	}

	if needData {
		return z.completeSpec(s, in)
	}

	if in.Rdata != nil && s.rtype == "" {
		return s, errMissing("rtype is required when rdata is given")
	}

	if in.Rdata != nil && s.domain != "" && s.rtype != rtypeSOA {
		rd, rerr := normRdata(s.domain, s.rtype, *in.Rdata, cmp.Or(s.ttl, soaTTL))
		if rerr != nil {
			return s, rerr
		}

		s.rdata = rd
	}

	return s, nil
}

func (z *Zone) specDomain(own, scope string) (string, *APIError) {
	raw := cmp.Or(own, scope)
	if raw == "" {
		return "", nil
	}

	d, ok := normName(raw, true)
	if !ok {
		return "", errParam("domain " + raw + " is not a valid domain name")
	}

	if !inZone(z.Name, d) {
		return "", errParam("domain " + d + " is not within zone " + z.Name)
	}

	if scope != "" && d != mustNorm(scope) {
		return "", errParam("record domain does not match the domain in the request path")
	}

	return d, nil
}

func specRtype(own, scope string) (string, *APIError) {
	rtype := strings.ToUpper(cmp.Or(own, scope))
	if scope != "" && rtype != strings.ToUpper(scope) {
		return "", errParam("record rtype does not match the rtype in the request path")
	}

	if rtype != "" && rtype != rtypeSOA && !supportedRtypes[rtype] {
		return "", errParam("record type " + rtype + " is not supported")
	}

	return rtype, nil
}

func mustNorm(s string) string {
	n, _ := normName(s, true)

	return n
}

func (z *Zone) completeSpec(s recSpec, in RecordIn) (recSpec, *APIError) {
	switch {
	case s.domain == "":
		return s, errMissing("domain is required")
	case s.rtype == "":
		return s, errMissing("rtype is required")
	case in.Rdata == nil:
		return s, errMissing("rdata is required")
	case !s.hasTTL:
		return s, errMissing("ttl is required")
	}

	rd, err := normRdata(s.domain, s.rtype, *in.Rdata, s.ttl)
	if err != nil {
		return s, err
	}

	s.rdata = rd

	return s, nil
}

// isManaged reports whether (domain, rtype) is a protected apex record set.
func (z *Zone) isManaged(domain, rtype string) bool {
	return domain == z.Name && (rtype == rtypeSOA || (rtype == rtypeNS && z.Scope == scopeGlobal))
}

func cloneRecords(m map[string]*RRSet) map[string]*RRSet {
	out := make(map[string]*RRSet, len(m))

	for k, v := range m {
		cp := *v
		cp.Rdata = slices.Clone(v.Rdata)
		out[k] = &cp
	}

	return out
}

func (z *Zone) addRecord(m map[string]*RRSet, s recSpec) {
	k := rrKey(s.domain, s.rtype)
	rs, ok := m[k]

	if !ok {
		m[k] = &RRSet{Domain: s.domain, Rtype: s.rtype, TTL: s.ttl, Rdata: []string{s.rdata}}

		return
	}

	rs.TTL = s.ttl

	if !slices.Contains(rs.Rdata, s.rdata) {
		rs.Rdata = append(rs.Rdata, s.rdata)
	}
}

func removeRecord(m map[string]*RRSet, s recSpec, hash string) {
	for k, rs := range m {
		if (s.domain != "" && rs.Domain != s.domain) || (s.rtype != "" && rs.Rtype != s.rtype) {
			continue
		}

		if s.rdata == "" && hash == "" {
			delete(m, k)

			continue
		}

		rs.Rdata = slices.DeleteFunc(rs.Rdata, func(rd string) bool {
			return (s.rdata == "" || rd == s.rdata) &&
				(hash == "" || recordHash(rs.Domain, rs.Rtype, rs.TTL, rd) == hash)
		})

		if len(rs.Rdata) == 0 {
			delete(m, k)
		}
	}
}

// checkExclusive enforces CNAME and ALIAS exclusivity and single-valued CNAME/ALIAS sets.
func (z *Zone) checkExclusive(m map[string]*RRSet) *APIError {
	types := map[string]map[string]bool{}

	for _, rs := range m {
		if types[rs.Domain] == nil {
			types[rs.Domain] = map[string]bool{}
		}

		types[rs.Domain][rs.Rtype] = true

		if (rs.Rtype == rtypeCNAME || rs.Rtype == rtypeALIAS) && len(rs.Rdata) > 1 {
			return errParam("a " + rs.Rtype + " record set at " + rs.Domain + " can hold only one record")
		}
	}

	for domain, ts := range types {
		if ts[rtypeCNAME] && (len(ts) > 1 || z.managedAt(domain)) {
			return errParam("a CNAME record at " + domain + " cannot coexist with other record types")
		}

		if ts[rtypeALIAS] && (ts[rtypeCNAME] || ts["A"] || ts["AAAA"]) {
			return errParam("an ALIAS record at " + domain + " cannot coexist with A, AAAA or CNAME records")
		}
	}

	return nil
}

func (z *Zone) managedAt(domain string) bool { return domain == z.Name }

func signature(m map[string]*RRSet) map[string]string {
	out := make(map[string]string, len(m))

	for k, rs := range m {
		rd := slices.Clone(rs.Rdata)
		slices.Sort(rd)
		out[k] = strconv.Itoa(rs.TTL) + "|" + strings.Join(rd, "\n")
	}

	return out
}

// commit installs next as the zone's records when it differs, bumping the version of the changed sets.
func (b *InMemoryBackend) commit(z *Zone, next map[string]*RRSet) {
	before, after := signature(z.Records), signature(next)
	if maps.Equal(before, after) {
		return
	}

	z.Version++

	for k, rs := range next {
		if before[k] != after[k] {
			rs.Version = z.Version
		}
	}

	z.Records = next
	z.touch(b.nowUTC())
}

func (z *Zone) matchesOp(rec recordWire, in RecordIn, s recSpec) bool {
	switch {
	case s.domain != "" && rec.Domain != s.domain:
		return false
	case s.rtype != "" && rec.Rtype != s.rtype:
		return false
	case s.rdata != "" && rec.Rdata != s.rdata:
		return false
	case s.hasTTL && rec.TTL != s.ttl:
		return false
	case in.RecordHash != nil && rec.RecordHash != *in.RecordHash:
		return false
	case in.RrsetVersion != nil && rec.RrsetVersion != *in.RrsetVersion:
		return false
	case in.IsProtected != nil && rec.IsProtected != *in.IsProtected:
		return false
	}

	return true
}

func (b *InMemoryBackend) preconditionHolds(z *Zone, in RecordIn, s recSpec) bool {
	for _, r := range b.allRecords(z) {
		if z.matchesOp(r, in, s) {
			return true
		}
	}

	return false
}

type patchStep struct {
	in RecordIn
	s  recSpec
}

func (z *Zone) parseSteps(items []RecordIn, domain, rtype string) ([]patchStep, *APIError) {
	steps := make([]patchStep, 0, len(items))

	for _, in := range items {
		op := strings.ToUpper(in.Operation)

		switch op {
		case opRequire, opProhibit, opAdd, opRemove:
		case "":
			return nil, errMissing("operation is required for each item")
		default:
			return nil, errParam("operation must be REQUIRE, PROHIBIT, ADD or REMOVE")
		}

		in.Operation = op

		s, err := z.spec(in, domain, rtype, op == opAdd)
		if err != nil {
			return nil, err
		}

		if op == opRemove && s.domain == "" && in.RecordHash == nil {
			return nil, errMissing("domain or recordHash is required")
		}

		steps = append(steps, patchStep{in: in, s: s})
	}

	return steps, nil
}

func (b *InMemoryBackend) checkPreconditions(z *Zone, steps []patchStep) *APIError {
	for _, st := range steps {
		want := map[string]bool{opRequire: true, opProhibit: false}

		holds, isPrecondition := want[st.in.Operation]
		if !isPrecondition {
			continue
		}

		if b.preconditionHolds(z, st.in, st.s) != holds {
			return errPrecondition("A " + st.in.Operation + " precondition on " + cmp.Or(st.s.domain, "the zone") +
				" was not met")
		}
	}

	return nil
}

// patchRecords applies PATCH operations; domain and rtype restrict the scope of the request.
func (b *InMemoryBackend) patchRecords(z *Zone, items []RecordIn, domain, rtype string) *APIError {
	steps, err := z.parseSteps(items, domain, rtype)
	if err != nil {
		return err
	}

	if err = b.checkPreconditions(z, steps); err != nil {
		return err
	}

	next := cloneRecords(z.Records)

	for _, st := range steps {
		if err = b.applyStep(z, next, st.in.Operation, st.in, st.s); err != nil {
			return err
		}
	}

	if err = z.checkExclusive(next); err != nil {
		return err
	}

	b.commit(z, next)

	return nil
}

func (b *InMemoryBackend) applyStep(z *Zone, next map[string]*RRSet, op string, in RecordIn, s recSpec) *APIError {
	switch op {
	case opAdd:
		if z.isManaged(s.domain, s.rtype) {
			if s.rtype == rtypeSOA || !managedHas(b.managed(z), s) {
				return errParam("the protected apex " + s.rtype + " records cannot be changed")
			}

			return nil
		}

		z.addRecord(next, s)
	case opRemove:
		if s.domain == z.Name && (s.rtype == rtypeSOA || (s.rtype == rtypeNS && z.Scope == scopeGlobal)) {
			return errParam("the protected apex SOA and NS records cannot be removed")
		}

		removeRecord(next, s, str(in.RecordHash))
	}

	return nil
}

// replaceRecords replaces the records inside the scope with items, keeping protected records.
func (b *InMemoryBackend) replaceRecords(z *Zone, items []RecordIn, domain, rtype string) *APIError {
	next := cloneRecords(z.Records)

	for k, rs := range next {
		if (domain == "" || rs.Domain == mustNorm(domain)) && (rtype == "" || rs.Rtype == strings.ToUpper(rtype)) {
			delete(next, k)
		}
	}

	ttls := map[string]int{}
	managed := b.managed(z)

	for _, in := range items {
		s, err := z.spec(in, domain, rtype, true)
		if err != nil {
			return err
		}

		if z.isManaged(s.domain, s.rtype) {
			if !managedHas(managed, s) {
				return errParam("the protected apex " + s.rtype + " records cannot be changed")
			}

			continue
		}

		k := rrKey(s.domain, s.rtype)
		if prev, seen := ttls[k]; seen && prev != s.ttl {
			return errParam("records of " + s.rtype + " at " + s.domain + " must share one ttl")
		}

		ttls[k] = s.ttl

		z.addRecord(next, s)
	}

	if err := z.checkExclusive(next); err != nil {
		return err
	}

	b.commit(z, next)

	return nil
}

func managedHas(managed []recordWire, s recSpec) bool {
	for _, m := range managed {
		if m.Domain == s.domain && m.Rtype == s.rtype && (s.rtype == rtypeSOA || m.Rdata == s.rdata) {
			return true
		}
	}

	return false
}

// deleteRecords removes user records at a domain, or one record set of it.
func (b *InMemoryBackend) deleteRecords(z *Zone, domain, rtype string) *APIError {
	d, ok := normName(domain, true)
	if !ok {
		return errParam("domain is not a valid domain name")
	}

	rt := strings.ToUpper(rtype)

	if rt != "" && z.isManaged(d, rt) {
		return errParam("the protected apex " + rt + " records cannot be deleted")
	}

	next := cloneRecords(z.Records)
	found := false

	for k, rs := range next {
		if rs.Domain == d && (rt == "" || rs.Rtype == rt) {
			delete(next, k)

			found = true
		}
	}

	if !found {
		return errNotFound(d)
	}

	b.commit(z, next)

	return nil
}
