package gcpdns

import (
	"maps"
	"slices"
	"strings"

	"github.com/miekg/dns"
)

// zoneNames maps every owner name holding records to its rrset types, leaving out names below a zone cut.
func (z *zoneAnswer) zoneNames() map[string][]uint16 {
	out := map[string][]uint16{}

	for _, rr := range z.rec.RRSets {
		cut := z.delegation(rr.Name)
		if cut != nil && (cut.Name != rr.Name || rr.Type != typeNS && rr.Type != "DS") {
			continue
		}

		if t, ok := dns.StringToType[rr.Type]; ok {
			out[rr.Name] = append(out[rr.Name], t)
		}
	}

	return out
}

func (z *zoneAnswer) bitmap(name string, types []uint16, nsec bool) []uint16 {
	bm := slices.Clone(types)

	if name == z.apex() {
		bm = append(bm, dns.TypeDNSKEY)
		if !nsec {
			bm = append(bm, dns.TypeNSEC3PARAM)
		}
	}

	bm = append(bm, dns.TypeRRSIG)

	if nsec {
		bm = append(bm, dns.TypeNSEC)
	}

	slices.Sort(bm)

	return slices.Compact(bm)
}

func labelsCompare(a, b string) int {
	la, lb := dns.SplitDomainName(a), dns.SplitDomainName(b)

	for i, j := len(la)-1, len(lb)-1; i >= 0 && j >= 0; i, j = i-1, j-1 {
		if c := strings.Compare(strings.ToLower(la[i]), strings.ToLower(lb[j])); c != 0 {
			return c
		}
	}

	return len(la) - len(lb)
}

func joinLabels(labels []string, n int) string {
	if n <= 0 {
		return "."
	}

	return strings.Join(labels[len(labels)-n:], ".") + "."
}

type nsecChain struct {
	types map[string][]uint16
	names []string
}

func (z *zoneAnswer) newNSECChain() nsecChain {
	types := z.zoneNames()
	names := make([]string, 0, len(types))

	for n := range types {
		names = append(names, n)
	}

	slices.SortFunc(names, labelsCompare)

	return nsecChain{names: names, types: types}
}

func (z *zoneAnswer) nsecMatchOrCover(c nsecChain, name string) dns.RR {
	i, found := slices.BinarySearchFunc(c.names, name, labelsCompare)
	if !found {
		i = (i - 1 + len(c.names)) % len(c.names)
	}

	owner := c.names[i]

	return &dns.NSEC{
		Hdr:        dns.RR_Header{Name: owner, Rrtype: dns.TypeNSEC, Class: dns.ClassINET, Ttl: z.negTTL()},
		NextDomain: c.names[(i+1)%len(c.names)],
		TypeBitMap: z.bitmap(owner, c.types[owner], true),
	}
}

type nsec3Entry struct {
	hash  string
	name  string
	types []uint16
}

func (z *zoneAnswer) nsec3Chain() []nsec3Entry {
	all := z.zoneNames()
	apexN := len(dns.SplitDomainName(z.apex()))

	for name := range maps.Clone(all) {
		labels := dns.SplitDomainName(name)
		for n := len(labels) - 1; n > apexN; n-- {
			if anc := joinLabels(labels, n); all[anc] == nil {
				all[anc] = []uint16{}
			}
		}
	}

	if all[z.apex()] == nil {
		all[z.apex()] = []uint16{}
	}

	out := make([]nsec3Entry, 0, len(all))
	for name, t := range all {
		out = append(out, nsec3Entry{hash: dns.HashName(name, dns.SHA1, 0, ""), name: name, types: t})
	}

	slices.SortFunc(out, func(a, b nsec3Entry) int { return strings.Compare(a.hash, b.hash) })

	return out
}

func (z *zoneAnswer) nsec3MatchOrCover(c []nsec3Entry, name string) dns.RR {
	h := dns.HashName(name, dns.SHA1, 0, "")

	i, found := slices.BinarySearchFunc(c, h, func(e nsec3Entry, t string) int { return strings.Compare(e.hash, t) })
	if !found {
		i = (i - 1 + len(c)) % len(c)
	}

	e := c[i]

	var bm []uint16
	if len(e.types) > 0 {
		bm = z.bitmap(e.name, e.types, false)
	}

	return &dns.NSEC3{
		Hdr: dns.RR_Header{
			Name:   strings.ToLower(e.hash) + "." + z.apex(),
			Rrtype: dns.TypeNSEC3, Class: dns.ClassINET, Ttl: z.negTTL(),
		},
		Hash:       dns.SHA1,
		NextDomain: c[(i+1)%len(c)].hash,
		TypeBitMap: bm,
	}
}

// closestEncloser returns the longest existing ancestor of q and the next-closer name below it.
func (z *zoneAnswer) closestEncloser(q string) (string, string) {
	labels := dns.SplitDomainName(q)
	apexN := len(dns.SplitDomainName(z.apex()))

	for n := len(labels) - 1; n > apexN; n-- {
		if cand := joinLabels(labels, n); z.exists(cand) {
			return cand, joinLabels(labels, n+1)
		}
	}

	return z.apex(), joinLabels(labels, min(apexN+1, len(labels)))
}

func (z *zoneAnswer) addProof(names ...string) {
	var recs []dns.RR

	if z.nsec3Enabled() {
		c := z.nsec3Chain()
		for _, n := range names {
			recs = append(recs, z.nsec3MatchOrCover(c, n))
		}
	} else if c := z.newNSECChain(); len(c.names) > 0 {
		for _, n := range names {
			recs = append(recs, z.nsecMatchOrCover(c, n))
		}
	}

	for _, rr := range recs {
		if !slices.ContainsFunc(z.resp.Ns, func(have dns.RR) bool {
			return have.Header().Name == rr.Header().Name && have.Header().Rrtype == rr.Header().Rrtype
		}) {
			z.resp.Ns = append(z.resp.Ns, rr)
		}
	}
}

func (z *zoneAnswer) addDenial() {
	ce, next := z.closestEncloser(z.qname)

	switch {
	case z.cut != "":
		if ds := z.rec.RRSets[rrKey(z.cut, "DS")]; ds != nil {
			z.resp.Ns = append(z.resp.Ns, toRRs(ds.Name, ds)...)

			return
		}

		z.addProof(z.cut)
	case z.wildcard != "" && len(z.resp.Answer) > 0:
		if z.nsec3Enabled() {
			z.addProof(next)
		} else {
			z.addProof(z.qname)
		}
	case z.proof == proofNameError && z.nsec3Enabled():
		z.addProof(ce, next, "*."+ce)
	case z.proof == proofNameError:
		z.addProof(z.qname, "*."+ce)
	case z.proof == proofNoData:
		z.addProof(z.qname)
	case z.proof == proofNone:
	}
}
