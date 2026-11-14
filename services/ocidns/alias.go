package ocidns

import "github.com/miekg/dns"

// resolveAlias answers an A or AAAA query at an ALIAS owner with the target's address records, taken from whichever
// GLOBAL zone of this backend holds the target; targets this emulator does not serve yield no answer.
func (a *zoneAnswer) resolveAlias(qtype uint16, owner string, al *RRSet, depth int) {
	if len(al.Rdata) != 1 || depth >= maxCNAMEChain {
		return
	}

	target, ok := normName(al.Rdata[0], false)
	if !ok {
		return
	}

	z := a.b.globalZoneFor(target)
	if z == nil {
		return
	}

	node := a.b.nodes(z)[target]

	if next := node[rtypeALIAS]; next != nil {
		a.resolveAlias(qtype, owner, &RRSet{Domain: owner, Rtype: rtypeALIAS, TTL: al.TTL, Rdata: next.Rdata}, depth+1)

		return
	}

	if cn := node[rtypeCNAME]; cn != nil && len(cn.Rdata) == 1 {
		a.resolveAlias(qtype, owner, &RRSet{Domain: owner, Rtype: rtypeALIAS, TTL: al.TTL, Rdata: cn.Rdata}, depth+1)

		return
	}

	set := node[dns.TypeToString[qtype]]
	if set == nil {
		return
	}

	a.resp.Answer = append(a.resp.Answer, toRRs(owner, &RRSet{
		Domain: owner, Rtype: set.Rtype, TTL: al.TTL, Rdata: set.Rdata,
	})...)
}
