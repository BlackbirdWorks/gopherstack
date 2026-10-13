package route53resolver

import (
	"fmt"
	"strings"

	"github.com/miekg/dns"

	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
)

const (
	zoneListPageSize = 100
	zoneListMaxItems = 1000
	defaultZoneTTL   = 60
)

// bestPrivateZone returns the most specific private zone associated with the
// VPC that contains name.
func (b *InMemoryBackend) bestPrivateZone(
	r53 route53backend.StorageBackend, vpcID, region, name string,
) (*route53backend.HostedZone, int) {
	if r53 == nil {
		return nil, -1
	}

	pg, err := r53.ListHostedZonesByVPC(vpcID, region, "", zoneListMaxItems)
	if err != nil {
		return nil, -1
	}

	var (
		best     *route53backend.HostedZone
		bestSpec = -1
	)

	for i := range pg.Data {
		spec := domainSpecificity(pg.Data[i].Name, name)
		if spec > bestSpec {
			best, bestSpec = &pg.Data[i], spec
		}
	}

	return best, bestSpec
}

// zoneAnswer answers req authoritatively from a private zone: records when the
// name has the type, NODATA when it has other types, NXDOMAIN otherwise.
func (r *DNSResolver) zoneAnswer(zone *route53backend.HostedZone, req *dns.Msg, name string) *dns.Msg {
	r53 := r.b.route53Backend()
	q := req.Question[0]

	m := new(dns.Msg)
	m.SetReply(req)
	m.Authoritative = true
	m.RecursionAvailable = true

	sets := zoneRecordSets(r53, zone.ID, name)
	if len(sets) == 0 {
		m.Rcode = dns.RcodeNameError

		return m
	}

	want := dns.TypeToString[q.Qtype]

	for _, rs := range sets {
		if rs.Type != want {
			continue
		}

		m.Answer = append(m.Answer, recordsFor(r53, zone.ID, q, rs)...)
	}

	if len(m.Answer) == 0 && q.Qtype != dns.TypeCNAME {
		for _, rs := range sets {
			if rs.Type == "CNAME" {
				m.Answer = append(m.Answer, recordsFor(r53, zone.ID, q, rs)...)
			}
		}
	}

	return m
}

func zoneRecordSets(r53 route53backend.StorageBackend, zoneID, name string) []route53backend.ResourceRecordSet {
	if r53 == nil {
		return nil
	}

	var (
		out                           []route53backend.ResourceRecordSet
		startName, startType, startID string
	)

	for {
		pg, err := r53.ListResourceRecordSets(zoneID, startName, startType, startID, zoneListPageSize)
		if err != nil {
			return out
		}

		for _, rs := range pg.Records {
			if normDomain(rs.Name) == name {
				out = append(out, rs)
			}
		}

		if !pg.IsTruncated {
			return out
		}

		startName, startType, startID = pg.NextName, pg.NextType, pg.NextIdentifier
	}
}

func recordsFor(
	r53 route53backend.StorageBackend, zoneID string, q dns.Question, rs route53backend.ResourceRecordSet,
) []dns.RR {
	values, err := r53.TestDNSAnswer(zoneID, rs.Name, rs.Type, route53backend.DNSQueryContext{})
	if err != nil {
		return nil
	}

	ttl := rs.TTL
	if ttl <= 0 {
		ttl = defaultZoneTTL
	}

	out := make([]dns.RR, 0, len(values))

	for _, v := range values {
		rr, perr := dns.NewRR(fmt.Sprintf("%s %d IN %s %s", q.Name, ttl, rs.Type, strings.TrimSpace(v)))
		if perr == nil {
			out = append(out, rr)
		}
	}

	return out
}
