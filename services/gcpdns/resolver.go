package gcpdns

import (
	"context"
	"strings"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/google/uuid"
	"github.com/miekg/dns"
	"google.golang.org/genproto/googleapis/api/monitoredres"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	dnsapi "google.golang.org/api/dns/v1"
)

const (
	rdataMax      = 260
	queryLogID    = "dns.googleapis.com%2Fdns_queries"
	queryLogRes   = "dns_query"
	queryLogLoc   = "global"
	targetPrivate = "private-zone"
	targetPublic  = "external"
	targetPolicy  = "response-policy"
)

// LogSink receives query log entries; *gcplogging.InMemoryBackend satisfies it.
type LogSink interface {
	WriteLogEntries(ctx context.Context, req *loggingpb.WriteLogEntriesRequest) error
}

type queryLog struct {
	project, targetType, targetName, network string
	qname, qtype, protocol, sourceIP         string
	authoritative                            bool
}

// SetLogSink directs DNS query logs (policy enableLogging, zone cloudLoggingConfig) to sink; nil disables them.
func (b *InMemoryBackend) SetLogSink(sink LogSink) {
	if sink == nil {
		b.sink.Store(nil)

		return
	}

	b.sink.Store(&sink)
}

func zoneNetworks(z *dnsapi.ManagedZone) []string {
	if z.PrivateVisibilityConfig == nil {
		return nil
	}

	var out []string
	for _, n := range z.PrivateVisibilityConfig.Networks {
		out = append(out, n.NetworkUrl)
	}

	return out
}

func zoneAttached(z *dnsapi.ManagedZone) bool {
	cfg := z.PrivateVisibilityConfig

	return cfg != nil && len(cfg.Networks)+len(cfg.GkeClusters) > 0
}

func responseNetworks(p *dnsapi.ResponsePolicy) []string {
	out := make([]string, 0, len(p.Networks))
	for _, n := range p.Networks {
		out = append(out, n.NetworkUrl)
	}

	return out
}

// zoneFor picks the zone with the longest dnsName enclosing qname. Every query is treated as coming from each
// network a private zone is attached to; forwarding and peering zones hold no data and are skipped.
func (b *InMemoryBackend) zoneFor(qname string) *zoneRec {
	var best *zoneRec

	for _, rec := range b.zones {
		z := rec.Zone
		if !dns.IsSubDomain(z.DnsName, qname) || z.ForwardingConfig != nil || z.PeeringConfig != nil {
			continue
		}

		if z.Visibility == visPrivate && !zoneAttached(z) {
			continue
		}

		if best == nil || zoneBetter(rec, best) {
			best = rec
		}
	}

	return best
}

func zoneBetter(a, b *zoneRec) bool {
	la, lb := len(a.Zone.DnsName), len(b.Zone.DnsName)

	switch {
	case la != lb:
		return la > lb
	case (a.Zone.Visibility == visPrivate) != (b.Zone.Visibility == visPrivate):
		return a.Zone.Visibility == visPrivate
	}

	return a.Zone.CreationTime+key(a.Project, a.Zone.Name) < b.Zone.CreationTime+key(b.Project, b.Zone.Name)
}

type matchedPolicy struct {
	*responseRec
	project string
}

func ruleMatch(ruleName, qname string) int {
	rn := canon(ruleName)

	if rn == qname {
		return len(rn) + 1
	}

	if suffix, ok := strings.CutPrefix(rn, "*."); ok && strings.HasSuffix(qname, "."+suffix) {
		return len(rn)
	}

	return 0
}

// responsePolicyFor returns the best rule over the response policies attached to a network or cluster.
func (b *InMemoryBackend) responsePolicyFor(qname string) (matchedPolicy, *dnsapi.ResponsePolicyRule) {
	var (
		bestPol  matchedPolicy
		bestRule *dnsapi.ResponsePolicyRule
		bestLen  int
	)

	for k, rp := range b.respPols {
		if len(rp.Policy.Networks)+len(rp.Policy.GkeClusters) == 0 {
			continue
		}

		for _, rule := range rp.Rules {
			if n := ruleMatch(rule.DnsName, qname); n > bestLen {
				project, _, _ := strings.Cut(k, "/")
				bestPol, bestRule, bestLen = matchedPolicy{rp, project}, rule, n
			}
		}
	}

	return bestPol, bestRule
}

func answerLocalData(resp *dns.Msg, q dns.Question, qname string, rule *dnsapi.ResponsePolicyRule) {
	resp.Authoritative = true

	if rule.LocalData == nil {
		return
	}

	for _, rs := range rule.LocalData.LocalDatas {
		t := dns.StringToType[rs.Type]
		if t == q.Qtype || q.Qtype == dns.TypeANY {
			resp.Answer = append(resp.Answer, toRRs(qname, rs)...)
		}
	}

	if len(resp.Answer) > 0 {
		return
	}

	for _, rs := range rule.LocalData.LocalDatas {
		if rs.Type == typeCNAME {
			resp.Answer = append(resp.Answer, toRRs(qname, rs)...)
		}
	}
}

// loggingNetwork returns a network of nets that a logging-enabled policy of project is attached to.
func (b *InMemoryBackend) loggingNetwork(project string, nets []string) string {
	for k, p := range b.policies {
		if !p.EnableLogging || !strings.HasPrefix(k, project+"/") {
			continue
		}

		for _, n := range p.Networks {
			for _, want := range nets {
				if n.NetworkUrl == want {
					return want
				}
			}
		}
	}

	return ""
}

func (b *InMemoryBackend) newQueryLog(q dns.Question, name, kind string, nets []string) *queryLog {
	for k, rp := range b.respPols {
		if rp.Policy.ResponsePolicyName != name {
			continue
		}

		project, _, _ := strings.Cut(k, "/")
		if n := b.loggingNetwork(project, nets); n != "" {
			return &queryLog{
				project: project, targetType: kind, targetName: name, network: n,
				qname: q.Name, qtype: dns.TypeToString[q.Qtype], authoritative: true,
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) zoneQueryLog(q dns.Question, rec *zoneRec) *queryLog {
	z := rec.Zone
	ql := &queryLog{
		project: rec.Project, targetName: z.Name, qname: q.Name, qtype: dns.TypeToString[q.Qtype], authoritative: true,
	}

	if z.Visibility == visPrivate {
		n := b.loggingNetwork(rec.Project, zoneNetworks(z))
		if n == "" {
			return nil
		}

		ql.targetType, ql.network = targetPrivate, n

		return ql
	}

	if z.CloudLoggingConfig == nil || !z.CloudLoggingConfig.EnableLogging {
		return nil
	}

	ql.targetType = targetPublic

	return ql
}

func (b *InMemoryBackend) emitQueryLog(ql *queryLog, resp *dns.Msg) {
	sink := b.sink.Load()
	if ql == nil || sink == nil {
		return
	}

	rdata := make([]string, 0, len(resp.Answer))
	for _, rr := range resp.Answer {
		rdata = append(rdata, strings.Join(strings.Fields(rr.String()), " "))
	}

	rd := strings.Join(rdata, ", ")
	if len(rd) > rdataMax {
		rd = rd[:rdataMax]
	}

	sourceType := "gce-vm"
	if ql.targetType == targetPublic {
		sourceType = "internet"
	}

	payload, err := structpb.NewStruct(map[string]any{
		"queryName": ql.qname, "queryType": ql.qtype, "responseCode": dns.RcodeToString[resp.Rcode],
		"rdata": rd, "protocol": ql.protocol, "sourceIP": ql.sourceIP, "authAnswer": resp.Authoritative,
		"sourceNetwork": ql.network, "source_type": sourceType, "target_type": ql.targetType,
		"target_name": ql.targetName, "project_id": ql.project, "location": queryLogLoc,
	})
	if err != nil {
		return
	}

	entry := &loggingpb.LogEntry{
		LogName: "projects/" + ql.project + "/logs/" + queryLogID,
		Resource: &monitoredres.MonitoredResource{Type: queryLogRes, Labels: map[string]string{
			"project_id":  ql.project,
			"location":    queryLogLoc,
			"target_type": ql.targetType,
			"target_name": ql.targetName,
		}},
		Timestamp: timestamppb.New(b.now()),
		InsertId:  strings.ReplaceAll(uuid.NewString(), "-", "")[:16],
		Payload:   &loggingpb.LogEntry_JsonPayload{JsonPayload: payload},
	}

	_ = (*sink).WriteLogEntries(
		context.Background(),
		&loggingpb.WriteLogEntriesRequest{Entries: []*loggingpb.LogEntry{entry}},
	)
}
