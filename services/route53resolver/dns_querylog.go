package route53resolver

import (
	"context"
	"encoding/json"
	"net"
	"strings"
	"time"

	"github.com/miekg/dns"

	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

const (
	logGroupArnMarker = ":log-group:"
	queryLogVersion   = "1.100000"
)

type queryLogEntry struct {
	src    net.Addr
	req    *dns.Msg
	resp   *dns.Msg
	fw     *firewallHit
	region string
	vpcID  string
}

type queryLogAnswer struct {
	Rdata string `json:"Rdata"`
	Type  string `json:"Type"`
	Class string `json:"Class"`
}

type queryLogRecord struct {
	FirewallRuleAction  string           `json:"firewall_rule_action,omitempty"`
	FirewallRuleGroupID string           `json:"firewall_rule_group_id,omitempty"`
	FirewallDomainList  string           `json:"firewall_domain_list_id,omitempty"`
	Version             string           `json:"version"`
	AccountID           string           `json:"account_id"`
	Region              string           `json:"region"`
	VPCID               string           `json:"vpc_id"`
	QueryTimestamp      string           `json:"query_timestamp"`
	QueryName           string           `json:"query_name"`
	QueryType           string           `json:"query_type"`
	QueryClass          string           `json:"query_class"`
	Rcode               string           `json:"rcode"`
	SrcAddr             string           `json:"srcaddr"`
	SrcPort             string           `json:"srcport"`
	Transport           string           `json:"transport"`
	Answers             []queryLogAnswer `json:"answers"`
}

// logQuery writes one record to the CloudWatch Logs, S3 or Firehose destination of every active
// query log config associated with the VPC.
func (r *DNSResolver) logQuery(ctx context.Context, e queryLogEntry) {
	var payload string

	for _, a := range r.b.ListResolverQueryLogConfigAssociations(ctx) {
		if a.ResourceID != e.vpcID || a.Status != statusActive {
			continue
		}

		cfg, err := r.b.GetResolverQueryLogConfig(ctx, a.ResolverQueryLogConfigID)
		if err != nil {
			continue
		}

		if payload == "" {
			payload = r.b.queryLogJSON(e)
		}

		r.deliverQueryLog(ctx, e, cfg, payload)
	}
}

func (r *DNSResolver) deliverQueryLog(
	ctx context.Context,
	e queryLogEntry,
	cfg *ResolverQueryLogConfig,
	payload string,
) {
	switch queryLogDestinationKind(cfg.DestinationARN) {
	case "CloudWatchLogs":
		group, ok := logGroupFromARN(cfg.DestinationARN)
		if logs := r.b.logsBackend(); ok && logs != nil {
			putQueryLog(ctx, logs, group, e.vpcID, payload)
		}
	case "S3":
		r.deliverToS3(ctx, e, cfg, payload)
	case "KinesisFirehose":
		r.deliverToFirehose(ctx, e, cfg, payload)
	}
}

func putQueryLog(ctx context.Context, logs cwlogsbackend.StorageBackend, group, stream, payload string) {
	wctx := cwlogsbackend.WithRegion(ctx, regionOf(ctx))
	_, _ = logs.CreateLogStream(wctx, group, stream)
	_, _ = logs.PutLogEvents(wctx, group, stream, "", []cwlogsbackend.InputLogEvent{
		{Message: payload, Timestamp: time.Now().UnixMilli()},
	})
}

func regionOf(ctx context.Context) string {
	r, _ := ctx.Value(regionContextKey{}).(string)

	return r
}

func logGroupFromARN(arn string) (string, bool) {
	_, rest, ok := strings.Cut(arn, logGroupArnMarker)
	if !ok || !strings.HasPrefix(arn, "arn:") {
		return "", false
	}

	rest = strings.TrimSuffix(rest, ":*")

	return rest, rest != ""
}

func (b *InMemoryBackend) queryLogJSON(e queryLogEntry) string {
	q := e.req.Question[0]
	host, port, _ := net.SplitHostPort(e.src.String())

	rec := queryLogRecord{
		Version:        queryLogVersion,
		AccountID:      b.accountID,
		Region:         e.region,
		VPCID:          e.vpcID,
		QueryTimestamp: time.Now().UTC().Format(time.RFC3339),
		QueryName:      q.Name,
		QueryType:      dns.TypeToString[q.Qtype],
		QueryClass:     dns.ClassToString[q.Qclass],
		Rcode:          dns.RcodeToString[e.resp.Rcode],
		SrcAddr:        host,
		SrcPort:        port,
		Transport:      strings.ToUpper(e.src.Network()),
		Answers:        make([]queryLogAnswer, 0, len(e.resp.Answer)),
	}

	if e.fw != nil {
		rec.FirewallRuleAction = e.fw.action
		rec.FirewallRuleGroupID = e.fw.ruleGroupID
		rec.FirewallDomainList = e.fw.domainListID
	}

	for _, rr := range e.resp.Answer {
		rec.Answers = append(rec.Answers, queryLogAnswer{
			Rdata: rdata(rr),
			Type:  dns.TypeToString[rr.Header().Rrtype],
			Class: dns.ClassToString[rr.Header().Class],
		})
	}

	out, _ := json.Marshal(rec)

	return string(out)
}

func rdata(rr dns.RR) string {
	return strings.TrimSpace(strings.TrimPrefix(rr.String(), rr.Header().String()))
}
