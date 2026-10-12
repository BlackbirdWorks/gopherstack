package route53resolver_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cwlogsbackend "github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
	route53backend "github.com/blackbirdworks/gopherstack/services/route53"
	"github.com/blackbirdworks/gopherstack/services/route53resolver"
)

const (
	testRegion  = "us-east-1"
	testAccount = "000000000000"
	vpc1        = "vpc-11111111"
	vpc2        = "vpc-22222222"
	vpc1Client  = "10.0.0.5"
	vpc2Client  = "10.0.0.6"
	localMarker = "198.51.100.1"
)

type siblings struct {
	r53  service.Registerable
	logs service.Registerable
}

func (s siblings) GetRoute53Handler() service.Registerable        { return s.r53 }
func (s siblings) GetCloudWatchLogsHandler() service.Registerable { return s.logs }

type dnsEnv struct {
	b     *route53resolver.InMemoryBackend
	r53   *route53backend.InMemoryBackend
	logs  *cwlogsbackend.InMemoryBackend
	res   *route53resolver.DNSResolver
	owned map[string]bool
}

func newDNSEnv(t *testing.T) *dnsEnv {
	t.Helper()

	e := &dnsEnv{
		b:     route53resolver.NewInMemoryBackend(testAccount, testRegion),
		r53:   route53backend.NewInMemoryBackend(),
		logs:  cwlogsbackend.NewInMemoryBackend(),
		owned: map[string]bool{},
	}

	e.b.SetAppConfig(siblings{r53: route53backend.NewHandler(e.r53), logs: cwlogsbackend.NewHandler(e.logs)})
	e.b.SetVPCLocator(func(src net.IP) (string, string, bool) {
		switch src.String() {
		case vpc1Client:
			return testRegion, vpc1, true
		case vpc2Client:
			return testRegion, vpc2, true
		default:
			return "", "", false
		}
	})
	e.res = e.b.NewDNSResolver(func(n string) bool { return e.owned[n] })

	return e
}

func localAnswer(req *dns.Msg) *dns.Msg {
	m := new(dns.Msg)
	m.SetReply(req)
	m.Answer = append(m.Answer, &dns.A{
		Hdr: dns.RR_Header{Name: req.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 5},
		A:   net.ParseIP(localMarker),
	})

	return m
}

func (e *dnsEnv) ask(src, name string, qtype uint16) *dns.Msg {
	req := new(dns.Msg)
	req.SetQuestion(dns.Fqdn(name), qtype)

	local := func() *dns.Msg { return localAnswer(req) }

	resp := e.res.Intercept(&net.UDPAddr{IP: net.ParseIP(src), Port: 40000}, req, local)
	if resp == nil {
		return local()
	}

	return resp
}

func answerIPs(m *dns.Msg) []string {
	var out []string

	for _, rr := range m.Answer {
		if a, ok := rr.(*dns.A); ok {
			out = append(out, a.A.String())
		}
	}

	return out
}

type upstream struct {
	ip   string
	port int32
	hits atomic.Int32
}

func startUpstream(t *testing.T, answerIP string) *upstream {
	t.Helper()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	u := &upstream{ip: answerIP, port: int32(pc.LocalAddr().(*net.UDPAddr).Port)}
	started := make(chan struct{})
	srv := &dns.Server{
		PacketConn:        pc,
		NotifyStartedFunc: func() { close(started) },
		Handler: dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
			u.hits.Add(1)

			m := new(dns.Msg)
			m.SetReply(r)
			m.Answer = append(m.Answer, &dns.A{
				Hdr: dns.RR_Header{Name: r.Question[0].Name, Rrtype: dns.TypeA, Class: dns.ClassINET, Ttl: 5},
				A:   net.ParseIP(answerIP),
			})
			_ = w.WriteMsg(m)
		}),
	}

	go func() { _ = srv.ActivateAndServe() }()

	<-started

	t.Cleanup(func() { _ = srv.Shutdown() })

	return u
}

func deadTarget(t *testing.T) route53resolver.TargetIP {
	t.Helper()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)

	port := pc.LocalAddr().(*net.UDPAddr).Port
	require.NoError(t, pc.Close())

	return route53resolver.TargetIP{IP: "127.0.0.1", Port: int32(port)}
}

func (u *upstream) target() route53resolver.TargetIP {
	return route53resolver.TargetIP{IP: "127.0.0.1", Port: u.port}
}

func (e *dnsEnv) rule(t *testing.T, domain, ruleType string, targets ...route53resolver.TargetIP) string {
	t.Helper()

	r, err := e.b.CreateResolverRule(
		context.Background(), "rule-"+domain, domain, ruleType, "", "", "", targets,
	)
	require.NoError(t, err)

	return r.ID
}

func (e *dnsEnv) associate(t *testing.T, ruleID string) {
	t.Helper()

	_, err := e.b.AssociateResolverRule(context.Background(), ruleID, vpc1, "assoc")
	require.NoError(t, err)
}
