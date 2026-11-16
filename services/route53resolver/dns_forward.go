package route53resolver

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"

	"github.com/miekg/dns"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const (
	dnsDefaultPort = 53
	dohDefaultPort = 443
	dohMaxResponse = 64 * 1024
	dohPath        = "/dns-query"
	dohMediaType   = "application/dns-message"
	protocolDo53   = "Do53"
	protocolDoH    = "DoH"
	protocolDoHFIP = "DoH-FIPS"
)

// forward sends req to the rule's targets in random order, failing over until
// one answers; SERVFAIL when none do. Do53 targets use UDP (TCP on truncation), DoH targets RFC 8484 POST.
func (r *DNSResolver) forward(rule *ResolverRule, req *dns.Msg) *dns.Msg {
	targets := make([]TargetIP, len(rule.TargetIps))
	copy(targets, rule.TargetIps)
	//nolint:gosec // non-cryptographic target ordering
	rand.Shuffle(len(targets), func(i, j int) { targets[i], targets[j] = targets[j], targets[i] })

	for _, t := range targets {
		addr, ok := targetAddr(t)
		if !ok {
			continue
		}

		var resp *dns.Msg
		if isDoH(t) {
			resp = r.exchangeDoH(addr, t.ServerNameIndication, req)
		} else {
			resp = r.exchange(addr, req)
		}

		if resp != nil {
			return resp
		}
	}

	return servfail(req)
}

func isDoH(t TargetIP) bool { return t.Protocol == protocolDoH || t.Protocol == protocolDoHFIP }

func targetAddr(t TargetIP) (string, bool) {
	if t.Protocol != "" && t.Protocol != protocolDo53 && !isDoH(t) {
		return "", false
	}

	host := t.IP
	if host == "" {
		host = t.Ipv6
	}

	if host == "" {
		return "", false
	}

	port := int(t.Port)

	if port == 0 {
		port = dnsDefaultPort
		if isDoH(t) {
			port = dohDefaultPort
		}
	}

	return net.JoinHostPort(host, strconv.Itoa(port)), true
}

func (r *DNSResolver) exchange(addr string, req *dns.Msg) *dns.Msg {
	out := req.Copy()
	out.RecursionDesired = true

	c := &dns.Client{Net: "udp", Timeout: r.timeout}

	resp, _, err := c.Exchange(out, addr)
	if err == nil && resp.Truncated {
		c.Net = "tcp"
		resp, _, err = c.Exchange(out, addr)
	}

	if err != nil || resp == nil {
		return nil
	}

	return resp
}

// exchangeDoH posts req to the DoH server at addr (RFC 8484). The certificate is verified against sni, or the
// target IP when none is set.
func (r *DNSResolver) exchangeDoH(addr, sni string, req *dns.Msg) *dns.Msg {
	out := req.Copy()
	out.RecursionDesired = true

	wire, err := out.Pack()
	if err != nil {
		return nil
	}

	host := sni
	if host == "" {
		host, _, _ = net.SplitHostPort(addr)
	}

	_, port, _ := net.SplitHostPort(addr)

	tr := httpclient.NewTransport()
	tr.DisableKeepAlives = true
	tr.Proxy = nil
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: r.dohRoots}
	tr.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{Timeout: r.timeout}).DialContext(ctx, network, addr)
	}

	defer tr.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	u := "https://" + net.JoinHostPort(host, port) + dohPath

	hreq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(wire))
	if err != nil {
		return nil
	}

	hreq.Header.Set("Content-Type", dohMediaType)
	hreq.Header.Set("Accept", dohMediaType)

	client := &http.Client{
		Transport:     tr,
		Timeout:       r.timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	resp, err := client.Do(hreq)
	if err != nil {
		return nil
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, dohMaxResponse))
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil
	}

	msg := new(dns.Msg)
	if msg.Unpack(body) != nil {
		return nil
	}

	msg.Id = req.Id

	return msg
}
