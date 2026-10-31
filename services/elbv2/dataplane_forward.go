package elbv2

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mrand "math/rand/v2"
)

const (
	dialTimeout          = 10 * time.Second
	targetTypeIP         = "ip"
	stickinessCookie     = "AWSALB"
	stickinessEnabled    = "stickiness.enabled"
	stickinessType       = "stickiness.type"
	stickinessLBCookie   = "lb_cookie"
	stickinessSourceIP   = "source_ip"
	stickinessLBDuration = "stickiness.lb_cookie.duration_seconds"
	traceHeader          = "X-Amzn-Trace-Id"
	traceRandomBytes     = 12
)

var hopByHop = []string{ //nolint:gochecknoglobals // static header set
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer",
	"Transfer-Encoding", "Upgrade",
}

// targetTLSConfig accepts any target certificate: ALBs do not validate target certs.
func targetTLSConfig() *tls.Config {
	return &tls.Config{
		//nolint:gosec // ALBs do not validate target certificates
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
	}
}

func newProxyClient() *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DisableCompression: true,
			TLSClientConfig:    targetTLSConfig(),
			DialContext:        (&net.Dialer{Timeout: dialTimeout}).DialContext,
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

func (h *Handler) serveForward(w http.ResponseWriter, req *dpRequest, a Action) {
	tgArn := pickWeightedGroup(forwardTuples(a))
	if tgArn == "" {
		writeDPError(w, http.StatusServiceUnavailable, msgNoTargets)

		return
	}

	groups, err := req.be.DescribeTargetGroups([]string{tgArn}, nil, "")
	if err != nil || len(groups) == 0 {
		writeDPError(w, http.StatusServiceUnavailable, msgNoTargets)

		return
	}

	tg := &groups[0]

	health, err := req.be.DescribeTargetHealth(tgArn)
	if err != nil {
		writeDPError(w, http.StatusServiceUnavailable, msgNoTargets)

		return
	}

	cands := candidateTargets(health)
	if len(cands) == 0 {
		writeDPError(w, http.StatusServiceUnavailable, msgNoTargets)

		return
	}

	t, sticky := h.dp.chooseTarget(tg, cands, req)

	if tg.TargetType == targetTypeLambda {
		h.serveLambda(w, req, tg, t)

		return
	}

	h.proxyTo(w, req, tg, t, sticky)
}

func forwardTuples(a Action) []TargetGroupTuple {
	if a.ForwardConfig != nil && len(a.ForwardConfig.TargetGroups) > 0 {
		return a.ForwardConfig.TargetGroups
	}

	if a.TargetGroupArn != "" {
		return []TargetGroupTuple{{TargetGroupArn: a.TargetGroupArn, Weight: 1}}
	}

	return nil
}

func pickWeightedGroup(tuples []TargetGroupTuple) string {
	total := 0

	for _, t := range tuples {
		total += int(t.Weight)
	}

	if len(tuples) == 0 {
		return ""
	}

	if total == 0 {
		return tuples[mrand.IntN(len(tuples))].TargetGroupArn //nolint:gosec // non-cryptographic pick
	}

	n := mrand.IntN(total) //nolint:gosec // non-cryptographic weighted routing pick

	for _, t := range tuples {
		n -= int(t.Weight)
		if n < 0 {
			return t.TargetGroupArn
		}
	}

	return tuples[len(tuples)-1].TargetGroupArn
}

// candidateTargets returns healthy targets, failing open to every non-draining target when none is healthy.
func candidateTargets(health []TargetHealthDescription) []Target {
	var healthy, open []Target

	for _, d := range health {
		if d.HealthReason == targetDrainingReason {
			continue
		}

		open = append(open, d.Target)

		if d.HealthState == healthStateHealthy {
			healthy = append(healthy, d.Target)
		}
	}

	if len(healthy) > 0 {
		return healthy
	}

	return open
}

func targetKeyOf(t Target) string { return targetHealthKey(t.ID, t.Port) }

func (d *dataPlane) chooseTarget(tg *TargetGroup, cands []Target, req *dpRequest) (Target, string) {
	slices.SortFunc(cands, func(a, b Target) int { return strings.Compare(targetKeyOf(a), targetKeyOf(b)) })

	attrs := tg.TargetGroupAttributes
	if attrs[stickinessEnabled] == attrValueTrue {
		switch attrs[stickinessType] {
		case stickinessSourceIP:
			hs := fnv.New32a()
			_, _ = hs.Write([]byte(req.client.String()))

			return cands[int(hs.Sum32()%uint32(len(cands)))], "" //nolint:gosec // len is small
		case stickinessLBCookie, "":
			if c, err := req.r.Cookie(stickinessCookie); err == nil {
				for _, t := range cands {
					if c.Value == stickyValue(tg.TargetGroupArn, t) {
						return t, ""
					}
				}
			}

			t := d.roundRobin(tg.TargetGroupArn, cands)

			return t, stickyValue(tg.TargetGroupArn, t)
		}
	}

	return d.roundRobin(tg.TargetGroupArn, cands), ""
}

func (d *dataPlane) roundRobin(tgArn string, cands []Target) Target {
	v, _ := d.rr.LoadOrStore(tgArn, new(atomic.Uint64))
	ctr, ok := v.(*atomic.Uint64)
	if !ok {
		return cands[0]
	}

	n := ctr.Add(1) - 1

	return cands[int(n%uint64(len(cands)))] //nolint:gosec // len is small
}

func stickyValue(tgArn string, t Target) string {
	return base64.RawURLEncoding.EncodeToString([]byte(tgArn + "|" + targetKeyOf(t)))
}

func (h *Handler) resolveAddr(tg *TargetGroup, t Target) (netip.AddrPort, bool) {
	port := t.Port
	if port == 0 {
		port = tg.Port
	}

	host := t.ID

	if tg.TargetType == targetTypeInstance {
		r := h.dp.instance.Load()
		if r == nil {
			return netip.AddrPort{}, false
		}

		ip, ok := (*r).InstanceAddress(t.ID)
		if !ok {
			return netip.AddrPort{}, false
		}

		host = ip
	} else if tg.TargetType != targetTypeIP {
		return netip.AddrPort{}, false
	}

	a, err := netip.ParseAddr(host)
	if err != nil || a.IsLinkLocalUnicast() || a.IsLinkLocalMulticast() || a.IsUnspecified() {
		return netip.AddrPort{}, false
	}

	return netip.AddrPortFrom(a.Unmap(), uint16(port)), true //nolint:gosec // validated port range
}

func (h *Handler) proxyTo(w http.ResponseWriter, req *dpRequest, tg *TargetGroup, t Target, sticky string) {
	ap, ok := h.resolveAddr(tg, t)
	if !ok {
		writeDPError(w, http.StatusServiceUnavailable, msgNoTargets)

		return
	}

	scheme := "http"
	if tg.Protocol == protoHTTPS {
		scheme = "https"
	}

	if isUpgradeRequest(req.r) {
		h.proxyUpgrade(w, req, ap, scheme, sticky, tg)

		return
	}

	target := scheme + "://" + ap.String() + req.rawPath
	if q := req.r.URL.RawQuery; q != "" {
		target += "?" + q
	}

	ctx, cancel := contextWithTimeout(req)
	defer cancel()

	out, err := http.NewRequestWithContext(ctx, req.r.Method, target, req.r.Body)
	if err != nil {
		writeDPError(w, http.StatusBadGateway, msgBadGateway)

		return
	}

	out.ContentLength = req.r.ContentLength
	copyHeaders(out.Header, req.r.Header)
	setForwardHeaders(out.Header, req)
	out.Host = req.host
	if req.host == hostWithoutPort(req.r.Host) {
		out.Host = req.r.Host
	}

	resp, err := h.dp.client.Do(out)
	if err != nil {
		writeDPError(w, http.StatusBadGateway, msgBadGateway)

		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)

	if sticky != "" {
		w.Header().Add("Set-Cookie", stickyCookie(tg, sticky))
	}

	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

func stickyCookie(tg *TargetGroup, v string) string {
	secs := 86400
	if n, err := strconv.Atoi(tg.TargetGroupAttributes[stickinessLBDuration]); err == nil && n > 0 {
		secs = n
	}

	return fmt.Sprintf("%s=%s; Max-Age=%d; Path=/", stickinessCookie, v, secs)
}

func copyHeaders(dst, src http.Header) {
	drop := map[string]bool{}

	for _, h := range hopByHop {
		drop[h] = true
	}

	for _, c := range src.Values("Connection") {
		for tok := range strings.SplitSeq(c, ",") {
			drop[http.CanonicalHeaderKey(strings.TrimSpace(tok))] = true
		}
	}

	for k, vs := range src {
		if drop[k] {
			continue
		}

		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

func setForwardHeaders(h http.Header, req *dpRequest) {
	applyXFF(h, req)
	h.Set("X-Forwarded-Proto", listenerScheme(req.listener))
	h.Set("X-Forwarded-Port", strconv.Itoa(int(req.listener.Port)))
	h.Set(traceHeader, nextTraceID(h.Get(traceHeader)))
}

func nextTraceID(existing string) string {
	var buf [traceRandomBytes]byte

	_, _ = rand.Read(buf[:])
	id := fmt.Sprintf("1-%08x-%s", time.Now().Unix(), hex.EncodeToString(buf[:]))

	if existing != "" {
		return existing + ";Self=" + id
	}

	return "Root=" + id
}

func contextWithTimeout(req *dpRequest) (context.Context, context.CancelFunc) {
	d := defaultIdleTimeout

	if n, err := strconv.Atoi(req.lb.Attributes[idleTimeoutAttr]); err == nil && n > 0 {
		d = time.Duration(n) * time.Second
	}

	return context.WithTimeout(req.r.Context(), d)
}
