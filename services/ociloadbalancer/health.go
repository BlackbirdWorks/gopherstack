package ociloadbalancer

import (
	"context"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"regexp"
	"slices"
	"time"
)

// Health statuses and check results.
const (
	hOK        = "OK"
	hWarning   = "WARNING"
	hCritical  = "CRITICAL"
	hUnknown   = "UNKNOWN"
	rOK        = "OK"
	rStatus    = "INVALID_STATUS_CODE"
	rTimeout   = "TIMED_OUT"
	rRegex     = "REGEX_MISMATCH"
	rConnect   = "CONNECT_FAILED"
	rIO        = "IO_ERROR"
	rOffline   = "OFFLINE"
	rUnknown   = "UNKNOWN"
	maxProbeIO = 1 << 20
)

// hstate is the runtime health of one backend.
type hstate struct {
	lastTime   time.Time
	nextDue    time.Time
	status     string
	lastResult string
	okRun      int
	failRun    int
	claimed    bool
}

func newHState() *hstate { return &hstate{status: hUnknown, lastResult: rUnknown} }

func (s *hstate) record(result string, retries int, at time.Time) {
	s.lastResult, s.lastTime = result, at
	threshold := retries + 1

	if result == rOK {
		s.okRun, s.failRun = s.okRun+1, 0
		if s.status != hCritical || s.okRun >= threshold {
			s.status = hOK
		}

		return
	}

	s.failRun, s.okRun = s.failRun+1, 0
	if s.failRun >= threshold {
		s.status = hCritical
	} else if s.status != hCritical {
		s.status = hWarning
	}
}

type probeTarget struct {
	rt      http.RoundTripper
	dialer  func(ctx context.Context, network, addr string) (net.Conn, error)
	key     string
	be      Backend
	hc      HealthChecker
	retries int
	tlsOn   bool
}

func healthKey(lbID, set, backend string) string { return lbID + "|" + set + "|" + backend }

// RefreshHealth runs every health check of a backend set that is due on the backend clock.
func (b *InMemoryBackend) RefreshHealth(lbID, set string) { b.dp.runDueChecks(lbID, set) }

func (d *dataPlane) collectTargets(lbID, set string) []probeTarget {
	d.b.mu.RLock("collectTargets")
	defer d.b.mu.RUnlock()

	var out []probeTarget

	for id, lb := range d.b.lbs {
		if lb.State != stateActive || (lbID != "" && id != lbID) {
			continue
		}

		for name, bs := range lb.BackendSets {
			if set != "" && name != set {
				continue
			}

			rt := d.transportFor(lb, bs)

			for _, be := range bs.Backends {
				out = append(out, probeTarget{
					rt: rt, be: be, key: healthKey(id, name, be.Name), hc: bs.HealthChecker, tlsOn: bs.SSL != nil,
					retries: bs.HealthChecker.Retries, dialer: d.dialer.DialContext,
				})
			}
		}
	}

	return out
}

// runDueChecks probes the backends whose check interval has elapsed. Offline backends are not probed.
func (d *dataPlane) runDueChecks(lbID, set string) {
	targets := d.collectTargets(lbID, set)
	if len(targets) == 0 {
		return
	}

	now := d.b.clock()

	var run []probeTarget

	d.mu.Lock("runDueChecks")

	for _, t := range targets {
		st := d.health[t.key]
		if st == nil {
			st = newHState()
			d.health[t.key] = st
		}

		if t.be.Offline {
			st.status, st.lastResult, st.lastTime = hUnknown, rOffline, now

			continue
		}

		if st.claimed || now.Before(st.nextDue) {
			continue
		}

		st.claimed = true
		st.nextDue = now.Add(time.Duration(t.hc.IntervalInMillis) * time.Millisecond)
		run = append(run, t)
	}

	d.mu.Unlock()

	done := make(chan struct{}, len(run))

	for _, t := range run {
		go func() {
			res := probe(t)

			d.mu.Lock("recordHealth")
			if st := d.health[t.key]; st != nil {
				st.record(res, t.retries, now)
				st.claimed = false
			}
			d.mu.Unlock()

			done <- struct{}{}
		}()
	}

	for range run {
		<-done
	}
}

func classify(err error) string {
	var ne net.Error

	switch {
	case errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()):
		return rTimeout
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return rIO
	}

	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return rConnect
	}

	return rIO
}

func probe(t probeTarget) string {
	port := t.hc.Port
	if port == 0 {
		port = t.be.Port
	}

	addr := net.JoinHostPort(t.be.IPAddress, itoa(port))
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(t.hc.TimeoutInMillis)*time.Millisecond)

	defer cancel()

	if t.hc.Protocol == protoTCP {
		conn, err := t.dialer(ctx, "tcp", addr)
		if err != nil {
			return classify(err)
		}

		_ = conn.Close()

		return rOK
	}

	scheme := schemeHTTP
	if t.tlsOn && !t.hc.IsForcePlainText {
		scheme = schemeHTTPS
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, scheme+"://"+addr+t.hc.URLPath, http.NoBody)
	if err != nil {
		return rIO
	}

	resp, err := (&http.Client{
		Transport: t.rt, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}).Do(req)
	if err != nil {
		return classify(err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxProbeIO))
	if err != nil {
		return classify(err)
	}

	if resp.StatusCode != t.hc.ReturnCode {
		return rStatus
	}

	if t.hc.ResponseBodyRegex != "" {
		if re, rerr := regexp.Compile(t.hc.ResponseBodyRegex); rerr != nil || !re.Match(body) {
			return rRegex
		}
	}

	return rOK
}

func (d *dataPlane) statusOf(lbID, set string, be Backend) (string, string, time.Time) {
	if be.Offline {
		return hUnknown, rOffline, time.Time{}
	}

	d.mu.RLock("statusOf")
	defer d.mu.RUnlock()

	st := d.health[healthKey(lbID, set, be.Name)]
	if st == nil {
		return hUnknown, rUnknown, time.Time{}
	}

	return st.status, st.lastResult, st.lastTime
}

type healthCheckResultWire struct {
	Timestamp         time.Time `json:"timestamp"`
	SubnetID          string    `json:"subnetId"`
	SourceIPAddress   string    `json:"sourceIpAddress"`
	HealthCheckStatus string    `json:"healthCheckStatus"`
}

// BackendHealthWire is the wire form of a backend's health.
type BackendHealthWire struct {
	Status             string                  `json:"status"`
	HealthCheckResults []healthCheckResultWire `json:"healthCheckResults"`
}

// BackendSetHealthWire is the wire form of a backend set's health.
type BackendSetHealthWire struct {
	Status                    string   `json:"status"`
	WarningStateBackendNames  []string `json:"warningStateBackendNames"`
	CriticalStateBackendNames []string `json:"criticalStateBackendNames"`
	UnknownStateBackendNames  []string `json:"unknownStateBackendNames"`
	TotalBackendCount         int      `json:"totalBackendCount"`
}

// LoadBalancerHealthWire is the wire form of a load balancer's health.
type LoadBalancerHealthWire struct {
	Status                       string   `json:"status"`
	WarningStateBackendSetNames  []string `json:"warningStateBackendSetNames"`
	CriticalStateBackendSetNames []string `json:"criticalStateBackendSetNames"`
	UnknownStateBackendSetNames  []string `json:"unknownStateBackendSetNames"`
	TotalBackendSetCount         int      `json:"totalBackendSetCount"`
}

// aggregate combines member statuses: OK only when all are OK, CRITICAL when none is OK and one is critical.
func aggregate(ok, warn, crit, unknown int) string {
	total := ok + warn + crit + unknown

	switch {
	case total == 0 || unknown == total:
		return hUnknown
	case ok == total:
		return hOK
	case ok == 0 && warn == 0 && crit > 0:
		return hCritical
	case warn == 0 && crit == 0:
		return hUnknown
	default:
		return hWarning
	}
}

func (d *dataPlane) setHealth(lb *LoadBalancer, bs *BackendSet) BackendSetHealthWire {
	out := BackendSetHealthWire{
		WarningStateBackendNames:  []string{},
		CriticalStateBackendNames: []string{},
		UnknownStateBackendNames:  []string{},
		TotalBackendCount:         len(bs.Backends),
	}

	var ok int

	for _, be := range bs.Backends {
		st, _, _ := d.statusOf(lb.ID, bs.Name, be)

		switch st {
		case hOK:
			ok++
		case hWarning:
			out.WarningStateBackendNames = append(out.WarningStateBackendNames, be.Name)
		case hCritical:
			out.CriticalStateBackendNames = append(out.CriticalStateBackendNames, be.Name)
		default:
			out.UnknownStateBackendNames = append(out.UnknownStateBackendNames, be.Name)
		}
	}

	out.Status = aggregate(ok, len(out.WarningStateBackendNames), len(out.CriticalStateBackendNames),
		len(out.UnknownStateBackendNames))

	return out
}

func (d *dataPlane) lbHealth(lb *LoadBalancer) LoadBalancerHealthWire {
	out := LoadBalancerHealthWire{
		WarningStateBackendSetNames: []string{}, CriticalStateBackendSetNames: []string{},
		UnknownStateBackendSetNames: []string{}, TotalBackendSetCount: len(lb.BackendSets),
	}

	var ok int

	for _, name := range slices.Sorted(maps.Keys(lb.BackendSets)) {
		switch d.setHealth(lb, lb.BackendSets[name]).Status {
		case hOK:
			ok++
		case hWarning:
			out.WarningStateBackendSetNames = append(out.WarningStateBackendSetNames, name)
		case hCritical:
			out.CriticalStateBackendSetNames = append(out.CriticalStateBackendSetNames, name)
		default:
			out.UnknownStateBackendSetNames = append(out.UnknownStateBackendSetNames, name)
		}
	}

	out.Status = aggregate(ok, len(out.WarningStateBackendSetNames), len(out.CriticalStateBackendSetNames),
		len(out.UnknownStateBackendSetNames))

	return out
}

func healthOps() map[string]opFunc {
	return map[string]opFunc{
		opGetBEHealth: (*Handler).getBackendHealth, opGetBSHealth: (*Handler).getBackendSetHealth,
		opGetLBHealth: (*Handler).getLBHealth, opListHealths: (*Handler).listLBHealths,
	}
}

func (h *Handler) getBackendHealth(q *req) error {
	lbID, set := q.param("loadBalancerId"), q.param("backendSetName")
	h.Backend.RefreshHealth(lbID, set)

	return readSub(h, q, func(lb *LoadBalancer) (BackendHealthWire, *APIError) {
		bs, err := lookup(lb.BackendSets, set)
		if err != nil {
			return BackendHealthWire{}, err
		}

		i := findBackend(bs, q.param("backendName"))
		if i < 0 {
			return BackendHealthWire{}, errNotFound(q.param("backendName"))
		}

		st, res, at := h.Backend.dp.statusOf(lb.ID, set, bs.Backends[i])
		if at.IsZero() {
			at = h.Backend.nowUTC()
		}

		sub, ip := "", ""
		if len(lb.SubnetIDs) > 0 {
			sub = lb.SubnetIDs[0]
		}

		if len(lb.IPAddresses) > 0 {
			ip = lb.IPAddresses[0].IPAddress
		}

		return BackendHealthWire{Status: st, HealthCheckResults: []healthCheckResultWire{{
			Timestamp: at, SubnetID: sub, SourceIPAddress: ip, HealthCheckStatus: res,
		}}}, nil
	})
}

func (h *Handler) getBackendSetHealth(q *req) error {
	set := q.param("backendSetName")
	h.Backend.RefreshHealth(q.param("loadBalancerId"), set)

	return readSub(h, q, func(lb *LoadBalancer) (BackendSetHealthWire, *APIError) {
		bs, err := lookup(lb.BackendSets, set)
		if err != nil {
			return BackendSetHealthWire{}, err
		}

		return h.Backend.dp.setHealth(lb, bs), nil
	})
}

func (h *Handler) getLBHealth(q *req) error {
	h.Backend.RefreshHealth(q.param("loadBalancerId"), "")

	return readSub(h, q, func(lb *LoadBalancer) (LoadBalancerHealthWire, *APIError) {
		return h.Backend.dp.lbHealth(lb), nil
	})
}

type lbHealthSummary struct {
	LoadBalancerID string `json:"loadBalancerId"`
	Status         string `json:"status"`
}

func (h *Handler) listLBHealths(q *req) error {
	compartment := q.query("compartmentId")
	if compartment == "" {
		return errMissing("compartmentId is required")
	}

	b := h.Backend

	b.mu.RLock("ListLoadBalancerHealths")

	var lbs []*LoadBalancer

	for _, lb := range b.lbs {
		if lb.CompartmentID == compartment && lb.State != stateDeleted {
			lbs = append(lbs, lb)
		}
	}

	b.mu.RUnlock()

	for _, lb := range lbs {
		b.RefreshHealth(lb.ID, "")
	}

	o := q.listOpts()
	pg := paginate(lbs, ListOpts{Page: o.Page, Limit: o.Limit}, func(l *LoadBalancer) string { return l.DisplayName },
		func(l *LoadBalancer) (time.Time, int64) { return l.TimeCreated, 0 })

	out := make([]lbHealthSummary, 0, len(pg.Data))
	for _, lb := range pg.Data {
		out = append(out, lbHealthSummary{LoadBalancerID: lb.ID, Status: b.dp.lbHealth(lb).Status})
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, out, "")
}
