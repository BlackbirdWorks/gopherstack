package gcpcompute

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sync"
	"time"

	computeapi "google.golang.org/api/compute/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const (
	// HealthEndpointKey is the instance metadata key that points autohealing probes at a real
	// emulator-run endpoint (host:port, loopback IP literal only), e.g. a container's published port.
	HealthEndpointKey = "gopherstack-health-endpoint"

	healthTimeout   = "TIMEOUT"
	actVerifying    = "VERIFYING"
	repairAction    = "REPAIR"
	noAction        = "DO_NOTHING"
)

// HealthProbe is one health check probe against an instance endpoint.
type HealthProbe struct {
	Type    string
	Address string
	Host    string
	Path    string
	Timeout time.Duration
}

// HealthProber probes an instance endpoint and returns HEALTHY, UNHEALTHY (reachable but not conforming) or
// TIMEOUT (unreachable).
type HealthProber interface {
	Probe(ctx context.Context, p HealthProbe) string
}

type netProber struct {
	http  *http.Client
	https *http.Client
}

func newNetProber() *netProber {
	tr := httpclient.NewTransport()
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true} //nolint:gosec // loopback probes only

	return &netProber{http: httpclient.New(0), https: &http.Client{Transport: tr}}
}

func (p *netProber) Probe(ctx context.Context, hp HealthProbe) string {
	ctx, cancel := context.WithTimeout(ctx, hp.Timeout)
	defer cancel()

	switch hp.Type {
	case protoHTTP, protoHTTPS, protoHTTP2:
		return p.probeHTTP(ctx, hp)
	case "SSL":
		d := tls.Dialer{Config: &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: true}} //nolint:gosec // loopback probes only

		return dialResult(d.DialContext(ctx, "tcp", hp.Address))
	}

	var d net.Dialer

	return dialResult(d.DialContext(ctx, "tcp", hp.Address))
}

func dialResult(c net.Conn, err error) string {
	if err != nil {
		return healthTimeout
	}

	_ = c.Close()

	return healthHealthy
}

func (p *netProber) probeHTTP(ctx context.Context, hp HealthProbe) string {
	scheme, cl := "http", p.http
	if hp.Type == protoHTTPS || hp.Type == protoHTTP2 {
		scheme, cl = "https", p.https
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, scheme+"://"+hp.Address+hp.Path, nil)
	if err != nil {
		return healthUnhealthy
	}

	if hp.Host != "" {
		req.Host = hp.Host
	}

	resp, err := cl.Do(req)
	if err != nil {
		return healthTimeout
	}

	_ = resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return healthHealthy
	}

	return healthUnhealthy
}

// SetHealthProber replaces the autohealing prober; tests inject a fake here.
func (b *InMemoryBackend) SetHealthProber(p HealthProber) {
	b.mu.Lock("SetHealthProber")
	defer b.mu.Unlock()

	b.prober = p
}

type probeJob struct {
	rec    *migRec
	member *memberRec
	probe  HealthProbe
	instID uint64
}

// TickGroups advances clock-driven group work: finishes deletions, steps rolling updates and runs the due
// autohealing probes (outside the lock), repairing members whose health check keeps failing.
func (b *InMemoryBackend) TickGroups(ctx context.Context) {
	jobs, prober := b.collectGroupWork()
	if len(jobs) == 0 {
		return
	}

	res := make([]string, len(jobs))

	var wg sync.WaitGroup

	for i := range jobs {
		wg.Add(1)

		go func() {
			defer wg.Done()

			res[i] = prober.Probe(ctx, jobs[i].probe)
		}()
	}

	wg.Wait()

	b.mu.Lock("ApplyProbeResults")
	defer b.mu.Unlock()

	for i, j := range jobs {
		b.applyProbeLocked(j, res[i])
	}
}

func (b *InMemoryBackend) collectGroupWork() ([]probeJob, HealthProber) {
	b.mu.Lock("TickGroups")
	defer b.mu.Unlock()

	if len(b.migs) == 0 {
		return nil, b.prober
	}

	b.sweepGroupsLocked()

	var jobs []probeJob

	for _, rec := range b.migs {
		jobs = append(jobs, b.healthJobsLocked(rec)...)
	}

	return jobs, b.prober
}

func (b *InMemoryBackend) healthCheckByLinkLocked(link string) *computeapi.HealthCheck {
	for _, h := range b.healths {
		if h.SelfLink == link {
			return h
		}
	}

	return nil
}

func runningAt(r *instRec, t Timing) time.Time {
	if r.Phase == phaseStart {
		return r.Since.Add(t.Stage)
	}

	return r.Since
}

func (b *InMemoryBackend) healthJobsLocked(rec *migRec) []probeJob {
	if len(rec.Mgr.AutoHealingPolicies) == 0 {
		return nil
	}

	hc := b.healthCheckByLinkLocked(rec.Mgr.AutoHealingPolicies[0].HealthCheck)
	if hc == nil {
		return nil
	}

	now := b.now().UTC()
	interval := time.Duration(hc.CheckIntervalSec) * time.Second

	var jobs []probeJob

	for _, m := range liveMembers(rec) {
		r, ok := b.instances[key(rec.Project, m.Zone, m.Name)]
		if !ok || !instRunning(r, now, b.timing) {
			continue
		}

		if m.InstanceID != r.Inst.Id {
			m.resetHealth()
			m.InstanceID = r.Inst.Id
		}

		if !m.LastProbe.IsZero() && now.Sub(m.LastProbe) < interval {
			continue
		}

		m.LastProbe = now

		probe, real := probeFor(hc, r)
		if !real {
			m.Health, m.Verified = healthHealthy, true

			continue
		}

		jobs = append(jobs, probeJob{rec: rec, member: m, probe: probe, instID: r.Inst.Id})
	}

	return jobs
}

func (m *memberRec) resetHealth() {
	m.Health, m.HealthyRun, m.UnhealthyRun, m.Verified = "", 0, 0, false
	m.LastProbe = time.Time{}
}

func probeFor(hc *computeapi.HealthCheck, r *instRec) (HealthProbe, bool) {
	var ep string

	if md := r.Inst.Metadata; md != nil {
		for _, it := range md.Items {
			if it.Key == HealthEndpointKey && it.Value != nil {
				ep = *it.Value
			}
		}
	}

	host, _, err := net.SplitHostPort(ep)
	if err != nil || !httpclient.IsLoopbackHost(host) {
		return HealthProbe{}, false
	}

	specs := hcSpecs(hc)
	if len(specs) == 0 {
		return HealthProbe{}, false
	}

	sp := specs[0]
	p := HealthProbe{Type: sp.typ, Address: ep, Path: "/", Timeout: time.Duration(hc.TimeoutSec) * time.Second}

	if sp.path != nil && *sp.path != "" {
		p.Path = *sp.path
	}

	switch {
	case hc.HttpHealthCheck != nil:
		p.Host = hc.HttpHealthCheck.Host
	case hc.HttpsHealthCheck != nil:
		p.Host = hc.HttpsHealthCheck.Host
	case hc.Http2HealthCheck != nil:
		p.Host = hc.Http2HealthCheck.Host
	}


	return p, true
}

func failureAction(m *computeapi.InstanceGroupManager) string {
	lp := m.InstanceLifecyclePolicy
	if lp == nil {
		return repairAction
	}

	switch lp.OnFailedHealthCheck {
	case repairAction, noAction:
		return lp.OnFailedHealthCheck
	}

	if lp.DefaultActionOnFailure == noAction {
		return noAction
	}

	return repairAction
}

func (b *InMemoryBackend) applyProbeLocked(j probeJob, result string) {
	rec, m := j.rec, j.member
	if cur, ok := b.migs[key(rec.Project, migLoc(rec), rec.Mgr.Name)]; !ok || cur != rec || m.Deleting {
		return
	}

	r, ok := b.instances[key(rec.Project, m.Zone, m.Name)]
	if !ok || r.Inst.Id != j.instID || len(rec.Mgr.AutoHealingPolicies) == 0 {
		return
	}

	hc := b.healthCheckByLinkLocked(rec.Mgr.AutoHealingPolicies[0].HealthCheck)
	if hc == nil {
		return
	}

	if result == healthHealthy {
		m.HealthyRun, m.UnhealthyRun = m.HealthyRun+1, 0

		switch {
		case m.HealthyRun >= int(hc.HealthyThreshold):
			m.Health, m.Verified = healthHealthy, true
		case m.Health == "":
			m.Health = healthUnknown
		}

		return
	}

	now := b.now().UTC()
	delay := time.Duration(rec.Mgr.AutoHealingPolicies[0].InitialDelaySec) * time.Second

	if !m.Verified && now.Before(runningAt(r, b.timing).Add(delay)) {
		m.UnhealthyRun = 0

		return
	}

	m.UnhealthyRun, m.HealthyRun = m.UnhealthyRun+1, 0
	if m.UnhealthyRun < int(hc.UnhealthyThreshold) {
		return
	}

	m.Health = result
	if failureAction(rec.Mgr) != repairAction {
		return
	}

	if err := b.recreateMemberLocked(rec.Project, rec, m); err == nil {
		m.resetHealth()
	}
}

func migLoc(rec *migRec) string {
	_, loc := migScopeOf(rec.Mgr)

	return loc
}

func (b *InMemoryBackend) healthStateLocked(rec *migRec, m *memberRec) []*computeapi.ManagedInstanceInstanceHealth {
	if len(rec.Mgr.AutoHealingPolicies) == 0 {
		return nil
	}

	st := m.Health
	if st == "" {
		st = healthUnknown
	}

	return []*computeapi.ManagedInstanceInstanceHealth{{
		DetailedHealthState: st, HealthCheck: rec.Mgr.AutoHealingPolicies[0].HealthCheck,
	}}
}

func needsVerify(rec *migRec, m *memberRec) bool {
	return len(rec.Mgr.AutoHealingPolicies) > 0 && !m.Verified
}
