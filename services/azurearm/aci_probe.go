package azurearm

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const (
	aciProbeKindLiveness  = "Liveness"
	aciProbeKindReadiness = "Readiness"
	aciEventUnhealthy     = "Unhealthy"
	aciEventKilling       = "Killing"
	aciKilledExit         = 137
)

// ACIHTTPProbe is one HTTP probe request the engine asks its prober to make.
type ACIHTTPProbe struct {
	Headers http.Header
	URL     string
	Timeout time.Duration
}

// ACIExecRuntime is implemented by runtimes that can run a command inside a container; exec probes need it.
type ACIExecRuntime interface {
	Exec(ctx context.Context, id string, cmd []string) (int, error)
}

// SetProbeHooks overrides how probes wait between attempts and issue HTTP requests; for tests. Nil keeps the
// default.
func (e *ACIEngine) SetProbeHooks(
	pause func(ctx context.Context, d time.Duration) bool,
	httpProbe func(ctx context.Context, p ACIHTTPProbe) (int, error),
) {
	e.mu.Lock("SetProbeHooks")
	defer e.mu.Unlock()

	if pause != nil {
		e.pause = pause
	}

	if httpProbe != nil {
		e.httpProbe = httpProbe
	}
}

func defaultACIHTTPProbe(ctx context.Context, p ACIHTTPProbe) (int, error) {
	tr := httpclient.NewTransport()
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} //nolint:gosec // probes ignore certificates, like kubelet

	client := &http.Client{Transport: tr, Timeout: p.Timeout}
	defer tr.CloseIdleConnections()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.URL, http.NoBody)
	if err != nil {
		return 0, fmt.Errorf("build probe request: %w", err)
	}

	req.Header = p.Headers

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("probe request: %w", err)
	}

	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))

	return resp.StatusCode, nil
}

func (e *ACIEngine) startProbes(
	ctx context.Context, run *aciRun, cfg aciConfig, wire aciContainerWire, c *aciCont, id string,
) {
	pr := wire.Properties

	if pr.LivenessProbe != nil {
		run.wg.Go(func() { e.probeLoop(ctx, run, cfg, c, id, pr.LivenessProbe, aciProbeKindLiveness) })
	}

	if pr.ReadinessProbe != nil {
		run.wg.Go(func() { e.probeLoop(ctx, run, cfg, c, id, pr.ReadinessProbe, aciProbeKindReadiness) })
	}
}

func (e *ACIEngine) probeLoop(
	ctx context.Context, run *aciRun, cfg aciConfig, c *aciCont, id string, p *aciProbeWire, kind string,
) {
	if p.InitialDelaySeconds > 0 && !e.pauseFor(ctx, time.Duration(p.InitialDelaySeconds)*time.Second) {
		return
	}

	okRun, failRun := 0, 0

	for {
		ok, detail := e.probeOnce(ctx, run, c, id, p)
		if ctx.Err() != nil {
			return
		}

		if ok {
			okRun, failRun = okRun+1, 0
		} else {
			okRun, failRun = 0, failRun+1
		}

		if e.applyProbe(ctx, run, cfg, c, id, p, kind, ok, detail, okRun, failRun) {
			return
		}

		if !e.pauseFor(ctx, time.Duration(p.PeriodSeconds)*time.Second) {
			return
		}
	}
}

func (e *ACIEngine) pauseFor(ctx context.Context, d time.Duration) bool {
	e.mu.RLock("pauseFor")
	pause := e.pause
	e.mu.RUnlock()

	return pause(ctx, d)
}

// applyProbe records one result and reports whether the probe loop should end (the container was killed).
func (e *ACIEngine) applyProbe(
	ctx context.Context, run *aciRun, cfg aciConfig, c *aciCont, id string, p *aciProbeWire,
	kind string, ok bool, detail string, okRun, failRun int,
) bool {
	e.mu.Lock("applyProbe")

	if !ok {
		e.record(&c.events, aciEventWarning, aciEventUnhealthy, kind+" probe failed: "+detail)
	}

	if kind == aciProbeKindReadiness {
		switch {
		case okRun >= p.SuccessThreshold:
			c.notReady = false
		case failRun >= p.FailureThreshold:
			c.notReady = true
		}

		e.mu.Unlock()

		return false
	}

	kill := failRun >= p.FailureThreshold && cfg.Props.RestartPolicy != aciRestartNever
	if kill {
		c.killed = true
		e.record(&c.events, aciEventNormal, aciEventKilling,
			fmt.Sprintf("container %s failed liveness probe, will be restarted", c.name))
	}
	e.mu.Unlock()

	if kill {
		killCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), aciTeardownTimeout)
		defer cancel()

		_ = run.rt.Remove(killCtx, id)
	}

	return kill
}

func (e *ACIEngine) probeOnce(ctx context.Context, run *aciRun, c *aciCont, id string, p *aciProbeWire) (bool, string) {
	timeout := time.Duration(p.TimeoutSeconds) * time.Second
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if p.Exec != nil {
		ex, ok := run.rt.(ACIExecRuntime)
		if !ok {
			return false, "the container runtime cannot run commands in a container"
		}

		code, err := ex.Exec(pctx, id, p.Exec.Command)
		switch {
		case err != nil:
			return false, err.Error()
		case code != 0:
			return false, fmt.Sprintf("command exited with code %d", code)
		}

		return true, ""
	}

	return e.probeHTTPGet(pctx, run, p, timeout)
}

func (e *ACIEngine) probeHTTPGet(ctx context.Context, run *aciRun, p *aciProbeWire, timeout time.Duration) (bool, string) {
	g := p.HTTPGet

	e.mu.RLock("probeHTTPGet")
	back, ok := run.backPorts[g.Port]
	do := e.httpProbe
	e.mu.RUnlock()

	if !ok {
		return false, fmt.Sprintf("port %d is not reachable", g.Port)
	}

	path := g.Path
	if path == "" || path[0] != '/' {
		path = "/" + path
	}

	hdr := http.Header{}
	for _, h := range g.HTTPHeaders {
		hdr.Add(h.Name, h.Value)
	}

	status, err := do(ctx, ACIHTTPProbe{
		URL:     g.Scheme + "://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(back)) + path,
		Headers: hdr, Timeout: timeout,
	})

	switch {
	case err != nil:
		return false, err.Error()
	case status < http.StatusOK || status >= http.StatusBadRequest:
		return false, fmt.Sprintf("HTTP probe failed with statuscode: %d", status)
	}

	return true, ""
}

func (e *ACIEngine) groupReady(run *aciRun) bool {
	e.mu.RLock("groupReady")
	defer e.mu.RUnlock()

	for _, c := range run.conts {
		if c.notReady {
			return false
		}
	}

	return true
}

// startGate listens on front and forwards to back while every readiness probe passes; otherwise it drops the
// connection.
func (e *ACIEngine) startGate(run *aciRun, front, back int) error {
	var lc net.ListenConfig

	ln, err := lc.Listen(run.ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(front)))
	if err != nil {
		return fmt.Errorf("listen on port %d: %w", front, err)
	}

	stop := context.AfterFunc(run.ctx, func() {
		_ = ln.Close()

		e.mu.Lock("gateClose")
		conns := run.gateConns
		run.gateConns = nil
		e.mu.Unlock()

		for _, c := range conns {
			_ = c.Close()
		}
	})
	_ = stop

	run.wg.Go(func() {
		for {
			conn, aerr := ln.Accept()
			if aerr != nil {
				return
			}

			run.wg.Go(func() { e.forward(run, conn, back) })
		}
	})

	return nil
}

func (e *ACIEngine) forward(run *aciRun, conn net.Conn, back int) {
	defer conn.Close()

	if !e.groupReady(run) {
		return
	}

	var d net.Dialer

	up, err := d.DialContext(run.ctx, "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(back)))
	if err != nil {
		return
	}
	defer up.Close()

	e.mu.Lock("forward")
	run.gateConns = append(run.gateConns, conn, up)
	e.mu.Unlock()

	done := make(chan struct{}, 2)

	go func() { _, _ = io.Copy(up, conn); done <- struct{}{} }()
	go func() { _, _ = io.Copy(conn, up); done <- struct{}{} }()

	<-done
}
