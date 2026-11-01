package ocicontainerinstances

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const tcpSettle = 250 * time.Millisecond

var errBadStatus = errors.New("unexpected HTTP status")

// probeHealth dials the container's loopback-published port; HTTP checks accept 200-399 and never use TLS.
func probeHealth(ctx context.Context, hc HealthCheck, hostPort int) error {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(hc.Timeout)*time.Second)
	defer cancel()

	addr := net.JoinHostPort(container.LoopbackHost, strconv.Itoa(hostPort))

	if hc.Type == hcTCP {
		var d net.Dialer

		conn, err := d.DialContext(ctx, "tcp", addr)
		if err != nil {
			return fmt.Errorf("tcp connect: %w", err)
		}

		defer conn.Close()

		return tcpStillOpen(conn)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+hc.Path, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	for _, h := range hc.Headers {
		if http.CanonicalHeaderKey(h.Name) == "Host" {
			req.Host = h.Value

			continue
		}

		req.Header.Set(h.Name, h.Value)
	}

	tr := httpclient.NewTransport()
	tr.DisableKeepAlives = true

	defer tr.CloseIdleConnections()

	client := &http.Client{
		Transport:     tr,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}

	_ = resp.Body.Close()

	if resp.StatusCode < httpOKMin || resp.StatusCode > httpOKMax {
		return fmt.Errorf("%w: %d", errBadStatus, resp.StatusCode)
	}

	return nil
}

// watchHealth starts one probe loop per health check and returns a function that stops them and waits.
func (e *Engine) watchHealth(r *run, c *Container) func() {
	if len(c.Health) == 0 {
		return func() {}
	}

	ctx, cancel := context.WithCancel(r.ctx)
	wg := &sync.WaitGroup{}

	for i, h := range c.Health {
		hp := r.hostPorts[c.ID+":"+strconv.Itoa(h.Port)]

		wg.Go(func() { e.probeLoop(ctx, r, c, i, *h, hp) })
	}

	return func() {
		cancel()
		wg.Wait()
	}
}

func (e *Engine) wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}

	e.mu.RLock("wait")
	timer := e.timer
	e.mu.RUnlock()

	ch, stop := timer(d)
	defer stop()

	select {
	case <-ch:
		return true
	case <-ctx.Done():
		return false
	}
}

func (e *Engine) probeLoop(ctx context.Context, r *run, c *Container, idx int, hc HealthCheck, hostPort int) {
	e.mu.RLock("probeLoop")
	probe := e.probe
	e.mu.RUnlock()

	if !e.wait(ctx, time.Duration(hc.InitialDelay)*time.Second) {
		return
	}

	var good, bad int

	for {
		err := probe(ctx, hc, hostPort)
		if ctx.Err() != nil {
			return
		}

		status, details := "", ""

		if err == nil {
			good, bad = good+1, 0
			if good >= hc.SuccessThresh {
				status = healthHealthy
			}
		} else {
			good, bad = 0, bad+1
			if bad >= hc.FailureThresh {
				status, details = healthUnhealty, err.Error()
			}
		}

		e.recordHealth(r, c, idx, status, details)

		if status == healthUnhealty && hc.FailureAction == actionKill {
			e.kill(r, c)

			return
		}

		if !e.wait(ctx, time.Duration(hc.Interval)*time.Second) {
			return
		}
	}
}

func (e *Engine) recordHealth(r *run, c *Container, idx int, status, details string) {
	now := e.sink.Now()

	e.sink.Mutate(r.inst.ID, func(in *Instance) {
		cc := findCtr(in, c.ID)
		if cc == nil || idx >= len(cc.Health) {
			return
		}

		h := cc.Health[idx]
		h.LastChecked = &now

		if status != "" {
			h.Status, h.StatusDetails = status, details
		}
	})
}

// kill stops the container after a failed health check; supervise then applies the restart policy.
func (e *Engine) kill(r *run, c *Container) {
	e.mu.Lock("kill")
	id := r.docker[c.ID]
	r.killed[c.ID] = true
	e.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.ctx), teardownTimeout)
	defer cancel()

	if err := r.rt.Kill(ctx, id); err != nil {
		logger.Load(ctx).WarnContext(ctx, "ocicontainerinstances: killing unhealthy container failed",
			"container", id, "error", err)
	}
}

// tcpStillOpen tells a listening container from docker's userland proxy, which accepts the connection on a
// published port even when nothing listens inside and then closes it at once.
func tcpStillOpen(conn net.Conn) error {
	_ = conn.SetReadDeadline(time.Now().Add(tcpSettle))

	var one [1]byte

	_, err := conn.Read(one[:])

	var ne net.Error
	if err == nil || (errors.As(err, &ne) && ne.Timeout()) {
		return nil
	}

	return fmt.Errorf("connection closed straight after connect: %w", err)
}
