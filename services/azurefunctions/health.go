package azurefunctions

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	healthPollInterval = 250 * time.Millisecond
	healthProbeTimeout = 10 * time.Second
	healthUserAgent    = "HealthCheck/1.0"
	// Learn: App Service "Monitor the health of App Service instances": pings every minute, replaced after an
	// hour of continuous failures; a single instance is never taken out of the load balancer.
	healthPingInterval = time.Minute
	healthReplaceAfter = time.Hour
)

// waitHealthy polls the app's health check path until it answers 2xx or ctx ends; an empty path needs no check.
func waitHealthy(ctx context.Context, port int, path string) error {
	if path == "" {
		return nil
	}

	url := "http://" + net.JoinHostPort(container.LoopbackHost, strconv.Itoa(port)) + path

	tr := httpclient.NewTransport()
	tr.DisableKeepAlives = true

	defer tr.CloseIdleConnections()

	client := &http.Client{Transport: tr, Timeout: healthProbeTimeout}
	tick := time.NewTicker(healthPollInterval)

	defer tick.Stop()

	last := 0

	for {
		if code, ok := probeHealth(ctx, client, url); ok {
			return nil
		} else if code != 0 {
			last = code
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("health check %s never answered 2xx (last status %d): %w", path, last, ctx.Err())
		case <-tick.C:
		}
	}
}

func probeHealth(ctx context.Context, c *http.Client, url string) (int, bool) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, false
	}

	req.Header.Set("User-Agent", healthUserAgent)

	resp, err := c.Do(req)
	if err != nil {
		return 0, false
	}

	_ = resp.Body.Close()

	return resp.StatusCode, resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices
}

// SetHealthTiming sets how often a running web app's health check path is pinged and how long continuous
// failures last before the instance is replaced.
func (b *InMemoryBackend) SetHealthTiming(every, replaceAfter time.Duration) {
	b.mu.Lock("SetHealthTiming")
	defer b.mu.Unlock()

	b.healthEvery, b.healthReplace = every, replaceAfter
}

// watchHealthLocked starts pinging a running web app's healthCheckPath; it ends when the instance is taken down.
func (b *InMemoryBackend) watchHealthLocked(a *app) {
	if !a.isWeb() || a.HealthCheckPath == "" {
		return
	}

	ctx := a.loops
	name, path, port, every, replace := a.key(), a.HealthCheckPath, a.hostPort, b.healthEvery, b.healthReplace

	b.wg.Go(func() { b.monitorHealth(ctx, name, path, port, every, replace) })
}

func (b *InMemoryBackend) monitorHealth(
	ctx context.Context, name, path string, port int, every, replace time.Duration,
) {
	tr := b.transport.Clone()
	defer tr.CloseIdleConnections()

	client := &http.Client{Transport: tr, Timeout: every}
	url := "http://" + net.JoinHostPort(container.LoopbackHost, strconv.Itoa(port)) + path
	tick := time.NewTicker(every)

	defer tick.Stop()

	var since time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}

		if _, ok := probeHealth(ctx, client, url); ok {
			since = time.Time{}

			continue
		}

		if ctx.Err() != nil {
			return
		}

		if since.IsZero() {
			since = b.now()
		}

		if b.now().Sub(since) >= replace {
			logger.Load(ctx).WarnContext(ctx, "azurefunctions: replacing web app instance that stayed unhealthy",
				"app", name, "path", path)
			_ = b.Restart(name)

			return
		}
	}
}
