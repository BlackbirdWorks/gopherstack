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
)

const (
	healthPollInterval = 250 * time.Millisecond
	healthProbeTimeout = 10 * time.Second
	healthUserAgent    = "HealthCheck/1.0"
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
