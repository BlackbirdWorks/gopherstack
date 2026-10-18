package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPort    = 8000
	healthPath     = "/_gopherstack/health"
	healthInterval = 150 * time.Millisecond
	healthTimeout  = 60 * time.Second
	probeTimeout   = 2 * time.Second
)

var (
	errServerTimeout = errors.New("server did not become healthy in time")
	errNoPort        = errors.New("no free TCP port available")
)

// resolvePort returns the port named by --port/PORT, or a free loopback
// port (preferring 8000), together with the args to pass to the server.
func resolvePort(args []string) (int, []string, error) {
	for i, a := range args {
		var v string

		switch {
		case a == "--port" && i+1 < len(args):
			v = args[i+1]
		case strings.HasPrefix(a, "--port="):
			v = strings.TrimPrefix(a, "--port=")
		default:
			continue
		}

		p, err := strconv.Atoi(strings.TrimPrefix(v, ":"))
		if err != nil {
			return 0, nil, fmt.Errorf("invalid --port %q: %w", v, err)
		}

		return p, args, nil
	}

	if v := os.Getenv("PORT"); v != "" {
		p, err := strconv.Atoi(strings.TrimPrefix(v, ":"))
		if err != nil {
			return 0, nil, fmt.Errorf("invalid PORT %q: %w", v, err)
		}

		return p, args, nil
	}

	p, err := freePort()
	if err != nil {
		return 0, nil, err
	}

	return p, append([]string{"--port", strconv.Itoa(p)}, args...), nil
}

func freePort() (int, error) {
	for _, addr := range []string{net.JoinHostPort("", strconv.Itoa(defaultPort)), "127.0.0.1:0"} {
		l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", addr)
		if err != nil {
			continue
		}

		tcp, ok := l.Addr().(*net.TCPAddr)
		_ = l.Close()

		if ok {
			return tcp.Port, nil
		}
	}

	return 0, errNoPort
}

func waitHealthy(ctx context.Context, port int) error {
	url := "http://127.0.0.1:" + strconv.Itoa(port) + healthPath
	client := &http.Client{Timeout: probeTimeout, Transport: &http.Transport{}}
	deadline := time.NewTimer(healthTimeout)

	defer deadline.Stop()

	tick := time.NewTicker(healthInterval)

	defer tick.Stop()

	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}

		resp, doErr := client.Do(req)
		if doErr == nil {
			_ = resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errServerTimeout
		case <-tick.C:
		}
	}
}
