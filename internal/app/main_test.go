package app

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMultipleServersStartupAndShutdown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		demo bool
	}{
		{
			name: "server startup without DEMO",
			demo: false,
		},
		{
			name: "server startup with DEMO",
			demo: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			port, stopChan, errChan := startServerRetryingPort(t, tt.demo)

			t.Logf("Server responding successfully on port :%d", port)

			close(stopChan)

			select {
			case runErr := <-errChan:
				require.NoError(t, runErr)
			case <-time.After(5 * time.Second):
				require.FailNow(t, "server did not shut down within timeout")
			}
		})
	}
}

// startServerRetryingPort starts Gopherstack on a freshly picked ephemeral port and waits for its
// server to answer as this attempt (STS account ID match). freeTCPPort's Listen-then-Close releases the port immediately, but the real
// bind happens much later, at the end of run's init chain -- a wide TOCTOU window in which something
// else can take the same port first. When that happens run exits with a bind error before the
// dashboard ever comes up; this retries on a fresh port rather than burning the rest of the deadline
// on a port already lost (gopherstack-tajh, following the pkgs/dns TOCTOU precedent fixed in
// gopherstack-nn94/7tbt). A plain timeout with no error from run is a genuine hang, not a port race,
// and is not retried -- retrying would not fix it and could make things worse under load.
func startServerRetryingPort(t *testing.T, demo bool) (int, chan struct{}, chan error) {
	t.Helper()

	const (
		maxAttempts   = 5
		dashboardWait = 10 * time.Second
		dashboardPoll = 100 * time.Millisecond
	)

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		port := freeTCPPort(t)
		accountID := uniqueAccountID(t)
		stopChan := make(chan struct{})
		errChan := make(chan error, 1)

		go func() {
			errChan <- startServerOnPort(t, port, accountID, demo, stopChan)
		}()

		up, runErr := waitForInstance(port, accountID, errChan, dashboardWait, dashboardPoll)
		if up {
			return port, stopChan, errChan
		}

		if runErr == nil {
			require.FailNow(t, fmt.Sprintf(
				"failed to reach server on :%d: timed out after %s with no error from run (attempt %d/%d)",
				port, dashboardWait, attempt, maxAttempts))

			return 0, nil, nil
		}

		if attempt == maxAttempts {
			require.FailNow(t, fmt.Sprintf(
				"server on :%d failed to start after %d attempts, last error: %v", port, maxAttempts, runErr))

			return 0, nil, nil
		}

		t.Logf("attempt %d/%d: server on :%d exited before answering (%v); retrying on a fresh port",
			attempt, maxAttempts, port, runErr)
	}

	return 0, nil, nil
}

// waitForInstance polls until the server on port reports accountID from STS GetCallerIdentity, run
// exits (delivering to errChan), or the deadline elapses. Matching the per-attempt account ID proves
// the responder is this attempt's server, not another test's server that grabbed the port first.
func waitForInstance(port int, accountID string, errChan <-chan error, timeout, interval time.Duration) (bool, error) {
	client := &http.Client{Timeout: 1 * time.Second}
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case runErr := <-errChan:
			return false, runErr
		default:
		}

		if instanceHasAccount(client, port, accountID) {
			return true, nil
		}

		if time.Now().After(deadline) {
			return false, nil
		}

		<-ticker.C
	}
}

func instanceHasAccount(client *http.Client, port int, accountID string) bool {
	resp, err := client.PostForm(
		fmt.Sprintf("http://localhost:%d/", port),
		url.Values{"Action": {"GetCallerIdentity"}, "Version": {"2011-06-15"}},
	)
	if err != nil {
		return false
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)

	return err == nil && resp.StatusCode == http.StatusOK &&
		strings.Contains(string(body), "<Account>"+accountID+"</Account>")
}

// uniqueAccountID returns a random 12-digit account ID identifying one server instance.
func uniqueAccountID(t *testing.T) string {
	t.Helper()

	n, err := rand.Int(rand.Reader, big.NewInt(1_000_000_000_000))
	require.NoError(t, err)

	return fmt.Sprintf("%012d", n)
}

// startServerOnPort starts Gopherstack on the given port using the CLI run path.
// It returns when the stopChan is closed.
func startServerOnPort(t *testing.T, port int, accountID string, demo bool, stopChan chan struct{}) error {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	cli := CLI{
		LogLevel:       "info",
		Port:           strconv.Itoa(port),
		Region:         "us-east-1",
		AccountID:      accountID,
		Demo:           demo,
		PortRangeStart: 10000,
		PortRangeEnd:   10100,
	}

	errChan := make(chan error, 1)

	go func() {
		errChan <- run(ctx, cli)
	}()

	select {
	case <-stopChan:
		cancel()

		return <-errChan
	case err := <-errChan:
		return err
	}
}

func freeTCPPort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	return l.Addr().(*net.TCPAddr).Port
}

func TestWaitForInstanceRejectsForeignServer(t *testing.T) {
	t.Parallel()

	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "<Account>111111111111</Account>")
	}))
	t.Cleanup(foreign.Close)

	port, err := strconv.Atoi(foreign.URL[strings.LastIndex(foreign.URL, ":")+1:])
	require.NoError(t, err)

	tests := []struct {
		name      string
		accountID string
		wantUp    bool
	}{
		{name: "other instance rejected", accountID: "222222222222", wantUp: false},
		{name: "matching instance accepted", accountID: "111111111111", wantUp: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			up, runErr := waitForInstance(port, tt.accountID, make(chan error), 300*time.Millisecond, 20*time.Millisecond)
			require.NoError(t, runErr)
			require.Equal(t, tt.wantUp, up)
		})
	}
}
