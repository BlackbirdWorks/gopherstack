package main

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestServerShutdown_NoGoroutineLeaks boots the full service composition,
// runs every background worker, shuts down, then asserts no goroutine
// started during the run outlived it.
//
// Can't run under synctest: run() opens a real net.Listener and services
// like the IoT MQTT broker do real blocking network I/O.
//
//nolint:paralleltest // uses t.Setenv via parseCLI, which is incompatible with t.Parallel.
func TestServerShutdown_NoGoroutineLeaks(t *testing.T) {
	baseline := goleak.IgnoreCurrent()

	port := freeTCPPort(t)
	cli := parseCLI(t, map[string]string{
		"PORT": strconv.Itoa(port),
	})

	ctx, cancel := context.WithCancel(t.Context())

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, cli)
	}()

	waitForServerReady(t, port)

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err, "server should shut down cleanly")
	case <-time.After(shutdownWaitTimeout):
		require.FailNow(t, "server did not shut down within timeout")
	}

	goleak.VerifyNone(t, append(testleak.DefaultIgnores(), baseline)...)
}
