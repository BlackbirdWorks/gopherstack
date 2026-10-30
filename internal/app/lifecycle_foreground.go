package app

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const lifecycleDefaultSocket = "/var/run/docker.sock"

// lifecycleExitError carries a container's non-zero exit code to the process exit status.
type lifecycleExitError struct{ Code int }

func (e *lifecycleExitError) Error() string {
	return fmt.Sprintf("container exited with code %d", e.Code)
}

// lifecycleExitCode reports the exit status requested by err, if any.
func lifecycleExitCode(err error) (int, bool) {
	if ee, ok := errors.AsType[*lifecycleExitError](err); ok {
		return ee.Code, true
	}

	return 0, false
}

func (c *StartCmd) validate() error {
	if c.Host && (c.Foreground || c.DockerSocket) {
		return errLifecycleHostOnly
	}

	if c.DockerSocketPath != "" && !c.DockerSocket {
		return errLifecycleSocketPath
	}

	return nil
}

func (c *StartCmd) dockerSocketPath(getenv func(string) string) string {
	if c.DockerSocketPath != "" {
		return c.DockerSocketPath
	}

	if p, ok := strings.CutPrefix(getenv("DOCKER_HOST"), "unix://"); ok && p != "" {
		return p
	}

	return lifecycleDefaultSocket
}

type exitResult struct {
	err  error
	code int
}

// attach streams the container's logs until it exits or ctx is cancelled (SIGINT/SIGTERM),
// then stops it gracefully and reports its exit code.
func (d *lifecycleDeps) attach(
	ctx context.Context,
	rt lifecycleRuntime,
	waiter container.ExitWaiter,
	target LifecycleTarget,
	id string,
) error {
	bg := context.WithoutCancel(ctx)
	logCtx, stopLogs := context.WithCancel(bg)
	logsDone := make(chan struct{})

	go func() {
		defer close(logsDone)

		_ = rt.ReadLogs(logCtx, id, "", true, d.Out, d.Err)
	}()

	waitCtx, stopWait := context.WithCancel(bg)
	defer stopWait()

	exited := make(chan exitResult, 1)

	go func() {
		code, err := waiter.WaitExit(waitCtx, id)
		exited <- exitResult{code: code, err: err}
	}()

	var res exitResult

	select {
	case res = <-exited:
	case <-ctx.Done():
		stopCtx, cancel := context.WithTimeout(bg, lifecycleStopGrace)
		defer cancel()

		if err := rt.StopAndRemove(stopCtx, id); err != nil && !notFoundErr(err) {
			stopLogs()
			<-logsDone

			return fmt.Errorf("stop container: %w", err)
		}

		select {
		case res = <-exited:
		case <-stopCtx.Done():
			res.err = stopCtx.Err()
		}
	}

	stopLogs()
	<-logsDone

	if err := rt.StopAndRemove(bg, id); err != nil && !notFoundErr(err) && ctx.Err() == nil {
		fmt.Fprintf(d.Err, "warning: remove container: %v\n", err)
	}

	d.dropState(target)

	if res.err != nil && res.code == 0 {
		return fmt.Errorf("wait for container: %w", res.err)
	}

	if res.code != 0 {
		return &lifecycleExitError{Code: res.code}
	}

	return nil
}
