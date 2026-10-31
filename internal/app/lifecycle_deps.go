package app

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

// lifecycleRuntime is the slice of the container runtime the lifecycle commands use.
type lifecycleRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	container.LogReader
}

// hostSpawn describes the background host process to launch.
type hostSpawn struct {
	LogFile string
	Dir     string
	Env     []string
}

// lifecycleDeps holds every side effect of the lifecycle commands so tests can fake them.
type lifecycleDeps struct {
	Out        io.Writer
	Err        io.Writer
	HTTP       *http.Client
	Runtime    func() (lifecycleRuntime, error)
	Spawn      func(spec hostSpawn) (int, error)
	Alive      func(pid int) bool
	Terminate  func(pid int, force bool) error
	Getenv     func(string) string
	Home       func() (string, error)
	NewContext func() (context.Context, context.CancelFunc)
}

func newLifecycleDeps() *lifecycleDeps {
	return &lifecycleDeps{
		Out:  os.Stdout,
		Err:  os.Stderr,
		HTTP: httpclient.New(lifecycleHTTPTimeout),
		Runtime: func() (lifecycleRuntime, error) {
			rt, err := container.NewRuntime(container.Config{})
			if err != nil {
				return nil, fmt.Errorf("container runtime: %w", err)
			}

			lr, ok := rt.(lifecycleRuntime)
			if !ok {
				return nil, errLifecycleNoRuntime
			}

			return lr, nil
		},
		Spawn:     spawnHostProcess,
		Alive:     processAlive,
		Terminate: terminateProcess,
		Getenv:    os.Getenv,
		Home:      os.UserHomeDir,
		NewContext: func() (context.Context, context.CancelFunc) {
			return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		},
	}
}

func spawnHostProcess(spec hostSpawn) (int, error) {
	self, err := os.Executable()
	if err != nil {
		return 0, fmt.Errorf("locate executable: %w", err)
	}

	logf, err := os.OpenFile(spec.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, lifecycleStateFilePerm)
	if err != nil {
		return 0, fmt.Errorf("open log file: %w", err)
	}
	defer logf.Close()

	cmd := exec.CommandContext(context.Background(), self, "serve")
	cmd.Env = append(os.Environ(), spec.Env...)
	cmd.Dir = spec.Dir
	cmd.Stdout = logf
	cmd.Stderr = logf
	detachProcess(cmd)

	if err = cmd.Start(); err != nil {
		return 0, fmt.Errorf("start host process: %w", err)
	}

	pid := cmd.Process.Pid
	_ = cmd.Process.Release()

	return pid, nil
}
