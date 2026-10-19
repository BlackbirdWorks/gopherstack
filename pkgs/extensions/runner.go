package extensions

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	stopGrace    = 5 * time.Second
	containerTTL = 30 * time.Second
	logBuf       = 1 << 20
)

// runner starts and stops one incarnation of an extension.
type runner interface {
	Start(ctx context.Context, env []string) error
	// Wait blocks until the extension exits (or Stop completes).
	Wait() error
	Stop()
}

type processRunner struct {
	log    *slog.Logger
	cmd    *exec.Cmd
	sink   func(line string)
	secret string
	spec   Spec
	done   atomic.Bool
}

func newProcessRunner(log *slog.Logger, spec Spec, secret string) *processRunner {
	return &processRunner{log: log, spec: spec, secret: secret}
}

func (p *processRunner) Start(ctx context.Context, env []string) error {
	cmd := exec.CommandContext( //nolint:gosec // operator-declared extension
		ctx,
		p.spec.Command[0],
		p.spec.Command[1:]...)
	cmd.Env = append(os.Environ(), env...)
	setProcessGroup(cmd)

	cmd.Cancel = func() error { return signalGroup(cmd, syscall.SIGTERM) }
	cmd.WaitDelay = stopGrace

	cmd.Stdout = &lineLogger{p: p, stream: "stdout"}
	cmd.Stderr = &lineLogger{p: p, stream: "stderr"}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start %s: %w", p.spec.Name, err)
	}

	p.cmd = cmd

	return nil
}

// lineLogger turns a byte stream into one log record per line.
type lineLogger struct {
	p      *processRunner
	stream string
	buf    []byte
}

func (l *lineLogger) Write(b []byte) (int, error) {
	l.buf = append(l.buf, b...)

	for {
		i := bytes.IndexByte(l.buf, '\n')
		if i < 0 {
			break
		}

		l.emit(string(l.buf[:i]))
		l.buf = l.buf[i+1:]
	}

	if len(l.buf) > logBuf {
		l.emit(string(l.buf))
		l.buf = nil
	}

	return len(b), nil
}

func (l *lineLogger) emit(line string) {
	if l.p.secret != "" {
		line = strings.ReplaceAll(line, l.p.secret, "[redacted]")
	}

	line = strings.TrimRight(line, "\r")

	if l.p.sink != nil {
		l.p.sink(l.stream + ": " + line)
	}

	l.p.log.Info(
		"extension output",
		"extension",
		l.p.spec.Name,
		"stream",
		l.stream,
		"line",
		line,
	)
}

func (p *processRunner) Wait() error {
	err := p.cmd.Wait()

	if !p.done.Swap(true) {
		// Leader exited on its own: reap whatever it left behind.
		_ = signalGroup(p.cmd, syscall.SIGKILL)
	}

	return err
}

// Stop signals the whole process group, then force-kills it after stopGrace.
func (p *processRunner) Stop() {
	if p.cmd == nil || p.cmd.Process == nil || p.done.Swap(true) {
		return
	}

	_ = signalGroup(p.cmd, syscall.SIGTERM)

	time.AfterFunc(stopGrace, func() { _ = signalGroup(p.cmd, syscall.SIGKILL) })
}

type containerRunner struct {
	rt      container.Runtime
	newRT   func() (container.Runtime, error)
	stopped chan struct{}
	id      string
	spec    Spec
	port    int
	once    sync.Once
}

func newContainerRunner(newRT func() (container.Runtime, error), spec Spec, port int) *containerRunner {
	return &containerRunner{newRT: newRT, spec: spec, port: port, stopped: make(chan struct{})}
}

func (c *containerRunner) Start(ctx context.Context, env []string) error {
	if c.newRT == nil {
		return container.ErrUnavailable
	}

	rt, err := c.newRT()
	if err != nil {
		return err
	}

	c.rt = rt

	has, err := rt.HasImage(ctx, c.spec.Image)
	if err != nil {
		return err
	}

	if !has {
		if err = rt.PullImage(ctx, c.spec.Image); err != nil {
			return err
		}
	}

	p := strconv.Itoa(c.port)
	c.id, err = rt.CreateAndStart(ctx, container.Spec{
		Image: c.spec.Image,
		Name:  "gopherstack-ext-" + c.spec.Name,
		Env:   env,
		Cmd:   c.spec.Args,
		Ports: []string{"127.0.0.1:" + p + ":" + p},
	})

	return err
}

func (c *containerRunner) Wait() error {
	<-c.stopped

	return nil
}

func (c *containerRunner) Stop() {
	c.once.Do(func() {
		defer close(c.stopped)

		if c.rt == nil || c.id == "" {
			return
		}

		ctx, cancel := context.WithTimeout(context.Background(), containerTTL)
		defer cancel()

		_ = c.rt.StopAndRemove(ctx, c.id)
	})
}

var errExited = errors.New("extension exited")
