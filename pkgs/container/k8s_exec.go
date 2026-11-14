package container

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
)

// ErrExecNoConfig is returned when pods/exec is requested on a runtime built from an injected client without
// an Executor.
var ErrExecNoConfig = errors.New("pods/exec needs a cluster connection or a PodExecutor")

// TermSize is a terminal size in character cells.
type TermSize struct{ Cols, Rows uint }

// ExecRequest is one pods/exec stream with a TTY: stdout and stderr share Stdout.
type ExecRequest struct {
	Stdin     io.Reader
	Stdout    io.Writer
	Resize    <-chan TermSize
	Pod       string
	Container string
	Command   []string
}

// PodExecutor streams a command in a Pod container until it ends or ctx is cancelled.
type PodExecutor interface {
	StreamExec(ctx context.Context, req ExecRequest) error
}

// PodShell is an interactive command attached to a Pod container.
type PodShell interface {
	io.ReadWriteCloser
	Resize(ctx context.Context, cols, rows uint) error
}

type spdyExecutor struct{ r *KubernetesRuntime }

func (e spdyExecutor) StreamExec(ctx context.Context, req ExecRequest) error {
	r := e.r

	u := r.cs.CoreV1().RESTClient().Post().Resource("pods").Namespace(r.ns).Name(req.Pod).SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: req.Container, Command: req.Command, Stdin: true, Stdout: true, TTY: true,
		}, scheme.ParameterCodec).URL()

	ex, err := remotecommand.NewSPDYExecutor(r.restCfg, "POST", u)
	if err != nil {
		return fmt.Errorf("pods/exec executor: %w", err)
	}

	err = ex.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin: req.Stdin, Stdout: req.Stdout, Tty: true, TerminalSizeQueue: sizeQueue{done: ctx.Done(), ch: req.Resize},
	})
	if err != nil {
		return fmt.Errorf("pods/exec %s/%s: %w", req.Pod, req.Container, err)
	}

	return nil
}

type sizeQueue struct {
	done <-chan struct{}
	ch   <-chan TermSize
}

func (q sizeQueue) Next() *remotecommand.TerminalSize {
	select {
	case s := <-q.ch:
		return &remotecommand.TerminalSize{Width: clampU16(s.Cols), Height: clampU16(s.Rows)}
	case <-q.done:
		return nil
	}
}

func clampU16(v uint) uint16 { return uint16(min(v, math.MaxUint16)) }

func (r *KubernetesRuntime) executor() (PodExecutor, error) {
	if r.exec != nil {
		return r.exec, nil
	}

	if r.restCfg == nil {
		return nil, ErrExecNoConfig
	}

	return spdyExecutor{r: r}, nil
}

type podShell struct {
	in     *io.PipeWriter
	out    *io.PipeReader
	resize chan TermSize
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// ExecInteractive runs command in a Pod container with a TTY. Closing the shell ends the command.
func (r *KubernetesRuntime) ExecInteractive(
	ctx context.Context, pod, containerName string, command []string, cols, rows uint,
) (PodShell, error) {
	ex, err := r.executor()
	if err != nil {
		return nil, err
	}

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	sh := &podShell{in: inW, out: outR, resize: make(chan TermSize, 1), done: make(chan struct{})}

	ctx, sh.cancel = context.WithCancel(context.WithoutCancel(ctx))
	sh.resize <- TermSize{Cols: cols, Rows: rows}

	go func() {
		defer close(sh.done)

		streamErr := ex.StreamExec(ctx, ExecRequest{
			Pod: pod, Container: containerName, Command: command, Stdin: inR, Stdout: outW, Resize: sh.resize,
		})

		_ = inR.Close()
		_ = outW.CloseWithError(streamErr)
	}()

	return sh, nil
}

func (s *podShell) Read(p []byte) (int, error) {
	n, err := s.out.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		return n, io.EOF
	}

	return n, err //nolint:wrapcheck // io.Reader contract
}

func (s *podShell) Write(p []byte) (int, error) {
	n, err := s.in.Write(p)
	if err != nil {
		return n, fmt.Errorf("pod shell write: %w", err)
	}

	return n, nil
}

func (s *podShell) Resize(ctx context.Context, cols, rows uint) error {
	select {
	case s.resize <- TermSize{Cols: cols, Rows: rows}:
	case <-s.done:
	case <-ctx.Done():
		return fmt.Errorf("pod shell resize: %w", ctx.Err())
	}

	return nil
}

func (s *podShell) Close() error {
	s.once.Do(func() {
		s.cancel()
		_ = s.in.Close()
		_ = s.out.Close()
		<-s.done
	})

	return nil
}
