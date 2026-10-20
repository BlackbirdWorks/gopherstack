package codebuild

import (
	"bytes"
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// lineWriter forwards each complete output line to the build log as it arrives.
type lineWriter struct {
	ctx     context.Context
	log     *buildLogger
	pending []byte
	all     bytes.Buffer
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.all.Write(p)
	w.pending = append(w.pending, p...)

	if i := bytes.LastIndexByte(w.pending, '\n'); i >= 0 {
		w.log.write(w.ctx, string(w.pending[:i+1]))
		w.pending = append(w.pending[:0], w.pending[i+1:]...)
	}

	return len(p), nil
}

func (w *lineWriter) flush() {
	if len(w.pending) > 0 {
		w.log.write(w.ctx, string(w.pending)+"\n")
		w.pending = nil
	}
}

// execLogged runs a script and logs its output, line by line when the runtime can stream.
func (r *buildRunner) execLogged(script string) (string, int, error) {
	cmd := shellInvocation(script)

	if se, ok := r.rt.(container.StreamExecer); ok {
		lw := &lineWriter{ctx: r.ctx, log: r.log}
		code, err := se.ExecStream(r.ctx, r.containerID, cmd, lw)
		lw.flush()

		return lw.all.String(), code, err
	}

	res, err := r.rt.Exec(r.ctx, r.containerID, cmd)
	out := res.Stdout + res.Stderr
	r.log.write(r.ctx, out)

	return out, res.ExitCode, err
}
