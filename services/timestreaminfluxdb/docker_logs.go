package timestreaminfluxdb

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	dirPerm     = 0o750
	maxLogBytes = 1 << 20
)

// logBuffer holds the output captured from one container until it is drained.
type logBuffer struct {
	cancel context.CancelFunc
	data   bytes.Buffer
	// seen counts the bytes already captured per stream, so a re-followed container does not replay them.
	seen [2]int64
	mu   sync.Mutex
}

type logWriter struct {
	buf    *logBuffer
	stream int
	pos    int64
}

func (w *logWriter) Write(p []byte) (int, error) {
	w.buf.mu.Lock()
	defer w.buf.mu.Unlock()

	start := w.pos
	w.pos += int64(len(p))

	if w.pos <= w.buf.seen[w.stream] {
		return len(p), nil
	}

	skip := max(w.buf.seen[w.stream]-start, 0)
	w.buf.seen[w.stream] = w.pos
	w.buf.data.Write(p[skip:])

	if over := w.buf.data.Len() - maxLogBytes; over > 0 {
		w.buf.data.Next(over)
	}

	return len(p), nil
}

func (d *DockerEngine) bufferFor(id string) *logBuffer {
	v, _ := d.logs.LoadOrStore(id, &logBuffer{})

	return asLogBuffer(v)
}

func asLogBuffer(v any) *logBuffer {
	if buf, ok := v.(*logBuffer); ok {
		return buf
	}

	return &logBuffer{}
}

// followLogs starts capturing a container's output when the runtime can stream it.
func (d *DockerEngine) followLogs(id string) {
	streamer, ok := d.Runtime.(container.LogStreamer)
	if !ok {
		return
	}

	buf := d.bufferFor(id)
	ctx, cancel := context.WithCancel(context.Background())

	buf.mu.Lock()
	if buf.cancel != nil {
		buf.cancel()
	}

	buf.cancel = cancel
	buf.mu.Unlock()

	if cur, ok := d.logs.Load(id); !ok || asLogBuffer(cur) != buf {
		cancel()

		return
	}

	var out, errw io.Writer = &logWriter{buf: buf, stream: 0}, &logWriter{buf: buf, stream: 1}

	go func() { _ = streamer.StreamLogs(ctx, id, out, errw) }()
}

// DrainLogs returns and clears the captured output of a container.
func (d *DockerEngine) DrainLogs(h EngineHandle) []byte {
	v, ok := d.logs.Load(h.ContainerID)
	if !ok {
		return nil
	}

	buf := asLogBuffer(v)

	buf.mu.Lock()
	defer buf.mu.Unlock()

	out := bytes.Clone(buf.data.Bytes())
	buf.data.Reset()

	return out
}

func (d *DockerEngine) dropLogs(id string) {
	if v, ok := d.logs.LoadAndDelete(id); ok {
		buf := asLogBuffer(v)

		buf.mu.Lock()
		cancel := buf.cancel
		buf.mu.Unlock()

		if cancel != nil {
			cancel()
		}
	}
}

// takeLogs removes a container's buffer and returns what it still holds.
func (d *DockerEngine) takeLogs(id string) []byte {
	out := d.DrainLogs(EngineHandle{ContainerID: id})
	d.dropLogs(id)

	return out
}

// restoreLogs seeds a replacement container's buffer with output captured from its predecessor.
func (d *DockerEngine) restoreLogs(id string, pending []byte) {
	if len(pending) == 0 {
		return
	}

	buf := d.bufferFor(id)

	buf.mu.Lock()
	defer buf.mu.Unlock()

	buf.data.Write(pending)
}
