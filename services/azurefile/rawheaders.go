package azurefile

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	maxHeadBytes   = 64 << 10
	maxQueuedHeads = 64
	headEnd        = "\r\n\r\n"
	crlf           = "\r\n"
)

type framePhase int

const (
	phaseHead framePhase = iota
	phaseBody
	phaseChunkSize
	phaseChunkData
	phaseTrailer
	phaseBroken
)

type requestHead struct {
	method string
	target string
	meta   []string
}

// headTracker frames the HTTP/1.1 requests of one connection to remember the metadata header names exactly as the
// client sent them: net/http canonicalises names while parsing, which would lose their case.
type headTracker struct {
	mu     *lockmetrics.RWMutex
	buf    []byte
	queue  []requestHead
	skip   int64
	phase  framePhase
	frames bool
}

func newHeadTracker(mu *lockmetrics.RWMutex) *headTracker { return &headTracker{mu: mu} }

func (t *headTracker) feed(p []byte) {
	t.mu.Lock("feedHead")
	defer t.mu.Unlock()

	for len(p) > 0 && t.phase != phaseBroken {
		p = t.advance(p)
	}
}

func (t *headTracker) advance(p []byte) []byte {
	switch t.phase {
	case phaseBody, phaseChunkData:
		n := min(int64(len(p)), t.skip)
		t.skip -= n

		if t.skip == 0 {
			t.phase = phaseHead
			if t.frames {
				t.phase = phaseChunkSize
			}
		}

		return p[n:]
	case phaseHead, phaseChunkSize, phaseTrailer:
		return t.line(p)
	default:
		return nil
	}
}

func (t *headTracker) line(p []byte) []byte {
	t.buf = append(t.buf, p...)

	terminator := crlf
	if t.phase == phaseHead {
		terminator = headEnd
	}

	i := bytes.Index(t.buf, []byte(terminator))
	if i < 0 {
		if len(t.buf) > maxHeadBytes {
			t.phase = phaseBroken
		}

		return nil
	}

	text := string(t.buf[:i])
	rest := append([]byte(nil), t.buf[i+len(terminator):]...)
	t.buf = t.buf[:0]

	switch t.phase {
	case phaseHead:
		t.head(text)
	case phaseChunkSize:
		t.chunkSize(text)
	default:
		if text == "" {
			t.phase, t.frames = phaseHead, false
		}
	}

	return rest
}

func (t *headTracker) head(text string) {
	lines := strings.Split(text, crlf)
	method, rest, _ := strings.Cut(lines[0], " ")
	target, _, _ := strings.Cut(rest, " ")
	h := requestHead{method: method, target: target}
	length, chunked := int64(0), false

	for _, l := range lines[1:] {
		name, val, ok := strings.Cut(l, ":")
		if !ok {
			continue
		}

		switch val = strings.TrimSpace(val); {
		case len(name) > len(metaPrefix) && strings.EqualFold(name[:len(metaPrefix)], metaPrefix):
			h.meta = append(h.meta, name)
		case strings.EqualFold(name, "Content-Length"):
			n, err := strconv.ParseInt(val, 10, 64)
			if err != nil || n < 0 {
				t.phase = phaseBroken

				return
			}

			length = n
		case strings.EqualFold(name, "Transfer-Encoding"):
			chunked = strings.Contains(strings.ToLower(val), "chunked")
		}
	}

	t.queue = append(t.queue, h)
	if len(t.queue) > maxQueuedHeads {
		t.queue = t.queue[1:]
	}

	switch {
	case chunked:
		t.phase, t.frames = phaseChunkSize, true
	case length > 0:
		t.phase, t.skip, t.frames = phaseBody, length, false
	default:
		t.phase = phaseHead
	}
}

func (t *headTracker) chunkSize(text string) {
	hex, _, _ := strings.Cut(text, ";")

	n, err := strconv.ParseInt(strings.TrimSpace(hex), 16, 64)
	if err != nil || n < 0 {
		t.phase = phaseBroken

		return
	}

	if n == 0 {
		t.phase = phaseTrailer

		return
	}

	t.phase, t.skip = phaseChunkData, n+int64(len(crlf))
}

// take returns the metadata header names of the queued head that matches the request, dropping older heads.
func (t *headTracker) take(method, target string) []string {
	t.mu.Lock("takeHead")
	defer t.mu.Unlock()

	for i, h := range t.queue {
		if h.method == method && h.target == target {
			t.queue = t.queue[i+1:]

			return h.meta
		}
	}

	return nil
}

type trackedConn struct {
	net.Conn

	t *headTracker
}

func (c *trackedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.t.feed(p[:n])
	}

	return n, err //nolint:wrapcheck // the transport owns connection errors
}

type trackedListener struct {
	net.Listener

	mu *lockmetrics.RWMutex
}

func (l trackedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err //nolint:wrapcheck // the transport owns listener errors
	}

	return &trackedConn{Conn: c, t: newHeadTracker(l.mu)}, nil
}

type headTrackerKey struct{}

func connContext(ctx context.Context, c net.Conn) context.Context {
	if tc, ok := c.(*trackedConn); ok {
		return context.WithValue(ctx, headTrackerKey{}, tc.t)
	}

	return ctx
}

// restoreMetadataCase renames the canonicalised metadata headers of r to the names the client sent.
func restoreMetadataCase(r *http.Request) {
	t, ok := r.Context().Value(headTrackerKey{}).(*headTracker)
	if !ok {
		return
	}

	for _, raw := range t.take(r.Method, r.RequestURI) {
		canon := http.CanonicalHeaderKey(raw)
		if raw == canon {
			continue
		}

		if vals, found := r.Header[canon]; found {
			r.Header[raw] = vals

			delete(r.Header, canon)
		}
	}
}
