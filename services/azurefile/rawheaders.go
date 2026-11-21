package azurefile

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	maxHeadBytes   = 64 << 10
	maxMetaBytes   = 16 << 10
	maxQueuedHeads = 16
	headEnd        = "\r\n\r\n"
	crlf           = "\r\n"
	chunkOverhead  = int64(len(crlf))
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

type rawMeta struct {
	name  string
	value string
}

type requestHead struct {
	method string
	target string
	meta   []rawMeta
}

// headTracker follows the HTTP/1.1 request framing of one connection to remember the metadata header names as the
// client sent them (net/http canonicalises names while parsing). It fails safe: on anything it cannot frame exactly
// as net/http does it stops recording for the connection, and take only returns names whose request line and
// metadata values match the request net/http parsed, so a desynchronised tracker can never rename headers of
// another request. Parser state is only touched by the connection's reader; the queue is a copy-on-write snapshot
// shared with the handler goroutine.
type headTracker struct {
	queue atomic.Pointer[[]requestHead]
	probe func()
	buf   []byte
	skip  int64
	phase framePhase
	chunk bool
}

func newHeadTracker() *headTracker { return &headTracker{} }

func (t *headTracker) update(f func([]requestHead) []requestHead) {
	for {
		old := t.queue.Load()

		var cur []requestHead
		if old != nil {
			cur = *old
		}

		next := f(cur)
		if t.queue.CompareAndSwap(old, &next) {
			return
		}
	}
}

func (t *headTracker) breakOff() {
	t.phase, t.buf = phaseBroken, nil
	t.queue.Store(nil)
}

// guardedFeed never lets a tracker panic escape Read: net/http runs the background read outside conn.serve's recover.
func (t *headTracker) guardedFeed(p []byte) {
	defer logger.Recover(context.Background(), "azurefile.headtracker", func(any) { t.breakOff() })

	if t.probe != nil {
		t.probe()
	}

	t.feed(p)
}

func (t *headTracker) feed(p []byte) {
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
			if t.chunk {
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
	terminator := crlf
	if t.phase == phaseHead {
		terminator = headEnd
	}

	from := max(0, len(t.buf)-len(terminator)+1)
	t.buf = append(t.buf, p...)

	i := bytes.Index(t.buf[from:], []byte(terminator))
	if i < 0 {
		if len(t.buf) > maxHeadBytes {
			t.breakOff()
		}

		return nil
	}

	i += from
	text := string(t.buf[:i])
	rest := p[len(p)-(len(t.buf)-i-len(terminator)):]
	t.buf = t.buf[:0]

	switch t.phase {
	case phaseHead:
		t.head(text)
	case phaseChunkSize:
		t.chunkSize(text)
	default:
		if text == "" {
			t.phase, t.chunk = phaseHead, false
		}
	}

	return rest
}

func isTokenName(s string) bool {
	if s == "" {
		return false
	}

	for i := range len(s) {
		c := s[i]
		if c <= ' ' || c >= 0x7f || strings.IndexByte("()<>@,;:\\\"/[]?={}", c) >= 0 {
			return false
		}
	}

	return true
}

type headFraming struct {
	length    int64
	hasLength bool
	chunked   bool
	hasTE     bool
	upgrade   bool
}

func (f *headFraming) add(name, val string) bool {
	switch {
	case strings.EqualFold(name, "Content-Length"):
		n, err := strconv.ParseUint(val, 10, 63)
		if err != nil || f.hasLength {
			return false
		}

		f.length, f.hasLength = int64(n), true
	case strings.EqualFold(name, "Transfer-Encoding"):
		if f.hasTE || !strings.EqualFold(val, "chunked") {
			return false
		}

		f.hasTE, f.chunked = true, true
	case strings.EqualFold(name, "Upgrade"):
		f.upgrade = true
	}

	return true
}

func (h *requestHead) metaBytes() int {
	n := 0
	for _, m := range h.meta {
		n += len(m.name) + len(m.value)
	}

	return n
}

func (h *requestHead) parseHeaders(lines []string) (headFraming, bool) {
	var fr headFraming

	for _, l := range lines {
		name, val, ok := strings.Cut(l, ":")
		if !ok || !isTokenName(name) {
			return fr, false
		}

		val = strings.Trim(val, " \t")

		if !fr.add(name, val) {
			return fr, false
		}

		if len(name) > len(metaPrefix) && strings.EqualFold(name[:len(metaPrefix)], metaPrefix) {
			h.meta = append(h.meta, rawMeta{name: name, value: val})
		}
	}

	return fr, true
}

func parseHead(text string) (requestHead, headFraming, bool) {
	var (
		h  requestHead
		fr headFraming
	)

	if strings.ContainsAny(strings.ReplaceAll(text, crlf, ""), "\r\n") {
		return h, fr, false
	}

	lines := strings.Split(text, crlf)
	reqLine := strings.Split(lines[0], " ")

	if len(reqLine) != 3 || !isTokenName(reqLine[0]) || reqLine[1] == "" || reqLine[2] != "HTTP/1.1" ||
		reqLine[0] == http.MethodConnect {
		return h, fr, false
	}

	h = requestHead{method: reqLine[0], target: reqLine[1]}

	fr, ok := h.parseHeaders(lines[1:])

	return h, fr, ok && !fr.upgrade && (!fr.chunked || !fr.hasLength)
}

func (t *headTracker) head(text string) {
	h, fr, ok := parseHead(text)
	if !ok {
		t.breakOff()

		return
	}

	if len(h.meta) > 0 && h.metaBytes() <= maxMetaBytes {
		t.update(func(q []requestHead) []requestHead {
			if len(q) >= maxQueuedHeads {
				q = q[len(q)-maxQueuedHeads+1:]
			}

			return append(append([]requestHead(nil), q...), h)
		})
	}

	switch {
	case fr.chunked:
		t.phase, t.chunk = phaseChunkSize, true
	case fr.length > 0:
		t.phase, t.skip, t.chunk = phaseBody, fr.length, false
	default:
		t.phase = phaseHead
	}
}

func (t *headTracker) chunkSize(text string) {
	hex, _, _ := strings.Cut(text, ";")

	n, err := strconv.ParseUint(strings.TrimRight(hex, " \t"), 16, 62)
	if err != nil {
		t.breakOff()

		return
	}

	if n == 0 {
		t.phase = phaseTrailer

		return
	}

	t.phase, t.skip = phaseChunkData, int64(n)+chunkOverhead
}

// match returns the raw names of the first queued head equal to the request, dropping it and every older head.
func (t *headTracker) match(method, target string, canon map[string][]string) map[string]string {
	var out map[string]string

	t.update(func(q []requestHead) []requestHead {
		out = nil

		for i, h := range q {
			if names, ok := h.matches(method, target, canon); ok {
				out = names

				return append([]requestHead(nil), q[i+1:]...)
			}
		}

		return q
	})

	return out
}

func (h *requestHead) matches(method, target string, canon map[string][]string) (map[string]string, bool) {
	if h.method != method || h.target != target {
		return nil, false
	}

	values := make(map[string][]string, len(canon))
	names := make(map[string]string, len(canon))
	mixed := make(map[string]bool)

	for _, m := range h.meta {
		c := http.CanonicalHeaderKey(m.name)
		values[c] = append(values[c], m.value)

		if prev, seen := names[c]; seen && prev != m.name {
			mixed[c] = true
		}

		names[c] = m.name
	}

	if len(values) != len(canon) {
		return nil, false
	}

	for c, want := range canon {
		got := values[c]
		if len(got) != len(want) {
			return nil, false
		}

		for i := range got {
			if got[i] != want[i] {
				return nil, false
			}
		}
	}

	for c := range mixed {
		delete(names, c)
	}

	return names, true
}

type trackedConn struct {
	net.Conn

	t *headTracker
}

func (c *trackedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.t.guardedFeed(p[:n])
	}

	return n, err //nolint:wrapcheck // the transport owns connection errors
}

type trackedListener struct {
	net.Listener
}

func (l trackedListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err //nolint:wrapcheck // the transport owns listener errors
	}

	return &trackedConn{Conn: c, t: newHeadTracker()}, nil
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

	canon := make(map[string][]string)

	for k, v := range r.Header {
		if len(k) > len(metaPrefix) && strings.EqualFold(k[:len(metaPrefix)], metaPrefix) {
			canon[k] = v
		}
	}

	if len(canon) == 0 {
		return
	}

	for c, raw := range t.match(r.Method, r.RequestURI, canon) {
		if raw != c {
			r.Header[raw] = r.Header[c]

			delete(r.Header, c)
		}
	}
}
