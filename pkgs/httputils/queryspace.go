package httputils

import (
	"bytes"
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"strconv"
)

const (
	maxRequestLine       = 8 << 10
	maxHeaderLine        = 64 << 10
	maxContentLenDg      = 18
	encodedSpace         = "%20"
	versionLen           = len("HTTP/1.1")
	maxMethodLen         = 16
	maxRequestLineSpaces = 2
)

type qsState uint8

const (
	qsRequestLine qsState = iota
	qsHeaders
	qsBody
	qsPassthrough
)

// NewQuerySpaceListener wraps ln so request lines whose query component holds
// raw ASCII spaces are rewritten to %20 before net/http parses them. Clients
// such as armauthorization send `$filter=a eq 'b'` unencoded, which net/http
// answers with 400; Azure's front door accepts it. Only the query part of an
// origin-form target of a well-formed line is touched. Anything unusual
// (control bytes, spaces before '?', odd framing) is passed through
// byte-identical, so net/http still rejects it exactly as before.
func NewQuerySpaceListener(ln net.Listener) net.Listener {
	return &qsListener{Listener: ln}
}

type qsListener struct{ net.Listener }

func (l *qsListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}

	return &qsConn{Conn: c}, nil
}

type connKey struct{}

// QuerySpaceConnContext is an http.Server.ConnContext that keeps the wrapped
// connection reachable so QuerySpaceTLS can restore Request.TLS.
func QuerySpaceConnContext(ctx context.Context, c net.Conn) context.Context {
	return context.WithValue(ctx, connKey{}, c)
}

// QuerySpaceTLS sets Request.TLS from the underlying *tls.Conn, which the
// listener wrapper hides from net/http.
func QuerySpaceTLS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			if qc, ok := r.Context().Value(connKey{}).(*qsConn); ok {
				if tc, isTLS := qc.Conn.(*tls.Conn); isTLS {
					st := tc.ConnectionState()
					r.TLS = &st
				}
			}
		}

		next.ServeHTTP(w, r)
	})
}

type qsConn struct {
	net.Conn

	readErr    error
	line       []byte
	buf        []byte
	out        []byte
	bodyLeft   int64
	contentLen int64
	state      qsState
	clSeen     bool
	connect    bool
	special    bool
}

func (c *qsConn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}

	if c.state == qsPassthrough && len(c.out) == 0 && c.readErr == nil {
		return c.Conn.Read(p)
	}

	if len(c.buf) < len(p) {
		c.buf = make([]byte, len(p))
	}

	for len(c.out) == 0 && c.readErr == nil {
		n, err := c.Conn.Read(c.buf[:len(p)])
		if n > 0 {
			c.feed(c.buf[:n])
		}

		if err != nil {
			if len(c.line) > 0 {
				c.passthrough()
			}

			c.readErr = err
		}
	}

	n := copy(p, c.out)
	c.out = c.out[n:]

	if len(c.out) == 0 && c.readErr != nil {
		err := c.readErr
		c.readErr = nil

		return n, err
	}

	return n, nil
}

// CloseWrite lets net/http half-close before Close so a pending response is
// not lost to an RST; the embedded net.Conn interface would hide it.
func (c *qsConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}

	return nil
}

func (c *qsConn) feed(data []byte) {
	for len(data) > 0 {
		switch c.state {
		case qsPassthrough:
			c.out = append(c.out, data...)

			return
		case qsBody:
			n := min(int64(len(data)), c.bodyLeft)

			c.out = append(c.out, data[:n]...)
			data = data[n:]
			c.bodyLeft -= n

			if c.bodyLeft == 0 {
				c.state = qsRequestLine
			}
		default:
			data = c.feedLine(data)
		}
	}
}

func (c *qsConn) limit() int {
	if c.state == qsRequestLine {
		return maxRequestLine
	}

	return maxHeaderLine
}

func (c *qsConn) feedLine(data []byte) []byte {
	i := bytes.IndexByte(data, '\n')
	if i < 0 {
		c.line = append(c.line, data...)
		if len(c.line) > c.limit() {
			c.passthrough()
		}

		return nil
	}

	c.line = append(c.line, data[:i+1]...)
	rest := data[i+1:]

	if len(c.line) > c.limit() {
		c.passthrough()

		return rest
	}

	line := c.line
	c.line = nil

	if c.state == qsRequestLine {
		c.requestLine(line)
	} else {
		c.headerLine(line)
	}

	return rest
}

func (c *qsConn) passthrough() {
	c.out = append(c.out, c.line...)
	c.line = nil
	c.state = qsPassthrough
}

func (c *qsConn) requestLine(line []byte) {
	if isBlankLine(line) {
		c.out = append(c.out, line...)

		return
	}

	c.connect = bytes.HasPrefix(line, []byte("CONNECT "))
	c.clSeen, c.special, c.contentLen = false, false, 0
	c.state = qsHeaders
	c.out = append(c.out, EncodeQuerySpaces(line)...)
}

func (c *qsConn) headerLine(line []byte) {
	c.out = append(c.out, line...)

	if isBlankLine(line) {
		c.endHeaders()

		return
	}

	if line[0] == ' ' || line[0] == '\t' {
		c.state = qsPassthrough

		return
	}

	name, val, ok := bytes.Cut(line, []byte(":"))
	if !ok {
		c.state = qsPassthrough

		return
	}

	switch string(bytes.ToLower(bytes.TrimSpace(name))) {
	case "transfer-encoding", "upgrade":
		c.special = true
	case "content-length":
		v := bytes.TrimSpace(val)
		if c.clSeen || !allDigits(v) {
			c.state = qsPassthrough

			return
		}

		n, err := strconv.ParseInt(string(v), 10, 64)
		if err != nil {
			c.state = qsPassthrough

			return
		}

		c.clSeen, c.contentLen = true, n
	}
}

func (c *qsConn) endHeaders() {
	switch {
	case c.special || c.connect:
		c.state = qsPassthrough
	case c.contentLen > 0:
		c.state, c.bodyLeft = qsBody, c.contentLen
	default:
		c.state = qsRequestLine
	}
}

func isBlankLine(l []byte) bool {
	return len(l) == 1 || (len(l) == 2 && l[0] == '\r')
}

func allDigits(v []byte) bool {
	if len(v) == 0 || len(v) > maxContentLenDg {
		return false
	}

	for _, b := range v {
		if b < '0' || b > '9' {
			return false
		}
	}

	return true
}

// EncodeQuerySpaces returns the request line (terminator included) with ASCII
// spaces in the origin-form query encoded as %20. A line that is not exactly
// `METHOD SP /path?query SP HTTP/1.x` with a clean target is returned as is.
func EncodeQuerySpaces(line []byte) []byte {
	body := bytes.TrimSuffix(line, []byte("\n"))
	body = bytes.TrimSuffix(body, []byte("\r"))

	if bytes.Count(body, []byte(" ")) <= maxRequestLineSpaces {
		return line
	}

	sp := bytes.IndexByte(body, ' ')
	last := bytes.LastIndexByte(body, ' ')

	method, target, version := body[:sp], body[sp+1:last], body[last+1:]
	if !validMethod(method) || len(version) != versionLen ||
		(string(version) != "HTTP/1.1" && string(version) != "HTTP/1.0") {
		return line
	}

	q := bytes.IndexByte(target, '?')
	if q < 0 || target[0] != '/' || bytes.IndexByte(target[:q], ' ') >= 0 {
		return line
	}

	for _, b := range target {
		if b < ' ' || b == 0x7f {
			return line
		}
	}

	out := make([]byte, 0, len(line)+2*bytes.Count(target, []byte(" ")))
	out = append(out, method...)
	out = append(out, ' ')
	out = append(out, target[:q]...)
	out = append(out, bytes.ReplaceAll(target[q:], []byte(" "), []byte(encodedSpace))...)
	out = append(out, ' ')
	out = append(out, version...)
	out = append(out, line[len(body):]...)

	return out
}

func validMethod(m []byte) bool {
	if len(m) == 0 || len(m) > maxMethodLen {
		return false
	}

	for _, b := range m {
		if b < 'A' || b > 'Z' {
			return false
		}
	}

	return true
}
