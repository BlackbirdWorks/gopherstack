package httputils_test

import (
	"bufio"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

type pipeListener struct{ ch chan net.Conn }

func (l *pipeListener) Accept() (net.Conn, error) {
	c, ok := <-l.ch
	if !ok {
		return nil, net.ErrClosed
	}

	return c, nil
}
func (l *pipeListener) Close() error   { return nil }
func (l *pipeListener) Addr() net.Addr { return &net.TCPAddr{} }

// wire feeds chunks through the wrapper and returns what the server side reads.
func wire(t *testing.T, chunks ...string) string {
	t.Helper()

	client, server := net.Pipe()
	pln := &pipeListener{ch: make(chan net.Conn, 1)}
	wrapped := httputils.NewQuerySpaceListener(pln)
	pln.ch <- server

	done := make(chan string, 1)

	go func() {
		c, err := wrapped.Accept()
		if err != nil {
			done <- ""

			return
		}

		b, _ := io.ReadAll(c)
		done <- string(b)
	}()

	for _, ch := range chunks {
		_, err := client.Write([]byte(ch))
		require.NoError(t, err)
	}

	require.NoError(t, client.Close())

	select {
	case got := <-done:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")

		return ""
	}
}

type connCase struct {
	name   string
	want   string
	chunks []string
}

func same(name, in string) connCase { return connCase{name: name, chunks: []string{in}, want: in} }

func TestQuerySpaceConn(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", 9<<10)
	req := "GET /p?$filter=a eq 'b' HTTP/1.1\r\nHost: x\r\n\r\n"
	enc := "GET /p?$filter=a%20eq%20'b' HTTP/1.1\r\nHost: x\r\n\r\n"

	inner := "GET /body?x y HTTP/1.1\r\nHost: z\r\n\r\n"
	cl := strconv.Itoa(len(inner))

	tests := []connCase{
		{name: "query spaces", chunks: []string{req}, want: enc},
		same("no spaces untouched", "GET /p?a=1 HTTP/1.1\r\n\r\n"),
		{name: "bare lf", chunks: []string{"GET /p?a b HTTP/1.1\n\n"}, want: "GET /p?a%20b HTTP/1.1\n\n"},
		{
			name:   "split writes",
			chunks: []string{"GET /p?$fil", "ter=a eq", " 'b' HTTP/1.1\r\nHo", "st: x\r\n\r\n"},
			want:   enc,
		},
		{
			name:   "space in path untouched",
			chunks: []string{"GET /a b?c d HTTP/1.1\r\n\r\n"},
			want:   "GET /a b?c d HTTP/1.1\r\n\r\n",
		},
		same("no query untouched", "GET /a b HTTP/1.1\r\n\r\n"),
		same("bad version untouched", "GET /p?a b HTTP/2.0\r\n\r\n"),
		same("bad method untouched", "get /p?a b HTTP/1.1\r\n\r\n"),
		same("cr injection untouched", "GET /p?a\rb c HTTP/1.1\r\n\r\n"),
		same("tab untouched", "GET /p?a\tb c HTTP/1.1\r\n\r\n"),
		same("nul untouched", "GET /p?a\x00b c HTTP/1.1\r\n\r\n"),
		same("absolute form untouched", "GET http://h/p?a b HTTP/1.1\r\n\r\n"),
		same("trailing space untouched", "GET /p?a b HTTP/1.1 \r\n\r\n"),
		{
			name:   "path not altered by header injection attempt",
			chunks: []string{"GET /p?a HTTP/1.1\r\nX: GET /q?a b HTTP/1.1\r\n\r\n"},
			want:   "GET /p?a HTTP/1.1\r\nX: GET /q?a b HTTP/1.1\r\n\r\n",
		},
		{
			name:   "pipelined",
			chunks: []string{req + req},
			want:   enc + enc,
		},
		{
			name: "body is opaque then next request encoded",
			chunks: []string{
				"POST /p?a b HTTP/1.1\r\nContent-Length: " + cl + "\r\n\r\n" + inner + req,
			},
			want: "POST /p?a%20b HTTP/1.1\r\nContent-Length: " + cl + "\r\n\r\n" + inner + enc,
		},
		{
			name:   "chunked disables rewriting",
			chunks: []string{"POST /p?a b HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n" + req},
			want:   "POST /p?a%20b HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n" + req,
		},
		{
			name:   "duplicate content-length disables rewriting",
			chunks: []string{"POST /p?a b HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx" + req},
			want:   "POST /p?a%20b HTTP/1.1\r\nContent-Length: 1\r\nContent-Length: 1\r\n\r\nx" + req,
		},
		{
			name:   "oversized request line passes through bounded",
			chunks: []string{"GET /p?" + long + " b HTTP/1.1\r\n\r\n" + req},
			want:   "GET /p?" + long + " b HTTP/1.1\r\n\r\n" + req,
		},
		{
			name:   "garbage",
			chunks: []string{"\x16\x03\x01 not http \xff\xfe"},
			want:   "\x16\x03\x01 not http \xff\xfe",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, wire(t, tt.chunks...))
		})
	}
}

func TestQuerySpaceServer(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, r.URL.Query().Get("$filter")+"|"+r.URL.Path)
		}),
	}

	go func() { _ = srv.Serve(httputils.NewQuerySpaceListener(ln)) }()

	t.Cleanup(func() { _ = srv.Close() })

	tests := []struct {
		name     string
		send     string
		wantBody []string
		wantCode []int
	}{
		{
			name: "filter decoded, keep-alive",
			send: "GET /s?$filter=principalId eq 'x' HTTP/1.1\r\nHost: h\r\n\r\n" +
				"GET /t?$filter=a b HTTP/1.1\r\nHost: h\r\n\r\n",
			wantCode: []int{200, 200},
			wantBody: []string{"principalId eq 'x'|/s", "a b|/t"},
		},
		{name: "space in path rejected", send: "GET /a b?c=d HTTP/1.1\r\nHost: h\r\n\r\n", wantCode: []int{400}},
		{name: "control byte rejected", send: "GET /a?b\x01 c HTTP/1.1\r\nHost: h\r\n\r\n", wantCode: []int{400}},
		{name: "cr in target rejected", send: "GET /a?b\rc d HTTP/1.1\r\nHost: h\r\n\r\n", wantCode: []int{400}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, derr := net.Dial("tcp", ln.Addr().String())
			require.NoError(t, derr)

			defer c.Close()

			_, werr := io.WriteString(c, tt.send)
			require.NoError(t, werr)

			br := bufio.NewReader(c)

			for i, code := range tt.wantCode {
				resp, rerr := http.ReadResponse(br, nil)
				require.NoError(t, rerr)

				assert.Equal(t, code, resp.StatusCode)

				if tt.wantBody != nil {
					b, _ := io.ReadAll(resp.Body)
					assert.Equal(t, tt.wantBody[i], string(b))
				}

				_ = resp.Body.Close()
			}
		})
	}
}

func TestEncodeQuerySpaces(t *testing.T) {
	t.Parallel()

	got := httputils.EncodeQuerySpaces([]byte("GET /p?a  b HTTP/1.0\r\n"))
	assert.Equal(t, []byte("GET /p?a%20%20b HTTP/1.0\r\n"), got)
	assert.Equal(t, []byte("GET /p?a HTTP/1.1\r\n"), httputils.EncodeQuerySpaces([]byte("GET /p?a HTTP/1.1\r\n")))
}

type errConn struct {
	net.Conn

	data []byte
	errs []error
}

func (c *errConn) Read(p []byte) (int, error) {
	if len(c.errs) == 0 {
		return 0, io.EOF
	}

	err := c.errs[0]
	c.errs = c.errs[1:]
	n := copy(p, c.data)
	c.data = nil

	return n, err
}

func TestQuerySpaceConnReadSemantics(t *testing.T) {
	t.Parallel()

	errBoom := io.ErrUnexpectedEOF

	tests := []struct {
		wantErr error
		name    string
		data    string
		zeroBuf bool
	}{
		{name: "error with data is surfaced", data: "GET /p HTTP/1.1\r\n\r\n", wantErr: errBoom},
		{name: "error with partial line is surfaced", data: "GET /p?a b", wantErr: errBoom},
		{name: "zero length read returns", zeroBuf: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, server := net.Pipe()
			t.Cleanup(func() { _ = client.Close(); _ = server.Close() })

			pln := &pipeListener{ch: make(chan net.Conn, 1)}
			pln.ch <- &errConn{Conn: server, data: []byte(tt.data), errs: []error{errBoom}}

			c, err := httputils.NewQuerySpaceListener(pln).Accept()
			require.NoError(t, err)

			done := make(chan error, 1)

			go func() {
				if tt.zeroBuf {
					_, rerr := c.Read(nil)
					done <- rerr

					return
				}

				var got []byte

				buf := make([]byte, 64)

				for {
					n, rerr := c.Read(buf)
					got = append(got, buf[:n]...)

					if rerr != nil {
						assert.Equal(t, tt.data, string(got))
						done <- rerr

						return
					}
				}
			}()

			select {
			case rerr := <-done:
				if tt.wantErr == nil {
					require.NoError(t, rerr)
				} else {
					require.ErrorIs(t, rerr, tt.wantErr)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("read did not return")
			}
		})
	}
}

func TestQuerySpaceConnCloseWrite(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		c, derr := net.Dial("tcp", ln.Addr().String())
		if derr == nil {
			defer c.Close()
			_, _ = io.Copy(io.Discard, c)
		}
	}()

	c, err := httputils.NewQuerySpaceListener(ln).Accept()
	require.NoError(t, err)

	defer c.Close()

	cw, ok := c.(interface{ CloseWrite() error })
	require.True(t, ok, "wrapped conn must expose CloseWrite")
	require.NoError(t, cw.CloseWrite())
}

func TestQuerySpaceTLS(t *testing.T) {
	t.Parallel()

	ts := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(ts.Close)

	rawLn, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	tlsLn := tls.NewListener(rawLn, &tls.Config{Certificates: ts.TLS.Certificates, MinVersion: tls.VersionTLS12})

	srv := &http.Server{
		ReadHeaderTimeout: 5 * time.Second,
		ConnContext:       httputils.QuerySpaceConnContext,
		Handler: httputils.QuerySpaceTLS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.WriteString(w, strconv.FormatBool(r.TLS != nil && r.TLS.HandshakeComplete)+"|"+
				r.URL.Query().Get("f")+"|"+r.Method)
		})),
	}

	go func() { _ = srv.Serve(httputils.NewQuerySpaceListener(tlsLn)) }()

	t.Cleanup(func() { _ = srv.Close() })

	chunkedThenGet := "POST /a?f=1 HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n" +
		"GET /b?f=2 HTTP/1.1\r\nHost: h\r\n\r\n"

	tests := []struct {
		name string
		send string
		want []string
	}{
		{
			name: "keep-alive requests all see tls",
			send: "GET /a?f=x y HTTP/1.1\r\nHost: h\r\n\r\nGET /a?f=p q HTTP/1.1\r\nHost: h\r\n\r\n",
			want: []string{"true|x y|GET", "true|p q|GET"},
		},
		{name: "after passthrough still tls", send: chunkedThenGet, want: []string{"true|1|POST", "true|2|GET"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c, derr := tls.Dial("tcp", rawLn.Addr().String(), &tls.Config{
				InsecureSkipVerify: true,
				NextProtos:         []string{"h2", "http/1.1"},
			})
			require.NoError(t, derr)

			defer c.Close()

			assert.NotEqual(t, "h2", c.ConnectionState().NegotiatedProtocol)

			_, werr := io.WriteString(c, tt.send)
			require.NoError(t, werr)

			br := bufio.NewReader(c)

			for _, want := range tt.want {
				resp, rerr := http.ReadResponse(br, nil)
				require.NoError(t, rerr)

				b, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()

				assert.Equal(t, want, string(b))
			}
		})
	}
}
