package httputils_test

import (
	"bufio"
	"io"
	"net"
	"net/http"
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

func TestQuerySpaceConn(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", 9<<10)
	req := "GET /p?$filter=a eq 'b' HTTP/1.1\r\nHost: x\r\n\r\n"
	enc := "GET /p?$filter=a%20eq%20'b' HTTP/1.1\r\nHost: x\r\n\r\n"

	inner := "GET /body?x y HTTP/1.1\r\nHost: z\r\n\r\n"
	cl := strconv.Itoa(len(inner))

	tests := []struct {
		name   string
		want   string
		chunks []string
	}{
		{name: "query spaces", chunks: []string{req}, want: enc},
		{name: "no spaces untouched", chunks: []string{"GET /p?a=1 HTTP/1.1\r\n\r\n"}, want: "GET /p?a=1 HTTP/1.1\r\n\r\n"},
		{name: "bare lf", chunks: []string{"GET /p?a b HTTP/1.1\n\n"}, want: "GET /p?a%20b HTTP/1.1\n\n"},
		{name: "split writes", chunks: []string{"GET /p?$fil", "ter=a eq", " 'b' HTTP/1.1\r\nHo", "st: x\r\n\r\n"}, want: enc},
		{
			name:   "space in path untouched",
			chunks: []string{"GET /a b?c d HTTP/1.1\r\n\r\n"},
			want:   "GET /a b?c d HTTP/1.1\r\n\r\n",
		},
		{name: "no query untouched", chunks: []string{"GET /a b HTTP/1.1\r\n\r\n"}, want: "GET /a b HTTP/1.1\r\n\r\n"},
		{name: "bad version untouched", chunks: []string{"GET /p?a b HTTP/2.0\r\n\r\n"}, want: "GET /p?a b HTTP/2.0\r\n\r\n"},
		{name: "bad method untouched", chunks: []string{"get /p?a b HTTP/1.1\r\n\r\n"}, want: "get /p?a b HTTP/1.1\r\n\r\n"},
		{name: "cr injection untouched", chunks: []string{"GET /p?a\rb c HTTP/1.1\r\n\r\n"}, want: "GET /p?a\rb c HTTP/1.1\r\n\r\n"},
		{name: "tab untouched", chunks: []string{"GET /p?a\tb c HTTP/1.1\r\n\r\n"}, want: "GET /p?a\tb c HTTP/1.1\r\n\r\n"},
		{name: "nul untouched", chunks: []string{"GET /p?a\x00b c HTTP/1.1\r\n\r\n"}, want: "GET /p?a\x00b c HTTP/1.1\r\n\r\n"},
		{name: "absolute form untouched", chunks: []string{"GET http://h/p?a b HTTP/1.1\r\n\r\n"}, want: "GET http://h/p?a b HTTP/1.1\r\n\r\n"},
		{name: "trailing space untouched", chunks: []string{"GET /p?a b HTTP/1.1 \r\n\r\n"}, want: "GET /p?a b HTTP/1.1 \r\n\r\n"},
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
			name:     "filter decoded, keep-alive",
			send:     "GET /s?$filter=principalId eq 'x' HTTP/1.1\r\nHost: h\r\n\r\nGET /t?$filter=a b HTTP/1.1\r\nHost: h\r\n\r\n",
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

	assert.Equal(t, []byte("GET /p?a%20%20b HTTP/1.0\r\n"), httputils.EncodeQuerySpaces([]byte("GET /p?a  b HTTP/1.0\r\n")))
	assert.Equal(t, []byte("GET /p?a HTTP/1.1\r\n"), httputils.EncodeQuerySpaces([]byte("GET /p?a HTTP/1.1\r\n")))
}
