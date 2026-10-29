package mcpserver

import (
	"crypto/subtle"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const inProcessBase = "http://gopherstack.internal"

// HandlerDoer returns a Doer that dispatches requests straight into h.
func HandlerDoer(h http.Handler) Doer { return handlerDoer{h: h} }

type handlerDoer struct{ h http.Handler }

func (d handlerDoer) Do(req *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	req.RemoteAddr = "127.0.0.1:0"
	d.h.ServeHTTP(rec, req)

	return rec.Result(), nil
}

// InProcessOptions returns Options for a server mounted inside the Gopherstack process.
func InProcessOptions(admin http.Handler, version string) Options {
	return Options{Doer: HandlerDoer(admin), BaseURL: inProcessBase, Version: version}
}

// NewHTTPHandler serves the streamable-HTTP MCP transport behind Auth.
//
// With a non-empty token every request must carry it as a Bearer credential.
// Without one, only loopback clients are accepted.
func NewHTTPHandler(opts Options, token string) http.Handler {
	srv := New(opts)
	inner := mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil)

	return Auth(token, inner)
}

// Auth guards next with a bearer token, or with a loopback-only check when token is empty.
func Auth(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if token == "" {
			if !isLoopback(r.RemoteAddr) {
				http.Error(
					w,
					"MCP endpoint is loopback-only; set a token to allow remote clients",
					http.StatusForbidden,
				)

				return
			}

			next.ServeHTTP(w, r)

			return
		}

		got, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="gopherstack-mcp"`)
			http.Error(w, "missing or invalid bearer token", http.StatusUnauthorized)

			return
		}

		next.ServeHTTP(w, r)
	})
}

func isLoopback(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}

	ip := net.ParseIP(host)

	return ip != nil && ip.IsLoopback()
}
