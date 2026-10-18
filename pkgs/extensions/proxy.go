package extensions

import (
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const amzTarget = "X-Amz-Target"

// newProxy builds a reverse proxy to ep that keeps the path, headers, body and Host.
func (m *Manager) newProxy(ep *url.URL) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(ep)
			pr.Out.URL.Path, pr.Out.URL.RawPath = pr.In.URL.Path, pr.In.URL.RawPath
			pr.Out.Host = pr.In.Host
			pr.SetXForwarded()
		},
		Transport:     m.transport,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			m.log.WarnContext(r.Context(), "extension proxy error", "endpoint", ep.Host, "error", err)
			writeJSON(w, http.StatusBadGateway, map[string]string{keyMessage: "extension unreachable"})
		},
	}
}

// Route returns the handler for r if an extension claims it, else nil. shadowed means a
// built-in service already matched; only allow_override extensions may take such requests.
func (m *Manager) Route(r *http.Request, shadowed bool) http.Handler {
	m.mu.RLock("Route")
	defer m.mu.RUnlock()

	for _, e := range m.exts {
		if e.reg == nil || (shadowed && !e.spec.AllowOverride) || !claims(e.reg.Routes, r) {
			continue
		}

		if e.status != StatusReady {
			name := e.spec.Name

			return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(
					w,
					http.StatusServiceUnavailable,
					map[string]string{keyMessage: "extension " + name + " unavailable"},
				)
			})
		}

		return e.proxy
	}

	return nil
}

func claims(rt Routes, r *http.Request) bool {
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	for _, h := range rt.Hosts {
		if strs.Equal(h, host) {
			return true
		}
	}

	if t := r.Header.Get(amzTarget); t != "" {
		for _, p := range rt.TargetPrefixes {
			if strings.HasPrefix(t, p) {
				return true
			}
		}
	}

	for _, p := range rt.PathPrefixes {
		if strings.HasPrefix(r.URL.Path, p) {
			return true
		}
	}

	return false
}
