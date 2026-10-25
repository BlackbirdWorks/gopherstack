package azurefunctions

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	invokeTimeout = 230 * time.Second
	welcomePage   = "<!DOCTYPE html><html><head><title>Your Azure Function App is up and running</title></head>" +
		"<body><h1>Your Azure Function App is up and running.</h1>" +
		"<p>Deploy your code to this app to start serving functions.</p></body></html>"
	scmLabel = "scm"
)

func hostOnly(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		hostport = h
	}

	return strings.ToLower(hostport)
}

// target is the app a request addresses and whether it addressed the app's Kudu (scm) site.
type target struct {
	name string
	scm  bool
}

// resolve maps the request Host to an app: <app>.<domain> or <app>.scm.<domain>, else a first-label match, else
// the only app.
func (b *InMemoryBackend) resolve(host string) (target, bool) {
	b.mu.RLock("resolve")
	defer b.mu.RUnlock()

	host = hostOnly(host)

	if net.ParseIP(host) == nil {
		if rest, ok := strings.CutSuffix(host, "."+strings.ToLower(b.domain)); ok {
			name, scm := strings.CutSuffix(rest, "."+scmLabel)
			if _, found := b.apps[strs.Fold(name)]; found {
				return target{name: b.apps[strs.Fold(name)].Name, scm: scm}, true
			}

			return target{}, false
		}

		if label, _, ok := strings.Cut(host, "."); ok {
			if a, found := b.apps[strs.Fold(label)]; found {
				return target{name: a.Name}, true
			}
		}
	}

	if len(b.apps) == 1 {
		for _, a := range b.apps {
			return target{name: a.Name}, true
		}
	}

	return target{}, false
}

// explicitAppHost reports whether host names an app's main (non-scm) site by its own label.
func (b *InMemoryBackend) explicitAppHost(host string) bool {
	b.mu.RLock("explicitAppHost")
	defer b.mu.RUnlock()

	rest, ok := strings.CutSuffix(hostOnly(host), "."+strings.ToLower(b.domain))

	return ok && !strings.HasSuffix(rest, "."+scmLabel)
}

// ServeHTTP routes a request to an app's function host or its Kudu API.
func (b *InMemoryBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "Kestrel")

	t, ok := b.resolve(r.Host)
	if !ok {
		http.Error(w, "no function app is served on this host", http.StatusNotFound)

		return
	}

	if t.scm || (isKuduPath(r.URL.Path) && !b.explicitAppHost(r.Host)) {
		b.serveKudu(w, r, t.name)

		return
	}

	b.serveInvoke(w, r, t.name)
}

func (b *InMemoryBackend) serveInvoke(w http.ResponseWriter, r *http.Request, name string) {
	b.mu.RLock("serveInvoke")

	a, err := b.appLocked(name)
	if err != nil {
		b.mu.RUnlock()
		http.Error(w, err.Error(), http.StatusNotFound)

		return
	}

	servable, stopped := a.servable(), a.Stopped
	b.mu.RUnlock()

	switch {
	case stopped:
		http.Error(w, "Error 403 - This web app is stopped.", http.StatusForbidden)

		return
	case !servable:
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(welcomePage))

			return
		}

		http.NotFound(w, r)

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), invokeTimeout)
	defer cancel()

	port, err := b.ensureStarted(ctx, name)
	if err != nil {
		code := http.StatusBadGateway
		if errors.Is(err, errNoRuntime) || errors.Is(err, errUnsupportedRT) {
			code = http.StatusServiceUnavailable
		}

		http.Error(w, "function host is not serving: "+err.Error(), code)

		return
	}

	gcpserve.Proxy(port, b.transport).ServeHTTP(w, r.WithContext(ctx))
}
