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
	notFoundPage  = "Error 404 - Web app not found."
	invokeTimeout = 230 * time.Second
	welcomePage   = "<!DOCTYPE html><html><head><title>Your Azure Function App is up and running</title></head>" +
		"<body><h1>Your Azure Function App is up and running.</h1>" +
		"<p>Deploy your code to this app to start serving functions.</p></body></html>"
	scmLabel = "scm"

	webWelcomePage = "<!DOCTYPE html><html><head><title>Your web app is running and waiting for your content" +
		"</title></head><body><h1>Your web app is running and waiting for your content.</h1>" +
		"<p>Deploy your code to this app to start serving it.</p></body></html>"
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

// resolve maps the request Host to an app: <app>.<domain> or <app>.scm.<domain>. Any other host is unknown.
func (b *InMemoryBackend) resolve(host string) (target, bool) {
	b.mu.RLock("resolve")
	defer b.mu.RUnlock()

	rest, ok := strings.CutSuffix(hostOnly(host), "."+strings.ToLower(b.domain))
	if !ok {
		return target{}, false
	}

	label, scm := strings.CutSuffix(rest, "."+scmLabel)
	if a, found := b.apps[strs.Fold(label)]; found && a.Slot == "" {
		return target{name: a.key(), scm: scm}, true
	}

	for _, a := range b.apps {
		if a.Slot != "" && strs.Equal(a.label(), label) {
			return target{name: a.key(), scm: scm}, true
		}
	}

	return target{}, false
}

// ServeHTTP routes a request to an app's function host or its Kudu API.
func (b *InMemoryBackend) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Server", "Kestrel")

	t, ok := b.resolve(r.Host)
	if !ok {
		w.Header().Set("Content-Type", "text/html")
		http.Error(w, notFoundPage, http.StatusNotFound)

		return
	}

	if t.scm {
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

	servable, stopped, web := a.servable(), a.Stopped, a.isWeb()
	b.mu.RUnlock()

	page := welcomePage
	if web {
		page = webWelcomePage
	}

	switch {
	case stopped:
		http.Error(w, "Error 403 - This web app is stopped.", http.StatusForbidden)

		return
	case !servable:
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte(page))

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

		what := "function host"
		if web {
			what = "web app"
		}

		http.Error(w, what+" is not serving: "+err.Error(), code)

		return
	}

	gcpserve.Proxy(port, b.transport).ServeHTTP(w, r.WithContext(ctx))
}
