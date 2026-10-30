package gcprun

import (
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"

	"cloud.google.com/go/run/apiv2/runpb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpscale"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

const localhostSuffix = ".localhost"

// IngressHandler serves <service>-<hash>-<region>.localhost (and <tag>---<that>) by proxying to the container
// of the revision picked from the service's traffic split.
func (b *InMemoryBackend) IngressHandler() http.Handler {
	return http.HandlerFunc(b.serveIngress)
}

func hostOnly(hostport string) string {
	if h, _, err := net.SplitHostPort(hostport); err == nil {
		hostport = h
	}

	return strings.ToLower(hostport)
}

func (b *InMemoryBackend) serveIngress(w http.ResponseWriter, r *http.Request) {
	host, ok := strings.CutSuffix(hostOnly(r.Host), localhostSuffix)
	if !ok {
		http.Error(w, "unknown host; use <service>-<hash>-<region>.localhost", http.StatusNotFound)

		return
	}

	tag, label := "", host
	if t, rest, found := strings.Cut(host, tagSeparator); found {
		tag, label = t, rest
	}

	revName, err := b.route(label, tag)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)

		return
	}

	if revName == "" {
		http.Error(w, "service has no ready revision to serve traffic", http.StatusServiceUnavailable)

		return
	}

	in, release, err := b.acquire(r.Context(), revName)
	if err != nil {
		code := http.StatusBadGateway

		switch {
		case errors.Is(err, errNoRuntime), errors.Is(err, errRevisionGone), errors.Is(err, gcpscale.ErrRetired):
			code = http.StatusServiceUnavailable
		case errors.Is(err, gcpscale.ErrSaturated):
			code = http.StatusTooManyRequests
		}

		http.Error(w, "revision is not serving: "+err.Error(), code)

		return
	}

	defer release()

	b.serveLogged(w, r, in.hostPort, b.ingressTarget(revName, in))
}

func (b *InMemoryBackend) ingressTarget(revName string, in *inst) gcpserve.LogTarget {
	b.mu.RLock("ingressTarget")
	defer b.mu.RUnlock()

	if rev := b.revisions[revName]; rev != nil {
		return revisionLogTarget(rev.pb, in.id())
	}

	return gcpserve.LogTarget{}
}

func (b *InMemoryBackend) proxy(port int) *httputil.ReverseProxy {
	target := &url.URL{Scheme: "http", Host: net.JoinHostPort(container.LoopbackHost, strconv.Itoa(port))}

	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = pr.In.Host
			pr.SetXForwarded()
		},
		Transport: b.transport,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, "container unreachable: "+err.Error(), http.StatusBadGateway)
		},
	}
}

// route returns the full revision name that should serve the next request for the service with the DNS label.
func (b *InMemoryBackend) route(label, tag string) (string, error) {
	b.mu.RLock("route")
	defer b.mu.RUnlock()

	for name, rec := range b.services {
		n, err := parseName(name, "services")
		if err != nil || hostLabel(n) != label {
			continue
		}

		short := pickRevision(rec.pb.GetTrafficStatuses(), tag, b.rr.Add(1)-1)
		if short == "" {
			if tag != "" {
				return "", errUnknownTag
			}

			return "", nil
		}

		return name + "/revisions/" + short, nil
	}

	return "", errUnknownService
}

var (
	errUnknownService = errors.New("unknown service")
	errUnknownTag     = errors.New("unknown traffic tag")
)

// pickRevision selects a revision by tag, or by walking the percent buckets with a rotating counter.
func pickRevision(statuses []*runpb.TrafficTargetStatus, tag string, n uint64) string {
	if tag != "" {
		for _, st := range statuses {
			if st.GetTag() == tag {
				return st.GetRevision()
			}
		}

		return ""
	}

	slot := int32(n % fullPercent)

	var acc int32

	for _, st := range statuses {
		acc += st.GetPercent()
		if slot < acc {
			return st.GetRevision()
		}
	}

	return ""
}
