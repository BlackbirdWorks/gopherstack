package gcpworkflows

import (
	"net/http"
	"net/url"
	"strings"
)

// googleHost names the emulated service behind a public REST host.
func googleHost(host string) (string, bool) {
	switch strings.ToLower(host) {
	case "pubsub.googleapis.com":
		return svcPubSub, true
	case "secretmanager.googleapis.com":
		return svcSecretManager, true
	case "firestore.googleapis.com":
		return svcFirestore, true
	case "storage.googleapis.com":
		return svcStorage, true
	default:
		return "", false
	}
}

// emulatedHosts sends http.* calls aimed at an emulated service's googleapis.com host to that emulator.
type emulatedHosts struct {
	b    *InMemoryBackend
	next http.RoundTripper
}

func (t emulatedHosts) RoundTrip(req *http.Request) (*http.Response, error) {
	svc, ok := googleHost(req.URL.Hostname())
	if !ok {
		return t.next.RoundTrip(req)
	}

	t.b.mu.RLock("emulatedHosts")
	eps := t.b.endpoints
	t.b.mu.RUnlock()

	if eps == nil {
		return t.next.RoundTrip(req)
	}

	base, err := url.Parse(eps.BaseURL(svc))
	if err != nil || base.Host == "" {
		return t.next.RoundTrip(req)
	}

	out := req.Clone(req.Context())
	out.URL.Scheme, out.URL.Host, out.Host = base.Scheme, base.Host, base.Host

	return t.next.RoundTrip(out)
}

// CloseIdleConnections releases the pooled connections of the wrapped transport.
func (t emulatedHosts) CloseIdleConnections() {
	if ci, ok := t.next.(interface{ CloseIdleConnections() }); ok {
		ci.CloseIdleConnections()
	}
}
