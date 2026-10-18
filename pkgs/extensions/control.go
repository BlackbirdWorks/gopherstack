package extensions

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

const (
	maxRegistrationBytes = 1 << 20
	reservedPrefix       = "/_gopherstack"
	tokenHeader          = "X-Gopherstack-Extension-Token" //nolint:gosec // header name, not a credential
	keyMessage           = "message"
	bearerPrefix         = "Bearer "
)

// ErrInvalidRegistration is returned for a rejected registration payload.
var ErrInvalidRegistration = errors.New("invalid registration")

// Routes are the requests an extension claims.
type Routes struct {
	PathPrefixes   []string `json:"path_prefixes,omitempty"`
	Hosts          []string `json:"hosts,omitempty"`
	TargetPrefixes []string `json:"target_prefixes,omitempty"`
}

// RequestHook asks gopherstack to call the extension around matched services.
type RequestHook struct {
	FailOpen  *bool    `json:"fail_open,omitempty"`
	Pre       string   `json:"pre,omitempty"`
	Post      string   `json:"post,omitempty"`
	Services  []string `json:"services"`
	TimeoutMs int      `json:"timeout_ms,omitempty"`
}

// Hooks are the extension's lifecycle and request callbacks (paths on its endpoint).
type Hooks struct {
	OnStartup  string        `json:"on_startup,omitempty"`
	OnReady    string        `json:"on_ready,omitempty"`
	OnShutdown string        `json:"on_shutdown,omitempty"`
	Request    []RequestHook `json:"request,omitempty"`
}

// Registration is the body of POST /_gopherstack/extensions/register.
type Registration struct {
	Name     string `json:"name"`
	Version  string `json:"version,omitempty"`
	Endpoint string `json:"endpoint,omitempty"`
	Routes   Routes `json:"routes"`
	Hooks    Hooks  `json:"hooks"`
}

// Info is the public, token-free view of an extension.
type Info struct {
	Registration *Registration `json:"registration,omitempty"`
	Name         string        `json:"name"`
	Type         string        `json:"type"`
	Status       Status        `json:"status"`
	Endpoint     string        `json:"endpoint,omitempty"`
	LastError    string        `json:"last_error,omitempty"`
	Restarts     int           `json:"restarts"`
}

// List returns the management view of all extensions.
func (m *Manager) List() []Info {
	m.mu.RLock("List")
	defer m.mu.RUnlock()

	out := make([]Info, 0, len(m.exts))

	for _, e := range m.exts {
		in := Info{Name: e.spec.Name, Type: "process", Status: e.status, Restarts: e.restarts, LastError: e.lastError}
		if e.spec.Image != "" {
			in.Type = "container"
		}

		if e.reg != nil {
			r := *e.reg
			in.Registration, in.Endpoint = &r, e.endpoint.String()
		}

		out = append(out, in)
	}

	return out
}

// ServeList handles GET /_gopherstack/extensions.
func (m *Manager) ServeList(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"extensions": m.List()})
}

// ServeRegister handles POST /_gopherstack/extensions/register.
func (m *Manager) ServeRegister(w http.ResponseWriter, r *http.Request) {
	e := m.authenticate(r)
	if e == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{keyMessage: "invalid extension token"})

		return
	}

	if e.spec.Image == "" && !isLoopback(r.RemoteAddr) {
		writeJSON(w, http.StatusForbidden, map[string]string{keyMessage: "control API is loopback-only"})

		return
	}

	var reg Registration

	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRegistrationBytes))
	if err := dec.Decode(&reg); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{keyMessage: "invalid JSON: " + err.Error()})

		return
	}

	if err := m.register(e, reg); err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, errClaimConflict) {
			code = http.StatusConflict
		}

		writeJSON(w, code, map[string]string{keyMessage: err.Error()})

		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"name": e.spec.Name, "status": string(StatusReady)})
}

func (m *Manager) authenticate(r *http.Request) *extension {
	tok := r.Header.Get(tokenHeader)
	if tok == "" {
		tok = strings.TrimPrefix(r.Header.Get("Authorization"), bearerPrefix)
	}

	if tok == "" {
		return nil
	}

	m.mu.RLock("authenticate")
	defer m.mu.RUnlock()

	var found *extension

	for _, e := range m.exts {
		if e.token != "" && subtle.ConstantTimeCompare([]byte(e.token), []byte(tok)) == 1 {
			found = e
		}
	}

	return found
}

func isLoopback(remote string) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}

	ip := net.ParseIP(host)

	return ip != nil && ip.IsLoopback()
}

func (m *Manager) register(e *extension, reg Registration) error {
	if reg.Name != "" && reg.Name != e.spec.Name {
		return fmt.Errorf("%w: name %q does not match token owner", ErrInvalidRegistration, reg.Name)
	}

	reg.Name = e.spec.Name

	m.mu.Lock("register")
	defer m.mu.Unlock()

	ep, err := validateRegistration(e, &reg)
	if err != nil {
		return err
	}

	if err = m.checkConflicts(e, reg.Routes); err != nil {
		return err
	}

	e.reg, e.endpoint, e.proxy = &reg, ep, m.newProxy(ep)
	e.status, e.lastError = StatusReady, ""

	select {
	case <-e.registered:
	default:
		close(e.registered)
	}

	return nil
}

func validateRegistration(e *extension, reg *Registration) (*url.URL, error) {
	raw := reg.Endpoint
	if raw == "" {
		raw = "http://127.0.0.1:" + strconv.Itoa(e.port)
		reg.Endpoint = raw
	}

	ep, err := url.Parse(raw)
	if err != nil || (ep.Scheme != "http" && ep.Scheme != "https") || ep.Host == "" {
		return nil, fmt.Errorf("%w: endpoint %q", ErrInvalidRegistration, raw)
	}

	if h := ep.Hostname(); h != "localhost" && !isLoopback(h) {
		return nil, fmt.Errorf("%w: endpoint must be loopback", ErrInvalidRegistration)
	}

	for _, p := range reg.Routes.PathPrefixes {
		if !strings.HasPrefix(p, "/") || (len(p) < 2 && !e.spec.AllowOverride) {
			return nil, fmt.Errorf("%w: path prefix %q", ErrInvalidRegistration, p)
		}

		if strings.HasPrefix(p, reservedPrefix) || strings.HasPrefix(reservedPrefix, p) {
			return nil, fmt.Errorf("%w: path prefix %q overlaps %s", ErrInvalidRegistration, p, reservedPrefix)
		}
	}

	if slices.Contains(slices.Concat(reg.Routes.Hosts, reg.Routes.TargetPrefixes), "") {
		return nil, fmt.Errorf("%w: empty host or target prefix", ErrInvalidRegistration)
	}

	return ep, validateHooks(reg.Hooks)
}

func validateHooks(h Hooks) error {
	paths := []string{h.OnStartup, h.OnReady, h.OnShutdown}
	for _, rh := range h.Request {
		if len(rh.Services) == 0 || (rh.Pre == "" && rh.Post == "") {
			return fmt.Errorf("%w: request hook needs services and pre or post", ErrInvalidRegistration)
		}

		paths = append(paths, rh.Pre, rh.Post)
	}

	for _, p := range paths {
		if p != "" && !strings.HasPrefix(p, "/") {
			return fmt.Errorf("%w: hook path %q must start with /", ErrInvalidRegistration, p)
		}
	}

	return nil
}

var errClaimConflict = errors.New("route already claimed by another extension")

func (m *Manager) checkConflicts(self *extension, r Routes) error {
	for _, o := range m.exts {
		if o == self || o.reg == nil {
			continue
		}

		if overlap(o.reg.Routes.PathPrefixes, r.PathPrefixes) ||
			overlap(o.reg.Routes.Hosts, r.Hosts) ||
			overlap(o.reg.Routes.TargetPrefixes, r.TargetPrefixes) {
			return fmt.Errorf("%w (%s)", errClaimConflict, o.spec.Name)
		}
	}

	return nil
}

func overlap(a, b []string) bool {
	for _, x := range a {
		for _, y := range b {
			if strings.EqualFold(x, y) {
				return true
			}
		}
	}

	return false
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
