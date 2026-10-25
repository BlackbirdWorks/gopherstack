package rds

import (
	"context"
	"crypto/subtle"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const (
	calloutAuthHeader    = "X-Gs-Token"
	calloutCredsParam    = "cred"
	calloutReadTimeout   = 30 * time.Second
	calloutUpstreamLimit = 5 * time.Minute
	calloutDefaultHost   = "172.17.0.1"
	calloutHostEnv       = "RDS_CALLOUT_HOST"
	calloutBindEnv       = "RDS_CALLOUT_BIND"

	featureS3Import = "s3Import"
	featureS3Export = "s3Export"
	featureLambda   = "Lambda"

	lambdaInvokePath = "/2015-03-31/functions/%s/invocations"
)

// calloutServer answers the aws_s3 / aws_lambda SQL extensions from inside database containers.
type calloutServer struct {
	srv      *http.Server
	client   *http.Client
	roles    func(key, feature string) bool
	tokens   map[string]string
	api      string
	hostPort string
	listenIP string
	port     int
	mu       sync.RWMutex
}

// bridgeGatewayIP is the host-side address of the container bridge, where containers reach the host.
func bridgeGatewayIP() string {
	for _, name := range []string{"docker0", "podman0", "cni-podman0"} {
		ifc, err := net.InterfaceByName(name)
		if err != nil {
			continue
		}

		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}

		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil {
				return ipn.IP.String()
			}
		}
	}

	return ""
}

func isDesktopOS() bool { return runtime.GOOS == "darwin" || runtime.GOOS == "windows" }

// calloutBind is the address the listener binds: never a wildcard.
func calloutBind() string {
	if h := os.Getenv(calloutBindEnv); h != "" {
		return h
	}

	if !isDesktopOS() {
		if ip := bridgeGatewayIP(); ip != "" {
			return ip
		}
	}

	return "127.0.0.1"
}

// calloutHost is the address containers dial to reach the listener.
func calloutHost() string {
	if h := os.Getenv(calloutHostEnv); h != "" {
		return h
	}

	if isDesktopOS() {
		return "host.docker.internal"
	}

	if ip := bridgeGatewayIP(); ip != "" {
		return ip
	}

	return calloutDefaultHost
}

func (e *dbEngine) startCallout() error {
	e.calloutMu.Lock()
	defer e.calloutMu.Unlock()

	if e.callout != nil {
		return nil
	}

	if e.cfg.APIEndpoint == "" || e.cfg.RoleLookup == nil {
		return nil
	}

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", net.JoinHostPort(calloutBind(), "0"))
	if err != nil {
		return fmt.Errorf("listen for aws_s3/aws_lambda callouts: %w", err)
	}

	port := ln.Addr().(*net.TCPAddr).Port //nolint:errcheck,forcetypeassert // tcp listener
	cs := &calloutServer{
		client:   httpclient.New(calloutUpstreamLimit),
		roles:    e.cfg.RoleLookup,
		tokens:   make(map[string]string),
		api:      strings.TrimRight(e.cfg.APIEndpoint, "/"),
		hostPort: net.JoinHostPort(calloutHost(), strconv.Itoa(port)),
		listenIP: ln.Addr().(*net.TCPAddr).IP.String(), //nolint:errcheck,forcetypeassert // tcp listener
		port:     port,
	}
	cs.srv = &http.Server{Handler: cs, ReadHeaderTimeout: calloutReadTimeout}

	go func() { _ = cs.srv.Serve(ln) }()

	e.callout = cs

	return nil
}

func (e *dbEngine) stopCallout() {
	e.calloutMu.Lock()
	cs := e.callout
	e.callout = nil
	e.calloutMu.Unlock()

	if cs != nil {
		ctx, cancel := context.WithTimeout(context.Background(), engineProbeTimeout)
		defer cancel()

		_ = cs.srv.Shutdown(ctx)
		cs.client.CloseIdleConnections()
	}
}

func (e *dbEngine) calloutAddr() string {
	e.calloutMu.Lock()
	defer e.calloutMu.Unlock()

	if e.callout == nil {
		return ""
	}

	return e.callout.hostPort
}

func (e *dbEngine) calloutToken(lb *liveDB) string {
	e.calloutMu.Lock()
	defer e.calloutMu.Unlock()

	if e.callout == nil {
		return ""
	}

	return e.callout.register(lb)
}

func (cs *calloutServer) register(lb *liveDB) string {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	for tok, key := range cs.tokens {
		if key == lb.key {
			return tok
		}
	}

	tok := randomPassword() + randomPassword()
	cs.tokens[tok] = lb.key

	return tok
}

func (cs *calloutServer) unitKey(r *http.Request) (string, bool) {
	got := r.Header.Get(calloutAuthHeader)

	cs.mu.RLock()
	defer cs.mu.RUnlock()

	for tok, key := range cs.tokens {
		if subtle.ConstantTimeCompare([]byte(tok), []byte(got)) == 1 {
			return key, true
		}
	}

	return "", false
}

func (cs *calloutServer) denied(key, feature string, r *http.Request) bool {
	if r.URL.Query().Get(calloutCredsParam) == "1" || r.Header.Get("X-Gs-Cred") == "1" {
		return false
	}

	return !cs.roles(key, feature)
}

func deniedMessage(feature string) string {
	if feature == featureLambda {
		return "AccessDeniedException: the DB instance has no IAM role associated for feature Lambda; " +
			"add one with AddRoleToDBInstance (FeatureName=Lambda)"
	}

	return "HTTP 403. Permission denied. Check bucket or provided credentials as they may no longer be valid: " +
		"the DB instance has no IAM role associated for feature " + feature +
		"; add one with AddRoleToDBInstance (FeatureName=" + feature + ")"
}

func (cs *calloutServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key, ok := cs.unitKey(r)
	if !ok {
		http.Error(w, "invalid token", http.StatusUnauthorized)

		return
	}

	path := r.URL.EscapedPath()

	switch {
	case strings.HasPrefix(path, "/auth/"):
		feature := strings.TrimPrefix(path, "/auth/")
		if cs.denied(key, feature, r) {
			http.Error(w, deniedMessage(feature), http.StatusForbidden)

			return
		}

		w.WriteHeader(http.StatusOK)
	case strings.HasPrefix(path, "/s3/"):
		cs.serveS3(w, r, key, strings.TrimPrefix(path, "/s3"))
	case strings.HasPrefix(path, "/lambda/"):
		cs.serveLambda(w, r, key, strings.TrimPrefix(path, "/lambda/"))
	default:
		http.NotFound(w, r)
	}
}

func (cs *calloutServer) serveS3(w http.ResponseWriter, r *http.Request, key, objPath string) {
	feature := featureS3Import
	if r.Method == http.MethodPut {
		feature = featureS3Export
	}

	if cs.denied(key, feature, r) {
		http.Error(w, deniedMessage(feature), http.StatusForbidden)

		return
	}

	cs.forward(w, r, cs.api+objPath, nil)
}

func (cs *calloutServer) serveLambda(w http.ResponseWriter, r *http.Request, key, fn string) {
	if cs.denied(key, featureLambda, r) {
		http.Error(w, deniedMessage(featureLambda), http.StatusForbidden)

		return
	}

	target := cs.api + fmt.Sprintf(lambdaInvokePath, fn)
	if q := r.URL.RawQuery; q != "" {
		target += "?" + q
	}

	hdr := http.Header{}
	for _, h := range []string{"X-Amz-Invocation-Type", "X-Amz-Log-Type", "X-Amz-Client-Context"} {
		if v := r.Header.Get(h); v != "" {
			hdr.Set(h, v)
		}
	}

	hdr.Set("Content-Type", "application/json")
	cs.forward(w, r, target, hdr)
}

func (cs *calloutServer) forward(w http.ResponseWriter, r *http.Request, target string, hdr http.Header) {
	if _, err := url.Parse(target); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	method := r.Method
	if strings.Contains(target, "/invocations") {
		method = http.MethodPost
	}

	//nolint:gosec // target derives from the configured emulator endpoint
	req, err := http.NewRequestWithContext(r.Context(), method, target, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)

		return
	}

	req.ContentLength = r.ContentLength
	maps.Copy(req.Header, hdr)

	//nolint:gosec // target derives from the configured emulator endpoint
	resp, err := cs.client.Do(req)
	if err != nil {
		http.Error(w, "emulator unreachable: "+err.Error(), http.StatusBadGateway)

		return
	}

	defer resp.Body.Close()

	for _, h := range []string{"Content-Length", "Content-Type", "X-Amz-Executed-Version", "X-Amz-Log-Result",
		"X-Amz-Function-Error"} {
		if v := resp.Header.Get(h); v != "" {
			w.Header().Set(h, v)
		}
	}

	w.WriteHeader(resp.StatusCode)

	if method != http.MethodHead {
		_, _ = io.Copy(w, resp.Body)
	}
}
