package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

const (
	registryExchangeTimeout = 30 * time.Second
	registryExchangeMaxBody = 64 << 10
)

var (
	errRegistryNotEmulated = errors.New("the registry is not the emulated container registry")
	errRegistryNonLoopback = errors.New("registry exchange refused a non-loopback peer")
	errRegistryExchange    = errors.New("registry token exchange failed")
)

// wireAzureFunctionsRegistry lets apps pull images from the emulated registry with their managed identity.
func wireAzureFunctionsRegistry(backend *azurefunctions.InMemoryBackend, byName map[string]service.Registerable) {
	h, ok := byName["AzureContainerRegistry"].(*azurecontainerregistry.Handler)
	if !ok {
		return
	}

	backend.SetRegistryExchanger(newRegistryExchanger(h.Port, h.Settings.TLSCertFile))
}

// newRegistryExchanger calls /oauth2/exchange on the emulated registry's own listener, never on another host: the
// image host must carry the listener's port and resolve to loopback only, the dial is loopback only and
// redirects are not followed. When the listener serves HTTPS with an operator certificate, that certificate is
// the only trust anchor.
func newRegistryExchanger(port int, certFile string) azurefunctions.RegistryExchanger {
	return newRegistryExchangerWith(port, certFile, net.DefaultResolver.LookupIPAddr)
}

type hostLookup func(ctx context.Context, host string) ([]net.IPAddr, error)

// emulatedRegistryName reports whether name can only reach this machine: a loopback address, localhost or a
// *.localhost name, or a name whose every address is loopback. The image reference is user input, and the
// refresh token is handed to the container runtime for that host.
func emulatedRegistryName(ctx context.Context, name string, lookup hostLookup) bool {
	if ip := net.ParseIP(name); ip != nil {
		return ip.IsLoopback()
	}

	if lower := strings.ToLower(name); lower == "localhost" || strings.HasSuffix(lower, ".localhost") {
		return true
	}

	addrs, err := lookup(ctx, name)
	if err != nil || len(addrs) == 0 {
		return false
	}

	for _, a := range addrs {
		if !a.IP.IsLoopback() {
			return false
		}
	}

	return true
}

func newRegistryExchangerWith(port int, certFile string, lookup hostLookup) azurefunctions.RegistryExchanger {
	return func(ctx context.Context, registryHost, tenant, accessToken string) (string, error) {
		name, p, err := net.SplitHostPort(registryHost)
		if err != nil || p != strconv.Itoa(port) || !emulatedRegistryName(ctx, name, lookup) {
			return "", errRegistryNotEmulated
		}

		client, scheme, err := registryClient(certFile, name)
		if err != nil {
			return "", err
		}

		defer client.CloseIdleConnections()

		form := url.Values{
			"grant_type": {"access_token"}, "service": {name}, "tenant": {tenant}, "access_token": {accessToken},
		}
		target := scheme + "://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(port)) + "/oauth2/exchange"

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader(form.Encode()))
		if err != nil {
			return "", fmt.Errorf("%w: %w", errRegistryExchange, err)
		}

		req.Host = registryHost
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := client.Do(req)
		if err != nil {
			return "", fmt.Errorf("%w: %w", errRegistryExchange, scrubTransportError(err))
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(io.LimitReader(resp.Body, registryExchangeMaxBody))
		if err != nil || resp.StatusCode != http.StatusOK {
			return "", fmt.Errorf("%w: the registry answered %d", errRegistryExchange, resp.StatusCode)
		}

		var out struct {
			RefreshToken string `json:"refresh_token"`
		}

		if json.Unmarshal(body, &out, jsonx.V1()) != nil || out.RefreshToken == "" {
			return "", fmt.Errorf("%w: no refresh token in the answer", errRegistryExchange)
		}

		return out.RefreshToken, nil
	}
}

func scrubTransportError(err error) error {
	if ue, ok := errors.AsType[*url.Error](err); ok {
		return ue.Err
	}

	return err
}

func registryClient(certFile, serverName string) (*http.Client, string, error) {
	tr := httpclient.NewTransport()
	tr.Proxy = nil
	tr.DialContext = httpclient.LoopbackDialer(func() bool { return false }, errRegistryNonLoopback).DialContext
	scheme := initSchemeHTTP

	if certFile != "" {
		pem, err := os.ReadFile(certFile)
		if err != nil {
			return nil, "", fmt.Errorf("%w: read the registry certificate", errRegistryExchange)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, "", fmt.Errorf("%w: the registry certificate is not PEM", errRegistryExchange)
		}

		tr.TLSClientConfig = &tls.Config{RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS12}
		scheme = lifecycleSchemeHTTPS
	}

	return &http.Client{
		Transport: tr, Timeout: registryExchangeTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}, scheme, nil
}
