package k3sengine

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net/http"
)

const maxProbeBody = 1 << 20

// ErrNotReady is returned while the API server has not answered /readyz with 200.
var ErrNotReady = errors.New("readyz not ok")

// Probe requires /readyz to answer 200 for the bearer token over TLS verified against the pre-provisioned CA.
func Probe(ctx context.Context, addr, token string, caPEM []byte) error {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return fmt.Errorf("%w: cluster CA is not a certificate", ErrNotReady)
	}

	_, err := HTTPGet(ctx, addr, "/readyz", token, &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})

	return err
}

// HTTPGet GETs https://addr/path with an optional bearer token and requires a 200.
func HTTPGet(ctx context.Context, addr, path, token string, tlsCfg *tls.Config) ([]byte, error) {
	tr := &http.Transport{TLSClientConfig: tlsCfg, DisableKeepAlives: true}
	defer tr.CloseIdleConnections()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr+path, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", path, err)
	}

	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxProbeBody))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: %s returned %d", ErrNotReady, path, resp.StatusCode)
	}

	return body, nil
}
