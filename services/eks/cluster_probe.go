package eks

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

var errNotReady = errors.New("readyz not ok")

// probeK3s fetches the server CA from /cacerts, then requires /readyz to answer 200 for the bearer token.
func probeK3s(ctx context.Context, addr, token string) ([]byte, error) {
	//nolint:gosec // bootstrap fetch of the public CA; every later request verifies against it
	ca, err := httpGet(ctx, addr, "/cacerts", "", &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		return nil, err
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("%w: /cacerts returned no certificate", errNotReady)
	}

	verified := &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	if _, err = httpGet(ctx, addr, "/readyz", token, verified); err != nil {
		return nil, err
	}

	return ca, nil
}

func httpGet(ctx context.Context, addr, path, token string, tlsCfg *tls.Config) ([]byte, error) {
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
		return nil, fmt.Errorf("%w: %s returned %d", errNotReady, path, resp.StatusCode)
	}

	return body, nil
}
