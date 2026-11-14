package azurefunctions

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// packageURL returns the http(s) URL WEBSITE_RUN_FROM_PACKAGE points at; "1", "0" and other values mean the
// deployed package is used.
func packageURL(settings map[string]string) (string, bool) {
	v := strings.TrimSpace(settings[settingRunFromPkg])

	u, err := url.Parse(v)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", false
	}

	return v, true
}

// sitePackage is the zip a host runs: the one WEBSITE_RUN_FROM_PACKAGE names by URL, else the deployed zip;
// Flex Consumption ignores the setting (Learn: functions-app-settings, "Flex Consumption plan deprecations").
func (b *InMemoryBackend) sitePackage(
	ctx context.Context,
	settings map[string]string,
	deployed []byte,
	flex bool,
) ([]byte, error) {
	raw, ok := packageURL(settings)
	if !ok || flex {
		return deployed, nil
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %w", errPackageFetch, settingRunFromPkg, err)
	}

	resp, err := (&http.Client{Transport: b.transport}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errPackageFetch, scrubURLError(err))
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: server answered %d", errPackageFetch, resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxPackageBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errPackageFetch, scrubURLError(err))
	}

	if len(data) > maxPackageBytes {
		return nil, fmt.Errorf("%w: package exceeds %d bytes", errPackageFetch, maxPackageBytes)
	}

	if err = validatePackage(data); err != nil {
		return nil, fmt.Errorf("%w: %w", errPackageFetch, err)
	}

	return data, nil
}

// scrubURLError drops the request URL (it may carry a SAS token) from a transport error.
func scrubURLError(err error) error {
	if ue, ok := err.(*url.Error); ok { //nolint:errorlint // only the outermost wrapper carries the URL
		return ue.Err
	}

	return err
}
