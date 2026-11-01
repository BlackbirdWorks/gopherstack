// Package mcpserver exposes Gopherstack's admin APIs as Model Context Protocol tools.
package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const (
	maxResponseBytes = 8 << 20
	maxErrorBytes    = 1 << 11
)

// ErrBackend is returned when the Gopherstack admin API rejects a call.
var ErrBackend = errors.New("gopherstack admin API error")

// Doer executes an HTTP request against a Gopherstack instance.
type Doer interface {
	Do(req *http.Request) (*http.Response, error)
}

type backend struct {
	doer Doer
	base string
}

func (b *backend) call(ctx context.Context, method, path string, q url.Values, in, out any) error {
	target := strings.TrimRight(b.base, "/") + path
	if len(q) > 0 {
		target += "?" + q.Encode()
	}

	var body io.Reader = http.NoBody

	if in != nil {
		raw, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}

		body = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, target, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}

	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := b.doer.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBackend, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		msg := strings.TrimSpace(string(raw))
		if len(msg) > maxErrorBytes {
			msg = msg[:maxErrorBytes] + "..."
		}

		return fmt.Errorf("%w: %s %s: HTTP %d: %s", ErrBackend, method, path, resp.StatusCode, msg)
	}

	if out == nil || len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}

	if err = json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}

	return nil
}
