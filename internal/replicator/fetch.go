package replicator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
)

var errTooLarge = errors.New("exceeds size cap")

type httpDoer struct{ c *http.Client }

// fetch downloads a (presigned) URL, refusing bodies larger than limit.
func (d *httpDoer) fetch(ctx context.Context, url string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := d.c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: status %d", errDownload, resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}

	if int64(len(body)) > limit {
		return nil, fmt.Errorf("%w (%d bytes)", errTooLarge, limit)
	}

	return body, nil
}

var errDownload = errors.New("download failed")

var errUnprocessed = errors.New("unprocessed items remain after retries")
