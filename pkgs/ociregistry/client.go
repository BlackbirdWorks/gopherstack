package ociregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
)

// ErrNotFound is returned when a repository, manifest or blob does not exist.
var ErrNotFound = errors.New("ociregistry: not found")

// ErrTooLarge is returned when a blob exceeds the size the caller allowed.
var ErrTooLarge = errors.New("ociregistry: blob too large")

// ErrRegistry is returned when the registry answers with an unexpected error status.
var ErrRegistry = errors.New("ociregistry: registry error")

const (
	catalogPage        = 1000
	maxManifestBytes   = 4 << 20
	maxPages           = 10000
	acceptManifestList = "application/vnd.docker.distribution.manifest.v2+json," +
		"application/vnd.docker.distribution.manifest.list.v2+json," +
		"application/vnd.oci.image.manifest.v1+json,application/vnd.oci.image.index.v1+json"
)

// Client calls a registry handler in-process, bypassing any authentication in front of it.
type Client struct{ h http.Handler }

// NewClient returns a Client for h.
func NewClient(h http.Handler) Client { return Client{h: h} }

// Manifest is a stored manifest.
type Manifest struct {
	Digest    string
	MediaType string
	Body      []byte
}

type recorder struct {
	header http.Header
	body   bytes.Buffer
	limit  int64
	status int
	over   bool
}

func (r *recorder) Header() http.Header { return r.header }

func (r *recorder) WriteHeader(code int) {
	if r.status == 0 {
		r.status = code
	}
}

func (r *recorder) Write(p []byte) (int, error) {
	r.WriteHeader(http.StatusOK)

	if r.limit > 0 && int64(r.body.Len()+len(p)) > r.limit {
		r.over = true

		return 0, ErrTooLarge
	}

	return r.body.Write(p)
}

func (c Client) do(
	ctx context.Context, method, path string, headers map[string]string, limit int64,
) (*recorder, error) {
	return c.doBody(ctx, method, path, nil, headers, limit)
}

func (c Client) doBody(
	ctx context.Context, method, path string, body []byte, headers map[string]string, limit int64,
) (*recorder, error) {
	req, err := http.NewRequestWithContext(ctx, method, "http://registry.invalid"+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("ociregistry: build request: %w", err)
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	rec := &recorder{header: http.Header{}, limit: limit}
	c.h.ServeHTTP(rec, req)

	if rec.status == 0 {
		rec.status = http.StatusOK
	}

	if rec.over {
		return nil, ErrTooLarge
	}

	return rec, nil
}

func statusErr(rec *recorder) error {
	switch {
	case rec.status == http.StatusNotFound:
		return ErrNotFound
	case rec.status >= http.StatusBadRequest:
		return fmt.Errorf("%w: status %d: %s", ErrRegistry, rec.status, rec.body.String())
	}

	return nil
}

type listResponse struct {
	Repositories []string `json:"repositories"`
	Tags         []string `json:"tags"`
}

func (c Client) pages(ctx context.Context, base string, pick func(listResponse) []string) ([]string, error) {
	var all []string

	last := ""

	for range maxPages {
		q := url.Values{"n": {strconv.Itoa(catalogPage)}}
		if last != "" {
			q.Set("last", last)
		}

		rec, err := c.do(ctx, http.MethodGet, base+"?"+q.Encode(), nil, 0)
		if err != nil {
			return nil, err
		}

		if err = statusErr(rec); err != nil {
			return nil, err
		}

		var lr listResponse
		if err = json.Unmarshal(rec.body.Bytes(), &lr); err != nil {
			return nil, fmt.Errorf("ociregistry: decode list: %w", err)
		}

		page := pick(lr)
		all = append(all, page...)

		if len(page) < catalogPage {
			return all, nil
		}

		last = page[len(page)-1]
	}

	return all, nil
}

// Catalog lists every repository that holds content, in lexical order.
func (c Client) Catalog(ctx context.Context) ([]string, error) {
	return c.pages(ctx, "/v2/_catalog", func(l listResponse) []string { return l.Repositories })
}

// Tags lists the tags of repo, in lexical order. A repository with no content has none.
func (c Client) Tags(ctx context.Context, repo string) ([]string, error) {
	tags, err := c.pages(ctx, "/v2/"+repo+"/tags/list", func(l listResponse) []string { return l.Tags })
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}

	return tags, err
}

// GetManifest fetches the manifest at repo:ref, where ref is a tag or a digest.
func (c Client) GetManifest(ctx context.Context, repo, ref string) (Manifest, error) {
	rec, err := c.do(ctx, http.MethodGet, "/v2/"+repo+"/manifests/"+ref,
		map[string]string{"Accept": acceptManifestList}, maxManifestBytes)
	if err != nil {
		return Manifest{}, err
	}

	if err = statusErr(rec); err != nil {
		return Manifest{}, err
	}

	return Manifest{
		Digest: rec.header.Get("Docker-Content-Digest"), MediaType: rec.header.Get("Content-Type"),
		Body: rec.body.Bytes(),
	}, nil
}

// PutManifest stores body as repo:ref (a tag or digest) and returns its digest.
func (c Client) PutManifest(ctx context.Context, repo, ref, mediaType string, body []byte) (string, error) {
	rec, err := c.doBody(ctx, http.MethodPut, "/v2/"+repo+"/manifests/"+ref, body,
		map[string]string{"Content-Type": mediaType}, 0)
	if err != nil {
		return "", err
	}

	if err = statusErr(rec); err != nil {
		return "", err
	}

	return rec.header.Get("Docker-Content-Digest"), nil
}

// DeleteManifest removes the manifest with the given digest (and every tag pointing at it).
func (c Client) DeleteManifest(ctx context.Context, repo, digest string) error {
	rec, err := c.do(ctx, http.MethodDelete, "/v2/"+repo+"/manifests/"+digest, nil, 0)
	if err != nil {
		return err
	}

	return statusErr(rec)
}

// GetBlob fetches a blob, failing with ErrTooLarge past maxBytes.
func (c Client) GetBlob(ctx context.Context, repo, digest string, maxBytes int64) ([]byte, error) {
	rec, err := c.do(ctx, http.MethodGet, "/v2/"+repo+"/blobs/"+digest, nil, maxBytes)
	if err != nil {
		return nil, err
	}

	if err = statusErr(rec); err != nil {
		return nil, err
	}

	return io.ReadAll(&rec.body)
}
