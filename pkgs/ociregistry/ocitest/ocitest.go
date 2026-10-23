// Package ocitest drives a Docker Registry v2 endpoint with the push, pull, delete and listing calls a
// registry client makes, so each registry emulator proves the same conformance behaviour.
package ocitest

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const (
	requestTimeout = 60 * time.Second
	headerType     = "Content-Type"
	layerPadding   = 600
	configChunks   = 2
	keyMediaType   = "mediaType"
	schemaVersion  = 2
)

// ManifestMediaType is the Docker schema 2 manifest type the helpers push.
const ManifestMediaType = "application/vnd.docker.distribution.manifest.v2+json"

// Client talks to one registry endpoint.
type Client struct {
	T *testing.T
	// HTTP performs the calls; nil uses a client without keep-alives.
	HTTP *http.Client
	// Prepare adds credentials (and anything else) to each request.
	Prepare func(*http.Request)
	// Base is the scheme and host:port requests are sent to.
	Base string
	// Host overrides the Host header, for host-routed registries.
	Host string
}

// Response is a completed call.
type Response struct {
	Header http.Header
	Body   []byte
	Status int
}

func (c Client) defaultClient() *http.Client {
	tr := httpclient.NewTransport()
	tr.DisableKeepAlives = true

	c.T.Cleanup(tr.CloseIdleConnections)

	return &http.Client{Transport: tr, Timeout: requestTimeout}
}

// Do sends one request and reads its body.
func (c Client) Do(method, path string, body []byte, headers map[string]string) Response {
	c.T.Helper()

	req, err := http.NewRequestWithContext(c.T.Context(), method, c.Base+path, bytes.NewReader(body))
	require.NoError(c.T, err)

	if c.Host != "" {
		req.Host = c.Host
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	if c.Prepare != nil {
		c.Prepare(req)
	}

	hc := c.HTTP
	if hc == nil {
		hc = c.defaultClient()
	}

	resp, err := hc.Do(req)
	require.NoError(c.T, err)

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(c.T, err)

	return Response{Status: resp.StatusCode, Header: resp.Header, Body: raw}
}

// Digest returns the sha256 digest of data.
func Digest(data []byte) string {
	sum := sha256.Sum256(data)

	return "sha256:" + hex.EncodeToString(sum[:])
}

func locationPath(c Client, resp Response) string {
	c.T.Helper()

	loc := resp.Header.Get("Location")
	require.NotEmpty(c.T, loc)

	u, err := url.Parse(loc)
	require.NoError(c.T, err)

	return u.RequestURI()
}

// PushBlob uploads data to repo in the given number of PATCH chunks (0 means one monolithic PUT) and
// returns its digest.
func (c Client) PushBlob(repo string, data []byte, chunks int) string {
	c.T.Helper()

	start := c.Do(http.MethodPost, "/v2/"+repo+"/blobs/uploads/", nil, nil)
	require.Equal(c.T, http.StatusAccepted, start.Status, string(start.Body))

	next := locationPath(c, start)
	digest := Digest(data)

	for i := range chunks {
		lo, hi := len(data)*i/chunks, len(data)*(i+1)/chunks
		patch := c.Do(http.MethodPatch, next, data[lo:hi], map[string]string{
			headerType:      "application/octet-stream",
			"Content-Range": fmt.Sprintf("%d-%d", lo, hi-1),
		})
		require.Equal(c.T, http.StatusAccepted, patch.Status, string(patch.Body))

		next = locationPath(c, patch)
	}

	final := data
	if chunks > 0 {
		final = nil
	}

	sep := "?"
	if strings.Contains(next, "?") {
		sep = "&"
	}

	done := c.Do(http.MethodPut, next+sep+"digest="+url.QueryEscape(digest), final,
		map[string]string{headerType: "application/octet-stream"})
	require.Equal(c.T, http.StatusCreated, done.Status, string(done.Body))
	assert.Equal(c.T, digest, done.Header.Get("Docker-Content-Digest"))

	return digest
}

// Image is a pushed manifest.
type Image struct {
	Digest string
	Body   []byte
	Layer  []byte
}

// PushImage uploads a one-layer image to repo and tags it.
func (c Client) PushImage(repo, tag string) Image {
	c.T.Helper()

	layer := []byte("layer for " + repo + ":" + tag + strings.Repeat("x", layerPadding))
	layerDigest := c.PushBlob(repo, layer, 0)

	cfg, err := json.Marshal(map[string]any{
		"architecture": "amd64", "os": "linux",
		"rootfs": map[string]any{"type": "layers", "diff_ids": []string{layerDigest}},
	})
	require.NoError(c.T, err)

	cfgDigest := c.PushBlob(repo, cfg, configChunks)

	body, err := json.Marshal(map[string]any{
		"schemaVersion": schemaVersion, keyMediaType: ManifestMediaType,
		"config": map[string]any{
			keyMediaType: "application/vnd.docker.container.image.v1+json", "size": len(cfg), "digest": cfgDigest,
		},
		"layers": []any{map[string]any{
			keyMediaType: "application/vnd.docker.image.rootfs.diff.tar.gzip", "size": len(layer),
			"digest": layerDigest,
		}},
	})
	require.NoError(c.T, err)

	put := c.Do(
		http.MethodPut,
		"/v2/"+repo+"/manifests/"+tag,
		body,
		map[string]string{headerType: ManifestMediaType},
	)
	require.Equal(c.T, http.StatusCreated, put.Status, string(put.Body))
	require.Equal(c.T, Digest(body), put.Header.Get("Docker-Content-Digest"))

	return Image{Digest: Digest(body), Body: body, Layer: layer}
}

// Names pages through a listing URL with n per page, following Link headers, and returns every name.
func (c Client) Names(path, field string, n int) []string {
	c.T.Helper()

	var all []string

	next := path + "?n=" + strconv.Itoa(n)

	for range 100 {
		resp := c.Do(http.MethodGet, next, nil, nil)
		require.Equal(c.T, http.StatusOK, resp.Status, string(resp.Body))

		var doc map[string]json.RawMessage
		require.NoError(c.T, json.Unmarshal(resp.Body, &doc))

		var page []string
		require.NoError(c.T, json.Unmarshal(doc[field], &page))

		all = append(all, page...)

		link := resp.Header.Get("Link")
		if link == "" {
			return all
		}

		start, end := strings.Index(link, "<"), strings.Index(link, ">")
		require.Greater(c.T, end, start)

		u, err := url.Parse(link[start+1 : end])
		require.NoError(c.T, err)

		next = u.RequestURI()
	}

	require.Fail(c.T, "pagination did not terminate")

	return all
}
