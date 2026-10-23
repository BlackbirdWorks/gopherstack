package ociregistry

import (
	"net/http"
	"strings"
)

// Observer hooks manifest traffic passing through ServeObserved. Every field is optional.
type Observer struct {
	// Permit may refuse a manifest read, write or delete; it must write the error response itself.
	Permit func(w http.ResponseWriter, repo, ref, op string) bool
	// Put runs after a manifest is stored (201); digest is the stored manifest's digest.
	Put func(repo, ref, digest string)
	// Delete runs after a manifest or tag is deleted (202).
	Delete func(repo, ref string)
}

type capture struct {
	http.ResponseWriter
	status int
}

func (c *capture) WriteHeader(code int) {
	if c.status == 0 {
		c.status = code
	}

	c.ResponseWriter.WriteHeader(code)
}

func (c *capture) Unwrap() http.ResponseWriter { return c.ResponseWriter }

// SplitManifestPath parses /v2/<repo>/manifests/<ref>; ok is false for every other path.
func SplitManifestPath(path string) (string, string, bool) {
	rest := strings.TrimPrefix(path, "/v2/")

	i := strings.LastIndex(rest, "/manifests/")
	if i <= 0 {
		return "", "", false
	}

	return rest[:i], rest[i+len("/manifests/"):], true
}

func opFor(method string) string {
	switch method {
	case http.MethodPut:
		return OpWrite
	case http.MethodDelete:
		return OpDelete
	case http.MethodGet, http.MethodHead:
		return OpRead
	}

	return ""
}

// ServeObserved serves r through app, reporting manifest writes and deletes to obs.
func ServeObserved(app http.Handler, w http.ResponseWriter, r *http.Request, obs Observer) {
	repo, ref, isManifest := SplitManifestPath(r.URL.Path)
	if !isManifest {
		app.ServeHTTP(w, r)

		return
	}

	if op := opFor(r.Method); op != "" && obs.Permit != nil && !obs.Permit(w, repo, ref, op) {
		return
	}

	cw := &capture{ResponseWriter: w}
	app.ServeHTTP(cw, r)

	switch {
	case r.Method == http.MethodPut && cw.status == http.StatusCreated && obs.Put != nil:
		obs.Put(repo, ref, cw.Header().Get("Docker-Content-Digest"))
	case r.Method == http.MethodDelete && cw.status == http.StatusAccepted && obs.Delete != nil:
		obs.Delete(repo, ref)
	}
}
