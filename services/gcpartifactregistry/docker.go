package gcpartifactregistry

import (
	"crypto/rand"
	"encoding/base64"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
)

var dockerHostRe = regexp.MustCompile(`^([a-z0-9-]+)-docker\.pkg\.dev(:\d+)?$`)

const (
	tokenBytes      = 24
	catalogLimit    = 1000
	minImageSegs    = 3
	tokenTTLSeconds = 3600
)

type regError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func writeRegError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
	w.WriteHeader(status)

	body, _ := marshalJSON(map[string][]regError{"errors": {{Code: code, Message: msg}}})
	_, _ = w.Write(body)
}

// DockerHandler serves the Docker Registry v2 API for DOCKER repositories at /v2/<project>/<repository>/<image>.
// Any credential is accepted, as oauth2accesstoken:<token> Basic auth or a Bearer token; a request without one
// is challenged to fetch a token from /v2/token.
func (b *InMemoryBackend) DockerHandler() http.Handler {
	return http.HandlerFunc(b.serveDocker)
}

func (b *InMemoryBackend) serveDocker(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v2/token" {
		serveToken(w)

		return
	}

	if r.Header.Get("Authorization") == "" {
		challengeDocker(w, r)

		return
	}

	rest := strings.TrimPrefix(r.URL.Path, "/v2")
	if rest == "" || rest == "/" {
		w.Header().Set("Docker-Distribution-Api-Version", "registry/2.0")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte("{}"))

		return
	}

	dp, ok := splitDockerPath(strings.TrimPrefix(rest, "/"))
	if !ok {
		writeRegError(w, http.StatusNotFound, "NAME_INVALID", "Repository path must be <project>/<repository>/<image>.")

		return
	}

	if !b.dockerRepoOK(w, r, dp.project, dp.id) {
		return
	}

	if dp.tail == "_catalog" {
		b.serveCatalog(w, r, dp.project, dp.id)

		return
	}

	b.proxy(w, r, dp.project, dp.id)
}

func challengeDocker(w http.ResponseWriter, r *http.Request) {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}

	w.Header().Set("WWW-Authenticate", `Bearer realm="`+scheme+`://`+r.Host+`/v2/token",service="`+r.Host+`"`)
	writeRegError(w, http.StatusUnauthorized, "UNAUTHORIZED", "Unauthenticated request.")
}

func serveToken(w http.ResponseWriter) {
	raw := make([]byte, tokenBytes)
	_, _ = rand.Read(raw) // crypto/rand.Read never fails on supported platforms
	tok := base64.RawURLEncoding.EncodeToString(raw)

	body, _ := marshalJSON(map[string]any{"token": tok, "access_token": tok, "expires_in": tokenTTLSeconds})

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// dockerPath is a registry path split into <project>/<repository>/<tail>.
type dockerPath struct {
	project string
	id      string
	tail    string
}

// splitDockerPath splits <project>/<repository>/<rest>, where rest is the image path and API tail.
func splitDockerPath(path string) (dockerPath, bool) {
	parts := strings.SplitN(path, "/", minImageSegs)
	if len(parts) < minImageSegs || parts[0] == "" || parts[1] == "" {
		return dockerPath{}, false
	}

	return dockerPath{project: parts[0], id: parts[1], tail: parts[2]}, true
}

func (b *InMemoryBackend) dockerRepoOK(w http.ResponseWriter, r *http.Request, project, id string) bool {
	info, ok := b.LookupDockerRepo(project, id)
	if !ok || !info.Docker {
		writeRegError(w, http.StatusNotFound, "NAME_UNKNOWN", "Repository \""+id+"\" not found")

		return false
	}

	if m := dockerHostRe.FindStringSubmatch(r.Host); m != nil && m[1] != info.Location {
		writeRegError(w, http.StatusNotFound, "NAME_UNKNOWN", "Repository \""+id+"\" not found in "+m[1])

		return false
	}

	return true
}

func (b *InMemoryBackend) proxy(w http.ResponseWriter, r *http.Request, project, id string) {
	app := b.Registry()

	ociregistry.ServeObserved(app, w, r, ociregistry.Observer{
		Permit: func(w http.ResponseWriter, repo, ref, op string) bool {
			if op == ociregistry.OpWrite && !ociregistry.IsDigest(ref) && b.immutableTag(project, id, repo, ref) {
				writeRegError(w, http.StatusConflict, "TAG_INVALID", "The tag is immutable.")

				return false
			}

			return true
		},
		Put:    func(repo, ref, digest string) { b.RecordManifestPut(repo, ref, digest) },
		Delete: func(repo, ref string) { b.RecordManifestDelete(repo, ref) },
	})
}

func (b *InMemoryBackend) immutableTag(project, id, repo, tag string) bool {
	info, ok := b.LookupDockerRepo(project, id)

	return ok && info.Immutable && b.TagExists(repo, tag)
}

// serveCatalog lists a repository's images from the ledger, paged with n and last.
func (b *InMemoryBackend) serveCatalog(w http.ResponseWriter, r *http.Request, project, id string) {
	images := b.ImageNames(project, id)
	sort.Strings(images)

	n := catalogLimit
	if v, err := strconv.Atoi(r.URL.Query().Get("n")); err == nil && v > 0 {
		n = min(v, catalogLimit)
	}

	start := 0
	if last := r.URL.Query().Get("last"); last != "" {
		start = sort.SearchStrings(images, last)
		if start < len(images) && images[start] == last {
			start++
		}
	}

	end := min(start+n, len(images))
	page := images[start:end]

	if end < len(images) && len(page) > 0 {
		q := url.Values{"n": {strconv.Itoa(n)}, "last": {page[len(page)-1]}}
		w.Header().Set("Link", "<"+r.URL.Path+"?"+q.Encode()+`>; rel="next"`)
	}

	body, _ := marshalJSON(map[string]any{"repositories": append([]string{}, page...)})

	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}
