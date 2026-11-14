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

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"

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

	dp, ok := splitDockerPath(r.Host, strings.TrimPrefix(rest, "/"))
	if ok && dp.createOnPush && isWrite(r.Method) {
		b.createGCRRepo(dp)
	}

	if !ok {
		writeRegError(w, http.StatusNotFound, "NAME_INVALID", "Repository path must be <project>/<repository>/<image>.")

		return
	}

	b.serveDockerAt(w, r, dp)
}

// createGCRRepo creates the project's gcr.io repository for a host the first time an image is pushed to it.
func (b *InMemoryBackend) createGCRRepo(dp dockerPath) {
	ref := dp.ref()
	pb := b.buildRepository(ref, &artifactregistrypb.Repository{
		Format: artifactregistrypb.Repository_DOCKER, Description: "gcr.io repository",
	})

	b.mu.Lock("createGCRRepo")
	defer b.mu.Unlock()

	if _, ok := b.repos[ref.full()]; !ok && len(b.repos) < maxRepositories {
		b.repos[ref.full()] = &repoRec{pb: pb}
	}
}

// serveDockerAt serves a request whose path is already split; a preset dp.location skips Host-based resolution.
func (b *InMemoryBackend) serveDockerAt(w http.ResponseWriter, r *http.Request, dp dockerPath) {
	if !b.dockerRepoOK(w, r, &dp) {
		return
	}

	if dp.tail == "_catalog" {
		b.serveCatalog(w, r, dp)

		return
	}

	if b.dockerModeHandled(w, r, dp) {
		return
	}

	if b.dockerPullDenied(r, dp) {
		writeRegError(w, http.StatusForbidden, "DENIED", "A download rule denies access to this package.")

		return
	}

	b.proxy(w, r, dp)
}

// dockerPullDenied reports whether a download rule blocks a manifest or blob read.
func (b *InMemoryBackend) dockerPullDenied(r *http.Request, dp dockerPath) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return false
	}

	for _, kind := range []string{"/manifests/", "/blobs/"} {
		if image, ref, ok := strings.Cut(
			dp.tail,
			kind,
		); ok && image != "" &&
			!strings.HasPrefix(dp.tail[len(image):], "/blobs/uploads") {
			res := ruleResource{pkg: image}
			if kind == "/manifests/" {
				res.version = ref
				if !strings.HasPrefix(ref, "sha256:") {
					res.tags = []string{ref}
				}
			}

			return b.downloadDenied(r.Context(), dp.ref(), res)
		}
	}

	return false
}

func challengeDocker(w http.ResponseWriter, r *http.Request) {
	scheme := requestScheme(r)

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

// dockerPath is a registry path split into <project>/<repository>/<tail>; location is resolved from the Host
// (or preset for a virtual repository's upstream) once the repository is looked up.
type dockerPath struct {
	project  string
	id       string
	tail     string
	location string

	createOnPush bool
}

func (dp dockerPath) ref() repoRef {
	return repoRef{project: dp.project, location: dp.location, id: dp.id}
}

var gcrHostRe = regexp.MustCompile(`^((?:us|eu|asia)\.)?gcr\.io(?::\d+)?$`)

// gcrLocations maps a Container Registry host to the multi-region of its gcr.io repository, per
// docs.cloud.google.com/artifact-registry/docs/transition/gcr-repositories.
func gcrLocation(prefix string) string {
	switch prefix {
	case "eu.":
		return "europe"
	case "asia.":
		return "asia"
	default:
		return "us"
	}
}

func isWrite(method string) bool {
	return method == http.MethodPost || method == http.MethodPut || method == http.MethodPatch
}

// splitDockerPath splits <project>/<repository>/<rest>, where rest is the image path and API tail. On a gcr.io
// host the path is <project>/<image path>: the repository is the host's predefined gcr.io repository.
func splitDockerPath(host, path string) (dockerPath, bool) {
	if m := gcrHostRe.FindStringSubmatch(host); m != nil {
		project, tail, ok := strings.Cut(path, "/")
		if !ok || project == "" || tail == "" {
			return dockerPath{}, false
		}

		return dockerPath{
			project: project, id: m[1] + "gcr.io", tail: tail, location: gcrLocation(m[1]), createOnPush: true,
		}, true
	}

	parts := strings.SplitN(path, "/", minImageSegs)
	if len(parts) < minImageSegs || parts[0] == "" || parts[1] == "" {
		return dockerPath{}, false
	}

	return dockerPath{project: parts[0], id: parts[1], tail: parts[2]}, true
}

func (b *InMemoryBackend) dockerRepoOK(w http.ResponseWriter, r *http.Request, dp *dockerPath) bool {
	loc := dp.location
	if m := dockerHostRe.FindStringSubmatch(r.Host); m != nil && loc == "" {
		loc = m[1]
	}

	info, ok := b.LookupDockerRepo(dp.project, dp.id, loc)
	if !ok || !info.Docker {
		msg := "Repository \"" + dp.id + "\" not found"
		if loc != "" {
			msg += " in " + loc
		}

		writeRegError(w, http.StatusNotFound, "NAME_UNKNOWN", msg)

		return false
	}

	dp.location = info.Location

	return true
}

// regRewriter maps the embedded registry's location-carrying names in Location and Link headers back to the
// project/repository path the client used.
type regRewriter struct {
	http.ResponseWriter
	from, to string
}

func (w *regRewriter) WriteHeader(code int) {
	h := w.Header()
	for _, k := range []string{"Location", "Link"} {
		if v := h.Get(k); v != "" {
			h.Set(k, strings.ReplaceAll(v, w.from, w.to))
		}
	}

	w.ResponseWriter.WriteHeader(code)
}

func (w *regRewriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (b *InMemoryBackend) proxy(w http.ResponseWriter, r *http.Request, dp dockerPath) {
	app := b.Registry()
	ref := dp.ref()
	from, to := "/v2/"+ref.dockerKey()+"/", "/v2/"+ref.clientPath()+"/"
	if dp.createOnPush {
		to = "/v2/" + ref.project + "/"
	}

	r2 := r.Clone(r.Context())
	r2.URL.Path = from + dp.tail
	r2.URL.RawPath = ""

	rw := &regRewriter{ResponseWriter: w, from: from, to: to}
	if strings.HasSuffix(dp.tail, "/tags/list") {
		rec := newBufResponse()
		app.ServeHTTP(rec, r2)

		name := strings.TrimPrefix(from, "/v2/")
		body := strings.ReplaceAll(rec.body.String(), `"`+name, `"`+strings.TrimPrefix(to, "/v2/"))
		rec.body.Reset()
		rec.body.WriteString(body)
		rec.header.Del("Content-Length")
		rec.replay(rw)

		return
	}

	ociregistry.ServeObserved(app, rw, r2, ociregistry.Observer{
		Permit: func(w http.ResponseWriter, repo, ref, op string) bool {
			if op == ociregistry.OpWrite && !ociregistry.IsDigest(ref) && b.immutableTag(dp, repo, ref) {
				writeRegError(w, http.StatusConflict, "TAG_INVALID", "The tag is immutable.")

				return false
			}

			return true
		},
		Put:    func(repo, ref, digest string) { b.RecordManifestPut(repo, ref, digest) },
		Delete: func(repo, ref string) { b.RecordManifestDelete(repo, ref) },
	})
}

func (b *InMemoryBackend) immutableTag(dp dockerPath, repo, tag string) bool {
	info, ok := b.LookupDockerRepo(dp.project, dp.id, dp.location)

	return ok && info.Immutable && b.TagExists(repo, tag)
}

// serveCatalog lists a repository's images from the ledger, paged with n and last.
func (b *InMemoryBackend) serveCatalog(w http.ResponseWriter, r *http.Request, dp dockerPath) {
	images := b.imageNames(dp.ref())
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
