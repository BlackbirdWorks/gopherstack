package gcpartifactregistry

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
)

const (
	dockerHubHost   = "registry-1.docker.io"
	maxDockerBody   = 1 << 28
	manifestAccepts = "application/vnd.oci.image.index.v1+json, " +
		"application/vnd.docker.distribution.manifest.list.v2+json, " +
		"application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"
)

var errDockerAuth = errors.New("the upstream registry refused authentication")

// dockerModeHandled serves virtual and remote Docker repositories. It reports true when it answered; false
// leaves the request to the regular local handler (standard repositories, and remote ones once filled).
func (b *InMemoryBackend) dockerModeHandled(w http.ResponseWriter, r *http.Request, dp dockerPath) bool {
	info, ok := b.LookupDockerRepo(dp.project, dp.id)
	if !ok {
		return false
	}

	switch info.Mode {
	case artifactregistrypb.Repository_VIRTUAL_REPOSITORY:
		b.serveDockerVirtual(w, r, dp)

		return true
	case artifactregistrypb.Repository_REMOTE_REPOSITORY:
		return b.fillDockerRemote(w, r, dp)
	case artifactregistrypb.Repository_MODE_UNSPECIFIED, artifactregistrypb.Repository_STANDARD_REPOSITORY:
		return false
	default:
		return false
	}
}

func (b *InMemoryBackend) serveDockerVirtual(w http.ResponseWriter, r *http.Request, dp dockerPath) {
	if !isRead(r) {
		writeRegError(w, http.StatusMethodNotAllowed, "UNSUPPORTED", "Virtual repositories are read-only.")

		return
	}

	rc, ok := b.repoOfFormat(dp.project, dp.id, artifactregistrypb.Repository_DOCKER)
	if !ok {
		writeRegError(w, http.StatusNotFound, "NAME_UNKNOWN", "Repository not found")

		return
	}

	for _, up := range b.upstreamsOf(rc) {
		r2 := r.Clone(r.Context())
		r2.URL.Path = "/v2/" + up.ref.project + "/" + up.ref.id + "/" + dp.tail

		rec := newBufResponse()
		b.serveDocker(rec, r2)

		if st := rec.status(); st < http.StatusBadRequest || st == http.StatusForbidden {
			rec.replay(w)

			return
		}
	}

	writeRegError(w, http.StatusNotFound, "NAME_UNKNOWN", "Not found in any upstream")
}

var manifestTailRe = regexp.MustCompile(`^(.+)/manifests/([^/]+)$`)

// fillDockerRemote pulls a manifest and its blobs from the upstream registry into the repository before the
// request is served from it. A tag is refreshed from the upstream on every read; a digest is fetched once.
func (b *InMemoryBackend) fillDockerRemote(w http.ResponseWriter, r *http.Request, dp dockerPath) bool {
	m := manifestTailRe.FindStringSubmatch(dp.tail)
	if m == nil || !isRead(r) {
		return false
	}

	image, ref := m[1], m[2]
	repo := dp.project + "/" + dp.id + "/" + image
	_, haveErr := b.client().GetManifest(r.Context(), repo, ref)
	have := haveErr == nil

	if have && strings.HasPrefix(ref, "sha256:") {
		return false
	}

	rc, ok := b.repoOfFormat(dp.project, dp.id, artifactregistrypb.Repository_DOCKER)
	if !ok {
		return false
	}

	err := b.pullDockerImage(r.Context(), rc, repo, image, ref)
	if err != nil && !have {
		code, reg := http.StatusBadGateway, "UNAVAILABLE"
		if errors.Is(err, errUpstreamStatus) && strings.HasSuffix(err.Error(), " 404") {
			code, reg = http.StatusNotFound, "MANIFEST_UNKNOWN"
		}

		writeRegError(
			w,
			code,
			reg,
			"Remote repository: "+err.Error()+" (enable fetching with --gcp-artifacts-remote-upstreams)",
		)

		return true
	}

	return false
}

func dockerUpstream(rc repoCtx) (string, string, error) {
	cfg := rc.pb.GetRemoteRepositoryConfig()
	d := cfg.GetDockerRepository()

	uri := firstNonEmpty(cfg.GetCommonRepository().GetUri(), d.GetCustomRepository().GetUri())
	if uri == "" {
		if d.GetPublicRepository() != artifactregistrypb.RemoteRepositoryConfig_DockerRepository_DOCKER_HUB {
			return "", "", errNoUpstream
		}

		return "https://" + dockerHubHost, dockerHubHost, nil
	}

	u, err := url.Parse(uri)
	if err != nil {
		return "", "", fmt.Errorf("upstream uri: %w", err)
	}

	return strings.TrimRight(uri, "/"), u.Host, nil
}

type dockerManifest struct {
	MediaType string `json:"mediaType"`
	Config    struct {
		Digest string `json:"digest"`
	} `json:"config"`
	Layers []struct {
		Digest string `json:"digest"`
	} `json:"layers"`
	Manifests []struct {
		Digest string `json:"digest"`
	} `json:"manifests"`
}

func (b *InMemoryBackend) pullDockerImage(ctx context.Context, rc repoCtx, repo, image, ref string) error {
	base, host, err := dockerUpstream(rc)
	if err != nil {
		return err
	}

	if ctx, err = b.withUpstreamAuth(ctx, rc, base); err != nil {
		return err
	}

	name := image
	if host == dockerHubHost && !strings.Contains(image, "/") {
		name = "library/" + image
	}

	return b.pullManifest(ctx, base, name, repo, ref)
}

func (b *InMemoryBackend) pullManifest(ctx context.Context, base, name, repo, ref string) error {
	body, hdr, err := b.dockerGet(ctx, base, name, "/manifests/"+ref, manifestAccepts)
	if err != nil {
		return err
	}

	var m dockerManifest
	if err = json.Unmarshal(body, &m); err != nil {
		return fmt.Errorf("upstream manifest: %w", err)
	}

	for _, child := range m.Manifests {
		if err = b.pullManifest(ctx, base, name, repo, child.Digest); err != nil {
			return err
		}
	}

	digests := []string{}
	if m.Config.Digest != "" {
		digests = append(digests, m.Config.Digest)
	}

	for _, l := range m.Layers {
		digests = append(digests, l.Digest)
	}

	for _, d := range digests {
		if err = b.pullBlob(ctx, base, name, repo, d); err != nil {
			return err
		}
	}

	mt := firstNonEmpty(m.MediaType, strings.Split(hdr.Get("Content-Type"), ";")[0])

	digest, err := b.client().PutManifest(ctx, repo, ref, mt, body)
	if err != nil {
		return fmt.Errorf("store manifest: %w", err)
	}

	b.RecordManifestPut(repo, ref, digest)

	return nil
}

func (b *InMemoryBackend) pullBlob(ctx context.Context, base, name, repo, digest string) error {
	if _, err := b.client().GetBlob(ctx, repo, digest, maxDockerBody); err == nil {
		return nil
	}

	data, _, err := b.dockerGet(ctx, base, name, "/blobs/"+digest, "")
	if err != nil {
		return err
	}

	return b.pushBlob(ctx, repo, digest, data)
}

// pushBlob stores a blob through the embedded registry's upload protocol.
func (b *InMemoryBackend) pushBlob(ctx context.Context, repo, digest string, data []byte) error {
	app := b.Registry()

	start, err := http.NewRequestWithContext(ctx, http.MethodPost, "/v2/"+repo+"/blobs/uploads/", nil)
	if err != nil {
		return fmt.Errorf("blob upload: %w", err)
	}

	rec := newBufResponse()
	app.ServeHTTP(rec, start)

	loc := rec.header.Get("Location")
	if rec.status() != http.StatusAccepted || loc == "" {
		return fmt.Errorf("blob upload: %w %d", errUpstreamStatus, rec.status())
	}

	u, err := url.Parse(loc)
	if err != nil {
		return fmt.Errorf("blob upload location: %w", err)
	}

	q := u.Query()
	q.Set("digest", digest)

	finish, err := http.NewRequestWithContext(ctx, http.MethodPut, u.Path+"?"+q.Encode(), bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("blob upload: %w", err)
	}

	finish.Header.Set("Content-Type", "application/octet-stream")

	rec = newBufResponse()
	app.ServeHTTP(rec, finish)

	if rec.status() != http.StatusCreated {
		return fmt.Errorf("blob upload: %w %d", errUpstreamStatus, rec.status())
	}

	return nil
}

var bearerParamRe = regexp.MustCompile(`(\w+)="([^"]*)"`)

// dockerGet GETs <base>/v2/<name><tail>, answering a Bearer challenge with an anonymous token as Docker Hub
// and most registries expect.
func (b *InMemoryBackend) dockerGet(ctx context.Context, base, name, tail, accept string) ([]byte, http.Header, error) {
	target := base + "/v2/" + name + tail
	token := ""

	for range 2 {
		data, code, hdr, err := b.fetchWith(ctx, target, accept, token)
		if err != nil {
			return nil, nil, err
		}

		switch {
		case code == http.StatusOK:
			return data, hdr, nil
		case code == http.StatusUnauthorized && token == "":
			if token, err = b.anonymousToken(ctx, hdr.Get("WWW-Authenticate")); err != nil {
				return nil, nil, err
			}
		default:
			return nil, nil, fmt.Errorf("upstream %s: %w %d", target, errUpstreamStatus, code)
		}
	}

	return nil, nil, errDockerAuth
}

func (b *InMemoryBackend) anonymousToken(ctx context.Context, challenge string) (string, error) {
	if !strings.HasPrefix(strings.ToLower(challenge), "bearer ") {
		return "", errDockerAuth
	}

	params := map[string]string{}
	for _, m := range bearerParamRe.FindAllStringSubmatch(challenge, -1) {
		params[m[1]] = m[2]
	}

	realm, err := url.Parse(params["realm"])
	if err != nil || params["realm"] == "" {
		return "", errDockerAuth
	}

	q := realm.Query()
	for _, k := range []string{"service", "scope"} {
		if params[k] != "" {
			q.Set(k, params[k])
		}
	}

	realm.RawQuery = q.Encode()

	data, code, _, err := b.fetchWith(allowAuthHost(ctx, realm.Host), realm.String(), "", "")
	if err != nil {
		return "", err
	}

	if code != http.StatusOK {
		return "", fmt.Errorf("token %s: %w %d", realm.Host, errUpstreamStatus, code)
	}

	var tok struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}

	if err = json.Unmarshal(data, &tok); err != nil {
		return "", fmt.Errorf("token response: %w", err)
	}

	return firstNonEmpty(tok.Token, tok.AccessToken), nil
}

// fetchWith is fetch with an Accept header and an optional Bearer token.
func (b *InMemoryBackend) fetchWith(
	ctx context.Context,
	rawURL, accept, token string,
) ([]byte, int, http.Header, error) {
	b.mu.RLock("fetchWith")
	enabled := b.remote.enabled
	b.mu.RUnlock()

	if !enabled {
		return nil, 0, nil, errRemoteDisabled
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("upstream request: %w", err)
	}

	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	applyUpstreamAuth(ctx, req)

	resp, err := b.upstreamClient().Do(req)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("upstream %s: %w", rawURL, err)
	}

	defer func() { _ = resp.Body.Close() }()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxDockerBody))
	if err != nil {
		return nil, 0, nil, fmt.Errorf("upstream %s: %w", rawURL, err)
	}

	return data, resp.StatusCode, resp.Header, nil
}
