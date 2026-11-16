package azurecontainerregistry

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	importTimeout   = 5 * time.Minute
	maxImportBlob   = 1 << 28
	maxImportDoc    = 4 << 20
	defaultImageTag = "latest"
	manifestAccepts = "application/vnd.oci.image.index.v1+json, " +
		"application/vnd.docker.distribution.manifest.list.v2+json, " +
		"application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json"
)

// ImportSource is where importImage reads from: an emulated registry by name, or a registry URI.
type ImportSource struct {
	Registry    string
	RegistryURI string
	Image       string
	Username    string
	Password    string
}

// ImportRequest is an importImage call.
type ImportRequest struct {
	Source          ImportSource
	TargetTags      []string
	UntaggedTargets []string
	Force           bool
}

// SetRemoteImports allows importImage to read from registries outside this process.
func (b *InMemoryBackend) SetRemoteImports(on bool) {
	b.mu.Lock("SetRemoteImports")
	defer b.mu.Unlock()

	b.remoteImports = on
}

type imageRef struct {
	repo string
	ref  string
}

// parseImage reads repo, repo:tag or repo@sha256:digest.
func parseImage(s string) (imageRef, error) {
	if s == "" {
		return imageRef{}, fmt.Errorf("%w: image name is empty", ErrImportInvalid)
	}

	if repo, digest, ok := strings.Cut(s, "@"); ok && repo != "" && ociregistry.IsDigest(digest) {
		return imageRef{repo: repo, ref: digest}, nil
	}

	repo, tag, ok := strings.Cut(s, ":")
	if !ok || tag == "" {
		return imageRef{repo: strings.TrimSuffix(repo, ":"), ref: ""}, nil
	}

	if repo == "" || strings.Contains(tag, "/") {
		return imageRef{}, fmt.Errorf("%w: image %q", ErrImportInvalid, s)
	}

	return imageRef{repo: repo, ref: tag}, nil
}

type importManifest struct {
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

type fetchedManifest struct {
	digest    string
	mediaType string
	body      []byte
}

type imageSource interface {
	manifest(ctx context.Context, repo, ref string) (fetchedManifest, error)
	blob(ctx context.Context, repo, digest string) ([]byte, error)
}

type localSource struct{ c ociregistry.Client }

func (s localSource) manifest(ctx context.Context, repo, ref string) (fetchedManifest, error) {
	m, err := s.c.GetManifest(ctx, repo, ref)
	if err != nil {
		if errors.Is(err, ociregistry.ErrNotFound) {
			return fetchedManifest{}, fmt.Errorf("%w: %s:%s", ErrImportSourceNotFound, repo, ref)
		}

		return fetchedManifest{}, fmt.Errorf("%w: %w", ErrImportUpstream, err)
	}

	return fetchedManifest{digest: m.Digest, mediaType: m.MediaType, body: m.Body}, nil
}

func (s localSource) blob(ctx context.Context, repo, digest string) ([]byte, error) {
	data, err := s.c.GetBlob(ctx, repo, digest, maxImportBlob)
	if err != nil {
		return nil, fmt.Errorf("%w: blob %s: %w", ErrImportUpstream, digest, err)
	}

	return data, nil
}

// ImportImage copies an image from the source into the registry, as POST importImage does.
func (b *InMemoryBackend) ImportImage(ctx context.Context, reg string, req ImportRequest) error {
	src, err := parseImage(req.Source.Image)
	if err != nil {
		return err
	}

	targets, err := importTargets(src, req)
	if err != nil {
		return err
	}

	app, ok := b.app(reg)
	if !ok {
		return ErrRegistryNotFound
	}

	from, err := b.importSource(req.Source)
	if err != nil {
		return err
	}

	dst := ociregistry.NewClient(app)

	if !req.Force {
		for _, t := range targets {
			if t.ref != "" && !ociregistry.IsDigest(t.ref) {
				if _, getErr := dst.GetManifest(ctx, t.repo, t.ref); getErr == nil {
					return fmt.Errorf("%w: %s:%s", ErrImportTagExists, t.repo, t.ref)
				}
			}
		}
	}

	srcRef := src.ref
	if srcRef == "" {
		srcRef = defaultImageTag
	}

	for _, t := range targets {
		digest, copyErr := copyImage(ctx, from, dst, src.repo, srcRef, t)
		if copyErr != nil {
			return copyErr
		}

		b.afterImport(ctx, reg, app, t, digest)
	}

	return nil
}

func importTargets(src imageRef, req ImportRequest) ([]imageRef, error) {
	srcTag := src.ref
	if srcTag == "" || ociregistry.IsDigest(srcTag) {
		srcTag = defaultImageTag
	}

	var out []imageRef

	for _, t := range req.TargetTags {
		ref, err := parseImage(t)
		if err != nil {
			return nil, err
		}

		if ref.ref == "" {
			ref.ref = srcTag
		}

		out = append(out, ref)
	}

	for _, repo := range req.UntaggedTargets {
		if repo == "" {
			return nil, fmt.Errorf("%w: empty untagged target repository", ErrImportInvalid)
		}

		out = append(out, imageRef{repo: repo})
	}

	if len(out) == 0 {
		out = append(out, imageRef{repo: src.repo, ref: srcTag})
	}

	return out, nil
}

func copyImage(
	ctx context.Context, from imageSource, dst ociregistry.Client, srcRepo, srcRef string, t imageRef,
) (string, error) {
	m, err := from.manifest(ctx, srcRepo, srcRef)
	if err != nil {
		return "", err
	}

	var doc importManifest
	if err = json.Unmarshal(m.body, &doc, jsonx.V1()); err != nil {
		return "", fmt.Errorf("%w: manifest: %w", ErrImportUpstream, err)
	}

	for _, child := range doc.Manifests {
		if _, err = copyImage(
			ctx,
			from,
			dst,
			srcRepo,
			child.Digest,
			imageRef{repo: t.repo, ref: child.Digest},
		); err != nil {
			return "", err
		}
	}

	digests := make([]string, 0, len(doc.Layers)+1)
	if doc.Config.Digest != "" {
		digests = append(digests, doc.Config.Digest)
	}

	for _, l := range doc.Layers {
		digests = append(digests, l.Digest)
	}

	for _, d := range digests {
		data, blobErr := from.blob(ctx, srcRepo, d)
		if blobErr != nil {
			return "", blobErr
		}

		if err = dst.PutBlob(ctx, t.repo, d, data); err != nil {
			return "", fmt.Errorf("store blob %s: %w", d, err)
		}
	}

	ref := t.ref
	if ref == "" {
		ref = m.digest
	}

	mediaType, _, _ := strings.Cut(m.mediaType, ";")
	if doc.MediaType != "" {
		mediaType = doc.MediaType
	}

	digest, err := dst.PutManifest(ctx, t.repo, ref, mediaType, m.body)
	if err != nil {
		return "", fmt.Errorf("store manifest: %w", err)
	}

	return digest, nil
}

func (b *InMemoryBackend) afterImport(ctx context.Context, reg string, app http.Handler, t imageRef, digest string) {
	ref := t.ref
	if ref == "" {
		ref = digest
	}

	b.RecordManifestPut(reg, t.repo, ref, digest)

	if !b.HasWebhooks(reg) {
		return
	}

	r, err := http.NewRequestWithContext(ctx, http.MethodPut, "/v2/"+t.repo+"/manifests/"+ref, nil)
	if err != nil {
		return
	}

	r.Host = strs.Fold(reg) + ".azurecr.localhost"

	if c, ok := loadChange(app, r, t.repo, digest); ok {
		c.Tag = ""
		if !ociregistry.IsDigest(ref) {
			c.Tag = ref
		}

		b.FireManifest(reg, c)
	}
}

// importSource picks the emulated registry named by Registry or RegistryURI, else the external registry.
func (b *InMemoryBackend) importSource(s ImportSource) (imageSource, error) {
	if s.Registry != "" {
		app, ok := b.app(s.Registry)
		if !ok {
			return nil, fmt.Errorf("%w: registry %s", ErrImportSourceNotFound, s.Registry)
		}

		return localSource{c: ociregistry.NewClient(app)}, nil
	}

	if s.RegistryURI == "" {
		return nil, fmt.Errorf("%w: source needs a resourceId or registryUri", ErrImportInvalid)
	}

	host := strings.TrimPrefix(strings.TrimPrefix(s.RegistryURI, schemeHTTPS+"://"), "http://")
	host, _, _ = strings.Cut(host, "/")
	host = hostOnly(host)

	if label, rest, ok := strings.Cut(host, "."); ok && emulatedHost(rest) {
		if name, found := b.registryNamed(label); found && b.sourceReadable(name, s) {
			app, _ := b.app(name)

			return localSource{c: ociregistry.NewClient(app)}, nil
		}
	}

	b.mu.RLock("importSource")
	enabled := b.remoteImports
	b.mu.RUnlock()

	if !enabled {
		return nil, ErrImportRemoteDisabled
	}

	return &remoteSource{b: b, base: remoteBase(s.RegistryURI), user: s.Username, pass: s.Password}, nil
}

// emulatedHost tells an emulated login server (name.azurecr.localhost) from a public Azure cloud one.
func emulatedHost(rest string) bool {
	return strings.HasPrefix(rest, "azurecr.") &&
		!slices.Contains([]string{"azurecr.io", "azurecr.cn", "azurecr.us"}, rest)
}

func (b *InMemoryBackend) registryNamed(label string) (string, bool) {
	info, ok := b.Registry(label)

	return info.Name, ok
}

// sourceReadable reports whether the credentials in s may read the emulated registry name.
func (b *InMemoryBackend) sourceReadable(name string, s ImportSource) bool {
	return b.anonymousPull(name) || b.VerifyAdmin(name, s.Username, s.Password) ||
		b.VerifyToken(name, s.Username, s.Password)
}

func remoteBase(uri string) string {
	if strings.HasPrefix(uri, "http://") || strings.HasPrefix(uri, schemeHTTPS+"://") {
		return strings.TrimRight(uri, "/")
	}

	return schemeHTTPS + "://" + strings.TrimRight(uri, "/")
}

type remoteSource struct {
	b    *InMemoryBackend
	base string
	user string
	pass string
}

var bearerParams = regexp.MustCompile(`(\w+)="([^"]*)"`)

func (s *remoteSource) manifest(ctx context.Context, repo, ref string) (fetchedManifest, error) {
	data, hdr, err := s.get(ctx, "/v2/"+repo+"/manifests/"+ref, manifestAccepts, maxImportDoc)
	if err != nil {
		return fetchedManifest{}, err
	}

	digest := hdr.Get("Docker-Content-Digest")

	return fetchedManifest{digest: digest, mediaType: hdr.Get("Content-Type"), body: data}, nil
}

func (s *remoteSource) blob(ctx context.Context, repo, digest string) ([]byte, error) {
	data, _, err := s.get(ctx, "/v2/"+repo+"/blobs/"+digest, "", maxImportBlob)

	return data, err
}

func (b *InMemoryBackend) importClient() *http.Client {
	b.mu.Lock("importClient")
	defer b.mu.Unlock()

	if b.importc == nil {
		b.importc = httpclient.New(importTimeout)
	}

	return b.importc
}

func (s *remoteSource) get(
	ctx context.Context, path, accept string, limit int64,
) ([]byte, http.Header, error) {
	target := s.base + path
	token := ""

	for range 2 {
		data, code, hdr, err := s.fetch(ctx, target, accept, token, limit)
		if err != nil {
			return nil, nil, err
		}

		switch {
		case code == http.StatusOK:
			return data, hdr, nil
		case code == http.StatusNotFound:
			return nil, nil, fmt.Errorf("%w: %s", ErrImportSourceNotFound, target)
		case code == http.StatusUnauthorized && token == "":
			if token, err = s.token(ctx, hdr.Get("WWW-Authenticate")); err != nil {
				return nil, nil, err
			}
		case code == http.StatusUnauthorized || code == http.StatusForbidden:
			return nil, nil, ErrImportAuth
		default:
			return nil, nil, fmt.Errorf("%w: %s answered %d", ErrImportUpstream, target, code)
		}
	}

	return nil, nil, ErrImportAuth
}

func (s *remoteSource) token(ctx context.Context, challenge string) (string, error) {
	if !strings.HasPrefix(strings.ToLower(challenge), "bearer ") {
		return "", ErrImportAuth
	}

	params := map[string]string{}
	for _, m := range bearerParams.FindAllStringSubmatch(challenge, -1) {
		params[m[1]] = m[2]
	}

	realm, err := url.Parse(params["realm"])
	if err != nil || params["realm"] == "" || (realm.Scheme != schemeHTTPS && realm.Scheme != "http") {
		return "", ErrImportAuth
	}

	q := realm.Query()
	for _, k := range []string{"service", "scope"} {
		if params[k] != "" {
			q.Set(k, params[k])
		}
	}

	realm.RawQuery = q.Encode()

	data, code, _, err := s.fetch(ctx, realm.String(), "", "", maxImportDoc)
	if err != nil {
		return "", err
	}

	if code != http.StatusOK {
		return "", ErrImportAuth
	}

	var tok struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
	}

	if err = json.Unmarshal(data, &tok, jsonx.V1()); err != nil {
		return "", fmt.Errorf("%w: token response: %w", ErrImportUpstream, err)
	}

	if tok.Token != "" {
		return tok.Token, nil
	}

	return tok.AccessToken, nil
}

func (s *remoteSource) fetch(
	ctx context.Context, rawURL, accept, bearer string, limit int64,
) ([]byte, int, http.Header, error) {
	req, err := http.NewRequestWithContext( //nolint:gosec // the registry is chosen by the caller of an opt-in import
		ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, nil, fmt.Errorf("%w: %w", ErrImportInvalid, err)
	}

	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	switch {
	case bearer != "":
		req.Header.Set("Authorization", "Bearer "+bearer)
	case s.user != "" || s.pass != "":
		req.SetBasicAuth(s.user, s.pass)
	}

	resp, err := s.b.importClient().Do(req) //nolint:gosec // the registry is chosen by the caller of an opt-in import
	if err != nil {
		return nil, 0, nil, fmt.Errorf("%w: %w", ErrImportUpstream, err)
	}

	defer func() { _ = resp.Body.Close() }()

	var buf bytes.Buffer

	if _, err = io.Copy(&buf, io.LimitReader(resp.Body, limit+1)); err != nil {
		return nil, 0, nil, fmt.Errorf("%w: %w", ErrImportUpstream, err)
	}

	if int64(buf.Len()) > limit {
		return nil, 0, nil, fmt.Errorf("%w: response larger than %d bytes", ErrImportUpstream, limit)
	}

	return buf.Bytes(), resp.StatusCode, resp.Header, nil
}
