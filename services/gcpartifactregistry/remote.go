package gcpartifactregistry

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const upstreamTimeout = 60 * time.Second

var (
	errRemoteDisabled = errors.New("remote repository access to the upstream is disabled")
	errNoUpstream     = errors.New("the repository has no usable upstream")
)

// remoteOptions controls whether remote repositories may reach their upstream.
type remoteOptions struct {
	client  *http.Client
	enabled bool
}

// SetRemoteUpstreams lets remote repositories fetch from their upstream over the network; off by default.
func (b *InMemoryBackend) SetRemoteUpstreams(on bool) {
	b.mu.Lock("SetRemoteUpstreams")
	defer b.mu.Unlock()

	b.remote.enabled = on
}

func (b *InMemoryBackend) upstreamClient() *http.Client {
	b.mu.Lock("upstreamClient")
	defer b.mu.Unlock()

	if b.remote.client == nil {
		b.remote.client = httpclient.New(upstreamTimeout)
	}

	return b.remote.client
}

var publicUpstreams = map[string]string{ //nolint:gochecknoglobals // immutable catalog
	"DOCKER_HUB":      "https://registry-1.docker.io",
	"MAVEN_CENTRAL":   "https://repo.maven.apache.org/maven2",
	"NPMJS":           "https://registry.npmjs.org",
	"PYPI":            "https://pypi.org",
	"DEBIAN":          "https://deb.debian.org/debian",
	"UBUNTU":          "http://archive.ubuntu.com/ubuntu",
	"DEBIAN_SNAPSHOT": "https://snapshot.debian.org/archive/debian",
	"CENTOS":          "https://vault.centos.org",
	"CENTOS_DEBUG":    "https://debuginfo.centos.org",
	"CENTOS_VAULT":    "https://vault.centos.org",
	"CENTOS_STREAM":   "https://mirror.stream.centos.org",
	"ROCKY":           "https://download.rockylinux.org/pub/rocky",
	"FEDORA_LINUX":    "https://dl.fedoraproject.org/pub/fedora/linux",
	"EPEL":            "https://dl.fedoraproject.org/pub/epel",
}

// upstreamBase resolves the remote repository's upstream URL: its custom or common repository URI, else the
// public repository it names. The mapping of public repositories to URLs is not published by Google (unverified).
func upstreamBase(cfg *artifactregistrypb.RemoteRepositoryConfig) (string, error) {
	uri := cfg.GetCommonRepository().GetUri()
	public := ""

	switch {
	case cfg.GetDockerRepository() != nil:
		d := cfg.GetDockerRepository()
		uri, public = firstNonEmpty(uri, d.GetCustomRepository().GetUri()), d.GetPublicRepository().String()
	case cfg.GetMavenRepository() != nil:
		m := cfg.GetMavenRepository()
		uri, public = firstNonEmpty(uri, m.GetCustomRepository().GetUri()), m.GetPublicRepository().String()
	case cfg.GetNpmRepository() != nil:
		n := cfg.GetNpmRepository()
		uri, public = firstNonEmpty(uri, n.GetCustomRepository().GetUri()), n.GetPublicRepository().String()
	case cfg.GetPythonRepository() != nil:
		p := cfg.GetPythonRepository()
		uri, public = firstNonEmpty(uri, p.GetCustomRepository().GetUri()), p.GetPublicRepository().String()
	case cfg.GetAptRepository() != nil:
		a := cfg.GetAptRepository()
		uri = firstNonEmpty(uri, a.GetCustomRepository().GetUri())

		if pr := a.GetPublicRepository(); uri == "" && pr != nil {
			return strings.TrimRight(
				publicUpstreams[pr.GetRepositoryBase().String()]+"/"+pr.GetRepositoryPath(),
				"/",
			), nil
		}
	case cfg.GetYumRepository() != nil:
		y := cfg.GetYumRepository()
		uri = firstNonEmpty(uri, y.GetCustomRepository().GetUri())

		if pr := y.GetPublicRepository(); uri == "" && pr != nil {
			return strings.TrimRight(
				publicUpstreams[pr.GetRepositoryBase().String()]+"/"+pr.GetRepositoryPath(),
				"/",
			), nil
		}
	}

	if uri == "" {
		uri = publicUpstreams[public]
	}

	if uri == "" {
		return "", errNoUpstream
	}

	return strings.TrimRight(uri, "/"), nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

// fetch GETs an upstream URL. It fails with errRemoteDisabled unless remote upstreams were enabled.
func (b *InMemoryBackend) fetch(ctx context.Context, rawURL string) ([]byte, int, http.Header, error) {
	return b.fetchWith(ctx, rawURL, "", "")
}

// fetchOK is fetch that treats any non-200 status as an error.
func (b *InMemoryBackend) fetchOK(ctx context.Context, rawURL string) ([]byte, http.Header, error) {
	data, code, hdr, err := b.fetch(ctx, rawURL)
	if err != nil {
		return nil, nil, err
	}

	if code != http.StatusOK {
		return nil, nil, fmt.Errorf("upstream %s: %w %d", rawURL, errUpstreamStatus, code)
	}

	return data, hdr, nil
}

var errUpstreamStatus = errors.New("unexpected status")

func remoteFailure(w http.ResponseWriter, err error) {
	if errors.Is(err, errUpstreamStatus) && strings.HasSuffix(err.Error(), " 404") {
		writePlain(w, http.StatusNotFound, "Not found upstream")

		return
	}

	writePlain(w, http.StatusBadGateway, "Remote repository: "+err.Error()+
		" (enable fetching with --gcp-artifacts-remote-upstreams)")
}

// localHit serves path from the repository's own store when it holds it.
func (b *InMemoryBackend) localHit(w http.ResponseWriter, r *http.Request, rc repoCtx, p string) bool {
	rec := newBufResponse()
	b.serveLocal(rec, r, rc, p)

	if st := rec.status(); st < http.StatusBadRequest || st == http.StatusForbidden {
		rec.replay(w)

		return true
	}

	return false
}

// serveRemote reads through to the upstream, caching artifacts in the repository.
func (b *InMemoryBackend) serveRemote(w http.ResponseWriter, r *http.Request, rc repoCtx, p string) {
	if !isRead(r) {
		writePlain(w, http.StatusMethodNotAllowed, "Remote repositories are read-only.")

		return
	}

	base, err := upstreamBase(rc.pb.GetRemoteRepositoryConfig())
	if err != nil {
		writePlain(w, http.StatusBadGateway, "Remote repository: "+err.Error())

		return
	}

	switch rc.pb.GetFormat() {
	case artifactregistrypb.Repository_NPM:
		b.remoteNPM(w, r, rc, base, p)
	case artifactregistrypb.Repository_PYTHON:
		b.remotePython(w, r, rc, base, p)
	case artifactregistrypb.Repository_MAVEN:
		b.remoteMaven(w, r, rc, base, p)
	case artifactregistrypb.Repository_APT, artifactregistrypb.Repository_YUM:
		b.remoteMirror(w, r, rc, base, p)
	default:
		b.serveLocal(w, r, rc, p)
	}
}

func tgzPackageJSON(data []byte) ([]byte, error) {
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}

	tr := tar.NewReader(zr)

	for {
		h, nerr := tr.Next()
		if nerr != nil {
			return nil, fmt.Errorf("package.json: %w", nerr)
		}

		if path.Base(h.Name) == "package.json" && strings.Count(h.Name, "/") == 1 {
			return io.ReadAll(io.LimitReader(tr, maxControl))
		}
	}
}

func (b *InMemoryBackend) remoteNPM(w http.ResponseWriter, r *http.Request, rc repoCtx, base, p string) {
	pkg, tail := splitNPMPath(p)
	if pkg == "" || strings.HasPrefix(p, "-/") {
		b.serveLocal(w, r, rc, p)

		return
	}

	escPkg := strings.Replace(pkg, "/", "%2F", 1)

	switch {
	case tail == "":
		data, _, err := b.fetchOK(r.Context(), base+"/"+escPkg)
		if err != nil {
			if !b.localHit(w, r, rc, p) {
				remoteFailure(w, err)
			}

			return
		}

		out, err := rewritePackument(data, baseURL(r, npmRoute, rc), pkg)
		if err != nil {
			writePlain(w, http.StatusBadGateway, "Remote repository: "+err.Error())

			return
		}

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(out)
	case strings.HasPrefix(tail, "-/"):
		if b.localHit(w, r, rc, p) {
			return
		}

		file := strings.TrimPrefix(tail, "-/")

		data, _, err := b.fetchOK(r.Context(), base+"/"+escPkg+"/-/"+file)
		if err != nil {
			remoteFailure(w, err)

			return
		}

		if err = b.cacheNPMTarball(rc, pkg, file, data); err != nil {
			writePlain(w, http.StatusBadGateway, "Remote repository: "+err.Error())

			return
		}

		b.serveLocal(w, r, rc, p)
	default:
		b.serveLocal(w, r, rc, p)
	}
}

func (b *InMemoryBackend) cacheNPMTarball(rc repoCtx, pkg, file string, data []byte) error {
	doc, err := tgzPackageJSON(data)
	if err != nil {
		return err
	}

	var m struct {
		Version string `json:"version"`
	}

	if err = json.Unmarshal(doc, &m); err != nil || m.Version == "" {
		return fmt.Errorf("package.json has no version: %w", errNoUpstream)
	}

	return b.putPackageFile(rc.ref, pkgFile{
		pkgID: pkg, display: pkg, version: m.Version, fileID: npmFileID(pkg, file), data: data, doc: doc,
	}, true)
}

// rewritePackument points every version's dist.tarball at the local repository.
func rewritePackument(data []byte, local, pkg string) ([]byte, error) {
	var doc map[string]any
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("upstream packument: %w", err)
	}

	versions, _ := doc["versions"].(map[string]any)
	for _, v := range versions {
		vm, _ := v.(map[string]any)
		dist, _ := vm["dist"].(map[string]any)

		if tb, _ := dist["tarball"].(string); tb != "" {
			dist["tarball"] = local + "/" + pkg + "/-/" + path.Base(tb)
		}
	}

	out, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("packument: %w", err)
	}

	return out, nil
}

var pyAnchorRe = regexp.MustCompile(`(?is)<a\s+([^>]*?)>([^<]*)</a>`)

var pyHrefRe = regexp.MustCompile(`(?i)href="([^"]*)"`)

var pyRequiresRe = regexp.MustCompile(`(?i)data-requires-python="([^"]*)"`)

type pyLink struct {
	href, filename, requires, fragment string
}

func parsePyLinks(page string, index *url.URL) []pyLink {
	var out []pyLink

	for _, m := range pyAnchorRe.FindAllStringSubmatch(page, -1) {
		hm := pyHrefRe.FindStringSubmatch(m[1])
		if hm == nil {
			continue
		}

		ref, err := url.Parse(html.UnescapeString(hm[1]))
		if err != nil {
			continue
		}

		l := pyLink{filename: strings.TrimSpace(html.UnescapeString(m[2])), fragment: ref.Fragment}
		ref.Fragment = ""
		l.href = index.ResolveReference(ref).String()

		if rm := pyRequiresRe.FindStringSubmatch(m[1]); rm != nil {
			l.requires = rm[1]
		}

		out = append(out, l)
	}

	return out
}

func pyVersionOf(filename string) string {
	stem := strings.TrimSuffix(filename, ".whl")
	if stem != filename {
		parts := strings.Split(stem, "-")
		if len(parts) > 1 {
			return parts[1]
		}
	}

	for _, ext := range []string{".tar.gz", ".tar.bz2", ".tar.xz", ".zip", ".egg"} {
		stem = strings.TrimSuffix(stem, ext)
	}

	if i := strings.LastIndex(stem, "-"); i >= 0 {
		return stem[i+1:]
	}

	return stem
}

func pyIndexURL(base, name string) string {
	root := strings.TrimRight(base, "/")
	if !strings.HasSuffix(root, "/simple") {
		root += "/simple"
	}

	return root + "/" + url.PathEscape(name) + "/"
}

func (b *InMemoryBackend) remotePython(w http.ResponseWriter, r *http.Request, rc repoCtx, base, p string) {
	switch {
	case strings.HasPrefix(p, "simple/") && strings.Trim(strings.TrimPrefix(p, "simple/"), "/") != "":
		b.remotePyProject(w, r, rc, base, pyNormalize(strings.Trim(strings.TrimPrefix(p, "simple/"), "/")))
	case strings.HasPrefix(p, "packages/"):
		segs := strings.Split(strings.TrimPrefix(p, "packages/"), "/")
		if b.localHit(w, r, rc, p) || len(segs) != pyPathSegs {
			return
		}

		b.remotePyFile(w, r, rc, base, segs)
	default:
		b.serveLocal(w, r, rc, p)
	}
}

func (b *InMemoryBackend) remotePyProject(w http.ResponseWriter, r *http.Request, rc repoCtx, base, norm string) {
	idx := pyIndexURL(base, norm)

	data, _, err := b.fetchOK(r.Context(), idx)
	if err != nil {
		if !b.localHit(w, r, rc, "simple/"+norm) {
			remoteFailure(w, err)
		}

		return
	}

	u, _ := url.Parse(idx)
	prefix := repoPrefix(false, pythonRoute, rc)

	var sb strings.Builder

	sb.WriteString(
		"<!DOCTYPE html>\n<html><head><meta name=\"pypi:repository-version\" content=\"1.0\"><title>Links for " +
			html.EscapeString(norm) + "</title></head><body>\n",
	)

	for _, l := range parsePyLinks(string(data), u) {
		href := prefix + "/packages/" + url.PathEscape(norm) + "/" + url.PathEscape(pyVersionOf(l.filename)) + "/" +
			url.PathEscape(l.filename)
		if l.fragment != "" {
			href += "#" + l.fragment
		}

		attr := ""
		if l.requires != "" {
			attr = " data-requires-python=\"" + l.requires + "\""
		}

		sb.WriteString(
			"<a href=\"" + html.EscapeString(href) + "\"" + attr + ">" + html.EscapeString(l.filename) + "</a><br>\n",
		)
	}

	sb.WriteString("</body></html>\n")
	writeHTML(w, sb.String())
}

func (b *InMemoryBackend) remotePyFile(w http.ResponseWriter, r *http.Request, rc repoCtx, base string, segs []string) {
	norm, version, filename := segs[0], segs[1], segs[2]

	idx := pyIndexURL(base, norm)

	page, _, err := b.fetchOK(r.Context(), idx)
	if err != nil {
		remoteFailure(w, err)

		return
	}

	u, _ := url.Parse(idx)

	for _, l := range parsePyLinks(string(page), u) {
		if l.filename != filename {
			continue
		}

		data, _, ferr := b.fetchOK(r.Context(), l.href)
		if ferr != nil {
			remoteFailure(w, ferr)

			return
		}

		if err = b.cachePyFile(rc, norm, version, filename, l, data); err != nil {
			writePlain(w, http.StatusBadGateway, "Remote repository: "+err.Error())

			return
		}

		b.serveLocal(w, r, rc, "packages/"+norm+"/"+version+"/"+filename)

		return
	}

	writePlain(w, http.StatusNotFound, "Not found upstream")
}

var errDigest = errors.New("the upstream file does not match its sha256")

func (b *InMemoryBackend) cachePyFile(rc repoCtx, norm, version, filename string, l pyLink, data []byte) error {
	sum := sha256Hex(data)
	if want, ok := strings.CutPrefix(l.fragment, "sha256="); ok && want != sum {
		return errDigest
	}

	doc, _ := json.Marshal(pyDoc{Name: norm, RequiresPython: html.UnescapeString(l.requires), SHA256: sum})

	return b.putPackageFile(rc.ref, pkgFile{
		pkgID: norm, display: norm, version: version, fileID: pyFileID(norm, version, filename), data: data, doc: doc,
	}, true)
}

func (b *InMemoryBackend) remoteMaven(w http.ResponseWriter, r *http.Request, rc repoCtx, base, p string) {
	t, ok := parseMavenPath(p)
	if !ok {
		b.serveLocal(w, r, rc, p)

		return
	}

	plain, _ := splitChecksum(p)

	if t.kind == mavenArtifact && b.localHit(w, r, rc, p) {
		return
	}

	data, _, err := b.fetchOK(r.Context(), base+"/"+plain)
	if err != nil {
		if t.kind != mavenArtifact && b.localHit(w, r, rc, p) {
			return
		}

		remoteFailure(w, err)

		return
	}

	if t.kind == mavenArtifact {
		if err = b.putPackageFile(rc.ref, pkgFile{
			pkgID: t.pkgID, display: t.pkgID, version: t.version, fileID: plain, data: data,
		}, true); err != nil {
			writePlain(w, http.StatusBadGateway, "Remote repository: "+err.Error())

			return
		}

		b.serveLocal(w, r, rc, p)

		return
	}

	if t.checksum != "" {
		h := checksumHash(t.checksum)
		_, _ = h.Write(data)
		writeBytes(w, "text/plain", []byte(hex.EncodeToString(h.Sum(nil))))

		return
	}

	writeBytes(w, mavenContentType(t.filename), data)
}

// remoteMirror proxies an apt or yum upstream: package files are cached under their upstream path, indices are
// fetched every time.
func (b *InMemoryBackend) remoteMirror(w http.ResponseWriter, r *http.Request, rc repoCtx, base, p string) {
	isDeb, isRPM := strings.HasSuffix(p, ".deb"), strings.HasSuffix(p, ".rpm")

	if f, ok := b.packageFile(rc.ref, p); ok && (isDeb || isRPM) {
		writeBytes(w, "application/octet-stream", f.Data)

		return
	}

	data, hdr, err := b.fetchOK(r.Context(), base+"/"+p)
	if err != nil {
		remoteFailure(w, err)

		return
	}

	if isDeb || isRPM {
		if cerr := b.cacheMirrorPackage(rc, p, data, isDeb); cerr != nil {
			writePlain(w, http.StatusBadGateway, "Remote repository: "+cerr.Error())

			return
		}
	}

	writeBytes(w, firstNonEmpty(hdr.Get("Content-Type"), "application/octet-stream"), data)
}

func (b *InMemoryBackend) cacheMirrorPackage(rc repoCtx, p string, data []byte, deb bool) error {
	if deb {
		c, ctl, err := parseDeb(data)
		if err != nil {
			return err
		}

		return b.putPackageFile(rc.ref, pkgFile{
			pkgID: c.Package, display: c.Package, version: c.Version, fileID: p, data: data, doc: ctl,
		}, true)
	}

	m, err := parseRPM(data)
	if err != nil {
		return err
	}

	return b.putPackageFile(rc.ref, pkgFile{
		pkgID: m.strs[tagName], display: m.strs[tagName], version: m.strs[tagVersion] + "-" + m.strs[tagRelease],
		fileID: p, data: data,
	}, true)
}
