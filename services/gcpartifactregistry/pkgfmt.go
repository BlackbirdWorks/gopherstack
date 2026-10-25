package gcpartifactregistry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"

	"github.com/blackbirdworks/gopherstack/pkgs/gcprest"
)

const (
	npmRoute    = "/npm/"
	pythonRoute = "/python/"
	mavenRoute  = "/maven/"
	routeSegs   = 3
)

// repoCtx is the repository a package-manager request addresses.
type repoCtx struct {
	pb  *artifactregistrypb.Repository
	ref repoRef
}

var pkgHostRe = regexp.MustCompile(`^([a-z0-9-]+)-(npm|python|maven)\.pkg\.dev(:\d+)?$`)

var hostFormats = map[string]artifactregistrypb.Repository_Format{ //nolint:gochecknoglobals // immutable catalog
	"npm": artifactregistrypb.Repository_NPM, "python": artifactregistrypb.Repository_PYTHON,
	"maven": artifactregistrypb.Repository_MAVEN,
}

var routeFormats = map[string]artifactregistrypb.Repository_Format{ //nolint:gochecknoglobals // immutable catalog
	npmRoute: artifactregistrypb.Repository_NPM, pythonRoute: artifactregistrypb.Repository_PYTHON,
	mavenRoute: artifactregistrypb.Repository_MAVEN,
}

// pkgHost parses <location>-<npm|python|maven>.pkg.dev[:port] into its location and format.
func pkgHost(host string) (string, artifactregistrypb.Repository_Format, bool) {
	m := pkgHostRe.FindStringSubmatch(host)
	if m == nil {
		return "", 0, false
	}

	return m[1], hostFormats[m[2]], true
}

// PackageExtras routes the npm, Python and Maven protocols on <location>-<format>.pkg.dev hosts, where the
// path is /<project>/<repository>/..., and at /<format>/<project>/<repository>/... on any other host.
func (b *InMemoryBackend) PackageExtras() gcprest.Extra {
	return gcprest.Extra{
		MatchRequest: func(r *http.Request) bool {
			if _, _, ok := pkgHost(r.Host); ok {
				return true
			}

			path := r.URL.Path

			return strings.HasPrefix(path, npmRoute) || strings.HasPrefix(path, pythonRoute) ||
				strings.HasPrefix(path, mavenRoute)
		},
		Handler: http.HandlerFunc(b.servePackageRoute),
	}
}

func (b *InMemoryBackend) servePackageRoute(w http.ResponseWriter, r *http.Request) {
	esc := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	location, hostFormat, hosted := pkgHost(r.Host)

	var route string

	format := hostFormat
	segs := routeSegs - 1

	if !hosted {
		route = "/" + strings.SplitN(esc, "/", routeSegs-1)[0] + "/"
		format = routeFormats[route]
		esc = strings.TrimPrefix(esc, route[1:])
	}

	parts := strings.SplitN(esc, "/", segs+1)

	var rest string
	if len(parts) > segs {
		rest = parts[segs]
	}

	if len(parts) < segs {
		writePlain(w, http.StatusNotFound, "Repository path must be <project>/<repository>.")

		return
	}

	rc, ok := b.repoOfFormat(parts[0], parts[1], format)
	if !ok || hosted && rc.ref.location != location {
		writePlain(w, http.StatusNotFound, "No "+strings.ToLower(format.String())+" repository "+parts[1]+".")

		return
	}

	path, err := url.PathUnescape(rest)
	if err != nil {
		writePlain(w, http.StatusBadRequest, "Invalid path encoding.")

		return
	}

	path = strings.Trim(path, "/")

	switch format {
	case artifactregistrypb.Repository_NPM:
		b.serveNPM(w, r, rc, path)
	case artifactregistrypb.Repository_PYTHON:
		b.servePython(w, r, rc, path)
	default:
		b.serveMaven(w, r, rc, path)
	}
}

// repoPrefix is the path prefix of a repository's package-manager endpoint: /<project>/<repository> on a
// pkg.dev host, /<format>/<project>/<repository> otherwise.
func isPkgHost(r *http.Request) bool { return pkgHostRe.MatchString(r.Host) }

func repoPrefix(hosted bool, route string, rc repoCtx) string {
	if hosted {
		return "/" + rc.ref.project + "/" + rc.ref.id
	}

	return route + rc.ref.project + "/" + rc.ref.id
}

func (b *InMemoryBackend) repoOfFormat(
	project, id string, format artifactregistrypb.Repository_Format,
) (repoCtx, bool) {
	b.mu.RLock("repoOfFormat")
	defer b.mu.RUnlock()

	name, rec, ok := b.repoByKey(project, id)
	if !ok || rec.pb.GetFormat() != format {
		return repoCtx{}, false
	}

	ref, err := parseRepo(name)
	if err != nil {
		return repoCtx{}, false
	}

	return repoCtx{ref: ref, pb: cloneRepo(rec.pb)}, true
}

func writePlain(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = w.Write([]byte(msg + "\n")) //nolint:gosec // text/plain, never rendered
}

func writeJSONBody(w http.ResponseWriter, code int, v any) {
	raw, err := json.Marshal(v)
	if err != nil {
		writePlain(w, http.StatusInternalServerError, err.Error())

		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(raw)
}

// pkgFile is a file to store under a package version.
type pkgFile struct {
	pkgID, display, version, fileID string
	data, doc                       []byte
}

// putPackageFile stores a file, creating its package and version. An existing file is replaced only when
// overwrite is set.
func (b *InMemoryBackend) putPackageFile(ref repoRef, in pkgFile, overwrite bool) error {
	pkg := ref.packageName(in.pkgID)
	ver := pkg + "/versions/" + in.version
	file := ref.fileName(in.fileID)
	now := b.now().UTC()

	b.mu.Lock("putPackageFile")
	defer b.mu.Unlock()

	created := now

	if old, ok := b.genFiles[file]; ok {
		if !overwrite {
			return errExists("File %q already exists.", file)
		}

		created = old.Created
	}

	touch(b.genPkgs, pkg, resMeta{Created: now, Updated: now, DisplayName: in.display})
	touch(b.genVers, ver, resMeta{Created: now, Updated: now})
	b.genFiles[file] = &genFile{Created: created, Updated: now, Owner: ver, Data: in.data, Doc: in.doc}

	return nil
}

func (b *InMemoryBackend) packageFile(ref repoRef, fileID string) (*genFile, bool) {
	b.mu.RLock("packageFile")
	defer b.mu.RUnlock()

	f, ok := b.genFiles[ref.fileName(fileID)]

	return f, ok
}

// removePackageVersion deletes a version with its files; an absent version is ignored.
func (b *InMemoryBackend) removePackageVersion(ref repoRef, pkgID, version string) {
	name := ref.packageName(pkgID) + "/versions/" + version

	b.mu.Lock("removePackageVersion")
	defer b.mu.Unlock()

	if _, ok := b.genVers[name]; ok {
		b.dropGenericVersionLocked(name)
	}
}

// removePackage deletes a package with every version, reporting whether it existed.
func (b *InMemoryBackend) removePackage(ref repoRef, pkgID string) bool {
	pkg := ref.packageName(pkgID)

	b.mu.Lock("removePackage")
	defer b.mu.Unlock()

	if _, ok := b.genPkgs[pkg]; !ok {
		return false
	}

	for name := range b.genVers {
		if strings.HasPrefix(name, pkg+"/versions/") {
			b.dropGenericVersionLocked(name)
		}
	}

	delete(b.genPkgs, pkg)
	delete(b.pkgTags, pkg)

	return true
}

// untagLocked drops the tags pointing at a deleted version and repoints latest at the newest remaining version.
// Must hold b.mu for writing.
func (b *InMemoryBackend) untagLocked(versionName string) {
	pkg, version, _ := strings.Cut(versionName, "/versions/")

	tags, ok := b.pkgTags[pkg]
	if !ok {
		return
	}

	hadLatest := tags["latest"] == version

	for tag, v := range tags {
		if v == version {
			delete(tags, tag)
		}
	}

	if hadLatest {
		var newest string

		var at time.Time

		for name, m := range b.genVers {
			if p, v, _ := strings.Cut(name, "/versions/"); p == pkg && name != versionName && !m.Created.Before(at) {
				newest, at = v, m.Created
			}
		}

		if newest != "" {
			tags["latest"] = newest
		}
	}

	if len(tags) == 0 {
		delete(b.pkgTags, pkg)
	}
}

// versionFiles lists the versions of a package with their files, oldest first.
type versionFiles struct {
	version string
	created time.Time
	updated time.Time
	files   []namedFile
}

type namedFile struct {
	file *genFile
	id   string
}

func (b *InMemoryBackend) versionsOfPackage(ref repoRef, pkgID string) []versionFiles {
	pkg := ref.packageName(pkgID)
	prefix := pkg + "/versions/"

	b.mu.RLock("versionsOfPackage")
	defer b.mu.RUnlock()

	var out []versionFiles

	for name, m := range b.genVers {
		if !strings.HasPrefix(name, prefix) {
			continue
		}

		vf := versionFiles{version: strings.TrimPrefix(name, prefix), created: m.Created, updated: m.Updated}

		for fn, f := range b.genFiles {
			if f.Owner == name {
				id, _ := unescapeImage(strings.TrimPrefix(fn, ref.full()+"/files/"))
				vf.files = append(vf.files, namedFile{id: id, file: f})
			}
		}

		sort.Slice(vf.files, func(i, j int) bool { return vf.files[i].id < vf.files[j].id })

		out = append(out, vf)
	}

	sort.Slice(out, func(i, j int) bool {
		if !out[i].created.Equal(out[j].created) {
			return out[i].created.Before(out[j].created)
		}

		return out[i].version < out[j].version
	})

	return out
}

// packageIDs lists the package ids of a repository, sorted, with their display names.
func (b *InMemoryBackend) packageIDs(ref repoRef) []string {
	prefix := ref.full() + "/packages/"

	b.mu.RLock("packageIDs")
	defer b.mu.RUnlock()

	var out []string

	for name := range b.genPkgs {
		if id, ok := unescapeImage(strings.TrimPrefix(name, prefix)); ok && strings.HasPrefix(name, prefix) {
			out = append(out, id)
		}
	}

	sort.Strings(out)

	return out
}

func (b *InMemoryBackend) packageDenied(ctx context.Context, rc repoCtx, pkgID string) bool {
	return b.downloadDenied(ctx, rc.ref.project, rc.ref.id, pkgID)
}
