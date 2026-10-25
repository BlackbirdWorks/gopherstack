package gcpartifactregistry

import (
	"crypto/sha1" //nolint:gosec // npm dist.shasum is SHA-1 by protocol
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
)

const maxNPMDocBytes = 2 * maxGenericBytes

var npmVersionRe = regexp.MustCompile(`^\d+\.\d+\.\d+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`)

type npmAttachment struct {
	ContentType string `json:"content_type"`
	Data        string `json:"data"`
	Length      int64  `json:"length"`
}

type npmPublishDoc struct {
	DistTags    map[string]string          `json:"dist-tags"`
	Versions    map[string]json.RawMessage `json:"versions"`
	Attachments map[string]npmAttachment   `json:"_attachments"`
	Name        string                     `json:"name"`
}

func writeNPMError(w http.ResponseWriter, code int, msg string) {
	writeJSONBody(w, code, map[string]string{"error": msg})
}

// splitNPMPath splits a registry path into the package name and what follows it.
func splitNPMPath(path string) (string, string) {
	segs := strings.Split(path, "/")

	n := 1
	if strings.HasPrefix(segs[0], "@") {
		n = 2
	}

	if len(segs) < n {
		return "", ""
	}

	return strings.Join(segs[:n], "/"), strings.Join(segs[n:], "/")
}

func npmBase(pkg string) string {
	_, base, ok := strings.Cut(pkg, "/")
	if !ok {
		return pkg
	}

	return base
}

func npmTarball(pkg, version string) string { return npmBase(pkg) + "-" + version + ".tgz" }

func npmFileID(pkg, tarball string) string { return pkg + "/-/" + tarball }

func (b *InMemoryBackend) serveNPM(w http.ResponseWriter, r *http.Request, rc repoCtx, path string) {
	switch {
	case path == "-/ping":
		writeJSONBody(w, http.StatusOK, map[string]string{})

		return
	case strings.HasPrefix(path, "-/package/"):
		b.serveNPMDistTags(w, r, rc, strings.TrimPrefix(path, "-/package/"))

		return
	case path == "" || strings.HasPrefix(path, "-/"):
		writeNPMError(w, http.StatusNotFound, "Not found")

		return
	}

	pkg, tail := splitNPMPath(path)

	switch {
	case tail == "" && (r.Method == http.MethodGet || r.Method == http.MethodHead):
		b.serveNPMPackument(w, r, rc, pkg)
	case tail == "" && r.Method == http.MethodPut:
		b.publishNPM(w, r, rc, pkg)
	case strings.HasPrefix(tail, "-/") && (r.Method == http.MethodGet || r.Method == http.MethodHead):
		b.serveNPMTarball(w, r, rc, pkg, strings.TrimPrefix(tail, "-/"))
	case strings.HasPrefix(tail, "-rev/") && r.Method == http.MethodPut:
		b.updateNPM(w, r, rc, pkg)
	case strings.HasPrefix(tail, "-rev/") && r.Method == http.MethodDelete:
		b.unpublishNPMPackage(w, rc, pkg)
	case strings.HasPrefix(tail, "-/") && strings.Contains(tail, "/-rev/") && r.Method == http.MethodDelete:
		file, _, _ := strings.Cut(strings.TrimPrefix(tail, "-/"), "/-rev/")
		b.unpublishNPMTarball(w, rc, pkg, file)
	default:
		writeNPMError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func readNPMDoc(w http.ResponseWriter, r *http.Request) (npmPublishDoc, bool) {
	var doc npmPublishDoc

	body := http.MaxBytesReader(w, r.Body, maxNPMDocBytes)
	if err := json.NewDecoder(body).Decode(&doc); err != nil {
		writeNPMError(w, http.StatusBadRequest, "Invalid package document: "+err.Error())

		return doc, false
	}

	return doc, true
}

func (b *InMemoryBackend) npmVersionSet(rc repoCtx, pkg string) map[string]bool {
	out := map[string]bool{}
	for _, v := range b.versionsOfPackage(rc.ref, pkg) {
		out[v.version] = true
	}

	return out
}

func (b *InMemoryBackend) publishNPM(w http.ResponseWriter, r *http.Request, rc repoCtx, pkg string) {
	doc, ok := readNPMDoc(w, r)
	if !ok {
		return
	}

	if doc.Name != pkg || len(doc.Versions) == 0 {
		writeNPMError(w, http.StatusBadRequest, "The package document must name "+pkg+" and carry a version.")

		return
	}

	existing := b.npmVersionSet(rc, pkg)
	files := map[string]pkgFile{}

	for version, manifest := range doc.Versions {
		pf, code, msg := npmVersionFile(pkg, version, manifest, doc.Attachments)
		if code != 0 {
			writeNPMError(w, code, msg)

			return
		}

		if existing[version] {
			writeNPMError(w, http.StatusConflict,
				"You cannot publish over the previously published versions: "+version+".")

			return
		}

		files[version] = pf
	}

	if len(doc.DistTags) == 0 {
		writeNPMError(w, http.StatusBadRequest, "You must specify a tag using --tag.")

		return
	}

	for tag, v := range doc.DistTags {
		if _, inDoc := files[v]; !inDoc && !existing[v] || tag == "" {
			writeNPMError(w, http.StatusBadRequest, "dist-tag "+tag+" points at unknown version "+v+".")

			return
		}
	}

	for _, pf := range files {
		if err := b.putPackageFile(rc.ref, pf, false); err != nil {
			writeNPMError(w, http.StatusConflict, err.Error())

			return
		}
	}

	b.setNPMTags(rc, pkg, doc.DistTags, false)
	writeJSONBody(w, http.StatusOK, map[string]any{"ok": true, "id": pkg})
}

func npmVersionFile(
	pkg, version string, manifest json.RawMessage, atts map[string]npmAttachment,
) (pkgFile, int, string) {
	if !npmVersionRe.MatchString(version) {
		return pkgFile{}, http.StatusBadRequest, "Invalid semver version " + version + "."
	}

	tarball := npmTarball(pkg, version)

	att, ok := atts[pkg+"-"+version+".tgz"]
	if !ok {
		att, ok = atts[tarball]
	}

	if !ok {
		return pkgFile{}, http.StatusBadRequest, "Missing attachment " + pkg + "-" + version + ".tgz."
	}

	data, err := base64.StdEncoding.DecodeString(att.Data)
	if err != nil {
		return pkgFile{}, http.StatusBadRequest, "Attachment " + tarball + " is not valid base64."
	}

	return pkgFile{
		pkgID: pkg, display: pkg, version: version, fileID: npmFileID(pkg, tarball), data: data, doc: manifest,
	}, 0, ""
}

// setNPMTags merges tags into the package's dist-tags, or replaces them when replace is set.
func (b *InMemoryBackend) setNPMTags(rc repoCtx, pkg string, tags map[string]string, replace bool) {
	name := rc.ref.packageName(pkg)

	b.mu.Lock("setNPMTags")
	defer b.mu.Unlock()

	if _, ok := b.genPkgs[name]; !ok {
		return
	}

	if replace || b.pkgTags[name] == nil {
		b.pkgTags[name] = map[string]string{}
	}

	for k, v := range tags {
		b.pkgTags[name][k] = v
	}
}

func (b *InMemoryBackend) npmTags(rc repoCtx, pkg string) map[string]string {
	b.mu.RLock("npmTags")
	defer b.mu.RUnlock()

	out := map[string]string{}
	for k, v := range b.pkgTags[rc.ref.packageName(pkg)] {
		out[k] = v
	}

	return out
}

func npmTime(t interface{ Format(string) string }) string { return t.Format("2006-01-02T15:04:05.000Z") }

func baseURL(r *http.Request, route string, rc repoCtx) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}

	return scheme + "://" + r.Host + route + rc.ref.project + "/" + rc.ref.id
}

func (b *InMemoryBackend) serveNPMPackument(w http.ResponseWriter, r *http.Request, rc repoCtx, pkg string) {
	versions := b.versionsOfPackage(rc.ref, pkg)
	if len(versions) == 0 {
		writeNPMError(w, http.StatusNotFound, "Not found")

		return
	}

	base := baseURL(r, npmRoute, rc)
	docs := map[string]json.RawMessage{}
	times := map[string]string{}
	rev := sha1.New() //nolint:gosec // opaque revision token

	for _, v := range versions {
		for _, nf := range v.files {
			if nf.file.Doc == nil {
				continue
			}

			docs[v.version] = withDist(nf.file, nf.file.Doc, base+"/"+pkg+"/-/"+npmTarball(pkg, v.version))
		}

		times[v.version] = npmTime(v.created.UTC())
		_, _ = io.WriteString(rev, v.version)
	}

	times["created"] = npmTime(versions[0].created.UTC())
	times["modified"] = npmTime(versions[len(versions)-1].updated.UTC())

	writeJSONBody(w, http.StatusOK, map[string]any{
		"_id": pkg, "_rev": fmt.Sprintf("%d-%x", len(versions), rev.Sum(nil)[:8]), "name": pkg,
		"dist-tags": b.npmTags(rc, pkg), "versions": docs, "time": times,
	})
}

// withDist adds the dist block npm clients install from to a stored version manifest.
func withDist(f *genFile, manifest []byte, tarballURL string) json.RawMessage {
	var m map[string]json.RawMessage
	if json.Unmarshal(manifest, &m) != nil {
		m = map[string]json.RawMessage{}
	}

	s1 := sha1.Sum(f.Data) //nolint:gosec // npm dist.shasum is SHA-1 by protocol
	s512 := sha512.Sum512(f.Data)
	dist, _ := json.Marshal(map[string]any{
		"tarball": tarballURL, "shasum": hex.EncodeToString(s1[:]),
		"integrity": "sha512-" + base64.StdEncoding.EncodeToString(s512[:]),
	})
	m["dist"] = dist

	out, _ := json.Marshal(m)

	return out
}

func (b *InMemoryBackend) serveNPMTarball(w http.ResponseWriter, r *http.Request, rc repoCtx, pkg, file string) {
	f, ok := b.packageFile(rc.ref, npmFileID(pkg, file))
	if !ok {
		writeNPMError(w, http.StatusNotFound, "Not found")

		return
	}

	if b.packageDenied(r.Context(), rc, pkg) {
		writeNPMError(w, http.StatusForbidden, "A download rule denies access to this package.")

		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(f.Data) //nolint:gosec // octet-stream download, never rendered
}

// updateNPM handles the PUT npm unpublish sends: the package document without the removed versions.
func (b *InMemoryBackend) updateNPM(w http.ResponseWriter, r *http.Request, rc repoCtx, pkg string) {
	doc, ok := readNPMDoc(w, r)
	if !ok {
		return
	}

	if doc.Name != pkg {
		writeNPMError(w, http.StatusBadRequest, "The package document must name "+pkg+".")

		return
	}

	for _, v := range b.versionsOfPackage(rc.ref, pkg) {
		if _, kept := doc.Versions[v.version]; !kept {
			b.removePackageVersion(rc.ref, pkg, v.version)
		}
	}

	if doc.DistTags != nil {
		b.setNPMTags(rc, pkg, doc.DistTags, true)
	}

	writeJSONBody(w, http.StatusOK, map[string]any{"ok": true, "id": pkg})
}

func (b *InMemoryBackend) unpublishNPMPackage(w http.ResponseWriter, rc repoCtx, pkg string) {
	if !b.removePackage(rc.ref, pkg) {
		writeNPMError(w, http.StatusNotFound, "Not found")

		return
	}

	writeJSONBody(w, http.StatusOK, map[string]bool{"ok": true})
}

func (b *InMemoryBackend) unpublishNPMTarball(w http.ResponseWriter, rc repoCtx, pkg, file string) {
	version := strings.TrimSuffix(strings.TrimPrefix(file, npmBase(pkg)+"-"), ".tgz")
	b.removePackageVersion(rc.ref, pkg, version)
	writeJSONBody(w, http.StatusOK, map[string]bool{"ok": true})
}

// serveNPMDistTags serves /-/package/<pkg>/dist-tags[/<tag>].
func (b *InMemoryBackend) serveNPMDistTags(w http.ResponseWriter, r *http.Request, rc repoCtx, path string) {
	pkg, tail := splitNPMPath(path)
	if !strings.HasPrefix(tail, "dist-tags") {
		writeNPMError(w, http.StatusNotFound, "Not found")

		return
	}

	tag := strings.TrimPrefix(strings.TrimPrefix(tail, "dist-tags"), "/")
	existing := b.npmVersionSet(rc, pkg)

	switch {
	case len(existing) == 0:
		writeNPMError(w, http.StatusNotFound, "Not found")
	case tag == "" && r.Method == http.MethodGet:
		writeJSONBody(w, http.StatusOK, b.npmTags(rc, pkg))
	case tag != "" && r.Method == http.MethodPut:
		b.putNPMTag(w, r, rc, pkg, tag, existing)
	case tag != "" && r.Method == http.MethodDelete:
		b.deleteNPMTag(w, rc, pkg, tag)
	default:
		writeNPMError(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func (b *InMemoryBackend) putNPMTag(
	w http.ResponseWriter, r *http.Request, rc repoCtx, pkg, tag string, existing map[string]bool,
) {
	var version string
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&version); err != nil {
		writeNPMError(w, http.StatusBadRequest, "The body must be a JSON string holding a version.")

		return
	}

	if !existing[version] {
		writeNPMError(w, http.StatusNotFound, "version not found: "+version)

		return
	}

	b.setNPMTags(rc, pkg, map[string]string{tag: version}, false)
	writeJSONBody(w, http.StatusOK, map[string]any{"ok": true, "id": pkg, "dist-tags": b.npmTags(rc, pkg)})
}

func (b *InMemoryBackend) deleteNPMTag(w http.ResponseWriter, rc repoCtx, pkg, tag string) {
	tags := b.npmTags(rc, pkg)
	if _, ok := tags[tag]; !ok {
		writeNPMError(w, http.StatusNotFound, "dist-tag not found: "+tag)

		return
	}

	delete(tags, tag)
	b.setNPMTags(rc, pkg, tags, true)
	writeJSONBody(w, http.StatusOK, map[string]any{"ok": true, "id": pkg})
}
