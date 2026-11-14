package gcpartifactregistry

import (
	"bytes"
	"compress/gzip"
	"crypto/md5"  //nolint:gosec // apt Release files list MD5Sum
	"crypto/sha1" //nolint:gosec // apt Release files list SHA1
	"encoding/hex"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
)

const (
	aptComponent     = "main"
	archAll          = "all"
	releaseSizeWidth = 16
	indexesPerArch   = 2
	aptDateFmt       = "Mon, 02 Jan 2006 15:04:05 UTC"
)

var defaultAptArchs = []string{"amd64", "arm64"} //nolint:gochecknoglobals // immutable catalog

func debFileName(c debControl) string {
	ver := c.Version
	if _, after, ok := strings.Cut(ver, ":"); ok {
		ver = after
	}

	return c.Package + "_" + ver + "_" + c.Arch + ".deb"
}

func debPoolPath(repo string, c debControl) string {
	prefix := c.Package[:1]
	if strings.HasPrefix(c.Package, "lib") && len(c.Package) > 3 {
		prefix = c.Package[:4]
	}

	return "pool/" + repo + "/" + aptComponent + "/" + prefix + "/" + c.Package + "/" + debFileName(c)
}

func aptArtifactPB(ref repoRef, c debControl, ctl []byte) *artifactregistrypb.AptArtifact {
	return &artifactregistrypb.AptArtifact{
		Name:        ref.full() + "/aptArtifacts/" + c.Package + ":" + c.Version + ":" + c.Arch,
		PackageName: c.Package, PackageType: artifactregistrypb.AptArtifact_BINARY, Architecture: c.Arch,
		Component: aptComponent, ControlFile: ctl,
	}
}

// addDeb stores a .deb under its package and version.
func (b *InMemoryBackend) addDeb(ref repoRef, data []byte) (*artifactregistrypb.AptArtifact, error) {
	c, ctl, err := parseDeb(data)
	if err != nil {
		return nil, errInvalid("The file is not a valid .deb: %v", err)
	}

	err = b.putPackageFile(ref, pkgFile{
		pkgID: c.Package, display: c.Package, version: c.Version, fileID: debPoolPath(ref.id, c), data: data, doc: ctl,
	}, true)
	if err != nil {
		return nil, err
	}

	return aptArtifactPB(ref, c, ctl), nil
}

func (b *InMemoryBackend) formatOf(ref repoRef) (artifactregistrypb.Repository_Format, error) {
	b.mu.RLock("formatOf")
	defer b.mu.RUnlock()

	rec, ok := b.repos[ref.full()]
	if !ok {
		return 0, errNotFound("Repository %q not found.", ref.full())
	}

	return rec.pb.GetFormat(), nil
}

func (b *InMemoryBackend) requireFormat(parent string, want artifactregistrypb.Repository_Format) (repoRef, error) {
	ref, err := parseRepo(parent)
	if err != nil {
		return ref, err
	}

	got, err := b.formatOf(ref)
	if err != nil {
		return ref, err
	}

	if got != want {
		return ref, errPrecondition("Repository %q is not a %s repository.", ref.full(), want)
	}

	return ref, nil
}

func storeApt(b *InMemoryBackend, parent string, _, data []byte) (*longrunningpb.Operation, error) {
	ref, err := b.requireFormat(parent, artifactregistrypb.Repository_APT)
	if err != nil {
		return nil, err
	}

	art, err := b.addDeb(ref, data)
	if err != nil {
		return nil, err
	}

	return b.doneAny(ref.parent(), "UploadAptArtifactResponse", art)
}

type debEntry struct {
	file *genFile
	path string
	pkg  string
	ctl  debControl
}

func (b *InMemoryBackend) debEntries(rc repoCtx) []debEntry {
	var out []debEntry

	for _, id := range b.packageIDs(rc.ref) {
		for _, v := range b.versionsOfPackage(rc.ref, id) {
			for _, nf := range v.files {
				out = append(out, debEntry{pkg: id, path: nf.id, file: nf.file, ctl: parseControl(nf.file.Doc)})
			}
		}
	}

	sortDebEntries(out)

	return out
}

func sortDebEntries(out []debEntry) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].pkg != out[j].pkg {
			return out[i].pkg < out[j].pkg
		}

		return out[i].path < out[j].path
	})
}

func (e debEntry) stanza() string {
	var sb strings.Builder

	for _, k := range e.ctl.order {
		sb.WriteString(k + ": " + e.ctl.fields[k] + "\n")
	}

	sb.WriteString("Filename: " + e.path + "\n")
	sb.WriteString("Size: " + strconv.Itoa(len(e.file.Data)) + "\n")
	sb.WriteString("MD5sum: " + md5Hex(e.file.Data) + "\n")
	sb.WriteString("SHA1: " + sha1Hex(e.file.Data) + "\n")
	sb.WriteString("SHA256: " + sha256Hex(e.file.Data) + "\n")

	return sb.String()
}

func packagesIndex(entries []debEntry, arch string) []byte {
	var sb strings.Builder

	for _, e := range entries {
		if e.ctl.Arch == arch || e.ctl.Arch == archAll {
			sb.WriteString(e.stanza() + "\n")
		}
	}

	return []byte(sb.String())
}

func gzipBytes(data []byte) []byte {
	var buf bytes.Buffer

	zw := gzip.NewWriter(&buf)
	_, _ = zw.Write(data)
	_ = zw.Close()

	return buf.Bytes()
}

func aptArchitectures(entries []debEntry) []string {
	seen := map[string]bool{}

	for _, e := range entries {
		if e.ctl.Arch != archAll {
			seen[e.ctl.Arch] = true
		}
	}

	if len(seen) == 0 {
		return defaultAptArchs
	}

	out := make([]string, 0, len(seen))
	for a := range seen {
		out = append(out, a)
	}

	sort.Strings(out)

	return out
}

func releaseFile(dist string, entries []debEntry, date time.Time) []byte {
	archs := aptArchitectures(entries)

	type idx struct {
		name string
		data []byte
	}

	idxs := make([]idx, 0, indexesPerArch*len(archs))

	for _, a := range archs {
		p := packagesIndex(entries, a)
		base := aptComponent + "/binary-" + a + "/Packages"
		idxs = append(idxs, idx{base, p}, idx{base + ".gz", gzipBytes(p)})
	}

	var sb strings.Builder

	sb.WriteString("Origin: Artifact Registry\nLabel: Artifact Registry\nSuite: " + dist + "\nCodename: " + dist + "\n")
	sb.WriteString("Date: " + date.UTC().Format(aptDateFmt) + "\n")
	sb.WriteString("Architectures: " + strings.Join(archs, " ") + "\nComponents: " + aptComponent + "\n")

	for _, h := range []struct {
		sum  func([]byte) string
		name string
	}{
		{name: "MD5Sum", sum: md5Hex},
		{name: "SHA1", sum: sha1Hex},
		{name: "SHA256", sum: sha256Hex},
	} {
		sb.WriteString(h.name + ":\n")

		for _, i := range idxs {
			sb.WriteString(" " + h.sum(i.data) + " " + padSize(len(i.data)) + " " + i.name + "\n")
		}
	}

	return []byte(sb.String())
}

const (
	aptKeyBinary  = "apt-key.gpg"
	aptKeyArmored = "apt-key.asc"
)

func padSize(n int) string {
	s := strconv.Itoa(n)

	return strings.Repeat(" ", max(0, releaseSizeWidth-len(s))) + s
}

func (b *InMemoryBackend) serveApt(w http.ResponseWriter, r *http.Request, rc repoCtx, p string) {
	if !isRead(r) {
		writePlain(w, http.StatusMethodNotAllowed, "Apt repositories are populated with the upload and import APIs.")

		return
	}

	b.serveAptPath(w, r, rc, b.debEntries(rc), p)
}

func (b *InMemoryBackend) serveAptPath(
	w http.ResponseWriter,
	r *http.Request,
	rc repoCtx,
	entries []debEntry,
	p string,
) {
	switch {
	case strings.HasPrefix(p, "dists/"+rc.ref.id+"/") && isAptSignedFile(strings.TrimPrefix(p, "dists/"+rc.ref.id+"/")):
		b.serveAptRelease(w, rc, entries, strings.TrimPrefix(p, "dists/"+rc.ref.id+"/"))
	case strings.HasPrefix(p, "dists/"+rc.ref.id+"/"+aptComponent+"/binary-"):
		serveAptPackages(w, entries, strings.TrimPrefix(p, "dists/"+rc.ref.id+"/"+aptComponent+"/binary-"))
	case strings.HasPrefix(p, "pool/"):
		b.serveDeb(w, r, rc, entries, p)
	default:
		writePlain(w, http.StatusNotFound, "Not found: "+p)
	}
}

func isAptSignedFile(name string) bool {
	switch name {
	case "Release", "InRelease", "Release.gpg", aptKeyBinary, aptKeyArmored:
		return true
	default:
		return false
	}
}

// serveAptRelease serves the Release file, its signed forms and the public key that verifies them. The key paths
// are the emulator's choice: Google documents no key URL for repositories other than its own.
func (b *InMemoryBackend) serveAptRelease(w http.ResponseWriter, rc repoCtx, entries []debEntry, name string) {
	var date time.Time

	for _, e := range entries {
		if e.file.Updated.After(date) {
			date = e.file.Updated
		}
	}

	if date.IsZero() {
		date = b.now()
	}

	release := releaseFile(rc.ref.id, entries, date)

	if name == "Release" {
		writeBytes(w, "text/plain; charset=utf-8", release)

		return
	}

	signer, err := b.signerFor(rc.ref)
	if err != nil {
		writePlain(w, http.StatusInternalServerError, "Signing key unavailable.")

		return
	}

	var (
		out  []byte
		kind = "text/plain; charset=utf-8"
	)

	switch name {
	case "InRelease":
		out, err = signer.clearSigned(release)
	case "Release.gpg":
		out, err = signer.detached(release)
	case aptKeyBinary:
		out, err = signer.publicKey(false)
		kind = "application/pgp-keys"
	default:
		out, err = signer.publicKey(true)
		kind = "application/pgp-keys"
	}

	if err != nil {
		writePlain(w, http.StatusInternalServerError, "Signing failed.")

		return
	}

	writeBytes(w, kind, out)
}

func serveAptPackages(w http.ResponseWriter, entries []debEntry, tail string) {
	arch, file, ok := strings.Cut(tail, "/")
	if !ok {
		writePlain(w, http.StatusNotFound, "Not found")

		return
	}

	idx := packagesIndex(entries, arch)

	switch file {
	case "Packages":
		writeBytes(w, "text/plain; charset=utf-8", idx)
	case "Packages.gz":
		writeBytes(w, "application/gzip", gzipBytes(idx))
	default:
		writePlain(w, http.StatusNotFound, "Not found")
	}
}

func (b *InMemoryBackend) serveDeb(w http.ResponseWriter, r *http.Request, rc repoCtx, entries []debEntry, p string) {
	for _, e := range entries {
		if e.path != p {
			continue
		}

		if b.packageDenied(r.Context(), rc, e.pkg, e.file.Owner) {
			writePlain(w, http.StatusForbidden, "A download rule denies access to this package.")

			return
		}

		writeBytes(w, "application/vnd.debian.binary-package", e.file.Data)

		return
	}

	writePlain(w, http.StatusNotFound, "Not found: "+p)
}

func writeBytes(w http.ResponseWriter, contentType string, data []byte) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	_, _ = w.Write(data)
}

func md5Hex(d []byte) string {
	s := md5.Sum(d) //nolint:gosec // apt Release lists MD5Sum

	return hex.EncodeToString(s[:])
}

func sha1Hex(d []byte) string {
	s := sha1.Sum(d) //nolint:gosec // apt Release lists SHA1

	return hex.EncodeToString(s[:])
}
