package gcpartifactregistry

import (
	"bytes"
	"crypto/md5"  //nolint:gosec // Maven checksums include MD5
	"crypto/sha1" //nolint:gosec // Maven checksums include SHA-1
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"hash"
	"io"
	"net/http"
	"path"
	"strings"
	"time"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
)

const (
	mavenMetadata = "maven-metadata.xml"
	snapshotSfx   = "-SNAPSHOT"
	minMavenSegs  = 4
)

func checksumHash(ext string) hash.Hash {
	switch ext {
	case ".sha1":
		return sha1.New() //nolint:gosec // Maven .sha1 checksum files
	case ".md5":
		return md5.New() //nolint:gosec // Maven .md5 checksum files
	case ".sha256":
		return sha256.New()
	default:
		return sha512.New()
	}
}

// mavenTarget is a parsed repository path.
type mavenTarget struct {
	pkgID    string
	version  string
	filename string
	fileID   string
	checksum string
	kind     mavenKind
}

type mavenKind int

const (
	mavenArtifact mavenKind = iota
	mavenArtifactMeta
	mavenVersionMeta
)

func splitChecksum(p string) (string, string) {
	switch ext := path.Ext(p); ext {
	case ".sha1", ".md5", ".sha256", ".sha512":
		return strings.TrimSuffix(p, ext), ext
	default:
		return p, ""
	}
}

func validSegments(segs []string) bool {
	for _, s := range segs {
		if s == "" || s == "." || s == ".." {
			return false
		}
	}

	return true
}

func parseMavenPath(p string) (mavenTarget, bool) {
	p, sum := splitChecksum(p)
	segs := strings.Split(p, "/")
	n := len(segs)
	t := mavenTarget{checksum: sum, filename: segs[n-1], fileID: p}

	switch {
	case !validSegments(segs):
		return t, false
	case t.filename == mavenMetadata && n >= minMavenSegs && strings.HasSuffix(segs[n-2], snapshotSfx):
		t.kind, t.version = mavenVersionMeta, segs[n-2]
		t.pkgID = strings.Join(segs[:n-3], ".") + ":" + segs[n-3]
	case t.filename == mavenMetadata && n >= minMavenSegs-1:
		t.kind = mavenArtifactMeta
		t.pkgID = strings.Join(segs[:n-2], ".") + ":" + segs[n-2]
	case n >= minMavenSegs:
		t.version = segs[n-2]
		t.pkgID = strings.Join(segs[:n-3], ".") + ":" + segs[n-3]

		return t, strings.HasPrefix(t.filename, segs[n-3]+"-"+strings.TrimSuffix(t.version, snapshotSfx))
	default:
		return t, false
	}

	return t, true
}

func (b *InMemoryBackend) serveMaven(w http.ResponseWriter, r *http.Request, rc repoCtx, p string) {
	t, ok := parseMavenPath(p)
	if !ok {
		writePlain(w, http.StatusBadRequest, "Not a Maven repository layout path: "+p)

		return
	}

	switch {
	case r.Method == http.MethodPut:
		b.putMaven(w, r, rc, t)
	case isRead(r):
		b.getMaven(w, r, rc, t)
	default:
		writePlain(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func mavenPolicyError(cfg *artifactregistrypb.Repository_MavenRepositoryConfig, version string) string {
	snapshot := strings.HasSuffix(version, snapshotSfx)

	switch cfg.GetVersionPolicy() {
	case artifactregistrypb.Repository_MavenRepositoryConfig_RELEASE:
		if snapshot {
			return "The repository's version policy is RELEASE; " + version + " is a snapshot."
		}
	case artifactregistrypb.Repository_MavenRepositoryConfig_SNAPSHOT:
		if !snapshot {
			return "The repository's version policy is SNAPSHOT; " + version + " is a release."
		}
	case artifactregistrypb.Repository_MavenRepositoryConfig_VERSION_POLICY_UNSPECIFIED:
	}

	return ""
}

func (b *InMemoryBackend) putMaven(w http.ResponseWriter, r *http.Request, rc repoCtx, t mavenTarget) {
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxGenericBytes))
	if err != nil {
		writePlain(w, http.StatusRequestEntityTooLarge, "The file is larger than the emulator accepts.")

		return
	}

	if t.kind == mavenArtifactMeta {
		writePlain(w, http.StatusCreated, "Created")

		return
	}

	if msg := mavenPolicyError(rc.pb.GetMavenConfig(), t.version); msg != "" {
		writePlain(w, http.StatusBadRequest, msg)

		return
	}

	if t.checksum != "" {
		base := t
		base.checksum = ""

		if _, found := b.mavenContent(rc, base); !found {
			writePlain(w, http.StatusNotFound, "No file to checksum: "+base.fileID)

			return
		}

		writePlain(w, http.StatusCreated, "Created")

		return
	}

	overwrite := t.kind == mavenVersionMeta ||
		(strings.HasSuffix(t.version, snapshotSfx) && rc.pb.GetMavenConfig().GetAllowSnapshotOverwrites())

	err = b.putPackageFile(rc.ref, pkgFile{
		pkgID: t.pkgID, display: t.pkgID, version: t.version, fileID: t.fileID, data: data,
	}, overwrite)
	if err != nil {
		writePlain(w, http.StatusConflict, "Cannot overwrite "+t.fileID+": the artifact already exists.")

		return
	}

	writePlain(w, http.StatusCreated, "Created")
}

// mavenContent returns the bytes behind a path without a checksum suffix: a stored file, or the generated
// artifact-level maven-metadata.xml.
func (b *InMemoryBackend) mavenContent(rc repoCtx, t mavenTarget) ([]byte, bool) {
	if t.kind != mavenArtifactMeta {
		f, ok := b.packageFile(rc.ref, t.fileID)
		if !ok {
			return nil, false
		}

		return f.Data, true
	}

	return b.mavenMetadataXML(rc, t.pkgID)
}

func (b *InMemoryBackend) mavenMetadataXML(rc repoCtx, pkgID string) ([]byte, bool) {
	versions := b.versionsOfPackage(rc.ref, pkgID)
	if len(versions) == 0 {
		return nil, false
	}

	group, artifact, _ := strings.Cut(pkgID, ":")

	var latest, release string

	var updated time.Time

	var vs bytes.Buffer

	for _, v := range versions {
		vs.WriteString("      <version>" + v.version + "</version>\n")

		latest = v.version
		if !strings.HasSuffix(v.version, snapshotSfx) {
			release = v.version
		}

		if v.updated.After(updated) {
			updated = v.updated
		}
	}

	out := "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<metadata>\n  <groupId>" + group + "</groupId>\n" +
		"  <artifactId>" + artifact + "</artifactId>\n  <versioning>\n    <latest>" + latest + "</latest>\n"
	if release != "" {
		out += "    <release>" + release + "</release>\n"
	}

	out += "    <versions>\n" + vs.String() + "    </versions>\n    <lastUpdated>" +
		updated.UTC().Format("20060102150405") + "</lastUpdated>\n  </versioning>\n</metadata>\n"

	return []byte(out), true
}

func mavenContentType(name string) string {
	switch path.Ext(name) {
	case ".pom", ".xml":
		return "application/xml"
	case ".jar":
		return "application/java-archive"
	default:
		return "application/octet-stream"
	}
}

func (b *InMemoryBackend) getMaven(w http.ResponseWriter, r *http.Request, rc repoCtx, t mavenTarget) {
	base := t
	base.checksum = ""

	data, ok := b.mavenContent(rc, base)
	if !ok {
		writePlain(w, http.StatusNotFound, "Not found")

		return
	}

	if t.kind != mavenArtifactMeta && b.packageDenied(r.Context(), rc, t.pkgID) {
		writePlain(w, http.StatusForbidden, "A download rule denies access to this package.")

		return
	}

	if t.checksum != "" {
		h := checksumHash(t.checksum)
		_, _ = h.Write(data)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(hex.EncodeToString(h.Sum(nil))))

		return
	}

	w.Header().Set("Content-Type", mavenContentType(t.filename))
	_, _ = w.Write(data) //nolint:gosec // artifact download, never rendered
}
