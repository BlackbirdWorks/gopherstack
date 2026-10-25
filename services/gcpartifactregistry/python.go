package gcpartifactregistry

import (
	"crypto/md5" //nolint:gosec // twine sends md5_digest by protocol
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

const (
	pyUploadMemory = 32 << 20
	pyPathSegs     = 3
)

var (
	pyNormalizeRe = regexp.MustCompile(`[-_.]+`)
	pyFileRe      = regexp.MustCompile(`(?i)^[A-Za-z0-9][A-Za-z0-9._+-]*\.(whl|tar\.gz|zip|egg|tar\.bz2|tar\.xz)$`)
)

type pyDoc struct {
	RequiresPython string `json:"requiresPython,omitempty"`
	Name           string `json:"name"`
	SHA256         string `json:"sha256"`
}

// pyNormalize is the PEP 503 project name normalisation.
func pyNormalize(name string) string {
	return strings.ToLower(pyNormalizeRe.ReplaceAllString(name, "-"))
}

func pyFileID(norm, version, filename string) string { return genFileID(norm, version, filename) }

func (b *InMemoryBackend) servePython(w http.ResponseWriter, r *http.Request, rc repoCtx, path string) {
	switch {
	case path == "" && r.Method == http.MethodPost:
		b.uploadPython(w, r, rc)
	case path == "simple" && isRead(r):
		b.pyIndex(w, rc)
	case strings.HasPrefix(path, "simple/") && isRead(r):
		b.pyProject(w, r, rc, strings.Trim(strings.TrimPrefix(path, "simple/"), "/"))
	case strings.HasPrefix(path, "packages/") && isRead(r):
		b.pyDownload(w, r, rc, strings.TrimPrefix(path, "packages/"))
	default:
		writePlain(w, http.StatusMethodNotAllowed, "Method not allowed")
	}
}

func isRead(r *http.Request) bool { return r.Method == http.MethodGet || r.Method == http.MethodHead }

func (b *InMemoryBackend) uploadPython(w http.ResponseWriter, r *http.Request, rc repoCtx) {
	r.Body = http.MaxBytesReader(w, r.Body, maxGenericBytes+1<<20)

	err := r.ParseMultipartForm(pyUploadMemory) //nolint:gosec // r.Body is wrapped in MaxBytesReader above
	if err != nil {
		writePlain(w, http.StatusBadRequest, "The upload must be a multipart form: "+err.Error())

		return
	}

	defer func() { _ = r.MultipartForm.RemoveAll() }()

	if a := r.FormValue(":action"); a != "file_upload" {
		writePlain(w, http.StatusBadRequest, "Unsupported :action "+a+"; use file_upload.")

		return
	}

	name, version := r.FormValue("name"), r.FormValue("version")

	data, filename, status, msg := pyContent(r)
	if status != 0 {
		writePlain(w, status, msg)

		return
	}

	sum := sha256.Sum256(data)
	md := md5.Sum(data) //nolint:gosec // verifies the md5_digest twine sends

	switch {
	case name == "" || version == "" || strings.Contains(version, "/"):
		writePlain(w, http.StatusBadRequest, "name and version are required.")

		return
	case !pyFileRe.MatchString(filename):
		writePlain(w, http.StatusBadRequest, "Invalid distribution filename "+filename+".")

		return
	case !digestOK(r.FormValue("sha256_digest"), sum[:]) || !digestOK(r.FormValue("md5_digest"), md[:]):
		writePlain(w, http.StatusBadRequest, "The digest does not match the uploaded file.")

		return
	}

	norm := pyNormalize(name)
	doc, _ := json.Marshal(
		pyDoc{Name: name, RequiresPython: r.FormValue("requires_python"), SHA256: hex.EncodeToString(sum[:])},
	)

	err = b.putPackageFile(rc.ref, pkgFile{
		pkgID: norm, display: name, version: version, fileID: pyFileID(norm, version, filename), data: data, doc: doc,
	}, false)
	if err != nil {
		writePlain(w, http.StatusConflict, "File already exists: "+filename)

		return
	}

	writePlain(w, http.StatusOK, "OK")
}

func digestOK(claimed string, sum []byte) bool {
	return claimed == "" || strings.EqualFold(claimed, hex.EncodeToString(sum))
}

func pyContent(r *http.Request) ([]byte, string, int, string) {
	files := r.MultipartForm.File["content"]
	if len(files) != 1 {
		return nil, "", http.StatusBadRequest, "Exactly one content file is required."
	}

	fh := files[0]

	f, err := fh.Open()
	if err != nil {
		return nil, "", http.StatusBadRequest, "Reading the upload failed."
	}

	defer func() { _ = f.Close() }()

	buf := make([]byte, fh.Size)
	if _, err = io.ReadFull(f, buf); err != nil {
		return nil, "", http.StatusBadRequest, "Reading the upload failed."
	}

	return buf, fh.Filename, 0, ""
}

func (b *InMemoryBackend) pyIndex(w http.ResponseWriter, rc repoCtx) {
	var sb strings.Builder

	sb.WriteString("<!DOCTYPE html>\n<html><head><meta name=\"pypi:repository-version\" content=\"1.0\">" +
		"<title>Simple index</title></head><body>\n")

	for _, id := range b.packageIDs(rc.ref) {
		sb.WriteString(
			"<a href=\"" + html.EscapeString(url.PathEscape(id)) + "/\">" + html.EscapeString(id) + "</a><br>\n",
		)
	}

	sb.WriteString("</body></html>\n")
	writeHTML(w, sb.String())
}

func writeHTML(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(body))
}

func (b *InMemoryBackend) pyProject(w http.ResponseWriter, r *http.Request, rc repoCtx, name string) {
	norm := pyNormalize(name)
	if norm != name {
		http.Redirect(w, r, "/python/"+rc.ref.project+"/"+rc.ref.id+"/simple/"+url.PathEscape(norm)+"/",
			http.StatusMovedPermanently)

		return
	}

	versions := b.versionsOfPackage(rc.ref, norm)
	if len(versions) == 0 {
		writePlain(w, http.StatusNotFound, "No project "+norm+".")

		return
	}

	var sb strings.Builder

	sb.WriteString(
		"<!DOCTYPE html>\n<html><head><meta name=\"pypi:repository-version\" content=\"1.0\"><title>Links for " +
			html.EscapeString(norm) + "</title></head><body>\n<h1>Links for " + html.EscapeString(norm) + "</h1>\n",
	)

	for _, v := range versions {
		for _, nf := range v.files {
			var d pyDoc
			_ = json.Unmarshal(nf.file.Doc, &d)

			filename := nf.id[strings.LastIndex(nf.id, ":")+1:]
			href := "/python/" + rc.ref.project + "/" + rc.ref.id + "/packages/" + url.PathEscape(norm) + "/" +
				url.PathEscape(v.version) + "/" + url.PathEscape(filename) + "#sha256=" + d.SHA256

			attr := ""
			if d.RequiresPython != "" {
				attr = " data-requires-python=\"" + html.EscapeString(d.RequiresPython) + "\""
			}

			sb.WriteString(
				"<a href=\"" + html.EscapeString(href) + "\"" + attr + ">" + html.EscapeString(filename) + "</a><br>\n",
			)
		}
	}

	sb.WriteString("</body></html>\n")
	writeHTML(w, sb.String())
}

func (b *InMemoryBackend) pyDownload(w http.ResponseWriter, r *http.Request, rc repoCtx, path string) {
	segs := strings.Split(path, "/")
	if len(segs) != pyPathSegs {
		writePlain(w, http.StatusNotFound, "Not found")

		return
	}

	f, ok := b.packageFile(rc.ref, pyFileID(segs[0], segs[1], segs[2]))
	if !ok {
		writePlain(w, http.StatusNotFound, "Not found")

		return
	}

	if b.packageDenied(r.Context(), rc, segs[0]) {
		writePlain(w, http.StatusForbidden, "A download rule denies access to this package.")

		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	_, _ = w.Write(f.Data)
}
