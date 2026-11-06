package azurefunctions

import (
	"archive/zip"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net/http"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	kuduVFS     = "/api/vfs/"
	vfsRoot     = "site/wwwroot"
	dirMIME     = "inode/directory"
	vfsHomePath = "/home/"
)

var errVFSConflict = errors.New("path conflicts with an existing file or directory")

type vfsEntry struct {
	Name  string `json:"name"`
	Size  int64  `json:"size"`
	MTime string `json:"mtime"`
	CTime string `json:"crtime"`
	MIME  string `json:"mime"`
	Href  string `json:"href"`
	Path  string `json:"path"`
}

type zipFile struct {
	mod  time.Time
	name string
	data []byte
}

func readZipFiles(data []byte) ([]zipFile, error) {
	if len(data) == 0 {
		return nil, nil
	}

	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, err
	}

	var out []zipFile

	for _, f := range zr.File {
		if strings.HasSuffix(f.Name, "/") {
			continue
		}

		rc, openErr := f.Open()
		if openErr != nil {
			return nil, openErr
		}

		raw, readErr := io.ReadAll(io.LimitReader(rc, maxUnpacked))
		_ = rc.Close()

		if readErr != nil {
			return nil, readErr
		}

		out = append(out, zipFile{name: f.Name, data: raw, mod: f.Modified})
	}

	return out, nil
}

func writeZipFiles(files []zipFile) ([]byte, error) {
	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for _, f := range files {
		w, err := zw.CreateHeader(&zip.FileHeader{Name: f.name, Method: zip.Deflate, Modified: f.mod})
		if err != nil {
			return nil, err
		}

		if _, err = w.Write(f.data); err != nil {
			return nil, err
		}
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// vfsRel maps a request path below /api/vfs/ to a path inside the site package.
func vfsRel(p string) (string, bool) {
	rel, ok := strings.CutPrefix(strings.Trim(p, "/"), vfsRoot)
	if !ok || (rel != "" && !strings.HasPrefix(rel, "/")) {
		return "", false
	}

	rel = strings.TrimPrefix(rel, "/")
	if strings.Contains(rel, "..") {
		return "", false
	}

	return path.Clean("/" + rel)[1:], true
}

func etagOf(data []byte) string {
	sum := sha256.Sum256(data)

	return `"` + hex.EncodeToString(sum[:8]) + `"`
}

func (b *InMemoryBackend) kuduVFS(w http.ResponseWriter, r *http.Request, name string) {
	rel, ok := vfsRel(strings.TrimPrefix(r.URL.Path, kuduVFS))
	if !ok {
		http.NotFound(w, r)

		return
	}

	isDir := strings.HasSuffix(r.URL.Path, "/")

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		b.vfsGet(w, r, name, rel, isDir)
	case http.MethodPut:
		b.vfsPut(w, r, name, rel, isDir)
	case http.MethodDelete:
		b.vfsDelete(w, r, name, rel)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (b *InMemoryBackend) vfsFiles(name string) ([]zipFile, error) {
	files, err := readZipFiles(b.Package(name))
	if err != nil {
		return nil, err
	}

	return files, nil
}

func (b *InMemoryBackend) vfsGet(w http.ResponseWriter, r *http.Request, name, rel string, isDir bool) {
	files, err := b.vfsFiles(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	if !isDir {
		for _, f := range files {
			if f.name == rel {
				w.Header().Set("ETag", etagOf(f.data))
				w.Header().Set("Content-Type", mimeOf(f.name))
				w.Header().Set("Last-Modified", f.mod.UTC().Format(http.TimeFormat))
				_, _ = w.Write(f.data)

				return
			}
		}
	}

	if listing, found := vfsList(r, files, rel); found {
		writeJSON(w, http.StatusOK, listing)

		return
	}

	http.NotFound(w, r)
}

func mimeOf(name string) string {
	if t := mime.TypeByExtension(path.Ext(name)); t != "" {
		return t
	}

	return "application/octet-stream"
}

func vfsList(r *http.Request, files []zipFile, rel string) ([]vfsEntry, bool) {
	prefix := ""
	if rel != "" {
		prefix = rel + "/"
	}

	seen := map[string]vfsEntry{}
	base := "http://" + r.Host + kuduVFS + vfsRoot + "/" + prefix

	for _, f := range files {
		rest, ok := strings.CutPrefix(f.name, prefix)
		if !ok {
			continue
		}

		head, tail, nested := strings.Cut(rest, "/")
		ts := f.mod.UTC().Format(time.RFC3339Nano)
		e := vfsEntry{
			Name: head, MTime: ts, CTime: ts, Size: int64(len(f.data)), MIME: mimeOf(head),
			Href: base + head, Path: vfsHomePath + vfsRoot + "/" + prefix + head,
		}

		if nested && tail != "" {
			e.Size, e.MIME, e.Href = 0, dirMIME, base+head+"/"
		}

		seen[head] = e
	}

	if len(seen) == 0 && rel != "" {
		return nil, false
	}

	out := make([]vfsEntry, 0, len(seen))
	for _, e := range seen {
		out = append(out, e)
	}

	sort.Slice(out, func(i, j int) bool { return strs.Fold(out[i].Name) < strs.Fold(out[j].Name) })

	return out, true
}

func (b *InMemoryBackend) vfsPut(w http.ResponseWriter, r *http.Request, name, rel string, isDir bool) {
	if rel == "" || isDir {
		w.WriteHeader(http.StatusOK)

		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxPackageBytes))
	if err != nil {
		http.Error(w, err.Error(), http.StatusRequestEntityTooLarge)

		return
	}

	files, err := b.vfsFiles(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	existing := -1

	for i, f := range files {
		switch {
		case f.name == rel:
			existing = i
		case strings.HasPrefix(f.name, rel+"/") || strings.HasPrefix(rel, f.name+"/"):
			http.Error(w, errVFSConflict.Error(), http.StatusConflict)

			return
		}
	}

	now := b.now().UTC()
	status := http.StatusCreated

	if existing >= 0 {
		if im := r.Header.Get("If-Match"); im != "*" && im != etagOf(files[existing].data) {
			http.Error(w, "the file exists; send If-Match: * to overwrite", http.StatusPreconditionFailed)

			return
		}

		files[existing] = zipFile{name: rel, data: body, mod: now}
		status = http.StatusNoContent
	} else {
		files = append(files, zipFile{name: rel, data: body, mod: now})
	}

	if !b.vfsStore(w, name, files) {
		return
	}

	w.WriteHeader(status)
}

func (b *InMemoryBackend) vfsDelete(w http.ResponseWriter, r *http.Request, name, rel string) {
	files, err := b.vfsFiles(name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return
	}

	kept := files[:0:0]

	for _, f := range files {
		if f.name != rel {
			kept = append(kept, f)
		}
	}

	if len(kept) == len(files) {
		http.NotFound(w, r)

		return
	}

	if b.vfsStore(w, name, kept) {
		w.WriteHeader(http.StatusOK)
	}
}

func (b *InMemoryBackend) vfsStore(w http.ResponseWriter, name string, files []zipFile) bool {
	data, err := writeZipFiles(files)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)

		return false
	}

	if err = b.replacePackage(name, data); err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)

		return false
	}

	return true
}

// replacePackage swaps the app's site content without recording a deployment and restarts its host.
func (b *InMemoryBackend) replacePackage(name string, data []byte) error {
	b.mu.Lock("replacePackage")

	a, err := b.appLocked(name)
	if err != nil {
		b.mu.Unlock()

		return err
	}

	a.Zip = data
	old := b.takeBoxLocked(a)
	b.mu.Unlock()

	b.teardown(old)
	b.autoStart(name)

	return nil
}

// IsFlex reports whether the app is on the Flex Consumption plan.
func (b *InMemoryBackend) IsFlex(name string) bool {
	b.mu.RLock("IsFlex")
	defer b.mu.RUnlock()

	a, err := b.appLocked(name)

	return err == nil && a.Flex
}
