package dashboard

import (
	"compress/gzip"
	"io"
	"io/fs"
	"mime"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

const spaGzipSuffix = ".gz"

// spaStat reports whether rel exists in fsys as a file, plain or pre-gzipped.
func spaStat(fsys fs.FS, rel string) bool {
	for _, name := range []string{rel, rel + spaGzipSuffix} {
		if info, err := fs.Stat(fsys, name); err == nil && !info.IsDir() {
			return true
		}
	}

	return false
}

// serveSPAFile serves rel from fsys. Assets stored as rel+".gz" (see
// ui/scripts/compress-spa.mjs) are sent verbatim to gzip-capable clients and
// inflated for the rest. It reports whether anything was served.
func serveSPAFile(c *echo.Context, fsys fs.FS, rel string) bool {
	if _, err := fs.Stat(fsys, rel); err == nil {
		http.ServeFileFS(c.Response(), c.Request(), fsys, rel)

		return true
	}

	f, err := fsys.Open(rel + spaGzipSuffix)
	if err != nil {
		return false
	}
	defer f.Close()

	req, h := c.Request(), c.Response().Header()
	if ct := mime.TypeByExtension(path.Ext(rel)); ct != "" {
		h.Set("Content-Type", ct)
	} else {
		h.Set("Content-Type", "application/octet-stream")
	}

	h.Add("Vary", "Accept-Encoding")

	var src io.Reader = f

	if acceptsGzip(req.Header.Get("Accept-Encoding")) {
		h.Set("Content-Encoding", "gzip")

		if info, statErr := f.Stat(); statErr == nil {
			h.Set("Content-Length", strconv.FormatInt(info.Size(), 10))
		}
	} else {
		zr, zerr := gzip.NewReader(f)
		if zerr != nil {
			http.Error(c.Response(), "corrupt embedded asset", http.StatusInternalServerError)

			return true
		}
		defer zr.Close()

		src = zr
	}

	c.Response().WriteHeader(http.StatusOK)

	if req.Method != http.MethodHead {
		_, _ = io.Copy(c.Response(), src)
	}

	return true
}

func acceptsGzip(header string) bool {
	for part := range strings.SplitSeq(header, ",") {
		name, params, _ := strings.Cut(strings.TrimSpace(part), ";")
		if !strings.EqualFold(strings.TrimSpace(name), "gzip") {
			continue
		}

		return strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(params), "q=")) != "0"
	}

	return false
}
